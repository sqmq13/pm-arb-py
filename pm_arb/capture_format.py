from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from pathlib import Path

FRAMES_MAGIC = b"PMCAPv01"
FRAMES_SCHEMA_VERSION = 1

FRAMES_HEADER_STRUCT = struct.Struct("<8sHHQII")
FRAMES_HEADER_LEN = FRAMES_HEADER_STRUCT.size

IDX_STRUCT = struct.Struct("<QQII")
IDX_ENTRY_LEN = IDX_STRUCT.size

FLAG_TEXT_PAYLOAD = 1
FLAG_BINARY_PAYLOAD = 2
FLAG_DECODED_PAYLOAD = 4


@dataclass(frozen=True)
class FrameRecord:
    offset: int
    schema_version: int
    flags: int
    rx_mono_ns: int
    payload_len: int
    payload_crc32: int
    payload: bytes


@dataclass(frozen=True)
class IdxRecord:
    offset_frames: int
    rx_mono_ns: int
    payload_len: int
    payload_crc32: int


def _crc32(payload: bytes) -> int:
    return zlib.crc32(payload) & 0xFFFFFFFF


def encode_frame(
    payload: bytes,
    rx_mono_ns: int,
    *,
    flags: int = 0,
    schema_version: int = FRAMES_SCHEMA_VERSION,
) -> bytes:
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise TypeError("payload must be bytes-like")
    payload_bytes = bytes(payload)
    payload_crc32 = _crc32(payload_bytes)
    header = FRAMES_HEADER_STRUCT.pack(
        FRAMES_MAGIC,
        schema_version,
        flags,
        rx_mono_ns,
        len(payload_bytes),
        payload_crc32,
    )
    return header + payload_bytes


def append_record(
    frames_fh,
    idx_fh,
    payload: bytes,
    rx_mono_ns: int,
    *,
    flags: int = 0,
    schema_version: int = FRAMES_SCHEMA_VERSION,
) -> FrameRecord:
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise TypeError("payload must be bytes-like")
    payload_bytes = bytes(payload)
    payload_crc32 = _crc32(payload_bytes)
    offset = frames_fh.tell()
    header = FRAMES_HEADER_STRUCT.pack(
        FRAMES_MAGIC,
        schema_version,
        flags,
        rx_mono_ns,
        len(payload_bytes),
        payload_crc32,
    )
    frames_fh.write(header)
    frames_fh.write(payload_bytes)
    if idx_fh is not None:
        idx_fh.write(IDX_STRUCT.pack(offset, rx_mono_ns, len(payload_bytes), payload_crc32))
    return FrameRecord(
        offset=offset,
        schema_version=schema_version,
        flags=flags,
        rx_mono_ns=rx_mono_ns,
        payload_len=len(payload_bytes),
        payload_crc32=payload_crc32,
        payload=payload_bytes,
    )


def read_frames(frames_path: Path) -> list[FrameRecord]:
    data = frames_path.read_bytes()
    size = len(data)
    offset = 0
    records: list[FrameRecord] = []
    while offset < size:
        if size - offset < FRAMES_HEADER_LEN:
            raise ValueError(f"truncated header at offset {offset}")
        magic, schema_version, flags, rx_mono_ns, payload_len, payload_crc32 = (
            FRAMES_HEADER_STRUCT.unpack_from(data, offset)
        )
        if magic != FRAMES_MAGIC:
            raise ValueError(f"bad magic at offset {offset}")
        record_len = FRAMES_HEADER_LEN + payload_len
        if offset + record_len > size:
            raise ValueError(f"truncated payload at offset {offset}")
        payload = data[offset + FRAMES_HEADER_LEN : offset + record_len]
        if _crc32(payload) != payload_crc32:
            raise ValueError(f"crc mismatch at offset {offset}")
        records.append(
            FrameRecord(
                offset=offset,
                schema_version=schema_version,
                flags=flags,
                rx_mono_ns=rx_mono_ns,
                payload_len=payload_len,
                payload_crc32=payload_crc32,
                payload=payload,
            )
        )
        offset += record_len
    return records


def read_idx(idx_path: Path) -> list[IdxRecord]:
    data = idx_path.read_bytes()
    size = len(data)
    if size % IDX_ENTRY_LEN != 0:
        raise ValueError("idx size is not a multiple of entry length")
    records: list[IdxRecord] = []
    for offset in range(0, size, IDX_ENTRY_LEN):
        offset_frames, rx_mono_ns, payload_len, payload_crc32 = IDX_STRUCT.unpack_from(
            data, offset
        )
        records.append(
            IdxRecord(
                offset_frames=offset_frames,
                rx_mono_ns=rx_mono_ns,
                payload_len=payload_len,
                payload_crc32=payload_crc32,
            )
        )
    return records


def verify_frames(frames_path: Path, idx_path: Path | None = None) -> dict:
    data = frames_path.read_bytes()
    size = len(data)
    offset = 0
    records_meta: list[tuple[int, int, int, int]] = []
    errors = 0
    crc_mismatch = 0
    truncated = False
    first_bad_offset: int | None = None
    schema_versions: set[int] = set()

    while offset < size:
        if size - offset < FRAMES_HEADER_LEN:
            truncated = True
            errors += 1
            if first_bad_offset is None:
                first_bad_offset = offset
            break
        magic, schema_version, flags, rx_mono_ns, payload_len, payload_crc32 = (
            FRAMES_HEADER_STRUCT.unpack_from(data, offset)
        )
        if magic != FRAMES_MAGIC:
            errors += 1
            if first_bad_offset is None:
                first_bad_offset = offset
            next_offset = data.find(FRAMES_MAGIC, offset + 1)
            if next_offset == -1:
                break
            offset = next_offset
            continue
        schema_versions.add(schema_version)
        record_len = FRAMES_HEADER_LEN + payload_len
        if offset + record_len > size:
            truncated = True
            errors += 1
            if first_bad_offset is None:
                first_bad_offset = offset
            break
        payload = data[offset + FRAMES_HEADER_LEN : offset + record_len]
        actual_crc32 = _crc32(payload)
        if actual_crc32 != payload_crc32:
            errors += 1
            crc_mismatch += 1
            if first_bad_offset is None:
                first_bad_offset = offset
            next_offset = data.find(FRAMES_MAGIC, offset + 1)
            if next_offset == -1:
                break
            offset = next_offset
            continue
        records_meta.append((offset, rx_mono_ns, payload_len, payload_crc32))
        offset += record_len

    last_good_offset = records_meta[-1][0] if records_meta else None
    summary = {
        "ok": errors == 0 and not truncated,
        "records": len(records_meta),
        "errors": errors,
        "crc_mismatch": crc_mismatch,
        "truncated": truncated,
        "first_bad_offset": first_bad_offset,
        "last_good_offset": last_good_offset,
        "frames_bytes": size,
        "schema_versions": sorted(schema_versions),
    }

    if idx_path is not None:
        if not idx_path.exists():
            raise FileNotFoundError(str(idx_path))
        idx_data = idx_path.read_bytes()
        idx_size = len(idx_data)
        idx_ok = True
        idx_errors = 0
        idx_first_bad_offset: int | None = None
        if idx_size % IDX_ENTRY_LEN != 0:
            idx_ok = False
            idx_errors += 1
            idx_first_bad_offset = idx_size - (idx_size % IDX_ENTRY_LEN)
        idx_records = idx_size // IDX_ENTRY_LEN
        idx_entries: list[tuple[int, int, int, int]] = []
        for idx_offset in range(0, idx_records * IDX_ENTRY_LEN, IDX_ENTRY_LEN):
            idx_entries.append(IDX_STRUCT.unpack_from(idx_data, idx_offset))
        if idx_records != len(records_meta):
            idx_ok = False
            idx_errors += 1
            if idx_first_bad_offset is None:
                idx_first_bad_offset = 0
        else:
            for idx, entry in enumerate(idx_entries):
                if entry != records_meta[idx]:
                    idx_ok = False
                    idx_errors += 1
                    if idx_first_bad_offset is None:
                        idx_first_bad_offset = idx * IDX_ENTRY_LEN
                    break
        summary.update(
            {
                "idx_ok": idx_ok,
                "idx_records": idx_records,
                "idx_errors": idx_errors,
                "idx_first_bad_offset": idx_first_bad_offset,
            }
        )
        summary["ok"] = summary["ok"] and idx_ok

    return summary
