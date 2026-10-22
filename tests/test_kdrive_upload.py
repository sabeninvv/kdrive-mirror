import asyncio
import gzip
from pathlib import Path

import pytest

import aiohttp
import pathspec

import kdrive_upload
from kdrive_upload import (
    BadResponse,
    Entry,
    KDrive,
    KDriveError,
    Local,
    SourceChanged,
    check_delete_limit,
    check_upload_url,
    checkpoint_due,
    dump_manifest,
    format_upload,
    is_current,
    parse_manifest,
    read_chunks,
    scan_tree,
)


def stat(size: int, mtime: int) -> Local:
    return Local("f", False, size, mtime)


def remote(size: int, mtime: int | None) -> Entry:
    return Entry(id=1, name="f", is_dir=False, size=size, mtime=mtime)


def test_is_current():
    assert is_current(remote(5, 100), stat(5, 100))
    assert not is_current(None, stat(5, 100))
    assert not is_current(remote(5, 100), stat(6, 100))
    assert not is_current(remote(5, 100), stat(5, 101))  # same size, but the file was edited
    assert not is_current(remote(5, None), stat(5, 100))


def test_check_delete_limit():
    check_delete_limit(0, 0, 10)
    check_delete_limit(1, 10, 10)
    with pytest.raises(KDriveError):
        check_delete_limit(2, 10, 10)
    with pytest.raises(KDriveError):
        check_delete_limit(5, 5, 10)  # empty source


def test_manifest_roundtrip():
    tree = {"": {"a.txt": Entry(1, "a.txt", False, 5, 100), "sub": Entry(2, "sub", True, None, None)}, "sub": {}}
    assert parse_manifest(dump_manifest(tree)) == tree


def test_parse_manifest_rejects_bad_input():
    with pytest.raises(KDriveError):
        parse_manifest(b"not gzip")
    with pytest.raises(KDriveError):
        parse_manifest(gzip.compress(b'{"version": 99, "dirs": {}}'))


def test_scan_tree(tmp_path):
    (tmp_path / "a.txt").write_text("aaaa")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_text("bb")
    (tmp_path / "skip.log").write_text("x")
    (tmp_path / "link").symlink_to(tmp_path / "a.txt")
    ignore = pathspec.GitIgnoreSpec.from_lines(["*.log"])
    snap = scan_tree(tmp_path, ignore)
    assert [(i.name, i.is_dir) for i in snap[""]] == [("a.txt", False), ("sub", True)]
    assert snap[""][0].size == 4
    assert [(i.name, i.size) for i in snap["sub"]] == [("b.txt", 2)]


def collect(path, size):
    async def run():
        return [chunk async for chunk in read_chunks(path, size)]

    return asyncio.run(run())


def test_read_chunks_sends_exactly_size_and_detects_shrink(tmp_path):
    f = tmp_path / "f"
    f.write_bytes(b"0123456789")
    assert b"".join(collect(f, 4)) == b"0123"  # the file grew after the snapshot: take exactly size
    with pytest.raises(SourceChanged):
        collect(f, 20)  # the file became shorter than in the snapshot
    with pytest.raises(SourceChanged):
        collect(tmp_path / "gone", 1)  # the file vanished


def test_read_chunks_offset(tmp_path):
    f = tmp_path / "f"
    f.write_bytes(b"0123456789")

    async def run(offset, size):
        return b"".join([chunk async for chunk in read_chunks(f, size, offset)])

    assert asyncio.run(run(4, 3)) == b"456"
    with pytest.raises(SourceChanged):
        asyncio.run(run(8, 5))  # the file became shorter than offset + size


def test_check_upload_url():
    assert check_upload_url("https://upload.infomaniak.com/x") == "https://upload.infomaniak.com/x"
    for bad in ("http://upload.infomaniak.com/x", "https://evil.example/x", "https://infomaniak.com.evil.example/x"):
        with pytest.raises(KDriveError):
            check_upload_url(bad)


class FakeDrive(KDrive):
    """KDrive whose API calls are recorded instead of sent; chunk bodies are consumed like aiohttp would."""

    def __init__(self, root, fail_on_chunk=None):
        super().__init__(None, 1, 1, root, {}, mirror=False, dry_run=False, manifest=False, checkpoint_sec=0)
        self.calls = []
        self.chunks = []
        self.fail_on_chunk = fail_on_chunk

    async def _call(self, method, path, *, url=None, **kwargs):
        self.calls.append((method, path, url, kwargs.get("json"), kwargs.get("params")))
        if path.endswith("/start"):
            return {"data": {"token": "tok", "upload_url": "https://1-1-v3-1.upload.kdrive.infomaniak.com"}}
        if path.endswith("/chunk"):
            if kwargs["params"]["chunk_number"] == self.fail_on_chunk:
                raise KDriveError("boom")
            self.chunks.append(b"".join([c async for c in kwargs["data"]]))
        if path.endswith("/finish"):
            return {"data": {"file": {"id": 7, "name": "big", "type": "file", "size": 10, "last_modified_at": 5}}}
        return {}


def test_upload_session_sends_sequential_chunks(tmp_path, monkeypatch):
    monkeypatch.setattr(kdrive_upload, "SESSION_CHUNK_SIZE", 4)
    (tmp_path / "big").write_bytes(b"0123456789")
    drive = FakeDrive(tmp_path)
    entry = asyncio.run(drive._upload_session(3, "big", tmp_path / "big", 10, 5))
    assert drive.chunks == [b"0123", b"4567", b"89"]
    assert entry == Entry(7, "big", False, 10, 5)
    start = drive.calls[0]
    assert start[1] == "/upload/session/start" and start[3]["total_chunks"] == 3 and start[3]["total_size"] == 10
    assert [c[4]["chunk_number"] for c in drive.calls[1:4]] == [1, 2, 3]
    assert {c[2] for c in drive.calls[1:4]} == {"https://1-1-v3-1.upload.kdrive.infomaniak.com/3/drive/1/upload/session/tok/chunk"}
    assert drive.calls[-1][1] == "/upload/session/tok/finish"


def test_upload_session_is_cancelled_on_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(kdrive_upload, "SESSION_CHUNK_SIZE", 4)
    (tmp_path / "big").write_bytes(b"0123456789")
    drive = FakeDrive(tmp_path, fail_on_chunk=2)
    with pytest.raises(KDriveError):
        asyncio.run(drive._upload_session(3, "big", tmp_path / "big", 10, 5))
    assert drive.calls[-1][:2] == ("DELETE", "/upload/session/tok")


class ExistingDirDrive(FakeDrive):
    def __init__(self, root, listing):
        super().__init__(root)
        self.listing = listing

    async def _call(self, method, path, **kwargs):
        if method == "POST" and path.endswith("/directory"):
            raise KDriveError("exists", "destination_already_exists")
        return await super()._call(method, path, **kwargs)

    async def list_dir(self, dir_id):
        return self.listing


def test_ensure_dir_takes_existing_folder_missing_from_stale_manifest(tmp_path):
    folder = Entry(9, "sub", True, None, None)
    assert asyncio.run(ExistingDirDrive(tmp_path, [folder]).ensure_dir(1, "sub", [])) == 9
    file_with_same_name = Entry(10, "sub", False, 5, 1)
    with pytest.raises(KDriveError):
        asyncio.run(ExistingDirDrive(tmp_path, [file_with_same_name]).ensure_dir(1, "sub", []))


class FlakyDrive(FakeDrive):
    def __init__(self, root, failures):
        super().__init__(root)
        self.failures = failures
        self.attempts = 0

    async def _send_once(self, dir_id, item, path):
        self.attempts += 1
        if self.failures:
            raise self.failures.pop(0)
        return Entry(1, item.name, False, item.size, item.mtime)


def test_send_retries_dropped_connection(tmp_path, monkeypatch):
    monkeypatch.setattr(kdrive_upload, "RETRY_DELAY_SEC", 0)
    item = Local("f", False, 1, 1)
    drive = FlakyDrive(tmp_path, [aiohttp.ServerDisconnectedError(), aiohttp.ServerDisconnectedError()])
    assert asyncio.run(drive._send(1, item, tmp_path / "f")).id == 1
    assert drive.attempts == 3  # two failures, then success
    drive = FlakyDrive(tmp_path, [aiohttp.ServerDisconnectedError()] * 3)
    with pytest.raises(aiohttp.ServerDisconnectedError):
        asyncio.run(drive._send(1, item, tmp_path / "f"))
    assert drive.attempts == 3  # gives up after UPLOAD_ATTEMPTS


def test_send_does_not_retry_api_errors(tmp_path, monkeypatch):
    monkeypatch.setattr(kdrive_upload, "RETRY_DELAY_SEC", 0)
    drive = FlakyDrive(tmp_path, [KDriveError("denied", "forbidden_error")])
    with pytest.raises(KDriveError):
        asyncio.run(drive._send(1, Local("f", False, 1, 1), tmp_path / "f"))
    assert drive.attempts == 1


class FakeResponse:
    def __init__(self, status, body):
        self.status = status
        self._body = body

    async def read(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    def __init__(self, status, body):
        self._response = FakeResponse(status, body)

    def request(self, method, url, **kwargs):
        return self._response


def call_with(tmp_path, status, body):
    drive = KDrive(FakeSession(status, body), 1, 1, tmp_path, {}, mirror=False, dry_run=False, manifest=False, checkpoint_sec=0)
    return asyncio.run(drive._call("GET", "/x"))


def test_call_reports_unreadable_body_as_retryable_error(tmp_path):
    assert call_with(tmp_path, 200, b'{"result":"success","data":1}')["data"] == 1
    for body in (b"", b"null", b"<html>502</html>"):
        with pytest.raises(BadResponse, match="HTTP 502"):
            call_with(tmp_path, 502, body)
    with pytest.raises(KDriveError) as err:
        call_with(tmp_path, 403, b'{"result":"error","error":{"code":"access_denied"}}')
    assert not isinstance(err.value, BadResponse) and err.value.code == "access_denied"


def test_send_retries_bad_response(tmp_path, monkeypatch):
    monkeypatch.setattr(kdrive_upload, "RETRY_DELAY_SEC", 0)
    drive = FlakyDrive(tmp_path, [BadResponse("empty")])
    assert asyncio.run(drive._send(1, Local("f", False, 1, 1), tmp_path / "f")).id == 1
    assert drive.attempts == 2


def test_checkpoint_due():
    assert checkpoint_due(dirty=True, now=100, last=0, interval=60)
    assert not checkpoint_due(dirty=True, now=59, last=0, interval=60)  # too early
    assert not checkpoint_due(dirty=False, now=100, last=0, interval=60)  # nothing to save
    assert not checkpoint_due(dirty=True, now=100, last=0, interval=0)  # 0 disables


def test_format_upload() -> None:
    line = format_upload(3211 * 1024 * 1024, 0.01, Path("/d/f.txt"))
    assert line == "upload   3211 Mb in    0.01 sec /d/f.txt"
