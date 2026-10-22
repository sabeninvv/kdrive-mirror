import asyncio
import gzip

import pytest

import pathspec

from kdrive_upload import (
    Entry,
    KDriveError,
    Local,
    SourceChanged,
    check_delete_limit,
    checkpoint_due,
    dump_manifest,
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


def test_checkpoint_due():
    assert checkpoint_due(dirty=True, now=100, last=0, interval=60)
    assert not checkpoint_due(dirty=True, now=59, last=0, interval=60)  # too early
    assert not checkpoint_due(dirty=False, now=100, last=0, interval=60)  # nothing to save
    assert not checkpoint_due(dirty=True, now=100, last=0, interval=0)  # 0 disables
