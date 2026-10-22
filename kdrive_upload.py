# /// script
# requires-python = ">=3.11"
# dependencies = ["aiofiles>=24.1", "aiohttp>=3.10", "pathspec>=0.12"]
# ///
"""Copies a folder to kDrive via the Infomaniak API (like `rclone copy`; with --mirror also like `rclone sync`).

The token (scope: drive) is required in the KDRIVE_TOKEN environment variable, the drive ID in KDRIVE_DRIVE_ID or --drive-id,
otherwise the script exits.
What not to upload is set by kdrive.ignore next to the script (.gitignore syntax).
With --mirror, extra files on the drive (absent from --src) go to the kDrive trash; --dry-run only shows the actions.

    uv run --env-file ~/.envs/kdrive.env kdrive_upload.py \
        --src ~/Docs --dest "Private/Backups/my-pc/2026-10-22" [--mirror] [--dry-run]
"""
import argparse
import asyncio
import contextlib
import gzip
import json
import logging
import os
import sys
import time
from collections.abc import AsyncIterator
from dataclasses import astuple, dataclass
from enum import StrEnum
from pathlib import Path, PurePosixPath

import aiofiles
import aiohttp
import pathspec

API = "https://api.infomaniak.com"
ROOT_ID = 1
DEFAULT_IGNORE_FILE = Path(__file__).with_name("kdrive.ignore")
CHUNK_SIZE = 1 << 20
NOT_FOUND = "object_not_found"
SERVICE_DIR = ".kdrive-sync"
MANIFEST_NAME = "manifest.json.gz"
MANIFEST_VERSION = 1

logger = logging.getLogger("kdrive_upload")


class Method(StrEnum):
    GET = "GET"
    POST = "POST"
    DELETE = "DELETE"


class KDriveError(RuntimeError):
    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


class SourceChanged(Exception):
    """A local file vanished or became shorter than in the snapshot."""


@dataclass(frozen=True)
class Entry:
    id: int
    name: str
    is_dir: bool
    size: int | None
    mtime: int | None


@dataclass(frozen=True)
class Local:
    name: str
    is_dir: bool
    size: int
    mtime: int


Snapshot = dict[str, list[Local]]  # folder path relative to --src ("" for the root) -> accepted entries
Tree = dict[str, dict[str, Entry]]  # folder path relative to --src ("" for the root) -> children on the drive by name


def join_rel(rel: str, name: str) -> str:
    return f"{rel}/{name}" if rel else name


def scan_tree(root: Path, ignore: pathspec.GitIgnoreSpec) -> Snapshot:
    """Walks --src once before any network call; syncing then follows this snapshot strictly."""
    snapshot: Snapshot = {}
    pending = [root]
    while pending:
        directory = pending.pop()
        rel_dir = "" if directory == root else directory.relative_to(root).as_posix()
        kept: list[Local] = []
        try:
            entries = sorted(directory.iterdir())
        except OSError as exc:
            logger.warning("skip unreadable %s (%r)", directory, exc)
            entries = []
        for entry in entries:
            try:
                is_dir = entry.is_dir()
                if entry.is_symlink() or not (is_dir or entry.is_file()):
                    logger.warning("skip non-regular %s", entry)
                    continue
                relative = join_rel(rel_dir, entry.name)
                if ignore.match_file(f"{relative}/" if is_dir else relative):
                    logger.info("ignore %s", entry)
                    continue
                st = entry.stat()
            except OSError as exc:
                logger.warning("skip vanished %s (%r)", entry, exc)
                continue
            kept.append(Local(entry.name, is_dir, st.st_size, int(st.st_mtime)))
            if is_dir:
                pending.append(entry)
        snapshot[rel_dir] = kept
    return snapshot


def to_entry(item: dict) -> Entry:
    return Entry(
        id=item["id"],
        name=item["name"],
        is_dir=item["type"] == "dir",
        size=item.get("size"),
        mtime=item.get("last_modified_at"),
    )


def dump_manifest(tree: Tree) -> bytes:
    dirs = {rel: [astuple(e) for e in kids.values()] for rel, kids in tree.items()}
    return gzip.compress(json.dumps({"version": MANIFEST_VERSION, "dirs": dirs}, separators=(",", ":")).encode())


def parse_manifest(raw: bytes) -> Tree:
    try:
        doc = json.loads(gzip.decompress(raw))
        if doc["version"] != MANIFEST_VERSION:
            raise KDriveError(f"manifest version {doc['version']} != {MANIFEST_VERSION}; run with --rescan")
        return {rel: {row[1]: Entry(*row) for row in rows} for rel, rows in doc["dirs"].items()}
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise KDriveError(f"manifest is unreadable ({exc}); run with --rescan") from exc


def is_current(remote: Entry | None, local: Local) -> bool:
    return remote is not None and remote.size == local.size and remote.mtime == local.mtime


def checkpoint_due(*, dirty: bool, now: float, last: float, interval: float) -> bool:
    return dirty and interval > 0 and now - last >= interval


def check_delete_limit(count: int, total: int, max_pct: float) -> None:
    if count * 100 > total * max_pct:
        raise KDriveError(f"refusing to delete {count} of {total} entries: more than {max_pct}% (see --max-delete-pct)")


async def read_chunks(path: Path, size: int) -> AsyncIterator[bytes]:
    """Yields exactly size bytes from the snapshot: a grown file is cut off, a vanished or shortened one raises SourceChanged."""
    remaining = size
    try:
        async with aiofiles.open(path, "rb") as f:
            while remaining:
                chunk = await f.read(min(CHUNK_SIZE, remaining))
                if not chunk:
                    raise SourceChanged(path)
                remaining -= len(chunk)
                yield chunk
    except OSError as exc:
        raise SourceChanged(path) from exc


class KDrive:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        drive_id: int,
        concurrency: int,
        root: Path,
        snapshot: Snapshot,
        *,
        mirror: bool,
        dry_run: bool,
        manifest: bool,
        checkpoint_sec: float,
    ) -> None:
        self._session = session
        self._root = root
        self._snapshot = snapshot
        self._drive_id = drive_id
        self._mirror = mirror
        self._dry_run = dry_run
        self._slots = asyncio.Semaphore(concurrency)
        self._manifest = manifest
        self._tree: Tree = {}
        self._service_id: int | None = None
        self._dirty = False
        self._dest_id: int | None = None
        self._checkpoint_sec = checkpoint_sec
        self._last_save = time.monotonic()
        self._saving = asyncio.Lock()
        self._seen = 0
        self._extras: list[tuple[str, Entry]] = []
        self.api_calls = 0

    async def _call(self, method: Method, path: str, *, api_version: int = 3, limited: bool = True, **kwargs) -> dict:
        url = f"{API}/{api_version}/drive/{self._drive_id}{path}"
        self.api_calls += 1
        slot = self._slots if limited else contextlib.nullcontext()
        async with slot, self._session.request(method, url, **kwargs) as resp:
            payload = await resp.json(content_type=None)
        if payload.get("result") != "success":
            error = payload.get("error")
            raise KDriveError(f"{method} {path}: {error}", error.get("code") if isinstance(error, dict) else None)
        return payload

    async def list_dir(self, dir_id: int) -> list[Entry]:
        entries: list[Entry] = []
        cursor = None
        while True:
            page = await self._call(Method.GET, f"/files/{dir_id}/files", params={"cursor": cursor} if cursor else None)
            entries += [to_entry(i) for i in page["data"]]
            if not page.get("has_more"):
                return entries
            cursor = page["cursor"]

    async def ensure_dir(self, parent_id: int | None, name: str, siblings: list[Entry]) -> int | None:
        for entry in siblings:
            if entry.is_dir and entry.name == name:
                return entry.id
        if self._dry_run:
            logger.info("would mkdir %s", name)
            return None
        created = await self._call(Method.POST, f"/files/{parent_id}/directory", json={"name": name})
        return created["data"]["id"]

    async def ensure_path(self, parts: tuple[str, ...]) -> int | None:
        dir_id: int | None = ROOT_ID
        for part in parts:
            siblings = await self.list_dir(dir_id) if dir_id is not None else []
            dir_id = await self.ensure_dir(dir_id, part, siblings)
        return dir_id

    async def children(self, rel: str, dir_id: int | None) -> list[Entry]:
        """Children of a folder: from the manifest/cache, or from a kDrive listing if absent there."""
        if rel not in self._tree:
            entries = await self.list_dir(dir_id) if dir_id is not None else []
            if not rel and not self._manifest and any(e.name == SERVICE_DIR for e in entries):
                logger.warning("%s on drive has a manifest that this run does not update; use --rescan next time", SERVICE_DIR)
            self._tree[rel] = {e.name: e for e in entries if rel or e.name != SERVICE_DIR}
        return list(self._tree[rel].values())

    async def _upload(
        self, dir_id: int | None, name: str, data: bytes | AsyncIterator[bytes], size: int, mtime: int, *, limited: bool = True
    ) -> Entry:
        page = await self._call(
            Method.POST,
            "/upload",
            params={
                "directory_id": dir_id,
                "file_name": name,
                "total_size": size,
                "last_modified_at": mtime,
                "conflict": "version",
            },
            data=data,
            headers={"Content-Type": "application/octet-stream", "Content-Length": str(size)},
            limited=limited,
        )
        return to_entry(page["data"])

    async def upload_file(self, rel: str, item: Local, dir_id: int | None) -> None:
        path = self._root / rel / item.name
        if self._dry_run:
            logger.info("would upload %s", path)
            return
        try:
            entry = await self._upload(dir_id, item.name, read_chunks(path, item.size), item.size, item.mtime)
        except (aiohttp.ClientConnectionError, SourceChanged) as exc:
            # aiohttp wraps an exception from the body generator in ClientConnectionError; the cause is in __cause__
            cause = exc if isinstance(exc, SourceChanged) else exc.__cause__
            if not isinstance(cause, SourceChanged):
                raise
            logger.warning("skip %s: removed or truncated during sync, next run will retry", path)
            return
        self._tree[rel][entry.name] = entry
        logger.info("upload %s", path)
        await self._changed()

    async def delete(self, rel: str, entry: Entry) -> None:
        path = self._root / rel / entry.name
        if self._dry_run:
            logger.info("would delete %s", path)
            return
        try:
            await self._call(Method.DELETE, f"/files/{entry.id}", api_version=2)  # to the kDrive trash
        except KDriveError as exc:
            if exc.code != NOT_FOUND:
                raise
            logger.warning("already gone on drive %s", path)
        self._tree[rel].pop(entry.name, None)
        if entry.is_dir:
            child = join_rel(rel, entry.name)
            for key in [k for k in self._tree if k == child or k.startswith(f"{child}/")]:
                del self._tree[key]
        logger.info("delete %s", path)
        await self._changed()

    async def _changed(self) -> None:
        """Marks the state as changed and saves the manifest if enough time has passed since the last save."""
        self._dirty = True
        due = checkpoint_due(
            dirty=self._dirty, now=time.monotonic(), last=self._last_save, interval=self._checkpoint_sec
        )
        if self._manifest and due and not self._saving.locked():
            try:
                await self.save_manifest()
            except (KDriveError, aiohttp.ClientError) as exc:
                logger.warning("checkpoint failed (%r), will retry later", exc)
                self._last_save = time.monotonic()

    async def load_manifest(self, dest_id: int | None, rescan: bool) -> None:
        """Loads state from the manifest; if there is none or --rescan is set, it is built from listings and saved."""
        self._dest_id = dest_id
        found = None
        if dest_id is not None:
            root = await self.list_dir(dest_id)
            service = next((e for e in root if e.is_dir and e.name == SERVICE_DIR), None)
            # the service folder is created up front: a checkpoint in the middle of uploads must not queue behind listings
            self._service_id = service.id if service else await self.ensure_dir(dest_id, SERVICE_DIR, root)
            if service and not rescan:
                found = next((e for e in await self.list_dir(service.id) if e.name == MANIFEST_NAME), None)
        if found:
            self.api_calls += 1
            url = f"{API}/2/drive/{self._drive_id}/files/{found.id}/download"
            async with self._slots, self._session.get(url) as resp:
                resp.raise_for_status()
                self._tree = parse_manifest(await resp.read())
            logger.info("manifest loaded: %d folders", len(self._tree))
            return
        logger.info("no manifest used: state will be built from listings")
        self._dirty = True

    async def save_manifest(self) -> None:
        if self._dry_run or not self._dirty or self._service_id is None:
            return
        async with self._saving:
            data = dump_manifest(self._tree)  # synchronous: the state snapshot is not smeared across parallel tasks
            self._dirty = False  # changes arriving during the upload raise the flag again
            try:
                # ponytail: the whole manifest on every save; with hundreds of thousands of files it is tens of MB, then raise --checkpoint-sec or split by folder
                await self._upload(self._service_id, MANIFEST_NAME, data, len(data), int(time.time()), limited=False)
            except BaseException:
                self._dirty = True
                raise
            self._last_save = time.monotonic()
            logger.info("manifest saved: %d bytes", len(data))

    async def delete_extras(self, max_pct: float) -> None:
        check_delete_limit(len(self._extras), self._seen, max_pct)
        async with asyncio.TaskGroup() as tasks:
            for rel, entry in self._extras:
                tasks.create_task(self.delete(rel, entry))

    async def copy_tree(self, rel: str, dest_id: int | None) -> None:
        local = self._snapshot[rel]
        siblings = await self.children(rel, dest_id)
        files = {e.name: e for e in siblings if not e.is_dir}
        async with asyncio.TaskGroup() as tasks:
            for item in local:
                if item.is_dir:
                    tasks.create_task(self._copy_subdir(rel, item.name, dest_id, siblings))
                elif is_current(files.get(item.name), item):
                    logger.info("skip   %s", self._root / rel / item.name)
                else:
                    tasks.create_task(self.upload_file(rel, item, dest_id))
        if self._mirror:
            kept = {item.name for item in local}
            self._seen += len(siblings)
            # ponytail: deleting an extra folder counts as 1, not per file inside; the kDrive trash is the safety net
            self._extras += [(rel, e) for e in siblings if e.name not in kept]

    async def _copy_subdir(self, rel: str, name: str, parent_id: int | None, siblings: list[Entry]) -> None:
        dir_id = await self.ensure_dir(parent_id, name, siblings)
        child = join_rel(rel, name)
        known = self._tree[rel]
        if dir_id is not None and name not in known:
            known[name] = Entry(dir_id, name, True, None, None)
            self._tree[child] = {}
            await self._changed()
        await self.copy_tree(child, dir_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Copies a local folder to kDrive; with --mirror also removes extras.")
    parser.add_argument("--src", type=Path, required=True, help="local folder")
    parser.add_argument("--dest", required=True, help="folder on the drive, e.g. Private/Backups/pc/2026-10-22")
    parser.add_argument(
        "--ignore-file", type=Path, default=DEFAULT_IGNORE_FILE, help="ignore file in .gitignore syntax"
    )
    parser.add_argument(
        "--drive-id",
        type=int,
        default=os.environ.get("KDRIVE_DRIVE_ID"),
        help="drive ID (the number after /drive/ in the kDrive URL); defaults to KDRIVE_DRIVE_ID",
    )
    parser.add_argument("--concurrency", type=int, default=8, help="number of concurrent requests")
    parser.add_argument("--mirror", action="store_true", help="delete on the drive (to the trash) whatever is absent from --src")
    parser.add_argument("--dry-run", action="store_true", help="only show the actions, change nothing")
    parser.add_argument(
        "--manifest", action="store_true", help="keep the drive state in a manifest on kDrive instead of listing folders on every run"
    )
    parser.add_argument(
        "--checkpoint-sec",
        type=float,
        default=300,
        help="with --manifest: save the manifest while running at least every N seconds (0 = only at the end)",
    )
    parser.add_argument("--rescan", action="store_true", help="with --manifest: rebuild the state from listings")
    parser.add_argument(
        "--max-delete-pct", type=float, default=10, help="refuse to delete if more than this percentage of entries would be removed"
    )
    return parser.parse_args()


async def main(args: argparse.Namespace) -> None:
    token = os.environ.get("KDRIVE_TOKEN")
    if not token:
        sys.exit("KDRIVE_TOKEN environment variable is required.")
    if args.drive_id is None:
        sys.exit("KDRIVE_DRIVE_ID environment variable or --drive-id is required.")
    if not args.src.is_dir():
        sys.exit(f"--src is not a directory: {args.src}")
    if args.rescan and not args.manifest:
        sys.exit("--rescan requires --manifest")
    if not args.ignore_file.is_file():
        sys.exit(f"--ignore-file not found: {args.ignore_file}")
    ignore = pathspec.GitIgnoreSpec.from_lines(args.ignore_file.read_text().splitlines())
    snapshot = scan_tree(args.src, ignore)  # the local snapshot is fixed before any network call

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)
    async with aiohttp.ClientSession(headers={"Authorization": f"Bearer {token}"}, timeout=timeout) as session:
        drive = KDrive(
            session,
            args.drive_id,
            args.concurrency,
            args.src,
            snapshot,
            mirror=args.mirror,
            dry_run=args.dry_run,
            manifest=args.manifest,
            checkpoint_sec=args.checkpoint_sec,
        )
        dest_id = await drive.ensure_path(PurePosixPath(args.dest).parts)
        if args.manifest:
            await drive.load_manifest(dest_id, args.rescan)
        try:
            await drive.copy_tree("", dest_id)
            if args.mirror:
                try:
                    await drive.delete_extras(args.max_delete_pct)
                except KDriveError as exc:
                    sys.exit(str(exc))
        finally:
            if args.manifest:
                await drive.save_manifest()
        logger.info("api calls: %d", drive.api_calls)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(main(parse_args()))
