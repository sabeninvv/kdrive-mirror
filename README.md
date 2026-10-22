# kdrive-mirror

An unofficial script that copies or mirrors a local folder to [kDrive](https://www.infomaniak.com/en/kdrive) (Infomaniak) through the REST API. The project is not affiliated with Infomaniak.

By default the script never deletes anything on the drive (like `rclone copy`). With `--mirror` it behaves like `rclone sync`: kDrive repeats the folder, and extras go to the kDrive trash. For large trees there is an optional manifest that removes the need to list every folder on each run.

## Why

One-way upload and mirroring of a folder to kDrive, for example:

- dumps and projects from computers (Windows, Linux/WSL, Mac) into dated folders;
- a mirror of a folder synced by Syncthing: one always-on node periodically pushes it to kDrive, and kDrive only receives updates and deletions;
- resuming an interrupted upload with the same command: files that are already on the drive with the same size and modification time are skipped.

## Why the REST API and not rclone/WebDAV

According to Infomaniak's official page, WebDAV for kDrive is not available on the kSuite Free and kSuite Standard plans, so `rclone` over WebDAV may fail to connect. This script uses the kDrive REST API with a `drive` token and is written with `asyncio` + `aiohttp` + `aiofiles`.

## What the script does

- Walks the local folder (`pathlib`) and repeats its structure on the drive, creating missing folders.
- Skips a file if the drive has a file with the same name, size and modification time. The modification time is sent on upload (`last_modified_at`) and read back from the listing, so an edit that keeps the size is also detected.
- Uploads new and changed files as a stream in 1 MB chunks, so large files are never held in memory whole. If a file with that name already exists, a new version is created.
- Uploads in parallel (8 concurrent requests by default) over a single shared `aiohttp` session with an `Authorization: Bearer …` header.
- Does not upload what is listed in `kdrive.ignore` (see below). Symbolic links and special files are skipped with a warning.
- Reads folder listings page by page, so it finds existing folders even in very large directories.

## Setup and token

You need [uv](https://docs.astral.sh/uv/). The dependencies (`aiohttp`, `aiofiles`, `pathspec`) are declared in the script itself, no separate installation is needed.

1. Create an API token in Infomaniak Manager (API tokens section of your profile) with the scope **`drive`**. This is not an app password: app passwords do not work for the API.
2. Find your drive ID: it is the number after `/drive/` in the kDrive URL in the browser.
3. Put the token and the drive ID into `~/.envs/kdrive.env` and restrict access to the file:

```bash
mkdir -p ~/.envs
# better create the file in an editor so the token does not end up in your shell history
cat > ~/.envs/kdrive.env <<'END'
KDRIVE_TOKEN=YOUR_TOKEN
KDRIVE_DRIVE_ID=NUMBER_FROM_THE_URL
END
chmod 600 ~/.envs/kdrive.env
```

The script reads the token only from the `KDRIVE_TOKEN` environment variable and the drive ID from `KDRIVE_DRIVE_ID` or the `--drive-id` option. If the token or the ID is missing, it exits immediately. The token never ends up in the repository or in the command line.

Never paste the token into a chat or into commands: it stays in history and logs. If the token leaks, revoke it in Manager and create a new one.

## Usage

```bash
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py \
    --src ~/Docs \
    --dest "Private/Backups/my-pc/2026-10-22"
```

Options:

| Option | Required | Meaning |
|---|---|---|
| `--src` | yes | local folder |
| `--dest` | yes | folder on the drive, for example `Private/Backups/<computer>/<date>`; created if missing |
| `--ignore-file` | no | ignore file (default: `kdrive.ignore` next to the script) |
| `--drive-id` | yes, unless `KDRIVE_DRIVE_ID` is set | drive ID, the number after `/drive/` in the kDrive URL |
| `--concurrency` | no | number of concurrent requests (default `8`) |
| `--mirror` | no | delete on the drive (to the kDrive trash) whatever is absent from `--src` |
| `--dry-run` | no | only show the actions (`would upload`, `would delete`), change nothing |
| `--manifest` | no | keep the drive state in a manifest on kDrive instead of listing folders on every run |
| `--checkpoint-sec` | no | with `--manifest`: save the manifest while running at least every N seconds (default `300`, `0` = only at the end) |
| `--rescan` | no | with `--manifest` only: ignore the manifest and rebuild the state from listings |
| `--max-delete-pct` | no | refuse to delete if more than this percentage of entries would be removed (default `10`) |

The drive root contains two folders: `Private` (personal space) and `Common documents` (shared). Use `Private` for personal data so it does not end up shared.

## Mirror mode (`--mirror`)

```bash
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py \
    --src ~/Sync/Docs --dest "Private/Mirror/docs" --mirror --dry-run   # look first
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py \
    --src ~/Sync/Docs --dest "Private/Mirror/docs" --mirror
```

- `--dest` must be a dedicated folder used only for the mirror: everything that is absent from `--src` will be deleted, including what you put there by hand.
- Anything that is now excluded in `kdrive.ignore` also counts as extra and is deleted from the drive.
- Deletion happens after the whole tree has been walked. If the extra entries exceed `--max-delete-pct` of all entries the script saw on the drive, it deletes nothing and exits with a message. This protects against an empty or unmounted `--src`.
- Deletion goes to the kDrive trash (through API v2: v3 has no such method), so files can be restored.
- For Syncthing, `kdrive.ignore` already lists `.stfolder`, `.stversions`, `.syncthing.*.tmp` and `~syncthing~*.tmp`.
- By default no state is stored anywhere: the comparison uses a live listing of every folder. For large trees there is `--manifest` (see below).

## Examples

All examples run from the script folder; `--src` is the local folder, `--dest` is the folder on kDrive.

```bash
# 1. Plain copy (delete nothing)
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py --src ~/Sync/Docs --dest "Private/Mirror/docs"

# 2. See what the mirror would do (nothing changes)
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py --src ~/Sync/Docs --dest "Private/Mirror/docs" --mirror --dry-run

# 3. Mirror with the default delete limit (10%)
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py --src ~/Sync/Docs --dest "Private/Mirror/docs" --mirror

# 4. Mirror after you deliberately removed a noticeable part of the files (allow up to 30%)
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py --src ~/Sync/Docs --dest "Private/Mirror/docs" --mirror --max-delete-pct 30

# 5. Mirror without a limit (allow deleting everything; use consciously)
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py --src ~/Sync/Docs --dest "Private/Mirror/docs" --mirror --max-delete-pct 100

# 6. Large tree: state in a manifest, no folder listings
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py --src ~/Sync/Docs --dest "Private/Mirror/docs" --mirror --manifest

# 7. The same with a raised delete limit
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py --src ~/Sync/Docs --dest "Private/Mirror/docs" --mirror --manifest --max-delete-pct 30

# 8. Manifest with frequent checkpoints (save every minute)
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py --src ~/Sync/Docs --dest "Private/Mirror/docs" --mirror --manifest --checkpoint-sec 60

# 9. The manifest drifted from the drive (something was changed on kDrive by hand): rebuild the state
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py --src ~/Sync/Docs --dest "Private/Mirror/docs" --mirror --manifest --rescan
```

How to choose `--max-delete-pct`:

- The limit is computed as the number of entries the script is about to delete divided by the number of entries it saw on the drive in the folders it walked. If the share is larger than the limit, the script deletes nothing and exits with a message such as `refusing to delete 18 of 67 entries: more than 10%`. New files have already been uploaded by that point.
- The default of 10 protects against an empty, unmounted or wrong `--src`: the share of deletions is then close to 100%.
- First run with `--dry-run` and `--max-delete-pct 100`: it prints every `would delete` line. If exactly the right things would be deleted, run the real command with a limit slightly above the reported share (the refusal message shows it: 18 of 67 is about 27%, so `--max-delete-pct 30` is enough).
- On small folders the 10% limit blocks even single deletions (1 file out of 5 is 20%), so raise it there.

## When the folder changes during a run

Before any request to kDrive the script walks `--src` once and takes a snapshot: for every folder, the list of accepted entries with their size and modification time. Syncing then follows the snapshot strictly; folders are not read a second time.

- A file added after the snapshot is not picked up in this run and is not deleted by the mirror; the next run uploads it.
- A file that vanished or became shorter by the time it is uploaded is skipped with a warning (`skip ...: removed or truncated during sync`), and the run continues.
- A file that grew during the run: exactly the bytes declared in the snapshot are sent, and the size and time from the snapshot are recorded on kDrive. The next run sees the new time and uploads the file again.
- Deleting something that is already gone on kDrive (`object_not_found`) counts as done (`already gone on drive`).
- Uploading a file whose content changes while it is being read can store a mix of old and new bytes, but with the snapshot metadata, so the next run is guaranteed to upload such a file again.

## Manifest (`--manifest`) for large trees

On tens or hundreds of thousands of files, listing every folder takes a lot of requests. With `--manifest` the script keeps the drive state in `<dest>/.kdrive-sync/manifest.json.gz` (gzip; for every folder: id, name, type, size, modification time) and reads it instead of listing.

```bash
uv run --env-file ~/.envs/kdrive.env kdrive_upload.py \
    --src ~/Sync/Docs --dest "Private/Mirror/docs" --mirror --manifest
```

- The first run finds no manifest, builds the state from listings and saves it. Later runs download a single file and no longer list folders: for a folder of 50 files a repeat run with no changes takes 10 requests (path lookup and manifest download), and that number does not grow with the size of the tree.
- The in-memory state is updated after every successful upload, folder creation and deletion. The manifest is saved to kDrive while running on a timer (`--checkpoint-sec`, every 5 minutes by default, if anything changed since the last save) and once more at the end, including when the run failed or refused because of the limit. A run with no changes writes nothing.
- If a run is killed (Ctrl+C, kill, power loss), the manifest keeps the state as of the last checkpoint. Files uploaded after it are uploaded again as a new version by the next run; to avoid that, run once with `--rescan` or use a smaller `--checkpoint-sec`.
- Every save creates a new version of the manifest file on kDrive. On large trees (tens of MB) do not set the interval to seconds.
- The manifest trusts itself: if a file was deleted or changed on kDrive by hand, a normal run will not notice. Run with `--rescan` to rebuild the state from listings.
- A corrupted manifest or one of a different version gives an error with a hint about `--rescan`, not a silent fallback.
- The `.kdrive-sync` service folder is excluded from the comparison and is never deleted by the mirror.
- If you run without `--manifest` where a manifest already exists, the script warns that the manifest is now stale; do the next `--manifest` run with `--rescan`.
- If a file from the manifest was already removed from the drive by hand and is also removed locally, the deletion returns an API error; run with `--rescan`.

## Running on a schedule (every 600 seconds)

Use the native scheduler of each system. Replace the paths with your own. `--manifest` is recommended here, so a run with no changes costs only a few requests.

### Windows (Task Scheduler)

Run in PowerShell:

```powershell
$action = New-ScheduledTaskAction -Execute "uv.exe" `
  -Argument "run --env-file $env:USERPROFILE\.envs\kdrive.env kdrive_upload.py --src C:\Path\To\Folder\ --dest Private/Mirror/folder --mirror --manifest" `
  -WorkingDirectory "C:\path\to\kdrive-mirror"

$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) `
  -RepetitionInterval (New-TimeSpan -Seconds 600)

$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable

Register-ScheduledTask -TaskName "kdrive-mirror" -Action $action -Trigger $trigger -Settings $settings
```

- `-WorkingDirectory` must be the folder that contains `kdrive_upload.py`.
- `-MultipleInstances IgnoreNew` skips a start while the previous run is still going.
- Test with `Start-ScheduledTask kdrive-mirror`; remove with `Unregister-ScheduledTask kdrive-mirror`.
- To run the script inside WSL instead, use `-Execute wsl.exe` and `-Argument '-e bash -lc "cd ~/projects/kdrive-mirror && uv run --env-file ~/.envs/kdrive.env kdrive_upload.py ..."'`.

### macOS (launchd)

Create `~/Library/LaunchAgents/com.user.kdrive-mirror.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.user.kdrive-mirror</string>
  <key>ProgramArguments</key>
  <array>
    <string>/opt/homebrew/bin/uv</string>
    <string>run</string>
    <string>--env-file</string>
    <string>/Users/YOU/.envs/kdrive.env</string>
    <string>kdrive_upload.py</string>
    <string>--src</string>
    <string>/Users/YOU/Sync/Docs/</string>
    <string>--dest</string>
    <string>Private/Mirror/docs</string>
    <string>--mirror</string>
    <string>--manifest</string>
  </array>
  <key>WorkingDirectory</key>
  <string>/Users/YOU/projects/kdrive-mirror</string>
  <key>StartInterval</key>
  <integer>600</integer>
  <key>RunAtLoad</key>
  <true/>
  <key>StandardOutPath</key>
  <string>/tmp/kdrive-mirror.log</string>
  <key>StandardErrorPath</key>
  <string>/tmp/kdrive-mirror.err</string>
</dict>
</plist>
```

```bash
launchctl load ~/Library/LaunchAgents/com.user.kdrive-mirror.plist     # enable
launchctl unload ~/Library/LaunchAgents/com.user.kdrive-mirror.plist   # disable
```

- launchd does not read your shell `PATH`, so give the full path to `uv` (`which uv`).
- launchd does not start a new run while the previous one is still running.
- If `--src` is in a protected folder (Documents, Desktop, iCloud), grant `uv` or your terminal Full Disk Access in System Settings → Privacy & Security.

## Layout on the drive

```text
Private/Backups/
├── linux-pc/2026-10-22/project-a/
└── windows-pc/2026-10-22/project-b/
```

## Exclusions: kdrive.ignore

[kdrive.ignore](kdrive.ignore) sits next to the script and is picked up by default. The syntax is that of `.gitignore` (the `pathspec` library): rules apply to both files and folders.

```text
name/      folders only
name       files and folders
*.ext      by pattern
/name      only in the root of the uploaded folder (--src)
!name      re-include something excluded above
```

- Paths are relative to the `--src` folder. An excluded folder is not walked at all, so its contents never reach the drive.
- For every file and folder the script checks the rules first and only then decides whether to upload it. Excluded entries are logged as `ignore <path>`.
- The default file contains: secrets (`.env`, `.env.*`, `*.session`, `*.pem`, `*.key`, `*.tokens`), `.venv`, `node_modules`, `__pycache__`, caches, OS junk and Syncthing service files. `.git` and ordinary hidden files such as `.gitignore` are uploaded.
- A different rule set: `--ignore-file path/to/file`.

Exclusions protect against accidentally uploading secrets but do not replace a check: look at what a new project contains before uploading it. Symbolic links are always skipped, because a link can point outside the folder (for example to a home directory) and pull unrelated data into the upload.

## Limitations

- A file is retried (up to 3 attempts, 5 and 10 seconds apart, from the start of the file) only when the connection drops, the host cannot be reached, or the response body is empty or not JSON (for example a gateway page). An API error is not retried.
- A file that still fails to upload does not stop the run: the error is logged at once (`failed <path> (...)`), the other files continue, and at the end the script prints the full list of failed files and exits with code 1. A repeat run skips what is already uploaded and tries the failed files again. Errors outside file uploads (creating folders, listing, deleting, the manifest) still stop the script.
- The comparison uses name, size and modification time (to the second), not content. An edit that keeps the size within the same second is not detected.
- Deleting an extra folder counts as one entry, not as the number of files inside; the trash is the safety net.
- Without `--mirror` the script only adds files and versions.
- A file and a folder with the same name in `--src` and on the drive are not handled.
- Files over 1 GB (the `POST /upload` limit) are sent through a kDrive upload session: 128 MiB chunks one after another, then `finish`. If any chunk fails, the session is cancelled and the whole file restarts from the first chunk on the next run. The session path is covered by unit tests with a stubbed API and has not yet been run against a real drive.

## Tests

```bash
uv run --with aiofiles --with aiohttp --with pathspec --with pytest python -m pytest -q
```

## License

[MIT](LICENSE)
