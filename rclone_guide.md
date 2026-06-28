# Downloading the Nikkei NEEDS Dataset from Google Drive with rclone

*A pragmatic manual: from getting access to a verified, complete local copy.*

## Mental model

The dataset lives in a Google Drive folder that someone shares with your account, and `rclone` mirrors it to a local disk. Think of `rclone copy` as a **one-way mirror**: it reads the remote folder and reproduces it locally, transferring only what is missing and **never deleting anything** at either end. That makes every run safe to repeat and trivially resumable after an interruption.

## What you need

- A Google account that has been granted access to the shared dataset folder.
- `rclone` installed — confirm with `rclone version`. Get it from rclone.org or your package manager.
- Local free space comfortably larger than the dataset (measure it in Step 4 before committing).

---

## Step 1 — Get access to the shared Drive

1. The dataset owner shares the top-level folder with your Google account.
2. In Google Drive (web), open **Shared with me** and confirm the folder is visible. Open it once to verify you can browse into it.
3. Note that this is a *Shared with me* item, **not** a Shared Drive (Team Drive). That distinction decides which rclone flag you need (Step 3).

If the share does not appear, ask the owner to confirm they shared it with the exact address of the Google account you will authorize in rclone.

## Step 2 — Configure an rclone remote

Run the interactive config:

```bash
rclone config
```

`rclone config` is a menu-driven Q&A. You will be asked a series of prompts; here is the full
sequence and what to answer:

1. **`n`** → new remote.
2. **Name**: a short name (referred to below as `REMOTE`). This is the prefix before every path: `REMOTE:`.
3. **Storage type**: choose **Google Drive** (`drive`). It is selected by number, and the number shifts
   between rclone versions — read the list, don't assume; type the digit shown next to "Google Drive".
4. **client_id / client_secret**: leave blank (Enter) unless you have your own Google API credentials.
5. **Scope**: full access (option `1`). Read-only (`2`) is sufficient for mirroring, but full avoids edge cases.
6. **root_folder_id / service_account_file**: leave blank (Enter).
7. **Edit advanced config?** → **`n`** (No).
8. **Use web browser to automatically authenticate rclone? / Use auto config?**
   - **Desktop with a browser: `y` (Yes).** rclone opens a browser tab, you sign in and click **Allow**.
   - **Headless server (no browser): `n` (No).** rclone prints a `config_token>` prompt and a
     `rclone authorize "drive" "..."` command; run that on a machine with a browser, then paste the full
     JSON token back. Desktop users should not see `config_token>` — if you do, you answered No to the
     browser question; Ctrl+C and restart, answering Yes.
9. **Configure this as a Shared Drive (Team Drive)?** → **`n` (No).** The dataset is a *Shared with me*
   item, not a Team Drive; access comes from the `--drive-shared-with-me` flag (Step 3).
10. **Keep this remote?** → **`y` (Yes)**, then `q` to quit.

Confirm the remote exists:

```bash
rclone listremotes
```

## Step 3 — The one flag that matters: `--drive-shared-with-me`

Because the data sits under *Shared with me*, the remote's default root shows only your own *My Drive*, which does not contain the dataset. **Every** command that touches the shared data must include:

```
--drive-shared-with-me
```

Forgetting it is the single most common reason a command returns "directory not found" or an empty listing.

This step is a rule, not a command — there is nothing to "run" for Step 3 by itself; you simply append
the flag to every command from here on.

## Step 4 — Map the structure and measure size

List the shared top-level folders:

```bash
rclone lsd REMOTE: --drive-shared-with-me
```

Drill into the dataset to learn its layout (folders only):

```bash
rclone lsf REMOTE:"<dataset>/" --dirs-only --drive-shared-with-me
rclone lsf REMOTE:"<dataset>/<year>/" --dirs-only --drive-shared-with-me
```

The hierarchy is generally: **dataset → year → product code → month (`YYYYMM`) → daily `.zip` files.**

Measure before you pull:

```bash
rclone size REMOTE:"<dataset>/<year>/<product>/" --drive-shared-with-me
```

> **Watch out — product codes are not constant across years.** Data providers revise file formats over time, and the folder/code for a given product can change at a boundary year. List the products per year with `lsf` rather than assuming one fixed set, and build your transfer list from what is actually there.

## Step 5 — Decide the local layout

Mirror the remote structure locally. This is recommended: it preserves provenance and makes future re-syncing trivial.

```
<LOCAL_ROOT>/<year>/<product>/<YYYYMM>/*.zip
```

`rclone copy SRC DST` copies the *contents* of `SRC` into `DST`, so pointing `DST` at `<LOCAL_ROOT>/<year>/<product>/` reproduces the monthly folders underneath it. When pulling multiple products into the same year, keep the product code in the path so months from different products do not collide.

## Step 6 — Smoke-test one slice first

Before a long run, copy a single month of a single product and inspect the result. This catches a wrong destination nesting or an encoding problem while it is still cheap to fix:

```bash
rclone copy REMOTE:"<dataset>/<year>/<product>/<YYYYMM>/" "<LOCAL_ROOT>/<year>/<product>/<YYYYMM>/" --drive-shared-with-me --progress
```

Confirm the local folder now holds the expected `.zip` files with the expected naming. If anything looks off, stop and fix the path before continuing.

## Step 7 — Run the full transfer

Use `rclone copy` (not `sync` — `sync` can delete at the destination). Loop over years and products.

**PowerShell**

```powershell
$years    = 2016..2025                      # adjust to your range
$products = "<PRODUCT_A>", "<PRODUCT_B>"    # confirm per year via lsf
foreach ($y in $years) {
  foreach ($p in $products) {
    rclone copy "REMOTE:<dataset>/$y/$p/" "<LOCAL_ROOT>/$y/$p/" `
      --drive-shared-with-me --progress `
      --transfers 8 --checkers 16 --retries 5 --low-level-retries 10 `
      --stats 30s --log-file "<LOCAL_ROOT>/_logs/${y}_${p}.log" --log-level INFO
  }
}
```

**bash**

```bash
for y in $(seq 2016 2025); do               # adjust to your range
  for p in <PRODUCT_A> <PRODUCT_B>; do      # confirm per year via lsf
    rclone copy "REMOTE:<dataset>/$y/$p/" "<LOCAL_ROOT>/$y/$p/" \
      --drive-shared-with-me --progress \
      --transfers 8 --checkers 16 --retries 5 --low-level-retries 10 \
      --stats 30s --log-file "<LOCAL_ROOT>/_logs/${y}_${p}.log" --log-level INFO
  done
done
```

If the product set differs by year, branch the product list inside the year loop instead of using one fixed array.

**The flags, briefly**

| Flag | Why |
|---|---|
| `--drive-shared-with-me` | Required — reads the *Shared with me* tree instead of My Drive. |
| `--transfers 8` | Files copied in parallel. |
| `--checkers 16` | Parallel existence/hash checks that decide what to skip. |
| `--retries 5`, `--low-level-retries 10` | Ride out transient network and API errors. |
| `--progress` | Live progress in the terminal. |
| `--stats 30s` | Periodic stats line (also lands in the log). |
| `--log-file … --log-level INFO` | A persistent per-run record for provenance and debugging. |

Remote paths always use forward slashes; local paths follow your OS convention.

## Step 8 — Verify the download

Compare local counts and sizes against the remote you measured in Step 4, then run an integrity check. Google Drive exposes MD5 hashes, so this is a real content check, not just a size comparison:

```bash
rclone check REMOTE:"<dataset>/<year>/<product>/" "<LOCAL_ROOT>/<year>/<product>/" --drive-shared-with-me --one-way
```

`--one-way` verifies that every remote file exists and matches locally without flagging extra local files. Re-run `rclone copy` for any product that reports differences or errored; because copy is idempotent, it only fetches what is missing.

## Step 9 — Finish and keep provenance

- Keep the `_logs/` directory next to the data. The per-run logs are your record of what was pulled and when.
- The local copy is reproducible: if it is lost, the same remote and the same commands rebuild it.

---

## Troubleshooting

- **Empty listing or "directory not found":** you omitted `--drive-shared-with-me`, or the path does not match exactly — copy the name straight from `rclone lsf` output.
- **Non-ASCII folder names (e.g. Japanese):** force a UTF-8 console. Windows: `chcp 65001` and `$OutputEncoding = [System.Text.Encoding]::UTF8`. Linux/macOS: use a UTF-8 locale.
- **`userRateLimitExceeded` or quota errors:** rclone backs off and retries automatically; if persistent, lower `--transfers` or add `--tpslimit`.
- **OAuth token expired:** `rclone config reconnect REMOTE:`.
- **Interrupted run:** simply re-run the same `rclone copy`; completed files are skipped and the transfer resumes from where it stopped.
- **Windows: a trailing backslash before a closing quote breaks the command.** A destination written as
  `"C:\...\YYYYMM\"` ends in `\"`, which Windows parses as an escaped quote — so the quote never closes and
  everything after it (including `--drive-shared-with-me`) is swallowed into the path and never passed to
  rclone, producing a misleading "directory not found". Fix: use forward slashes for local paths
  (`"C:/.../YYYYMM"`), or drop the trailing backslash. Also never paste placeholder text like
  `<LOCAL_ROOT>` literally — `<` and `>` are redirection operators in cmd.exe.
