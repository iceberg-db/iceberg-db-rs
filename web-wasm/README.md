# iceberg-db in the browser (WASM)

## Build failed → HTTP 404

Trunk only serves pages after a **successful** WASM build. If `cargo` exits **101**, fix the build first.

### Windows: `clang` not found (most common)

Iceberg links **zstd**, which compiles C code for `wasm32`. You need **LLVM** and `clang` on `PATH`.

```powershell
winget install --id LLVM.LLVM -e
```

Then use the helper script (adds LLVM to PATH for this session):

```powershell
cd C:\Users\chand\.cursor\projects\empty-window\iceberg-db-rs\web-wasm
.\serve.ps1
```

Do **not** rely on an old terminal opened before LLVM was installed — either open a **new** terminal or use `serve.ps1`.

Verify manually:

```powershell
& "${env:ProgramFiles}\LLVM\bin\clang.exe" --version
```

## Prerequisites

```powershell
rustup target add wasm32-unknown-unknown
cargo install trunk
```

### WASM: `time not implemented on this platform`

**Cause:** Connect used to load every Iceberg table at once; building tables touched **moka**, which calls `std::time::Instant::now()` (unsupported on `wasm32-unknown-unknown`).

**Fix:** Rebuild WASM after pulling latest code. `serve.ps1` runs `cargo clean -p idb-wasm` so Trunk does not serve an old `.wasm`. Connect now only lists table names; tables load on first query.

If you still see the panic after a rebuild, from repo root:

```powershell
.\scripts\vendor-iceberg-patch.ps1
```

Uncomment `[patch.crates-io]` for `iceberg` in the root `Cargo.toml`, then run `.\web-wasm\serve.ps1` again.

### Chrome: `message channel closed before a response was received`

That line is usually a **browser extension** (password manager, ad blocker, etc.), not iceberg-db. Ignore it unless Connect fails with no WASM panic. Use a private window with extensions disabled to confirm.

## Manual build (see full errors)

```powershell
$env:Path = "$env:ProgramFiles\LLVM\bin;$env:Path"
$env:CC_wasm32_unknown_unknown = "clang"
cd C:\Users\chand\.cursor\projects\empty-window\iceberg-db-rs
cargo build -p idb-wasm --target wasm32-unknown-unknown --features horizon
```

First build: 10–20+ minutes.

## Snowflake Horizon in the browser

The WASM build (`horizon` feature, default) uses the **same** REST catalog + PAT OAuth + vended `loadTable` path as `idb-cli`.

1. Open the UI (`.\serve.ps1` → http://127.0.0.1:8080).
2. Enter **account host** (e.g. `qtfneqx-er54214`), **warehouse**, **schema**, **scope**, and **PAT**.
3. Click **Connect**.

Example config shape: `config/snowflake-horizon.wasm.example.yaml`.

### CORS (important)

Snowflake does not allow browser calls from `http://127.0.0.1:8080` (no `Access-Control-Allow-Origin`). **`serve.ps1` starts a dev proxy automatically:**

| Step | URL |
|------|-----|
| Browser (WASM) | `http://127.0.0.1:8787/<account>/polaris/api/catalog/...` (CORS from UI on :8080) |
| `idb-sf-proxy` | forwards to `https://<account>.snowflakecomputing.com/...` |

The UI talks to the proxy **directly on port 8787** (not Trunk `/sf/`). Some Trunk versions drop OAuth POST bodies on `/sf/` → 403 with an empty body.

`serve.ps1` builds and runs **`idb-sf-proxy`** on 8787. Optional Trunk `[[proxy]]` `/sf/` remains for curl tests only.

**S3 reads:** Iceberg metadata and Parquet objects are fetched through the same proxy (`GET http://127.0.0.1:8787/_s3?u=…`) with SigV4 headers, so the browser never talks to `*.amazonaws.com` directly (no S3 CORS needed in dev).

Alternative: native **`idb-cli`** (no CORS).

PAT is sent only from your browser through the local proxy to Snowflake — not stored in this repository.

### Demo mode

Click **Demo data** for in-memory `demo.customers` (no network).

## Native vs WASM

| | `idb-cli` | Browser WASM |
|--|-----------|--------------|
| Snowflake Horizon | Yes | Yes (if CORS/proxy allows) |
| PAT | `$env:SNOWFLAKE_ACCESS_TOKEN` | Paste in UI |
| Local Hadoop warehouse | Yes | No |
| Demo tables | No | Yes (`Demo data` button) |
