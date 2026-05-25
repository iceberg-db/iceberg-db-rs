## Summary

- Status bar Fetched shows distinct S3 files and bytes while a query runs (polls WASM every 400ms).
- Counts each S3 object once per query; bytes sum all HTTP response bodies (S3 + catalog REST).

Builds on merged parallel S3 / proxy CORS work in main.

## Test plan

- [ ] cd web-wasm; run serve.ps1 then hard-refresh browser
- [ ] Horizon: run SELECT with LIMIT; Fetched grows (e.g. 3 files and 1.2 MB)
- [ ] Demo mode: Fetched stays 0 files and 0 B
