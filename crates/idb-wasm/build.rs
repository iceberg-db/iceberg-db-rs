fn main() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    println!("cargo:rustc-env=IDB_WASM_BUILD={stamp}");
    println!("cargo:rustc-env=IDB_WASM_BUILD_TAG=v4-fresh-metadata");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=../idb-sql/src/lib.rs");
    println!("cargo:rerun-if-changed=../idb-sql/src/wasm_lazy_catalog.rs");
    println!("cargo:rerun-if-changed=../idb-catalog/src/wasm_local.rs");
    println!("cargo:rerun-if-changed=../idb-catalog/src/wasm_s3_storage.rs");
}
