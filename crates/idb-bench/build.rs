fn main() {
    #[cfg(target_os = "windows")]
    {
        // DuckDB's Windows build references Restart Manager APIs.
        println!("cargo:rustc-link-lib=dylib=rstrtmgr");
    }
}
