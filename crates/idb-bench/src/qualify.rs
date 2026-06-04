//! Qualify unqualified TPC-DS table names for DataFusion (e.g. `store_sales` → `tpcds.store_sales`).

use regex::Regex;

use crate::tpcds::tables::TPCDS_TABLES;

/// Prefix bare TPC-DS table identifiers with `{schema}.` when not already qualified.
pub fn qualify_tpcds_sql(sql: &str, schema: &str) -> String {
    let mut tables: Vec<&str> = TPCDS_TABLES.to_vec();
    tables.sort_by_key(|t| std::cmp::Reverse(t.len()));
    let mut out = sql.to_string();
    for table in tables {
        // No look-around: the `regex` build on some targets rejects `(?<!…)`.
        // Delimiter classes avoid matching `store` inside `store_sales` and `tpcds.store_sales`.
        let pattern = format!(
            r"(?i)(^|[^.\w]){table}([^.\w]|$)",
            table = regex::escape(table)
        );
        let re = match Regex::new(&pattern) {
            Ok(re) => re,
            Err(_) => continue,
        };
        let replacement = format!("${{1}}{schema}.{table}${{2}}");
        out = re.replace_all(&out, replacement.as_str()).into_owned();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualifies_from_clause() {
        let sql = "SELECT 1 FROM store_sales, date_dim WHERE x = 1";
        let got = qualify_tpcds_sql(sql, "tpcds");
        assert_eq!(
            got,
            "SELECT 1 FROM tpcds.store_sales, tpcds.date_dim WHERE x = 1"
        );
    }

    #[test]
    fn does_not_double_qualify() {
        let sql = "SELECT 1 FROM tpcds.store_sales";
        let got = qualify_tpcds_sql(sql, "tpcds");
        assert!(!got.contains("tpcds.tpcds."));
    }
}
