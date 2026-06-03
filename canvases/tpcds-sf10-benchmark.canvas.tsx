import {
  BarChart,
  Card,
  CardBody,
  CardHeader,
  H1,
  H2,
  LineChart,
  Row,
  Stack,
  Stat,
  Table,
  Text,
} from "cursor/canvas";

const SP = { xs: 4, sm: 8, md: 12, lg: 24 } as const;

const CANVAS_REVISION = "2026-06-03 · tier1-dynamic-fact-warm";

/** Latest recorded run — bench-tier1-4iter, 4 timed iters/query, median wall time. */
const LATEST = {
  label: "tier1-dynamic-fact-warm",
  recordedAt: "2026-06-03",
  config: "bench-tier1-4iter",
  rows: [
    { query: "q01", icebergMs: 169, duckdbMs: 94, rowsOk: true },
    { query: "q02", icebergMs: 1415, duckdbMs: 182, rowsOk: true },
    { query: "q03", icebergMs: 595, duckdbMs: 135, rowsOk: true },
    { query: "q07", icebergMs: 538, duckdbMs: 291, rowsOk: true },
  ],
} as const;

/** Tier-1 path — q07 iceberg median (ms). */
const TIER1_Q07 = {
  categories: [
    "tier1 1st",
    "star 1×",
    "date 4×",
    "blooms 4×",
    "dyn cold 4×",
    "dyn warm 4×",
  ],
  iceberg: [855, 636, 1140, 1082, 1177, 538],
  duckdb: [345, 269, 664, 513, 643, 291],
} as const;

/** Selected q07 milestones (iceberg ms). */
const Q07_ARC = {
  categories: [
    "05-31 pushdown",
    "05-31 join reorder",
    "06-02 IN filter",
    "06-02 tier1 1×",
    "06-03 blooms 4×",
    "06-03 dyn warm 4×",
  ],
  iceberg: [9652, 1302, 2602, 636, 1082, 538],
  duckdb: [660, 334, 645, 269, 513, 291],
} as const;

const HISTORY_ROWS = [
  ["tier1-dynamic-fact-warm", "2026-06-03", "4iter", "2717", "702", "538", "291", "0.54×"],
  ["tier1-dynamic-fact-warm-v2", "2026-06-03", "4iter", "2752", "745", "554", "312", "0.56×"],
  ["tier1-dynamic-fact-warm-v1", "2026-06-03", "4iter", "3303", "1436", "1153", "593", "0.51×"],
  ["tier1-dynamic-cold-4iter", "2026-06-03", "4iter", "5797", "1553", "1177", "643", "0.55×"],
  ["tier1-real-blooms", "2026-06-03", "4iter", "4970", "1378", "1082", "513", "0.47×"],
  ["date-fk-range-4iter", "2026-06-02", "4iter", "5269", "1690", "1140", "664", "0.58×"],
  ["tier1-star-layout", "2026-06-02", "1×", "2553", "664", "636", "269", "0.42×"],
  ["in-hash-filter-stacking", "2026-06-02", "bench", "5408", "1475", "2602", "645", "0.25×"],
  ["post-pushdown-patch", "2026-05-31", "bench", "12291", "1627", "9652", "660", "0.07×"],
] as const;

const MILESTONES = [
  {
    when: "post-pushdown-patch",
    q07Ms: 9652,
    note: "First timed bench; fact scan not pruned",
  },
  {
    when: "in-hash-filter-stacking",
    q07Ms: 2602,
    note: "FK IN + hash filter; ~75K fact rows",
  },
  {
    when: "tier1-star-layout (1×)",
    q07Ms: 636,
    note: "Best single-shot; star warehouse + tp=4",
  },
  {
    when: "tier1-real-blooms (4×)",
    q07Ms: 1082,
    note: "Parquet blooms; planned_files=2",
  },
  {
    when: "tier1-dynamic-fact-warm (4×)",
    q07Ms: 538,
    note: "Runtime dynamic filters on FK-pushed fact; warm cache",
  },
] as const;

export default function TpcdsSf10Benchmark() {
  const latestTotalIb = LATEST.rows.reduce((s, r) => s + r.icebergMs, 0);
  const latestTotalDk = LATEST.rows.reduce((s, r) => s + r.duckdbMs, 0);
  const q07Ib = LATEST.rows.find((r) => r.query === "q07")!.icebergMs;
  const q07Dk = LATEST.rows.find((r) => r.query === "q07")!.duckdbMs;
  const bestTier1Q07Single = 636;
  const vsSingle = (bestTier1Q07Single / q07Ib).toFixed(2);
  const vsPushdown = (9652 / q07Ib).toFixed(1);
  const vsBlooms = (1082 / q07Ib).toFixed(2);

  return (
    <Stack gap={SP.lg} style={{ padding: SP.lg, maxWidth: 980 }}>
      <div>
        <H1>TPC-DS SF10 — performance history</H1>
        <Text tone="tertiary">
          Revision {CANVAS_REVISION} · Source: results/tpcds-history.jsonl · queries q01, q02,
          q03, q07 · rows_ok=yes · bench-data/tpcds-sf10
        </Text>
      </div>

      <Row gap={SP.md} style={{ flexWrap: "wrap" }}>
        <Stat
          label="Latest suite (iceberg)"
          value={`${latestTotalIb.toLocaleString()} ms`}
          tone="info"
        />
        <Stat
          label="Latest suite (duckdb)"
          value={`${latestTotalDk.toLocaleString()} ms`}
          tone="success"
        />
        <Stat label="Latest q07 (iceberg)" value={`${q07Ib} ms`} tone="info" />
        <Stat label="Latest q07 (duckdb)" value={`${q07Dk} ms`} tone="success" />
        <Stat label="Best tier1 q07 1×" value={`${bestTier1Q07Single} ms`} tone="neutral" />
      </Row>

      <Text tone="tertiary" size="small">
        Latest: {LATEST.label} · q07 {q07Ib} ms vs DuckDB {q07Dk} ms ({(q07Ib / q07Dk).toFixed(2)}
        ×) · {vsPushdown}× faster than May baseline · {vsBlooms}× vs blooms 4× median · {vsSingle}
        × vs best 1× single-shot
      </Text>

      <Card>
        <CardHeader>Latest run — wall time by query (ms)</CardHeader>
        <CardBody>
          <BarChart
            height={280}
            valueSuffix=" ms"
            categories={LATEST.rows.map((r) => r.query)}
            series={[
              {
                name: "iceberg-db-rs",
                data: LATEST.rows.map((r) => r.icebergMs),
                tone: "info",
              },
              {
                name: "duckdb-iceberg",
                data: LATEST.rows.map((r) => r.duckdbMs),
                tone: "success",
              },
            ]}
          />
          <Text tone="tertiary" size="small">
            Run 23 · {LATEST.config} · {LATEST.recordedAt} · median over 4 timed iterations (warm
            back-to-back after FK + dynamic-fact fix)
          </Text>
        </CardBody>
      </Card>

      <Card>
        <CardHeader>Tier-1 path — q07 wall time (ms)</CardHeader>
        <CardBody>
          <LineChart
            height={280}
            categories={[...TIER1_Q07.categories]}
            valueSuffix=" ms"
            series={[
              {
                name: "iceberg-db-rs q07",
                data: [...TIER1_Q07.iceberg],
                tone: "info",
              },
              {
                name: "duckdb-iceberg q07",
                data: [...TIER1_Q07.duckdb],
                tone: "success",
              },
            ]}
          />
          <Text tone="tertiary" size="small">
            Y-axis: q07 median latency (ms). “dyn warm” = fact dynamic filters + warm OS cache.
          </Text>
        </CardBody>
      </Card>

      <Card>
        <CardHeader>q07 arc — selected milestones (ms)</CardHeader>
        <CardBody>
          <LineChart
            height={300}
            categories={[...Q07_ARC.categories]}
            valueSuffix=" ms"
            series={[
              {
                name: "iceberg-db-rs q07",
                data: [...Q07_ARC.iceberg],
                tone: "info",
              },
              {
                name: "duckdb-iceberg q07",
                data: [...Q07_ARC.duckdb],
                tone: "success",
              },
            ]}
          />
          <Text tone="tertiary" size="small">
            Y-axis: q07 latency (ms). Excludes failed runs and sub-100ms outliers.
          </Text>
        </CardBody>
      </Card>

      <Stack gap={SP.sm}>
        <H2>Recorded runs (newest first)</H2>
        <Table
          headers={[
            "Label",
            "Date",
            "Config",
            "Suite iceberg",
            "Suite duckdb",
            "q07 iceberg",
            "q07 duckdb",
            "q07 ratio",
          ]}
          columnAlign={["left", "left", "left", "right", "right", "right", "right", "right"]}
          rows={HISTORY_ROWS.map((r) => [...r])}
        />
      </Stack>

      <Stack gap={SP.sm}>
        <H2>q07 milestones</H2>
        <Table
          headers={["Run", "q07 iceberg (ms)", "Note"]}
          columnAlign={["left", "right", "left"]}
          rows={MILESTONES.map((m) => [m.when, m.q07Ms.toLocaleString(), m.note])}
        />
      </Stack>
    </Stack>
  );
}
