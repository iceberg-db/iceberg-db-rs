/**
 * Dev reverse proxy: /{account}/polaris/... → https://{account}.snowflakecomputing.com/polaris/...
 * Used with Trunk [[proxy]] rewrite="/sf/" → http://127.0.0.1:8787/
 */
import http from "node:http";
import https from "node:https";
import { URL } from "node:url";

const PORT = Number(process.env.SNOWFLAKE_PROXY_PORT || 8787);

function forward(req, res) {
  const incoming = new URL(req.url || "/", `http://127.0.0.1:${PORT}`);
  const segments = incoming.pathname.split("/").filter(Boolean);
  const account = segments.shift();
  if (!account) {
    res.writeHead(400, { "Content-Type": "text/plain" });
    res.end("Snowflake proxy: expected /{account}/polaris/...");
    return;
  }

  const upstreamPath = `/${segments.join("/")}${incoming.search}`;
  const target = new URL(upstreamPath, `https://${account}.snowflakecomputing.com`);

  const headers = { ...req.headers, host: target.host };
  delete headers["connection"];

  const upstream = https.request(
    target,
    { method: req.method, headers },
    (up) => {
      res.writeHead(up.statusCode || 502, up.headers);
      up.pipe(res);
    }
  );

  upstream.on("error", (err) => {
    console.error(`[snowflake-proxy] ${req.method} ${target} → ${err.message}`);
    if (!res.headersSent) {
      res.writeHead(502, { "Content-Type": "text/plain" });
    }
    res.end(`Bad gateway: ${err.message}`);
  });

  req.pipe(upstream);
}

const server = http.createServer(forward);
server.listen(PORT, "127.0.0.1", () => {
  console.log(`[snowflake-proxy] listening on http://127.0.0.1:${PORT}`);
});
