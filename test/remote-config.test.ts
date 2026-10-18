import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { loadConfig } from "../src/config.js";
import { publishDesktopConnection } from "../src/desktop-connection.js";

test("remote mode resolves its dynamic origin from a connection file without including credentials in config", async context => {
  const home = await mkdtemp(join(tmpdir(), "remote-config-"));
  context.after(() => rm(home, { recursive: true, force: true, maxRetries: 5, retryDelay: 20 }));
  const file = join(home, "connection.json");
  await writeFile(file, JSON.stringify({ origin: "http://127.0.0.1:53412", cookie: "dsh=PRIVATE-cookie" }));
  const config = loadConfig({ DSH_PROTOCOL: "remote", DSH_CONNECTION_FILE: file, DSH_BRIDGE_HOME: home });
  assert.equal(config.hostUrl, "http://127.0.0.1:53412");
  assert.equal(JSON.stringify(config).includes("PRIVATE-cookie"), false);
});

test("remote mode fails closed without its explicit connection file", () => {
  assert.throws(() => loadConfig({ DSH_PROTOCOL: "remote" }), /DSH_CONNECTION_FILE/);
  assert.throws(() => loadConfig({ DSH_PROTOCOL: "typo" }), /DSH_PROTOCOL/);
});

test("desktop publication uses the authenticated Host URL and never stores a launch token", async context => {
  const home = await mkdtemp(join(tmpdir(), "desktop-publish-"));
  context.after(() => rm(home, { recursive: true, force: true, maxRetries: 5, retryDelay: 20 }));
  const file = join(home, "connection.json");
  await publishDesktopConnection(file, { origin: "http://127.0.0.1:53412", cookie: "dsh=private-cookie" });
  const config = loadConfig({ DSH_PROTOCOL: "remote", DSH_CONNECTION_FILE: file, DSH_BRIDGE_HOME: home });
  assert.equal(config.hostUrl, "http://127.0.0.1:53412");
});
