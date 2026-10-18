#!/usr/bin/env node
import { isAbsolute } from "node:path";
import { pathToFileURL } from "node:url";
import { connectRemoteHost } from "./remote-client.js";
import { atomicInstallText } from "./setup-engine.js";

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  if (args.length !== 2 || args[0] !== "--out" || !args[1] || !isAbsolute(args[1])) {
    throw new Error("Usage: dsh-agentlink-connect --out <absolute connection.json path>; supply the DSH launch URL on stdin");
  }
  let input = "";
  for await (const chunk of process.stdin) {
    input += chunk.toString();
    if (input.length > 8192) throw new Error("DSH launch URL input is too large");
  }
  const connection = await connectRemoteHost(input.trim());
  await atomicInstallText({ path: args[1], content: `${JSON.stringify(connection)}\n`, backupLabel: "connection" });
  console.log(`Connected to ${connection.origin}. Connection record saved to ${args[1]}.`);
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  void main().catch(error => { console.error(error instanceof Error ? error.message : "DSH connection failed"); process.exitCode = 1; });
}
