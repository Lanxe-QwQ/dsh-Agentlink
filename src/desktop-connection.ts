import { isAbsolute } from "node:path";
import { connectRemoteHost, remoteOrigin, type RemoteConnection } from "./remote-client.js";
import { atomicInstallText } from "./setup-engine.js";

export async function publishDesktopConnection(path: string, connection: RemoteConnection): Promise<void> {
  remoteOrigin(connection.origin);
  await atomicInstallText({ path, content: `${JSON.stringify(connection)}\n`, backupLabel: "connection" });
}

export const name = "agentlink-desktop-connection";
export const inject = ["connection", "webServer"];
interface DesktopContext {
  connection: { authenticatedUrl(origin: string): string };
  webServer: { protocol: string; host: string; port: number };
  effect<T>(start: () => Promise<T>, label?: string): void;
}

export function apply(ctx: DesktopContext, config: { connectionFile: string }): void {
  if (!config?.connectionFile || !isAbsolute(config.connectionFile)) throw new Error("Agentlink connectionFile must be an absolute private file path");
  ctx.effect(async () => {
    const protocol = ctx.webServer.protocol.replace(/:\/?\/?$/, "");
    const origin = remoteOrigin(`${protocol}://127.0.0.1:${ctx.webServer.port}`);
    const connection = await connectRemoteHost(ctx.connection.authenticatedUrl(origin));
    await publishDesktopConnection(config.connectionFile, connection);
  }, "agentlink: publish authenticated desktop connection");
}
