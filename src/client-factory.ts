import type { BridgeConfig } from "./config.js";
import { DshClient } from "./dsh-client.js";
import type { DshApi } from "./dsh-types.js";
import { readRemoteConnection, RemoteDshClient } from "./remote-client.js";

export function createDshClient(config: BridgeConfig): DshApi {
  if (config.protocol !== "remote") return new DshClient(config.hostUrl, config.requestTimeoutMs);
  if (!config.connectionFile) throw new Error("Remote mode requires DSH_CONNECTION_FILE");
  const file = config.connectionFile;
  return new RemoteDshClient(() => {
    const connection = readRemoteConnection(file);
    config.hostUrl = connection.origin;
    return connection;
  }, config.requestTimeoutMs, config.declaredDshVersion);
}
