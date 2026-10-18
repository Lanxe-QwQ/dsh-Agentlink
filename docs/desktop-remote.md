# DSH Desktop 0.2 / Windows

This fork supports the authenticated Remote API shipped with DSH 0.2.1-rc.2.
It keeps the original rc.6/rc.7 client as the default. Set `DSH_PROTOCOL=remote`
only for the new API. Node.js 22 or later is required.

## Build

Clone this fork into a persistent directory, install the lockfile, and build:

```powershell
git clone https://github.com/Lance-QwQ/dsh-Agentlink.git
cd dsh-Agentlink
npm ci
npm run build
```

The examples below use `D:\AIData\ProjectWork\dsh-Agentlink` for the checkout
and `D:\AIData\ProjectWork\dsh-Agentlink-state` for generated bridge data.
Use your actual absolute paths. Keep the installed DSH program and its existing
Home, accounts, sessions, model selection, and permission configuration in place.

## Connect an existing Desktop Host

DSH Desktop uses a random loopback port and authenticates every RPC/stream.
The small host-side `dist/desktop-connection.js` plugin obtains a signed cookie
through DSH's normal root launch-URL exchange and publishes an origin/cookie
record. It does not read browser databases, API keys, or cookie signing secrets.

After building, append this entry to the existing array in
`$DSH_HOME/profiles/desktop/cordis.patch.yml` (use `profiles/web` for a Web Host):

```yaml
- insert:
    - id: agentlink-desktop-connection
      name: 'D:/AIData/ProjectWork/dsh-Agentlink/dist/desktop-connection.js'
      config:
        connectionFile: 'D:/AIData/ProjectWork/dsh-Agentlink-state/connection.json'
```

Back up the existing patch and preserve its other entries. Restart DSH when its
current work can be interrupted. The plugin publishes a fresh record after
startup, including the new random port. The bridge rereads the record for RPCs
and reconnect attempts. The service never starts, stops, or restarts DSH itself.

The connection record grants the local bridge access as the DSH owner. Store it
outside Git in a directory restricted to your account; do not share it. On Windows,
POSIX `chmod` bits do not implement ACL isolation. Existing records are backed up
when replaced; treat those backups as credentials too.

## Codex MCP entry

Add a separate MCP entry, using the real Node executable and checkout paths:

```toml
[mcp_servers.dsh_agentlink]
command = 'C:\path\to\node.exe'
args = ['D:\AIData\ProjectWork\dsh-Agentlink\dist\index.js']

[mcp_servers.dsh_agentlink.env]
DSH_PROTOCOL = 'remote'
DSH_CONNECTION_FILE = 'D:\AIData\ProjectWork\dsh-Agentlink-state\connection.json'
DSH_BRIDGE_HOME = 'D:\AIData\ProjectWork\dsh-Agentlink-state\bridge'
DSH_HOST_VERSION = '0.2.1-rc.2'

[mcp_servers.dsh_agentlink.tools.dsh_resolve_approval]
approval_mode = 'prompt'
```

Reload Codex to load the entry. `DSH_HOST_URL` is resolved from the connection
record, so a fixed `3080` URL is not needed. The cookie never enters MCP TOML or
diagnostic config output. The existing setup wizard is for the legacy backend;
use this explicit configuration for Remote mode.

Use the existing `dsh_delegate`, `dsh_followup`, `dsh_status`, `dsh_wait`,
`dsh_tail`, and `dsh_cancel` tools. `dsh_delegate` is detached by default.
Approval responses remain human-gated, session-bound, and non-retried. A
connection interruption invalidates pending Remote event IDs; do not replay
answers from an older connection.

Remote mode follows DSH's configured default preset. Explicit preset-roster
selection is currently unavailable and fails before prompt submission. Direct
one-shot and continuable subagent history is supported; external subagent
providers are not projected into this bridge's older child contract.

## Manual pairing without a host plugin

If the Host operator already has its current root launch URL, supply it to the
connection command through stdin, never as a command-line argument:

```powershell
Get-Content -Raw -LiteralPath 'D:\private\dsh-launch-url.txt' |
  node dist/connect.js --out 'D:\AIData\ProjectWork\dsh-Agentlink-state\connection.json'
```

The command accepts a literal loopback root URL with one `token` parameter,
exchanges it without following redirects, and stores only the signed cookie
and origin. A manual pairing must be refreshed if the Host port changes.
HTTP 401 means the connection needs refreshing, not that DSH is stopped.

## Verification

```powershell
npm run check
$env:DSH_PROTOCOL = 'remote'
$env:DSH_CONNECTION_FILE = 'D:\AIData\ProjectWork\dsh-Agentlink-state\connection.json'
$env:DSH_BRIDGE_HOME = 'D:\AIData\ProjectWork\dsh-Agentlink-state\bridge'
npm run doctor
```

Windows tests check the supported read-only attribute rather than pretending
Unix mode bits enforce ACLs. Symlink fixtures skip only when Windows returns
`EPERM` because Developer Mode/symlink privilege is unavailable. Temporary fixture
cleanup uses bounded retries for Windows file-handle release. CI covers Linux
and Windows on Node 22 and 24.

`scripts/smoke-remote.mjs` runs a full opt-in integration check against an installed
DSH CLI and a deterministic HTTP model stub on loopback. It creates its own DSH
Home/profile/workspace, loads the connection-publisher plugin, and verifies
detached delegation, result collection, same-session follow-up, and active-turn
cancellation. It never uses the daily DSH Home or calls an external model.
Set `DSH_SMOKE_ROOT` to an absolute test-output directory and `DSH_SMOKE_COMMAND`
to your CLI launcher; `DSH_SMOKE_ARGS` is an optional JSON array prepended to
`web` arguments. For the desktop executable's Node mode also set
`DSH_SMOKE_ELECTRON=1` and pass its bundled CLI entry in `DSH_SMOKE_ARGS`.
`DSH_SMOKE_VERSION` labels the operator-selected runtime in the report.

The 2026-10-18 Windows validation used DSH **0.2.1-rc.2**, Node **24.19.0**, and
three local stub requests. Authentication, host-side connection publication,
prompt admission, terminal evidence, final answer retrieval, follow-up, and
cancellation passed. DSH reports a cancelled active turn with reason `aborted`.
Question/approval translation is covered by protocol tests; a real hosted model
and real human approval were not exercised by this smoke run.
