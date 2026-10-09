# Apache Doris Web UI

`ui/` is the Doris FE Web UI. It replaces the previous AngularJS application,
which this directory no longer contains. `./build.sh --fe` builds it and packs
the result into `doris-fe.jar`, so the standard FE build produces this UI with
no extra flags.

## Prerequisites

- Node.js 22.12 or newer
- npm 10 or newer
- A local Doris FE HTTP endpoint; the development default is `http://127.0.0.1:8030`

## Development

```bash
npm install
npm run dev
```

The Vite server listens on port 5173. Relative `/rest` and `/api` requests are proxied to the local FE. Override the target only when needed:

```bash
DORIS_FE_HTTP_TARGET=http://127.0.0.1:8030 npm run dev
```

The development port can also be overridden without changing repository files:

```bash
npm run dev -- --port 8090
```

## FE integration

`./build.sh --fe` runs `npm install` and `npm run build` in this directory and
copies `ui/dist` into the FE resources, so no extra step is required:

```bash
./build.sh --fe
```

To package a prebuilt directory instead of rebuilding here, use the existing
`CUSTOM_UI_DIST` escape hatch:

```bash
CUSTOM_UI_DIST="$PWD/ui/dist" ./build.sh --fe
```

The resulting FE serves the selected static assets from its configured
`http_port`, which is 8030 by default. Vite, port 5173, and port 8090 are only
development conveniences and are not required in a production deployment.

The Playground's persistent JDBC-backed SQL sessions are enabled by default.
They can be disabled explicitly in FE configuration when the HTTP capability is not wanted:

```text
enable_web_sql_session = false
```

## Quality gates

```bash
npm run lint
npm run typecheck
npm run test
npm run build
npm run test:e2e
```

Playwright requires its Chromium runtime. The browser path may be isolated with `PLAYWRIGHT_BROWSERS_PATH`.

## Implemented scope

- English-only login and logout for Doris users with global `ADMIN` privilege.
  UI Next reuses the legacy `/rest/v1/login` and `/rest/v1/logout` endpoints
  and uses a thin `/rest/v1/ui/me` bootstrap endpoint for the authenticated
  user and CSRF token.
- Home version information and complete frontend/backend node tables. The page
  reuses the existing hardware-version and `/rest/v1/system` APIs.
- Playground SQL editing and formatting, a database/table/column object tree,
  and FE-managed JDBC sessions that preserve SQL session state across
  statements. The Web SQL API is exposed at `/rest/v1/sql-sessions`; it accepts
  either HTTP Basic authentication or the existing Doris login cookie. Both
  modes require global `ADMIN` privilege, and cookie-authenticated mutations
  additionally require the CSRF token returned by `/rest/v1/ui/me`.
- System proc navigation and active Sessions views, both backed by existing
  Doris HTTP APIs.
- Log configuration, content, and verbose logger management, reusing the
  existing `/rest/v1/log` API.
- Query Profile list and raw Text details, reusing the legacy current-FE
  `/rest/v1/query_profile*` APIs. The list includes only entries whose Doris
  `Task Type` is `QUERY`. Text details support search navigation, exact copy,
  and download without transforming the Profile.
- Visual Profile for structurally compatible `MergedProfile` text. The browser parses the same raw
  text in a bounded module Worker, lays out Fragment compound graphs with
  ELK, and renders a read-only React Flow canvas with slow-operator focus,
  search, Fragment visibility, Fit/Reset, MiniMap, and operator details.
- FE and BE Configuration tables backed by the existing configuration APIs.
  The page is read-only: it never writes configuration, so no configuration
  mutation endpoint accepts the UI session cookie. Changing a setting goes
  through `ADMIN SET FRONTEND CONFIG` or the backend configuration API, which
  authenticate the operator directly. The FE table shows the configuration of
  the FE serving the page; open another FE web port to read that node's
  settings. Long values are truncated in the table and remain available on
  hover.
- UI entry and the PR-added bootstrap/Web SQL endpoints require global
  `ADMIN`. Reused legacy `/rest/v1` endpoints retain their existing controller
  authorization behavior; this UI replacement does not make every legacy HTTP
  endpoint globally ADMIN-only.
- React, TypeScript, Vite, React Router, Ant Design, TanStack Query, CodeMirror,
  React Flow, ELK, Vitest, Testing Library, and Playwright infrastructure.

The Visual Profile parser and graph implementation are adapted from
`apache/doris-website` PR #4043 at commit
`133f948c235995a917b2e1f6d4e9d764b6d62726` (Apache License 2.0). It does not
include the website uploader, AI analysis, hCaptcha, polling, recovery,
storage, or Docusaurus wrapper. Compatibility is detected from the
`MergedProfile -> Fragment -> Pipeline -> Operator` structure rather than the
Doris release string. A missing `MergedProfile` is unavailable; a present but
incompatible structure is reported as a parse failure with its reason.

## Backend scope

The FE addition is intentionally concentrated on the persistent Web SQL
session lifecycle: session ownership, one JDBC connection per active session,
statement execution and cancellation, idle cleanup, and bounded results.
The endpoint lives under `httpv2.websql` because it is usable by any HTTP
client and is not coupled to UI Next.
Existing Doris HTTP controllers remain the preferred backend for the other UI
pages.

`enable_web_ui=true` controls the browser UI and its dedicated login/bootstrap
and stateful SQL endpoints without disabling the FE HTTP server or shared HTTP
APIs. `enable_web_sql_session`, `web_sql_session_idle_timeout_seconds`,
`web_sql_max_sessions`, and `web_sql_max_result_bytes` are mutable and are read
by running Web SQL operations without an FE restart.
