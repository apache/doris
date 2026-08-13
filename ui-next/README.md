# Apache Doris UI Next

`ui-next` is an isolated, in-progress replacement for the legacy Doris Web UI.
It does not import from or write build output into the legacy `ui/` directory.
The legacy UI remains the default production build while this application is
under development.

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

Build the application first, then use the existing Doris `CUSTOM_UI_DIST`
escape hatch when building FE:

```bash
cd ui-next
npm install
npm run build
cd ..
CUSTOM_UI_DIST="$PWD/ui-next/dist" ./build.sh --fe
```

The resulting FE serves the selected static assets from its configured
`http_port`, which is 8030 by default. Vite, port 5173, and port 8090 are only
development conveniences and are not required in a production deployment.

The Playground's persistent JDBC-backed SQL sessions are disabled by default.
Set the following FE configuration and restart FE before testing that feature:

```text
enable_web_sql_session = true
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
  statements.
- System proc navigation and active Sessions views, both backed by existing
  Doris HTTP APIs.
- Log configuration, content, and verbose logger management, reusing the
  existing `/rest/v1/log` API.
- React, TypeScript, Vite, React Router, Ant Design, TanStack Query, CodeMirror,
  Vitest, Testing Library, and Playwright infrastructure.

Query Profiles and Configuration remain intentional placeholders in this
draft implementation.

## Backend scope

The FE addition is intentionally concentrated on the persistent Web SQL
session lifecycle: session ownership, one JDBC connection per active session,
statement execution and cancellation, idle cleanup, and bounded results.
Existing Doris HTTP controllers remain the preferred backend for the other UI
pages.
