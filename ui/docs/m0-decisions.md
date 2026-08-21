# M0 Baseline and Frozen Decisions

Date: 2026-08-11

## Source and runtime baseline

- Doris source HEAD: `8ad4fe202190bfc8944c42e716c29f735c6d35fe`.
- Legacy `ui/` baseline tree: `91a76d311c66a124536dfcf87fcb753708171f19`. That AngularJS application has since been removed; its pre-existing Axios change is not part of this UI.
- Verified a123 FE PID: `3332046`, recovery deployment, HTTP `8030`, MySQL `59330`.
- Verified a123 BE PID: `3400721`, recovery deployment, heartbeat `59050`, alive.
- FE and BE data directories must not be initialized, deleted, or overwritten.
- Node toolchain: official Node.js `22.23.2` LTS, verified with the release SHA-256 manifest.

The earlier development-process assumption that a123 FE HTTP used `58330` is obsolete. Live `SHOW FRONTENDS` and the listening socket both report `8030`; Vite therefore proxies to `http://127.0.0.1:8030` by default.

## Frozen product decisions

1. Ordinary Doris users may sign in, use Playground according to their Doris privileges, and view their own Query Profiles. Operational pages remain capability-gated. Administrators can view all Profiles and perform authorized mutations.
2. Configuration contains both FE and BE tabs and is read-only. The UI never writes configuration: no configuration mutation endpoint accepts the UI session cookie, and changing a setting goes through `ADMIN SET FRONTEND CONFIG` or the backend configuration API. The `Mutable` column stays visible because it tells an operator whether a setting can be changed at runtime at all.
3. The exact official-site Visual Profile source is not present locally. M12 is blocked until its repository, path, approved commit, license, dependencies, and fixtures are supplied. This does not block M0-M11 or M13-M16.
4. Web SQL V1 sessions are FE-process-local and require sticky routing. A session handle will carry an owner-FE hint plus cryptographically random material; ownership and current authenticated user are checked server-side.
5. Web SQL V1 accepts exactly one SQL statement per execute request. Multi-statement parsing is deferred until a Doris-aware parser contract is approved.

## API foundation decisions

- Browser-facing UI APIs use `/rest/v1/ui/**`.
- UI APIs authenticate with the existing opaque HttpOnly login cookie.
- The browser never stores a password or Basic Authorization value.
- UI APIs return stable English DTOs and correct HTTP statuses.
- Error envelope: `code`, `message`, `requestId`, optional `details`.
- Success envelope: `data`, `requestId`.
- Mutations require `X-Doris-CSRF-Token`; `/me` returns the token associated with the login session.
- Authorization is operation-specific. The legacy `/rest/v1/**` behavior is not globally weakened.

## M0 response contract policy

The unused synthetic M0 fixtures were removed because they did not match several real FE response shapes and therefore could not serve as contract evidence. API response assumptions must be covered by an executable test against a response shape verified from the corresponding FE controller or a captured, sanitized runtime response.
