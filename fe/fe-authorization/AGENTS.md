# AGENTS.md — fe-authorization

Architecture, the module map and the new-plugin walkthrough live in `README.md`
next to this file — read that first. This file is operational: how to build and
test, and which obligations and invariants you must not break.

## Build and Test Recipes

Run from the repository root; `-am` also builds upstream reactor deps.

```bash
# One module and its tests
mvn -f fe/pom.xml -pl :fe-authorization-spi -am test

# A single test class. Keep -DfailIfNoTests=false: with -am, upstream modules
# have no matching tests and would fail the build otherwise.
mvn -f fe/pom.xml -pl :fe-authorization-spi -am test \
    -Dtest=AuthorizationPluginSurfaceTest -DfailIfNoTests=false

# A plugin module: package, NOT test. The plugin zip binds to the package
# phase, so a test-level run skips what actually ships. Disable the build cache
# or the module is restored wholesale and its tests never run - see below.
mvn -f fe/pom.xml -pl :fe-authorization-plugin-ranger-doris -am package \
    -Dmaven.build.cache.enabled=false

# The engine side: routing, the behaviour baseline, the installed-plugin e2e
mvn -f fe/pom.xml -pl :fe-core -am test -DfailIfNoTests=false \
    -Dtest='AccessControllerManagerTest,AccessTranslationTest,LegacyAccessControllerPluginTest,AccessControlBehaviorBaselineTest,AuthorizationPluginFromDirectoryTest'
```

Three things about the build cache (`fe/.mvn/maven-build-cache-config.xml`),
each of which produces a green build that proved nothing:

- **A restored module does not run its tests.** The cache key does not include
  `-DskipTests`, so a `package` run right after a `-DskipTests` one restores the
  module and reports SUCCESS with its test classes never executed. Pass
  `-Dmaven.build.cache.enabled=false` whenever the point of the run is the
  tests.
- **`install` fails outright while the cache is on**, at
  `fe-extension-spi: The packaging for this project did not assign a file to the
  build artifact` — a restored module has no artifact file to install. Use
  `package`, or `install -Dmaven.build.cache.enabled=false`.
- It is **mandatory** to disable the cache when you bump the plugin API
  version — see obligation 2 — and worth disabling when validating deletions or
  refactors, where a cached artifact can mask a stale one.

And three about the modules themselves:

- fe-core carries `fe-authorization-plugin-ranger-doris` as a **test**
  dependency (the behaviour baseline runs the production controller), so a
  breaking change in that plugin fails fe-core's tests, not only the plugin's.
  Build order is therefore api → spi → ranger-common → ranger-doris → fe-core.
- The api and spi modules carry no mocking framework: the parent pom
  contributes JUnit only, and the contract tests use hand-written fakes. Keep it
  that way — what `AuthorizationPluginContractTest` proves is what the SPI's
  *default* methods do for a real implementation, and a mock of
  `AuthorizationPlugin` would stub out the very defaults under test. Mockito
  (`mockito-inline`, needed for `mockConstruction`) is declared per plugin
  module, where the thing being faked is a Ranger policy engine.
- Checkstyle is part of the build (`validate` phase) and scans test sources too.

## Machine-Checked Obligations

1. **The frozen plugin API surface.** `AuthorizationPluginSurfaceTest`
   (fe-authorization-spi) freezes
   `src/test/resources/authorization-plugin-surface.txt`. Any drift is a MAJOR
   change: the SAME commit must refresh the baseline (run the test, copy the
   "actual" block from the failure message) **and** increment the major of
   `<authorization.plugin.api.version>` in `fe/fe-authorization/pom.xml`,
   zeroing its minor. Additions count. One more `ResourceKind` or
   `AccessAction` constant turns every deployed plugin's "a kind I do not
   recognise" branch — which the contract requires to be a refusal — into a
   denial of something that used to be allowed, in plugins nobody rebuilt.
   The frozen set is a computed closure over everything a plugin can see, so a
   change to `fe-authorization-api` moves it exactly as much as one to the spi.
   Each line carries the declaration kind, the full generic types and, for a
   member, the modifiers a plugin's compiled code depends on — `default` vs
   `abstract` above all, since every "silence means refusal" default in this
   contract is one, and a `default` turned `abstract` is an `AbstractMethodError`
   in every plugin nobody rebuilt — plus the checked exceptions it declares,
   which are source-incompatible in both directions: one added breaks every
   plugin overriding the method, one removed breaks every plugin catching it.
   One exception to the bump: making the *renderer* record more, as the modifiers
   were added, rewrites every line without any API having changed. Refresh the
   baseline and do not bump — but prove it is only the rendering by checking that
   stripping the new part off every new line reproduces the old file exactly. If
   anything else moved, that part is a real change and does need the bump.
   The other four families render less than this one does: their baselines carry
   erased types, no declaration kind, no constructors, no modifiers and no
   thrown types. So a
   change to `fe-extension-spi` turns all five red only when it changes a method
   signature — a `final` removed from `PluginContext`, a constructor added to it
   or a type parameter changed shows up in this baseline alone. Until those
   renderers match this one, treat a shared-type change as a five-family bump by
   reading the change, not by waiting for four more red tests.
2. **The version property and the build cache.** The version reaches a jar
   through a filtered resource whose *source text* is the literal `${...}`
   placeholder, and maven-build-cache-extension (enabled in `fe/.mvn`) hashes a
   module from `src/**`, its dependencies and its `<build><plugins>` — never
   from `<properties>`. The `maven-jar-plugin` `<manifestEntries>` block in
   `fe/fe-authorization/pom.xml` exists solely to put the value inside
   `<build><plugins>`; do not remove it. Verify any bump with
   `-Dmaven.build.cache.enabled=false`, or the cached jar ships the old number
   in `META-INF/doris/authorization-plugin-api-version.properties` and the FE
   serves a contract nobody declared. This was reproduced in
   `fe/fe-authentication/pom.xml`.
3. **The behaviour baseline.** `AccessControlBehaviorBaselineTest` (fe-core)
   records every decision the manager makes over (resource kind × action ×
   source × caller) into
   `fe/fe-core/src/test/resources/access-control-behavior-baseline.txt`. After
   a change meant to be structural, `git diff` on that file must be empty. A
   line that does change is a behaviour change: read each changed cell, then
   say in the PR description what now decides differently and why. Never
   regenerate it to make a build green.
4. **The selectors a source is named by.** `AuthorizationSourceSelectorCompatibilityTest`
   (fe-core) and each plugin's own factory test pin, as literals, every string
   an operator may select a source by. `access_controller.class` is persisted
   with the catalog and read back verbatim by later releases, so a value that
   ever worked has to keep working. Moving or renaming a factory class means
   adding the old fully-qualified name to `SOURCES_THAT_LEFT_THE_KERNEL` in
   `AccessControllerManager` in the same commit.
5. **Per-family version wiring.** `PluginApiVersionWiringTest` (fe-core) proves
   this family passes a gate at all, that the gate is built from its own kernel
   resource, and that its version moves independently of the other families'.
   What no test can check is that the `<manifestEntries>` element name in the
   pom equals the attribute name the gate derives — those are pinned as
   literals there, to be read against the pom in review.
6. **License gates (CI).**
   - The ASF header check (`license-eye`, `.licenserc.yaml`) runs on every new
     file. Golden files a test reads back line by line must stay header-free and
     be listed in `paths-ignore` instead —
     `authorization-plugin-surface.txt` and
     `access-control-behavior-baseline.txt` are there for that reason.
   - `Dependency License Review`
     (`.github/workflows/third_party_review.yml`) reads every changed pom and
     rejects anything outside `allow-licenses`. Moving a declaration between
     poms does not avoid it. A Category-B dependency needs a named
     `allow-dependencies-licenses` PURL with the reason recorded next to it, and
     `dist/LICENSE-dist.txt` must already cover it — `com.sun.jersey:jersey-client`
     is the worked precedent.

## Invariants Without a Gate

Guarded by tests and reviewed comments rather than build gates. Every one has a
concrete failure mode.

- **Routing stays one function.** `AccessControllerManager.controllerOf` is the
  whole of it. No source-name branching anywhere else, no second opinion, no
  privilege established before a source is asked. Combining two sources or
  granting first would have to happen there, and deliberately does not — the
  behaviour baseline is what would catch it. Three exemptions predate the rule
  and are listed in `README.md`: `isSkipAuth()`, `skip_catalog_priv_check` on a
  catalog with its own source, and the two literal `root`/`admin` accounts being
  exempt from data policies in `LogicalCheckPolicy`. Adding a fourth means
  amending that list, not quietly widening one of these.
- **A source installed instance-wide answers for administration itself.**
  `grantedByGlobalScopeAuthority` answers false when the asking source *is* the
  instance-scope authority, on purpose: it would otherwise ask itself a question
  it is about to answer. A source without an administration rule of its own
  makes the FE unadministrable from its first statement.
- **The api and spi jars are never bundled in a plugin.** `provided` scope, and
  absent from the zip. `org.apache.doris.authorization.` is parent-first, so a
  bundled copy is a second set of types the engine refuses to recognise as the
  ones it asked for.
- **TCCL is pinned for the factory call.** `AccessControllerManager.create`
  swaps the thread context classloader to the plugin's own for the duration.
  Without it, a bundled library resolving class names through the TCCL (Hadoop's
  `Configuration`, which both Ranger sources drag in) loads half its classes
  from the engine's copy and startup dies with "class X not Y". Library worker
  threads created inside the call inherit the loader, which is what keeps them
  working afterwards.
- **A refusal stays cheap.** `AccessDeniedException` is built with no stack
  trace, no cause and no suppression, and composes its message lazily; it is
  thrown once per object a user may not see. Do not let it acquire a stack
  trace, do not wrap it in an exception that fills one in, and do not build its
  message eagerly to log it.
- **A spec compares unequal once its policy changes.** `RowFilterSpec` and
  `DataMaskSpec` are values with real equality because the SQL result cache
  compares them to detect a policy change. Two specs that stay equal while the
  policy underneath moves make a stale plan look current; specs that do not
  compare equal when identical evict the cache on every lookup. Which field
  carries the change is the source's business: Ranger edits a policy in place
  keeping its id, so its `policyIdent` is `<policyId>:<version>`; `RowPolicy`
  has no in-place edit and its `filterSql` moves with the predicate, so its
  ident is the policy name.
- **Security-relevant flags parse strictly.** A property that switches an
  exemption on or off must reject anything that is not exactly its allowed
  values. Reading a typo leniently silently changes who may reach what — see
  `RangerAccessController.DEFER_TO_GLOBAL_SCOPE_AUTHORITY`.
- **One service descriptor per plugin directory.** The loader admits exactly one
  factory per directory; a second descriptor is a load failure, not a second
  source. That is why `fe-authorization-plugin-ranger-common` publishes none and
  ships only into both plugins' `lib/`.
- **`checkAction` is not an engine entry point.** The engine always asks about a
  whole requirement, so `checkAction` is only ever reached through the default
  `checkPrivilege`. A source implements one or the other; do not add a caller
  that reaches `checkAction` directly, or a source that answers whole
  requirements suddenly has a method it never meant to provide on its path.
- **The two `build.sh` module lists stay identical.** The build list (`_authz_mod`)
  and the deploy list (`AUTHZ_PLUGIN_DIR`). The deploy step unzips whatever
  archive is left in a module's `target/`, so a divergence ships a stale plugin
  without failing anything.

## Task Recipes

- **Change the SPI or the api surface**: edit → run fe-authorization-spi's own
  suite → refresh `authorization-plugin-surface.txt` and bump the major in the
  same commit → adjust the in-tree sources (`ranger-doris`, `ranger-hive`,
  `InternalAuthorizationPlugin`, `LegacyAccessControllerPlugin`, the example in
  fe-core's test tree) → run fe-core's authorization tests, including the
  behaviour baseline.
- **Add an authorization plugin**: follow README "Adding a New Authorization
  Plugin" top to bottom; the obligations and invariants above apply from the
  first commit.
- **Fix a plugin bug**: module-scoped `package` with the cache off → fe-core's
  behaviour baseline if the fix changes a decision → a regression suite under
  `regression-test/suites/ranger_p2/` for anything only visible against a live
  Ranger.
- **Change routing or the manager**: fe-core's tests are the gate here —
  `AccessControlBehaviorBaselineTest` for what is decided,
  `AuthorizationPluginFromDirectoryTest` for what an installed plugin can
  actually do, `AuthorizationSourceSelectorCompatibilityTest` for what an
  upgraded deployment still resolves.

## Commit Conventions

See the repository-root `AGENTS.md` for commit-message format, the PR template
and code-review checkpoints. Nothing here overrides it.
