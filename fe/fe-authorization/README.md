# fe-authorization Developer Guide

This directory holds the authorization plugin framework of the Doris FE: the
contract an authorization source implements, the vocabulary it decides with,
and the sources shipped with the release. An *authorization source* decides,
for the resources it governs, what a user may do with them — without fe-core
knowing anything about how it decides.

Three companion documents divide the work with this one:

- `AGENTS.md` (next to this file) — build/test recipes, the machine-checked
  obligations, and the invariants that are not expressible as a gate. Read it
  before changing code here.
- `fe-authorization-spi/README.md` — the plugin author's quickstart: a minimal
  plugin end to end, the `META-INF/services` line, the installed directory
  layout, and the API-version manifest entry. Copy from there; this file
  explains the framework the copy lands in.
- Generic plugin machinery is NOT defined here: contracts live in
  `fe/fe-extension-spi` and directory loading in `fe/fe-extension-loader`. Each
  has its own README.

## What This Is

Four design rules shape everything in this directory:

1. **One source answers, and its answer is the whole answer.** Which source is
   asked follows from the resource alone: the plugin a catalog is bound to
   answers for everything inside that catalog, the plugin installed for the
   instance answers for everything else. Nothing grants access before a plugin
   is asked and no second plugin is consulted after it. `AccessControllerManager`
   (fe-core) is pure routing — it establishes no privilege of its own and never
   combines two verdicts — which is what makes the policies in force on an
   object readable from the configuration.
2. **Refusing is throwing, and silence refuses.** A check that returns has
   allowed the access; a check that refuses throws `AccessDeniedException`.
   There is no third outcome and no boolean for a caller to ignore. Every check
   method defaults to refusing, so an omission costs you access control you did
   not think about, never a hole. The two data-policy methods are the exception:
   their empty default means "this source defines no policy", which is not the
   same as allowing anything.
3. **A plugin never imports fe-core.** It compiles against
   `fe-authorization-api` and `fe-authorization-spi` and nothing else of Doris —
   the plugin modules simply do not have fe-core on their classpath, which is a
   stronger guarantee than a gate. What a source cannot decide alone it asks the
   engine for through `AuthorizationContext` (Doris roles, instance-scope
   authority, ownership).
4. **The engine adds no caching and grants no exemptions.** It cannot know when
   an external policy changed, so whatever caching a source needs belongs inside
   the plugin, where it can be invalidated on that source's own terms. And
   exemptions that used to be the engine's — "an administrator may go anywhere" —
   are each plugin's own to grant or refuse.

## Module Map

Roles only — one line each. For anything deeper, read the javadoc; it is kept
authoritative.

**Contracts** (both loaded from the FE, never from a plugin jar — see
"Classloading")

| Module | Role |
|---|---|
| `fe-authorization-api` | The decision vocabulary shared by the engine and every source: `AuthorizedSubject`, `AuthorizedResource` (a closed hierarchy) with `ResourceKind`, `AccessAction`, `AccessRequirement` / `ActionMatch` and the named `AccessRequirements`, `AccessContext`, `AccessDeniedException`, and the data-policy payloads `RowFilterSpec` / `DataMaskSpec` / `RowFilterMergeType`. No dependencies at all. |
| `fe-authorization-spi` | The contract itself: `AuthorizationPluginFactory` (what a jar publishes), `AuthorizationPlugin` (the decisions), `AuthorizationContext` (what the engine answers when a source asks it something). Depends on the api and on `fe-extension-spi`. Its javadoc **is** the API reference. |

Two modules where fe-connector has one, because here the split is acyclic: a
plugin decides with api types and implements spi interfaces, and the api never
mentions the spi. (fe-connector's boundary is bidirectional, which is why
splitting it by "who implements" would be circular there.)

**Plugins** — one module per source, each installed from its own plugin
directory.

| Module | Role |
|---|---|
| `fe-authorization-plugin-ranger-common` | What the Ranger sources share: asking a Ranger policy engine and reading its answer, plus the row-filter/data-mask translation. A library, not a plugin — it publishes no service descriptor and ships no zip, so it can sit in both plugins' `lib/` without either directory appearing to publish two sources. |
| `fe-authorization-plugin-ranger-doris` | A Ranger service of type `doris`. Answers about every resource kind, which is why it is the one Ranger source installable for a whole instance (`access_controller_type = ranger-doris`). |
| `fe-authorization-plugin-ranger-hive` | A Ranger service of type `hive`, for one external catalog (`"access_controller.class" = "ranger-hive"`). Also carries the audit handler. |

One source per module is not a style choice: `DirectoryPluginRuntimeManager`
admits exactly one factory per plugin directory, so two service descriptors
under one directory is a load failure rather than a pair of sources.

**Not in this directory, but part of the picture:** the built-in privilege model
is `InternalAuthorizationPlugin` (fe-core, name `default`), an authorization
source like any other and what `access_controller_type` defaults to. It lives
with `Auth` because it *is* `Auth`'s front door. Read it as the reference for
how a source that governs everything answers the whole contract.

## How an Authorization Source Runs

**Startup.** `Env` builds `AccessControllerManager` (fe-core,
`org.apache.doris.mysql.privilege`), which discovers factories in two rounds:
first `ServiceLoader` on the classpath (built-ins and tests), then
`DirectoryPluginRuntimeManager` over the roots named by
`Config.authorization_plugins_dir` (production). A classpath factory keeps its
name against a directory one, so dropping a jar into the plugin directory can
never displace a source shipped with the FE. Directory plugins pass an API
version gate; classpath ones deliberately do not — what is on the classpath was
built from this same source tree, so the version there would be a number
compared against itself. A directory that fails is logged and skipped, because
one unusable plugin must not stop an FE from starting; if the failed one is the
source the configuration names, the manager's constructor refuses right
afterwards with the rejection reason appended to the error.

There is also a deprecated channel: `AccessControllerFactory` (fe-core), read
from loose jars at the *root* of the same directory. It is still loaded and
wrapped in `LegacyAccessControllerPlugin`, and a name published both ways
resolves to the newer publication — but it carries no declared API version, so a
plugin built against an older Doris is admitted with no diagnosis. Do not write
new sources against it.

**Selection.** Two channels, both naming the source by the string
`AuthorizationPluginFactory.name()` returns:

| Where | Key | Meaning |
|---|---|---|
| `fe.conf` | `access_controller_type` | The source governing the instance. Defaults to `default`, the built-in privilege model. |
| `fe.conf` | `authorization_plugins_dir` | Plugin roots, comma-separated. Default `${DORIS_HOME}/plugins/authorization`. |
| `fe.conf` | `authorization_config_file_path` | Properties handed to the instance-wide source, as a flat properties file. Default `/conf/authorization.conf`, resolved under `DORIS_HOME`. |
| catalog property | `access_controller.class` | The source governing that one external catalog. |
| catalog property | `access_controller.properties.<k>` | Properties handed to that source, with the prefix stripped. |

`access_controller.class` is persisted with the catalog and read back verbatim
by every later FE, so it also accepts the *class name* of the publishing
factory, and `AccessControllerManager.SOURCES_THAT_LEFT_THE_KERNEL` keeps
working the class names of sources that have since moved out of fe-core. This
is why renaming a factory class is a compatibility event; see AGENTS.md.

**Routing a check.** Every access decision in fe-core funnels through
`AccessControllerManager.decide` (yes/no) or `decideColumns` (which reports the
column that was refused), and both route through `controllerOf(resource)`:
global, resource, workload group, storage vault, the cloud kinds — and
*catalog-level* grants, which only the instance-wide source ever stores — go to
the source installed for the instance; database, table and columns go to the
source their catalog is bound to. Row filters and column masks
(`evalRowFilterPolicies`, `evalDataMaskPolicy`) route the same way.

**Lifecycle.** A source is created once and kept: unlike an authentication
attempt, an authorization decision happens many times within a single statement,
so a source that caches policies has to be the same instance throughout. The
engine builds a new one only when what configures it changes, and calls
`close()` on the one it replaces (also on catalog DROP or reset). Note that
`Plugin.initialize(PluginContext)` is **never called** for this family:
everything a source needs arrives in `create(properties, context)`, which is
also the only moment the `AuthorizationContext` can be handed over — the context
has to name the source it belongs to, and the source does not exist until its
factory has run.

**Cost.** These methods are on the path of every statement, several times over:
planning one query checks each table it reads, and listing what a user may see
checks every object that exists. That shapes the contract in two visible places —
requirements are asked about as a whole rather than one action at a time, and
`AccessDeniedException` records no stack trace and composes its message only if
somebody reads it.

**Classloading.** Each plugin directory gets its own child-first classloader.
`org.apache.doris.authorization.` is parent-first for this family, so the api
and spi types crossing the boundary exist exactly once — a plugin carrying its
own copy would hand back objects the engine refuses to recognise as the types it
asked for. Keep both jars `<scope>provided</scope>` and out of your zip. The
loader's mandatory parent-first prefixes apply on top and are additive:
`java.`, `javax.`, `sun.`, `com.sun.`, `org.slf4j.`, `org.apache.logging.`,
`org.apache.doris.extension.spi.`, `org.apache.doris.connector.spi.`. Two
consequences worth knowing before debugging a `ClassCastException` here:

- `org.apache.hadoop.` is **child-first** for this family (unlike the filesystem
  and connector families), so a plugin bundling Hadoop gets its own copy.
- `com.sun.` being parent-first splits a bundled Jersey: the host's
  `com.sun.jersey.core` wins, which is why fe-core declares `jersey-client`
  itself *and* ranger-common bundles it as a `findClass` fallback. Neither side
  alone is enough.

The factory call itself runs with the thread context classloader pinned to the
plugin's own loader, because a bundled library that resolves class names through
the TCCL (Hadoop's `Configuration` is the recurring case) would otherwise load
half its classes from the engine's copy. Worker threads such a library starts
inherit that loader, which is what keeps them working after the swap is undone.

**Packaging.** Each plugin module assembles
`target/doris-fe-authorization-<name>.zip` via
`src/main/assembly/plugin-zip.xml`: the module jar at the zip root — the only
place scanned for the service descriptor — and everything else under `lib/`.
`build.sh` unzips each into its own subdirectory of
`output/fe/plugins/authorization/`, which is also where an administrator drops a
third-party source. That subdirectory's name is free: the loader takes the
source's name from the factory, never from the directory, so the two need not
match (they do for the sources shipped here, and keeping them equal is kind to
whoever reads the deployment).

## Reading the API

This document does not list SPI methods. The truth lives in code, behind four
mechanisms:

1. **Javadoc is the API reference.** Start at `AuthorizationPlugin` and
   `AuthorizationPluginFactory`, then `AuthorizationContext`. Every method that
   decides has a default that refuses, so each one's javadoc states when a
   source is expected to override it and what the default does instead.
2. **The recorded surface.**
   `fe-authorization-spi/src/test/resources/authorization-plugin-surface.txt`
   freezes everything a plugin can see — not just the SPI interfaces but the
   whole `fe-authorization-api` vocabulary a plugin *decides* with, computed as
   a closure rather than hand-listed. `AuthorizationPluginSurfaceTest` fails on
   any drift. That file is the API inventory; do not maintain one in prose.
3. **Named requirements, but decisions from the action set.**
   `AccessRequirements` names the questions the engine asks — `VISIBILITY`,
   `SELECT`, `ADMINISTRATION`, `ANY_PRIVILEGE`, … — as values, so a source can
   recognise which one it is being asked. Recognising them is optional and
   matching on them exclusively is a bug: requirements are also composed at run
   time (granting a privilege requires holding it *and* the right to grant it),
   and a source that only answers the named ones looks, from outside, like a
   source with a mysteriously incomplete policy. Decide from
   `requirement.getActions()` and `requirement.isSatisfiedBy(granted)`.
4. **Unknown is refusal.** `ResourceKind` is closed and known when a plugin is
   compiled, and the API version gate admits only matching majors, so a kind a
   source does not recognise means the plugin was built against a different
   Doris — refuse it, never guess. A kind that *exists* but this source does not
   govern is likewise a refusal, not an error.

The worked example is
`fe/fe-core/src/test/java/org/apache/doris/authorizationexample/`: a factory, a
plugin that grants by Doris role with one row filter, and a test that installs
it from a plugin directory and puts SQL through it. It is the shortest thing
that answers the questions a first plugin runs into.

## Adding a New Authorization Plugin

Copy from the reference implementations: **`fe-authorization-plugin-ranger-doris`**
exercises the whole framework (instance-wide scope, every resource kind, row
filters and masks, a shared library module, a plugin zip);
`ExampleAuthorizationPlugin` in fe-core's test tree is the minimal contrast;
`InternalAuthorizationPlugin` (fe-core) shows how a source that governs
everything answers the whole contract.

Steps 2, 12 and 13 are for a source shipped in this repository. A third-party
source needs none of them — only the two jars, the service descriptor and the
manifest entry from `fe-authorization-spi/README.md`.

1. **Decide the scope first**, because it decides what you must answer. A
   catalog-bound source only ever sees `DATABASE`, `TABLE`, `COLUMNS` and the
   data policies on tables. An instance-wide source is asked about every kind,
   including `GLOBAL`, `RESOURCE`, `WORKLOAD_GROUP`, `STORAGE_VAULT` and the
   `CLOUD_*` kinds — **and it answers for administration itself.**
   `AuthorizationContext.grantedByGlobalScopeAuthority` returns false when the
   source asking *is* the instance-scope authority, so an instance-wide source
   with no administration rule of its own locks out every account, including the
   one that would fix the configuration.
2. **Module.** Create
   `fe-authorization-plugins/fe-authorization-plugin-<name>/` and register it in
   `fe-authorization-plugins/pom.xml` `<modules>`. Start the pom from
   ranger-doris: keep `fe-authorization-api` and `fe-authorization-spi` at
   `<scope>provided</scope>`, set `<finalName>doris-fe-authorization-<name></finalName>`,
   and bind `maven-assembly-plugin` to the `package` phase.
3. **Name.** Whatever `AuthorizationPluginFactory.name()` returns is the whole
   selector — the value of `access_controller_type` and of
   `access_controller.class`. Make it globally unique across sources, and note
   that it is matched **case-sensitively** (`default` is the one exception,
   compared case-insensitively because it predates all of this). It is persisted
   in catalog properties, so renaming it later is a compatibility event; pin it
   and the factory's class name as literals in a test (copy
   `RangerDorisAccessControllerFactoryTest#testTheSelectorsThisSourceIsNamedBy`).
4. **Factory.** Implement `AuthorizationPluginFactory` with a no-arg
   constructor, and register it in
   `src/main/resources/META-INF/services/org.apache.doris.authorization.spi.AuthorizationPluginFactory` —
   exactly one class, and exactly one such descriptor per plugin directory.
   Do not implement the no-arg `create()`; the SPI default already refuses it
   with the right message. If your source starts background threads (a policy
   refresher, say), decide explicitly whether a second binding gets a second
   instance — ranger-doris returns a singleton and warns that the later
   properties are ignored.
5. **Plugin.** Implement `AuthorizationPlugin`. Override `checkPrivilege` when
   your source can answer about a set of actions in one pass — a bit set, or a
   walk down a resource hierarchy that remembers what an outer level already
   granted. Override `checkAction` alone when it cannot, and let the SPI default
   take the requirement apart; leaving the other at its default is not a hole,
   because the engine only ever asks about a whole requirement.
6. **Refuse by throwing, and name yourself.** `AccessDeniedException.of(subject,
   resource, requirement, name())` for the ordinary case;
   `AccessDeniedException.withMessage(...)` where the wording *is* the answer, as
   for a column check that must say which column failed. Naming the source is
   what later lets an operator tell which of the configured sources said no.
7. **Deference to instance scope.** If the source is catalog-bound, decide
   whether an administrator of the instance may reach what you govern, and
   express it by calling `AuthorizationContext.grantedByGlobalScopeAuthority`
   rather than by testing for a built-in privilege — the answer comes from
   whoever actually governs instance scope, which may itself be a plugin. Make
   it configurable if a deployment might want it off (see
   `RangerAccessController.DEFER_TO_GLOBAL_SCOPE_AUTHORITY`), and parse that flag
   strictly: reading a typo as `false` silently takes away every administrator's
   access to every object you govern.
8. **Roles.** `AuthorizationContext.rolesOf` is the bridge for policies written
   against Doris roles — the engine, not your source, knows who holds them. Ask
   for them inside the check that needs them; they are deliberately not carried
   on the subject, because listing what an account may see walks thousands of
   objects per statement.
9. **Row filters and column masks.** Both are SQL text in Doris dialect; the
   engine parses, type-checks and plans them. Give every spec a `policyIdent`
   that **changes when the policy changes** (`<policyId>:<version>` is the
   shape): the SQL result cache decides "did the policies move?" by comparing
   specs, so a constant ident makes an edited policy look unchanged, while a
   spec that does not compare equal to an identical one evicts the cache on
   every lookup. Restrictive filters are ANDed and permissive ones ORed — the
   engine owns the merge. Returning nothing means "no policy here", never a
   refusal.
10. **Caching.** Inside the plugin, invalidated on your source's terms. The
    engine adds none and cannot. Your instance is long-lived, so instance state
    is the place for it.
11. **Properties.** Instance-wide sources are configured from
    `conf/authorization.conf`, catalog-bound ones from
    `access_controller.properties.*` — both arrive as a flat
    `Map<String, String>` with no binder. Validate in the factory or the
    constructor and fail loudly; a source built with an unparseable setting is
    worse than one that refused to be built.
12. **Packaging.** Add `src/main/assembly/plugin-zip.xml` (copy from
    ranger-doris): the module jar at the zip root, everything else in `lib/`,
    log4j and slf4j excluded because logging is the host's. Verify through
    `package`, not `test` — the zip only materialises then. Unzip it once and
    look: the jar at the root, no `fe-authorization-api`/`-spi`, no logging
    implementation.
13. **Ship it.** Add the module to **both** lists in `build.sh` — the build list
    (search `_authz_mod`) and the deploy list (search `AUTHZ_PLUGIN_DIR`).
    Missing from one is not a no-op: the deploy step unzips whatever archive is
    left in the module's `target/`, so a plugin built but not deployed, or
    deployed but not rebuilt, ships stale without failing anything.
14. **API version.** Nothing to do in tree: `fe/fe-authorization/pom.xml` stamps
    `Doris-Authorization-Plugin-Api-Version` into every jar built under it. Out
    of tree, add the `maven-jar-plugin` `<manifestEntries>` block from
    `fe-authorization-spi/README.md` — a jar that declares nothing is refused,
    so a plugin written with no awareness of this contract cannot slip through.
15. **Third-party dependencies.** Bundle what you need rather than depending on
    what the host happens to carry; a plugin whose dependencies come half from
    the host is a plugin whose behaviour changes when the host is upgraded. Two
    CI gates apply: the ASF header check on every new file, and the dependency
    license review on every changed pom (see AGENTS.md).
16. **Tests.** Module-level unit tests with Mockito, as the Ranger plugins do.
    If the new source changes what the engine decides, it belongs in fe-core's
    behaviour baseline too — see "Testing and Verification".

## Testing and Verification

- **Unit tests** live in each module; recipes are in `AGENTS.md`.
- **The frozen contract** is guarded by fe-authorization-spi's own suite:
  `AuthorizationPluginSurfaceTest` (the surface baseline) and
  `AuthorizationPluginContractTest` (silence refuses; `ANY` vs `ALL` are taken
  apart the way the requirement says). Run that module's tests after any change
  to the api or the spi — a consumer-only run will not catch a stale baseline.
- **The behaviour baseline.** `AccessControlBehaviorBaselineTest` (fe-core)
  records every decision over the matrix (resource kind × action × source ×
  privilege level of the caller) into
  `fe/fe-core/src/test/resources/access-control-behavior-baseline.txt`. The
  built-in half runs against a real FE with real `GRANT` statements; the Ranger
  half runs the production controller over a deterministic stub policy engine.
  After a change meant to be structural, `git diff` on that file must be empty.
- **Installed-plugin end to end.** `AuthorizationPluginFromDirectoryTest`
  (fe-core) writes a plugin jar into a temporary `authorization_plugins_dir`,
  starts an FE on it and checks from SQL that the plugin really decides — that
  an account the built-in model granted nothing can read, that an account it
  granted `SELECT` cannot, and that the row filter is planned. Copy it whenever
  a new mechanism can only be proved from SQL.
- **Version wiring** is `PluginApiVersionWiringTest` (fe-core), which proves
  each family's gate is built from its own kernel resource and moves
  independently of the others.
- **Selector compatibility** is `AuthorizationSourceSelectorCompatibilityTest`
  (fe-core): every string an older release let an operator select a source by
  still selects it.
- **Against a live Ranger**: suites under `regression-test/suites/ranger_p2/`,
  environment under `docker/thirdparties/docker-compose/ranger/`.
- **At runtime**: `SELECT * FROM information_schema.extensions` lists what was
  actually admitted, per family — a plugin refused on its API version is absent
  there and explained in `fe.log`.

## When to Update This Document

Update this file ONLY when a framework-level fact changes:

- a module is added, removed or renamed under `fe/fe-authorization/`;
- the loading, selection, routing or lifecycle model changes;
- a durable invariant appears that no gate or test can express.

Do NOT update it for SPI method changes (javadoc is the API reference,
`authorization-plugin-surface.txt` is the recorded surface), for a new
`AccessAction` or `ResourceKind` constant, or for bug fixes.
