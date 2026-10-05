# fe-extension-loader Developer Integration Guide

This guide is for business module developers. It focuses on:

1. What Loader solves.
2. When to use it.
3. What each core class is responsible for.
4. How to integrate it in business modules.
5. How to diagnose common failures.

## 1. Module Positioning

`fe-extension-loader` is the reusable plugin runtime foundation for Doris FE.

It unifies repeated loading logic across modules, including:

1. Scanning `pluginRoots`.
2. Resolving jars under plugin directories.
3. Building child-first classloaders.
4. Discovering typed factories via `ServiceLoader`.
5. Aggregating load successes and failures.

## 2. Applicable Scenarios

### 2.1 Good Fit For Loader

Use Loader if your module needs:

1. External plugin loading from directories.
2. Shared loading framework across multiple business modules.
3. Standardized failure semantics (`scan`, `resolve`, `discover`, etc.).
4. Less duplicated runtime loading code.

### 2.2 Not A Good Fit For Loader

Loader may not be a direct fit if:

1. Plugin source is not directory-based and you already have a custom loading pipeline.
2. You only need SPI contracts and no runtime loading.

In those cases, use `fe-extension-spi` only.

## 3. Core Class Guide

### 3.1 `DirectoryPluginRuntimeManager<F>`

This is the runtime entry class and unified facade.

Primary methods:

1. `loadAll(...)`
2. `get(pluginName)`
3. `list()`

Business modules should depend on this class directly.

### 3.2 Core Flow Inside `DirectoryPluginRuntimeManager`

The manager performs:

1. Scan plugin subdirectories under each root.
2. Collect jars from `pluginDir/*.jar` and `pluginDir/lib/*.jar`.
3. Create classloader.
4. Discover and validate factory (exactly one per directory).
5. Handle duplicate names and record failures.
6. Return `LoadReport`.

### 3.3 `PluginLoader`

Low-level utility class. It does not scan directories. It only handles:

1. Classloader creation.
2. Typed factory discovery.

Use it when you already own classloader lifecycle externally.

### 3.4 `ChildFirstClassLoader`

Classloading behavior:

1. Child-first by default.
2. Parent-first for allowlisted package prefixes.

Purpose:

1. Reduce dependency conflict with FE process classpath.
2. Keep SPI interface class source consistent and avoid type-isolation `ClassCastException`.

### 3.5 `ClassLoadingPolicy`

Used to configure parent-first prefixes:

1. Mandatory prefixes (always included by default).
2. Business prefixes (append per module).

Common business examples:

1. `org.apache.doris.authentication.`
2. `org.apache.doris.authorization.`

### 3.6 `PluginHandle<F>`

Represents one successfully loaded plugin, including:

1. `pluginName`
2. `pluginDir`
3. `resolvedJars`
4. `classLoader`
5. `factory`
6. `loadedAt`

Business modules typically consume `pluginName + factory` for registration.

### 3.7 `LoadFailure`

Represents one failed plugin directory load, including:

1. `pluginDir`
2. `stage`
3. `message`
4. `cause`

Failure stages:

1. `scan`
2. `resolve`
3. `createClassLoader`
4. `discover`
5. `instantiate`
6. `conflict`

### 3.8 `LoadReport<F>`

Represents the full result of one `loadAll` call, including:

1. Success list: `successes`
2. Failure list: `failures`
3. Statistics: `rootsScanned`, `dirsScanned`

### 3.9 Runtime Handle Storage (Built-In)

`DirectoryPluginRuntimeManager` stores loaded handles in an internal concurrent map.

No separate `PluginRuntimeRegistry` abstraction is exposed in current implementation.

## 4. Standard Integration Steps

### 4.1 Prepare Typed Factory

Your business factory interface should extend `PluginFactory`.

### 4.2 Initialize Runtime Manager

```java
DirectoryPluginRuntimeManager<MyPluginFactory> runtime =
        new DirectoryPluginRuntimeManager<>();
```

### 4.3 Configure Classloading Policy

```java
ClassLoadingPolicy policy = new ClassLoadingPolicy(
        Collections.singletonList("org.apache.doris.mybiz."));
```

### 4.4 Call `loadAll`

```java
LoadReport<MyPluginFactory> report = runtime.loadAll(
        pluginRoots,
        Thread.currentThread().getContextClassLoader(),
        MyPluginFactory.class,
        policy);
```

### 4.5 Process Load Result

```java
for (LoadFailure failure : report.getFailures()) {
    LOG.warn("plugin load failure: dir={}, stage={}, message={}",
            failure.getPluginDir(), failure.getStage(), failure.getMessage(), failure.getCause());
}

for (PluginHandle<MyPluginFactory> handle : report.getSuccesses()) {
    factoryMap.putIfAbsent(handle.getPluginName(), handle.getFactory());
}
```

## 5. Directory Convention

Recommended layout:

```text
<pluginRoot>/
  <pluginA>/
    pluginA.jar
    lib/
      dep1.jar
      dep2.jar
  <pluginB>/
    pluginB.jar
```

Rules:

1. Only direct subdirectories under `pluginRoot` are scanned.
2. Each plugin directory must contain at least one jar.
3. Each plugin directory must discover exactly one factory.

## 6. Conflict And Failure Handling

### 6.1 Duplicate Plugin Name

Current default strategy:

1. Keep the first successfully loaded plugin.
2. Record later duplicates as `conflict`.
3. Continue loading other directories.

Business recommendations:

1. Treat `conflict` as warning/alert.
2. Avoid duplicate plugin names in production plugin roots.

### 6.2 Should All Failures Throw

Loader returns `LoadReport` and does not force exception.

Business modules choose policy by semantics:

1. Tolerant mode: log failures and continue startup.
2. Strict mode: fail-fast when no successful plugin exists.

### 6.3 Detailed `LoadReport` Handling Strategy

`LoadReport` should be startup decision input, not just logs.

Recommended goals:

1. All successful plugins are registered.
2. Every failure is traceable by stage and directory.
3. Startup behavior is deterministic (strict or tolerant).
4. Conflict/failure paths do not leak resources.

Recommended processing order:

1. Log summary metrics: `rootsScanned`, `dirsScanned`, `successes.size`, `failures.size`.
2. Log each failure with `pluginDir + stage + message + cause`.
3. Register successful plugins to business map (`pluginName -> factory`).
4. Apply startup decision logic (strict/tolerant).

Suggested stage severity grouping:

1. Environment/config issues: `scan`, `resolve`.
2. SPI/implementation issues: `discover`, `instantiate`.
3. Runtime construction issues: `createClassLoader`.
4. Naming conflict: `conflict` (usually warning, not immediate stop).

Recommended decision rules:

1. `dirsScanned == 0`: often means empty roots or no external setup.
2. `dirsScanned > 0 && successes.isEmpty()` in strict mode: fail-fast.
3. `dirsScanned > 0 && successes.isEmpty()` in tolerant mode: warn and continue only if business allows no external plugin.
4. If `requiredPluginNames` exists, enforce presence even when partial loads succeeded.

### 6.4 Copy-And-Adapt Template

```java
public static <F extends PluginFactory> void processLoadReport(
        LoadReport<F> report,
        Map<String, F> factoryMap,
        boolean strictMode,
        Set<String> requiredPluginNames) {
    Objects.requireNonNull(report, "report");
    Objects.requireNonNull(factoryMap, "factoryMap");
    Objects.requireNonNull(requiredPluginNames, "requiredPluginNames");

    // Step 1: summary metrics
    LOG.info("plugin load summary: rootsScanned={}, dirsScanned={}, successCount={}, failureCount={}",
            report.getRootsScanned(),
            report.getDirsScanned(),
            report.getSuccesses().size(),
            report.getFailures().size());

    // Step 2: failure details
    LoadFailure firstNonConflictFailure = null;
    for (LoadFailure failure : report.getFailures()) {
        LOG.warn("plugin load failure: dir={}, stage={}, message={}",
                failure.getPluginDir(), failure.getStage(), failure.getMessage(), failure.getCause());
        if (!LoadFailure.STAGE_CONFLICT.equals(failure.getStage()) && firstNonConflictFailure == null) {
            firstNonConflictFailure = failure;
        }
    }

    // Step 3: register successful plugins
    int registered = 0;
    for (PluginHandle<F> handle : report.getSuccesses()) {
        F existing = factoryMap.putIfAbsent(handle.getPluginName(), handle.getFactory());
        if (existing != null) {
            // If business map already contains the name, close discarded external classloader.
            closeClassLoaderQuietly(handle.getClassLoader());
            LOG.warn("skip duplicated plugin name in business map: {}", handle.getPluginName());
            continue;
        }
        registered++;
    }

    // Step 4: startup decision (strict/tolerant)
    if (strictMode && report.getDirsScanned() > 0 && registered == 0 && firstNonConflictFailure != null) {
        throw new IllegalStateException(
                "No plugin loaded in strict mode: stage=" + firstNonConflictFailure.getStage()
                        + ", dir=" + firstNonConflictFailure.getPluginDir()
                        + ", message=" + firstNonConflictFailure.getMessage(),
                firstNonConflictFailure.getCause());
    }

    // Step 5: required plugin checks
    for (String required : requiredPluginNames) {
        if (!factoryMap.containsKey(required)) {
            throw new IllegalStateException("Required plugin is missing: " + required);
        }
    }
}
```

The `closeClassLoaderQuietly` implementation pattern can be referenced from:

`../fe-authentication/fe-authentication-handler/src/main/java/org/apache/doris/authentication/handler/AuthenticationPluginManager.java`

### 6.5 Authentication Module Mapping

Current authentication module handling is:

1. Iterate `report.getFailures()` and log warnings.
2. Iterate `report.getSuccesses()` and register factories (close duplicated external classloader if needed).
3. Throw `AuthenticationException` when directories were scanned but no external plugin was loaded.

Reference implementation:

`../fe-authentication/fe-authentication-handler/src/main/java/org/apache/doris/authentication/handler/AuthenticationPluginManager.java`

## 7. Current Runtime Scope (V1)

Supported:

1. `loadAll`
2. `get`
3. `list`

Not supported:

1. `reload`
2. `unload`

Do not depend on runtime hot-reload semantics in V1.

## 8. Frequently Asked Questions

### 8.1 Factory Not Found

Check:

1. Plugin jar includes `META-INF/services/<factoryType>`.
2. Service file class name is correct.
3. Provider class is included in final jar.

### 8.2 Multiple Factories Found

Check:

1. Multiple jars may declare the same factory type.
2. Parent classpath may pollute service resources.

Note:

`DirectoryPluginRuntimeManager` includes parent service-resource filtering and prefers plugin-directory-local discovery.

### 8.3 Loaded Plugin Not Released

Check:

1. Business layer may keep stale handle references.
2. Conflict/failure paths may skip classloader close.
3. Plugin instance `close()` may not release resources.

## 9. Integration With Authentication Module

Authentication integration sample:

`../fe-authentication/fe-authentication-handler/src/main/java/org/apache/doris/authentication/handler/AuthenticationPluginManager.java`

Key integration points:

1. Use `DirectoryPluginRuntimeManager<AuthenticationPluginFactory>`.
2. Append authentication parent-first prefix.
3. Register successful factories into authentication factory map.
4. Close classloader for discarded conflicting handles.

## Related Docs

1. Chinese developer guide: `README_CN.md`
2. Unified runtime design (CN): `../fe-authentication/EXTENSION_LOADER_UNIFIED_DESIGN_CN.md`
3. SPI contracts: `../fe-extension-spi/README.md`
