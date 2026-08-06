# fe-extension-spi Developer Integration Guide

This guide is for plugin authors and business module integrators. It answers three practical questions:

1. What `fe-extension-spi` is responsible for.
2. When to use SPI only, and when to combine SPI with Loader.
3. How to implement a plugin package that Doris FE can discover and execute.

## 1. Module Positioning

`fe-extension-spi` provides contracts only. It does not provide runtime loading.

Core contracts in this module:

1. `Plugin`: plugin instance lifecycle.
2. `PluginFactory`: plugin factory contract.
3. `PluginContext`: runtime configuration carrier.
4. `PluginException`: common runtime exception type.

Capabilities intentionally excluded from this module:

1. Directory scanning.
2. Jar discovery and resolution.
3. Classloader creation and close.
4. Load failure aggregation and staging.
5. Loaded plugin runtime registry.

Those runtime capabilities belong to `fe-extension-loader`.

## 2. Scenario Selection

### 2.1 When SPI Only Is Enough

Use `fe-extension-spi` directly if:

1. Your module already has a custom loading pipeline.
2. Your module already has a classloader framework.
3. You only need compile-time contracts for plugin implementers.

### 2.2 When To Use SPI + Loader

Use SPI + Loader if:

1. You want directory-driven loading by `pluginRoots`.
2. You want unified child-first classloading behavior.
3. You want standardized load failure stages (`scan`, `resolve`, `discover`, etc.).
4. You want to reduce duplicated loading code in each business module.

## 3. Core Interface Guide

### 3.1 `Plugin`

Responsibilities:

1. `initialize(context)`: initialize plugin instance.
2. `close()`: release plugin resources.

Recommendations:

1. Keep `initialize` minimal and deterministic.
2. Make `close` idempotent.
3. Do not open external connections in static initializers.

### 3.2 `PluginFactory`

Responsibilities:

1. `name()`: return stable logical plugin name.
2. `create()`: create a plugin instance.
3. `create(context)`: optional context-aware create path (defaults to `create()`).

Recommendations:

1. Keep `name()` stable across environments.
2. Keep `create()` lightweight and side-effect-free.
3. Let business modules decide instance caching policy.

### 3.3 `PluginContext`

Responsibilities:

1. Carry runtime config (`Map<String, String>`).
2. Provide input for plugin initialization.

Recommendations:

1. Use stable key prefixes (for example: `ldap.*`, `auth.*`).
2. Validate required keys and apply defaults in plugin code.

### 3.4 `PluginException`

Responsibilities:

1. Represent common plugin runtime failures.

Recommendations:

1. Include useful context in message: plugin name, stage, and key config item.
2. Keep original cause to preserve root-cause traceability.

## 4. Minimal Integration Steps

### 4.1 Implement Plugin Class

```java
public final class DemoPlugin implements Plugin {
    @Override
    public void initialize(PluginContext context) {
        // init
    }

    @Override
    public void close() {
        // cleanup
    }
}
```

### 4.2 Implement Factory Class

```java
public final class DemoPluginFactory implements PluginFactory {
    @Override
    public String name() {
        return "demo";
    }

    @Override
    public Plugin create() {
        return new DemoPlugin();
    }
}
```

### 4.3 Add ServiceLoader Declaration

File path:

`META-INF/services/org.apache.doris.extension.spi.PluginFactory`

File content:

```text
com.example.demo.DemoPluginFactory
```

### 4.4 Package Artifacts

Your plugin jar should include:

1. Factory implementation class.
2. Plugin implementation class.
3. `META-INF/services/...` service declaration file.

## 5. Constraints for Plugin Authors

1. `name()` must not be empty and should be globally distinguishable.
2. Fail fast during invalid initialization to avoid partial state.
3. Release thread pools and connection pools in `close()`.
4. Avoid global mutable state inside plugin logic.

## 6. Common Errors And Troubleshooting

### 6.1 Factory Not Discovered

Check in this order:

1. Is `META-INF/services/...` file present?
2. Is provider class name fully qualified and correctly spelled?
3. Is the factory class included in final jar?

### 6.2 Plugin Creation Failure

Check in this order:

1. Does factory `create()` depend on uninitialized resources?
2. Are required `PluginContext` entries missing?
3. Are plugin dependency jars complete?

### 6.3 Resource Leak After Runtime

Check in this order:

1. Does `close()` actually release pools and handles?
2. Is business code holding strong references to old plugin instances?
3. In external loading mode, is classloader close strategy applied?

## 7. Relationship With Loader

Recommended composition:

1. Define plugin contracts in `fe-extension-spi`.
2. Perform directory-driven loading in `fe-extension-loader`.
3. Do factory registration, instance caching, and routing in business modules.

Detailed Loader integration guide:

`../fe-extension-loader/README.md`

## Related Docs

1. Chinese developer guide: `README_CN.md`
2. Loader module runtime guide: `../fe-extension-loader/README.md`
3. Unified runtime design (CN): `../fe-authentication/EXTENSION_LOADER_UNIFIED_DESIGN_CN.md`
