# fe-foundation Module Design Document

## 1. Background & Motivation

The existing `fe-common` module has accumulated heavy dependencies over time (Guava, Hadoop, Trino, 
ANTLR, Alibaba MaxCompute SDK, etc.), making it unsuitable as a lightweight shared library for the 
plugin/SPI ecosystem. When SPI plugin authors depend on `fe-common`, they are forced to pull in 
dozens of transitive dependencies that have nothing to do with their plugin logic.

We need a **zero-dependency foundation module** that sits below `fe-common` and `fe-extension-spi` 
in the dependency hierarchy, providing only the most essential, general-purpose utilities that any 
module — including SPI plugins — can safely depend on.

## 2. Naming Decision

| Candidate        | Verdict                                                                 |
|------------------|-------------------------------------------------------------------------|
| `fe-foundation`  | **Chosen.** Clear "below-common" semantics. No ambiguity with existing modules. |
| `fe-base`        | Too generic; widely used in other contexts.                             |
| `fe-essentials`  | Good semantics but verbose.                                             |
| `fe-primitives`  | Implies primitive types; misleading.                                    |
| `fe-toolkit`     | Industry convention maps "toolkit" to heavier helper libs (e.g., Trino).|
| `fe-kernel`      | Conflicts with database "kernel" terminology.                           |

**Industry references:**
- Trino: `trino-spi` (zero-dep contract) + `lib/trino-plugin-toolkit` (optional helpers)
- Flink: `flink-annotations` (lightest) → `flink-core-api` → `flink-core`
- Iceberg: `common/` (pure utilities) → `api/` (interfaces) → `core/` (implementation)
- SeaTunnel: `seatunnel-common` (utilities) + `seatunnel-api` (SPI)

## 3. Module Positioning

```
fe-foundation          ← Zero third-party dependencies. Pure JDK utilities.
    │
    ├── fe-extension-spi       ← Plugin contracts (Plugin, PluginFactory, PluginContext)
    │       │
    │       └── fe-extension-loader    ← Plugin classloading & discovery
    │
    ├── fe-common              ← Heavier shared code (Gson, Guava, Hadoop, etc.)
    │
    └── fe-core                ← Main FE module (optimizer, catalog, transaction)
```

**Key principle:** `fe-foundation` has **ZERO** compile-scope third-party dependencies. Only JDK.

## 4. Admission Criteria

A class qualifies for `fe-foundation` if it meets **ALL** of:

1. **Zero third-party runtime dependencies** — only `java.*` imports
2. **No coupling to Doris business logic** — no references to catalog, optimizer, planner, etc.
3. **General-purpose** — useful across multiple modules (SPI plugins, fe-common, fe-core, etc.)
4. **Stable API** — unlikely to change frequently

## 5. Package Structure

```
org.apache.doris.foundation/
├── property/
│   ├── ConnectorProperty.java           # @annotation: marks config fields
│   ├── ConnectorPropertiesUtils.java    # Reflection-based KV → Bean binder
│   ├── StoragePropertiesException.java  # Property-related RuntimeException
│   └── ParamRules.java                  # Fluent parameter validation DSL
├── type/
│   └── ResultOr.java                    # Rust-style Result<T, E> type
├── format/
│   └── FormatOptions.java               # Immutable data formatting config
└── util/
    ├── BitUtil.java                     # Bit manipulation helpers
    ├── ByteBufferUtil.java              # Unsigned ByteBuffer reads
    ├── SerializationUtils.java          # Deep clone via Java serialization
    └── PathUtils.java                   # URI path comparison utilities
```

## 6. Classes Included (First Iteration)

### 6.1 Property Framework (from fe-core)

| Class | Original Location | External Deps | Description |
|-------|-------------------|---------------|-------------|
| `ConnectorProperty` | `o.a.d.datasource.property` | None | Runtime annotation for connector config fields |
| `ConnectorPropertiesUtils` | `o.a.d.datasource.property` | None (Guava/commons-lang3 removed) | Reflection-based KV→Bean binding |
| `ParamRules` | `o.a.d.datasource.property` | None | Fluent validation DSL with chained rules |
| `StoragePropertiesException` | `o.a.d.datasource.property.storage.exception` | None | RuntimeException for property errors |

### 6.2 Type Utilities (from fe-common)

| Class | Original Location | External Deps | Description |
|-------|-------------------|---------------|-------------|
| `ResultOr<T,E>` | `o.a.d.common` | None | Success-or-error result type |

### 6.3 Format Utilities (from fe-common)

| Class | Original Location | External Deps | Description |
|-------|-------------------|---------------|-------------|
| `FormatOptions` | `o.a.d.common` | None | Immutable formatting configuration |

### 6.4 General Utilities (from fe-core)

| Class | Original Location | External Deps | Description |
|-------|-------------------|---------------|-------------|
| `BitUtil` | `o.a.d.common.util` | None | log2, power-of-2 rounding |
| `ByteBufferUtil` | `o.a.d.common.util` | None | Unsigned byte buffer reads |
| `SerializationUtils` | `o.a.d.common.util` | None | Deep clone via serialization |
| `PathUtils` | `o.a.d.common.util` | None | URI path comparison with S3 handling |

## 7. Classes NOT Included (and Why)

| Class | Reason |
|-------|--------|
| `Pair`, `Triple` | Depend on `@SerializedName` (Gson) for persistence. Moving would require adding Gson or breaking persistence compatibility. |
| `Writable`, `Codec`, `CountingDataOutputStream`, `DataInputBuffer`, `DataOutputBuffer`, etc. | Deeply embedded in the persistence layer (194+ importers for Writable). High-risk migration better done in a separate phase. |
| `CloudCredential` | Depends on `commons-lang3`. Could be migrated after trivial refactoring. |
| `GZIPUtils`, `EnvUtils` | Depend on `commons-io` / Guava. Could be migrated after trivial refactoring. |

## 8. Serialization Safety Analysis

All 10 classes are **SAFE** to move (package rename):

- **None** implement `java.io.Serializable` (except `StoragePropertiesException` via `Throwable`, 
  but it is never serialized to disk)
- **None** are registered in `RuntimeTypeAdapterFactory` (no class name stored in JSON)
- **None** have `serialVersionUID`
- All are either annotations, static utility classes, or transient runtime objects
- No class name is stored in any editlog, metadata image, or checkpoint

## 9. Migration Strategy

### Phase 1 (This PR): Backward-Compatible Migration

1. Create `fe-foundation` module with zero dependencies
2. Copy classes to new `org.apache.doris.foundation.*` packages
3. In original locations, replace class bodies with **extends/delegation** to the foundation class:
   ```java
   // fe-core: org.apache.doris.datasource.property.ParamRules
   // Now just re-exports the foundation class
   package org.apache.doris.datasource.property;
   public class ParamRules extends org.apache.doris.foundation.property.ParamRules {}
   ```
4. No existing code needs to change import statements
5. New code should prefer importing from `org.apache.doris.foundation.*`

### Phase 2 (Future): Gradually update imports across the codebase

- Update import statements in fe-core to use foundation packages directly
- Deprecate and eventually remove the re-export shims
- Migrate more classes from fe-common (IO utilities, Pair/Triple after decoupling Gson)

## 10. Build Configuration

```xml
<!-- fe-foundation/pom.xml -->
<artifactId>fe-foundation</artifactId>
<packaging>jar</packaging>
<name>Doris FE Foundation</name>
<description>Zero-dependency foundation utilities for Doris FE modules and SPI plugins</description>

<dependencies>
    <!-- Intentionally empty. This module has ZERO third-party dependencies. -->
</dependencies>
```

## 11. Dependency Graph After Migration

```
fe-foundation (0 deps)
    ↑
    ├── fe-extension-spi (depends on fe-foundation)
    │       ↑
    │       └── fe-extension-loader
    │
    ├── fe-common (depends on fe-foundation + Guava + Gson + Hadoop + ...)
    │       ↑
    │       └── fe-core
    │
    └── fe-core (depends on fe-foundation + fe-common + ...)
```
