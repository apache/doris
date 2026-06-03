<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Doris FE Filesystem Module

The `fe-filesystem` module provides a **pluggable filesystem abstraction** for the Doris FE
(Frontend). It decouples the query engine (`fe-core`) from concrete storage backends (S3, HDFS,
Azure Blob, etc.) at both the Maven dependency level and the runtime classpath level.

Each storage backend is a self-contained plugin that is discovered and loaded at FE startup—no
modification to `fe-core` is required to add a new storage backend.

## Module Structure

```
fe-filesystem/                        (aggregator POM — no Java code)
│
├── fe-filesystem-api/                [API]  Core abstractions (FileSystem, Location, …)
├── fe-filesystem-spi/                [SPI]  Provider interface + object-storage contracts
│
├── fe-filesystem-s3/                 [IMPL] AWS S3 / S3-compatible (MinIO, …)
├── fe-filesystem-oss/                [IMPL] Alibaba Cloud OSS  (delegates to S3)
├── fe-filesystem-cos/                [IMPL] Tencent Cloud COS  (delegates to S3)
├── fe-filesystem-obs/                [IMPL] Huawei Cloud OBS   (delegates to S3)
├── fe-filesystem-azure/              [IMPL] Azure Blob Storage
├── fe-filesystem-hdfs/               [IMPL] HDFS / ViewFS / OFS / JFS
├── fe-filesystem-local/              [IMPL] Local filesystem   (testing only)
└── fe-filesystem-broker/             [IMPL] Doris Broker process (Thrift RPC)
```

### Layering

| Layer | Module | Compiled into fe-core? | Deployed as plugin? |
|-------|--------|:-----:|:------:|
| **API** | `fe-filesystem-api` | ✅ Yes | ❌ No |
| **SPI** | `fe-filesystem-spi` | ✅ Yes | ❌ No |
| **IMPL** | `fe-filesystem-s3`, `-hdfs`, … | ❌ No | ✅ Yes |

* **API** — Pure-JDK interfaces and value types consumed by `fe-core` (zero third-party
  dependencies). Defines `FileSystem`, `Location`, `FileEntry`, `DorisInputFile`,
  `DorisOutputFile`, etc.
* **SPI** — The `FileSystemProvider` interface (extends `PluginFactory` from `fe-extension-spi`)
  plus the object-storage layer (`ObjStorage`, `ObjFileSystem`, `HadoopAuthenticator`). Also
  compiled into `fe-core`.
* **IMPL** — Concrete backends. Each one depends on `fe-filesystem-spi` (and transitively on
  `fe-filesystem-api`). S3-delegating backends (OSS, COS, OBS) also depend on `fe-filesystem-s3`
  to reuse `S3FileSystem`. They **must not** depend on `fe-core`, `fe-common`, or `fe-catalog`.

## How It Works

### Plugin Discovery & Loading

At FE startup, `Env.initFileSystemPluginManager()` creates a `FileSystemPluginManager` and loads
providers in two phases:

1. **ServiceLoader scan** — Discovers `FileSystemProvider` implementations already on the
   classpath (built-in providers, test overrides).
2. **Directory plugin scan** — Uses `DirectoryPluginRuntimeManager` to scan the directory
   configured by `Config.filesystem_plugin_root` (default: `${DORIS_HOME}/plugins/filesystem`).
   Each direct child directory is treated as an **unpacked** plugin directory. The runtime
   manager resolves `pluginDir/*.jar` (root-level jars, scanned for ServiceLoader registration)
   and `pluginDir/lib/*.jar` (dependency jars, available for class loading only).

Classpath providers have higher priority than directory-loaded providers.

### Provider Selection

When `fe-core` needs a filesystem, it calls `FileSystemFactory.getFileSystem(properties)`:

```
Map<String, String> properties
        │
        ▼
FileSystemPluginManager.createFileSystem(properties)
        │
        │  for each registered FileSystemProvider:
        │    1. provider.supports(properties)  ← cheap, no I/O
        │    2. if true → provider.create(properties) → return FileSystem
        │
        ▼
First matching provider wins
```

Each provider's `supports()` method examines property keys to decide if it can handle the
request. For example:

| Provider | Matching Logic |
|----------|---------------|
| S3 | `AWS_ACCESS_KEY` + (`AWS_ENDPOINT` or `AWS_REGION`) |
| OSS | Endpoint contains `aliyuncs.com` or `_STORAGE_TYPE_` = `"OSS"` |
| HDFS | `_STORAGE_TYPE_` = `"HDFS"` or URI scheme is `hdfs`/`viewfs`/`ofs`/`jfs`/`oss` |
| Azure | `AZURE_ACCOUNT_NAME` or endpoint contains `blob.core.windows.net` |
| Local | URI starts with `file://` or `local://` |
| Broker | `_STORAGE_TYPE_` = `"BROKER"` and `BROKER_HOST` present |

### Plugin Packaging & Class Loading

Each implementation module uses `maven-assembly-plugin` to produce a **zip build artifact**.
The zip must be **unpacked** before deployment. At runtime, `DirectoryPluginRuntimeManager`
expects each plugin to be an unpacked directory under `filesystem_plugin_root`:

```
${DORIS_HOME}/plugins/filesystem/
├── s3/                                   ← one directory per plugin
│   ├── doris-fe-filesystem-s3.jar        ← plugin jar at root (scanned for ServiceLoader)
│   └── lib/
│       ├── aws-sdk-*.jar                 ← third-party dependencies
│       └── ...
├── hdfs/
│   ├── doris-fe-filesystem-hdfs.jar
│   └── lib/...
└── ...
```

The Maven build produces a zip with the same layout. To deploy, unzip it into the appropriate
subdirectory (e.g., `unzip doris-fe-filesystem-s3.zip -d plugins/filesystem/s3/`). Dropping
the raw `.zip` file into the directory will **not** work — `DirectoryPluginRuntimeManager`
resolves `pluginDir/*.jar` and `pluginDir/lib/*.jar` from unpacked directories only.

Jars that are already on the fe-core classpath (`fe-filesystem-api`, `fe-filesystem-spi`,
`fe-extension-spi`) are **excluded** from the zip to avoid duplication.

At runtime, `DirectoryPluginRuntimeManager` creates an isolated `ClassLoader` per plugin. A
`ClassLoadingPolicy` ensures that shared framework classes (`org.apache.doris.filesystem.*`,
`software.amazon.awssdk.*`, `org.apache.hadoop.*`) are loaded parent-first to avoid cross-
ClassLoader cast failures.

### S3 Delegation Pattern

Several cloud providers (OSS, COS, OBS) are S3-compatible. Instead of duplicating the S3
implementation, they follow a **delegation pattern**:

1. Translate cloud-native property keys to S3-compatible keys.
2. Extend `ObjStorage` to override cloud-specific operations (pre-signed URLs, STS tokens).
3. Delegate core I/O to `S3FileSystem` (from `fe-filesystem-s3`).

```
CosFileSystemProvider
  └─→ creates S3FileSystem(CosObjStorage)
        │                     │
        │  FileSystem ops     │  Overrides getPresignedUrl(), getStsToken()
        └─────────────────────┘
```

## Relationship to Other Modules

```
┌──────────────────────────────────────────────────────────┐
│                        fe-core                           │
│  Uses: FileSystem, Location, FileEntry (from API)        │
│  Depends on: fe-filesystem-api, fe-filesystem-spi        │
│  Loads plugins from: ${DORIS_HOME}/plugins/filesystem    │
└────────────┬────────────────────────┬────────────────────┘
             │ compile-time           │ runtime (plugin)
             ▼                        ▼
  ┌──────────────────┐    ┌─────────────────────────┐
  │ fe-filesystem-api│    │ fe-filesystem-s3 (zip)  │
  │ fe-filesystem-spi│    │ fe-filesystem-hdfs (zip)│
  │ (compiled in)    │    │ fe-filesystem-xxx (zip) │
  └──────────────────┘    └─────────────────────────┘
```

* **`fe-core`** — Compile-time dependency on `fe-filesystem-api` and `fe-filesystem-spi`.
  Implementation modules are **not** Maven dependencies of `fe-core`; they are loaded at runtime
  from the plugin directory.
* **`fe-extension-spi`** — Provides the `PluginFactory` / `Plugin` interfaces and the
  `DirectoryPluginRuntimeManager` class used by the plugin loading infrastructure.
* **`fe-filesystem-local`** — Used in `fe-core` unit tests (test-scope dependency) so that tests
  can exercise `FileSystem` operations without cloud credentials.

## Adding a New Filesystem Sub-Module

Follow these steps to add support for a new storage backend (e.g., Google Cloud Storage):

### 1. Create the Maven module

Create a new directory `fe-filesystem/fe-filesystem-gcs/` with this structure:

```
fe-filesystem-gcs/
├── pom.xml
└── src/
    ├── main/
    │   ├── assembly/
    │   │   └── plugin-zip.xml
    │   ├── java/org/apache/doris/filesystem/gcs/
    │   │   ├── GcsFileSystemProvider.java
    │   │   ├── GcsFileSystem.java
    │   │   └── GcsObjStorage.java          (if object-storage based)
    │   └── resources/META-INF/services/
    │       └── org.apache.doris.filesystem.spi.FileSystemProvider
    └── test/
        └── java/org/apache/doris/filesystem/gcs/
            └── ...Test.java
```

### 2. Write `pom.xml`

```xml
<project>
    <parent>
        <groupId>org.apache.doris</groupId>
        <artifactId>fe-filesystem</artifactId>
        <version>${revision}</version>
    </parent>

    <artifactId>fe-filesystem-gcs</artifactId>
    <packaging>jar</packaging>
    <name>Doris FE Filesystem - GCS</name>

    <dependencies>
        <!-- REQUIRED: SPI contract (transitively includes API) -->
        <dependency>
            <groupId>org.apache.doris</groupId>
            <artifactId>fe-filesystem-spi</artifactId>
            <version>${revision}</version>
        </dependency>

        <!-- Cloud SDK -->
        <dependency>
            <groupId>com.google.cloud</groupId>
            <artifactId>google-cloud-storage</artifactId>
            <version>...</version>
        </dependency>

        <!-- Test -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <finalName>doris-fe-filesystem-gcs</finalName>
        <plugins>
            <!-- Assembly: produces the plugin zip -->
            <plugin>
                <artifactId>maven-assembly-plugin</artifactId>
                <configuration>
                    <appendAssemblyId>false</appendAssemblyId>
                    <descriptors>
                        <descriptor>src/main/assembly/plugin-zip.xml</descriptor>
                    </descriptors>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals><goal>single</goal></goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

**Important**: Do **not** add dependencies on `fe-core`, `fe-common`, or `fe-catalog`.

### 3. Write the assembly descriptor

Copy `plugin-zip.xml` from an existing module (e.g., `fe-filesystem-s3/src/main/assembly/`).
The key points:

* Place the plugin jar at the **root** of the zip (for ServiceLoader discovery).
* Place all runtime dependencies in `lib/`.
* **Exclude** `fe-filesystem-api`, `fe-filesystem-spi`, and `fe-extension-spi` (they are
  already on the fe-core classpath).

### 4. Implement `FileSystemProvider`

```java
package org.apache.doris.filesystem.gcs;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import java.io.IOException;
import java.util.Map;

public class GcsFileSystemProvider implements FileSystemProvider {

    // Public no-arg constructor — required by ServiceLoader
    public GcsFileSystemProvider() {}

    @Override
    public boolean supports(Map<String, String> properties) {
        // Must be cheap (no network calls) and deterministic.
        String type = properties.get("_STORAGE_TYPE_");
        return "GCS".equalsIgnoreCase(type);
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return new GcsFileSystem(properties);
    }

    @Override
    public String name() {
        return "GCS";
    }
}
```

### 5. Implement `FileSystem`

You have two choices:

* **Object-storage backend** — Extend `ObjFileSystem` (from SPI) and implement `ObjStorage<C>`.
  This gives you `exists()`, `close()`, and cloud-specific delegates for free.
* **Custom backend** — Implement `FileSystem` directly (like `DFSFileSystem` for HDFS).

At minimum, you must implement:

| Method | Description |
|--------|-------------|
| `exists(Location)` | Check if a file/directory exists |
| `mkdirs(Location)` | Create directories |
| `delete(Location, boolean)` | Delete file or directory |
| `rename(Location, Location)` | Rename/move |
| `list(Location)` | List directory contents |
| `newInputFile(Location)` | Open a file for reading |
| `newOutputFile(Location)` | Open a file for writing |
| `close()` | Release resources |

### 6. Register via ServiceLoader

Create the file:

```
src/main/resources/META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider
```

With content:

```
org.apache.doris.filesystem.gcs.GcsFileSystemProvider
```

### 7. Register the module in the parent POM

Add your module to `fe-filesystem/pom.xml`:

```xml
<modules>
    ...
    <module>fe-filesystem-gcs</module>
</modules>
```

### 8. Add tests

* **Unit tests** — Test your `FileSystem` and `ObjStorage` implementations with mocked cloud
  clients. Place in `src/test/java/`.
* **Environment tests** — Tests that require real cloud credentials should be tagged with
  `@Tag("environment")`. They are excluded by default and can be enabled with
  `-Dtest.excludedGroups=none`.

### 9. Build and deploy

```bash
# Build the new module (must go through the reactor so sibling SNAPSHOTs resolve)
cd fe
mvn package -pl fe-filesystem/fe-filesystem-gcs --also-make -DskipTests

# The build produces a zip at:
#   fe-filesystem/fe-filesystem-gcs/target/doris-fe-filesystem-gcs.zip
# Deploy by unpacking into the plugin directory:
mkdir -p ${DORIS_HOME}/plugins/filesystem/gcs
unzip fe-filesystem/fe-filesystem-gcs/target/doris-fe-filesystem-gcs.zip \
  -d ${DORIS_HOME}/plugins/filesystem/gcs/

# The unpacked layout should be:
#   plugins/filesystem/gcs/
#   ├── doris-fe-filesystem-gcs.jar
#   └── lib/
#       └── *.jar
#
# NOTE: Do NOT drop the .zip file directly — it must be unpacked.
```

### Checklist

- [ ] Module depends only on `fe-filesystem-spi` (not `fe-core`/`fe-common`/`fe-catalog`)
- [ ] `FileSystemProvider` has a public no-arg constructor
- [ ] `supports()` is cheap — no network calls
- [ ] `META-INF/services` file is present and correct
- [ ] Assembly descriptor excludes `fe-filesystem-api`, `fe-filesystem-spi`, `fe-extension-spi`
- [ ] Module is listed in `fe-filesystem/pom.xml` `<modules>`
- [ ] Unit tests pass; environment tests are tagged `@Tag("environment")`
