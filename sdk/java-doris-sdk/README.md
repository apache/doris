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

# 🚀 Doris Java SDK

[![Java Version](https://img.shields.io/badge/Java-%3E%3D%208-blue.svg)](https://www.oracle.com/java/)
[![Thread Safe](https://img.shields.io/badge/Thread%20Safe-✅-brightgreen.svg)](#-thread-safety)

A lightweight Java stream load client for Apache Doris — easy to use, high performance, and production-ready. Maintained by the Apache Doris core contributor team.

## ✨ Features

**Easy to Use**: Clean Builder API that encapsulates HTTP configuration, multi-format support, and intelligent retry logic.

**High Performance**: Built-in efficient concurrency and batch loading best practices — buffer once, retry multiple times, compress once for Gzip.

**Production Ready**: Battle-tested in large-scale, high-pressure production environments with full observability.

## 📦 Installation

> **Note**: This SDK has not yet been published to Maven Central. You need to build and install it locally first.

### Step 1: Build and install locally

```bash
git clone https://github.com/apache/doris.git
cd doris/sdk/java-doris-sdk
mvn install -DskipTests
```

### Step 2: Add the dependency to your `pom.xml`

```xml
<dependency>
    <groupId>org.apache.doris</groupId>
    <artifactId>java-doris-sdk</artifactId>
    <version>1.0.0</version>
</dependency>
```

## 🚀 Quick Start

### CSV Load

```java
DorisConfig config = DorisConfig.builder()
        .endpoints(Arrays.asList("http://127.0.0.1:8030"))
        .user("root")
        .password("password")
        .database("test_db")
        .table("users")
        .format(DorisConfig.defaultCsvFormat())
        .retry(DorisConfig.defaultRetry())
        .groupCommit(GroupCommitMode.ASYNC)
        .build();

try (DorisLoadClient client = DorisClient.newClient(config)) {
    String data = "1,Alice,25\n2,Bob,30\n3,Charlie,35";
    LoadResponse response = client.load(DorisClient.stringStream(data));

    if (response.getStatus() == LoadResponse.Status.SUCCESS) {
        System.out.println("Loaded rows: " + response.getRespContent().getNumberLoadedRows());
    }
}
```

### JSON Load

```java
DorisConfig config = DorisConfig.builder()
        .endpoints(Arrays.asList("http://127.0.0.1:8030"))
        .user("root")
        .password("password")
        .database("test_db")
        .table("users")
        .format(DorisConfig.defaultJsonFormat()) // JSON Lines format
        .retry(DorisConfig.defaultRetry())
        .groupCommit(GroupCommitMode.ASYNC)
        .build();

try (DorisLoadClient client = DorisClient.newClient(config)) {
    String jsonData = "{\"id\":1,\"name\":\"Alice\",\"age\":25}\n"
                    + "{\"id\":2,\"name\":\"Bob\",\"age\":30}\n"
                    + "{\"id\":3,\"name\":\"Charlie\",\"age\":35}\n";

    LoadResponse response = client.load(DorisClient.stringStream(jsonData));
}
```

## 🛠️ Configuration

### Basic Configuration

```java
DorisConfig config = DorisConfig.builder()
        // Required fields
        .endpoints(Arrays.asList(
                "http://fe1:8030",
                "http://fe2:8030"    // Multiple FE nodes supported, randomly load-balanced
        ))
        .user("your_username")
        .password("your_password")
        .database("your_database")
        .table("your_table")

        // Optional fields
        .labelPrefix("my_app")            // label prefix
        .label("custom_label_001")        // custom label
        .format(DorisConfig.defaultCsvFormat())
        .retry(DorisConfig.defaultRetry())
        .groupCommit(GroupCommitMode.ASYNC)
        .options(new HashMap<String, String>() {{
            put("timeout", "3600");
            put("max_filter_ratio", "0.1");
            put("strict_mode", "true");
        }})
        .build();
```

### Data Format

```java
// 1. Use default formats (recommended)
DorisConfig.defaultJsonFormat()   // JSON Lines, read_json_by_line=true
DorisConfig.defaultCsvFormat()    // CSV, comma-separated, \n line delimiter

// 2. Custom JSON format
new JsonFormat(JsonFormat.Type.OBJECT_LINE)  // JSON Lines
new JsonFormat(JsonFormat.Type.ARRAY)        // JSON Array

// 3. Custom CSV format
new CsvFormat("|", "\\n")   // pipe-separated
```

### Retry Configuration

```java
// 1. Default retry (recommended)
DorisConfig.defaultRetry()   // 6 retries, total time limit 60s
// Backoff sequence: 1s, 2s, 4s, 8s, 16s, 32s

// 2. Custom retry
RetryConfig.builder()
        .maxRetryTimes(3)       // max 3 retries
        .baseIntervalMs(2000)   // base interval 2 seconds
        .maxTotalTimeMs(30000)  // total time limit 30 seconds
        .build()

// 3. Disable retry
.retry(null)
```

### Group Commit Mode

```java
GroupCommitMode.ASYNC  // async mode, highest throughput
GroupCommitMode.SYNC   // sync mode, immediately visible after return
GroupCommitMode.OFF    // disabled, use traditional stream load
```

> ⚠️ **Note**: When Group Commit is enabled, all label configuration is automatically ignored and a warning is logged.

### Gzip Compression

```java
DorisConfig config = DorisConfig.builder()
        // ... other config
        .enableGzip(true)  // SDK compresses the request body automatically
        .build();
```

> The SDK transparently compresses the request body before sending and sets the `compress_type=gz` header automatically. Compression runs only once and is reused across retries.

## 🔄 Concurrent Usage

### Basic Concurrent Example

```java
DorisLoadClient client = DorisClient.newClient(config);  // thread-safe, share across threads

ExecutorService executor = Executors.newFixedThreadPool(10);
for (int i = 0; i < 10; i++) {
    final int workerId = i;
    executor.submit(() -> {
        // Each thread uses its own independent data
        String data = generateWorkerData(workerId);
        LoadResponse response = client.load(DorisClient.stringStream(data));

        if (response.getStatus() == LoadResponse.Status.SUCCESS) {
            System.out.printf("Worker %d loaded %d rows%n",
                    workerId, response.getRespContent().getNumberLoadedRows());
        }
    });
}
```

### ⚠️ Thread Safety

```java
// ✅ Correct: DorisLoadClient is thread-safe, share one instance across threads
DorisLoadClient client = DorisClient.newClient(config);
for (int i = 0; i < 10; i++) {
    executor.submit(() -> {
        String data = generateData();          // each thread has its own data
        client.load(DorisClient.stringStream(data));
    });
}

// ❌ Wrong: never share the same InputStream across threads
InputStream sharedStream = new FileInputStream("data.csv");
for (int i = 0; i < 10; i++) {
    executor.submit(() -> client.load(sharedStream));  // concurrent reads on shared stream cause data corruption
}
```

## 📊 Response Handling

```java
LoadResponse response = client.load(data);

if (response.getStatus() == LoadResponse.Status.SUCCESS) {
    RespContent resp = response.getRespContent();
    System.out.println("Load succeeded!");
    System.out.println("Loaded rows: " + resp.getNumberLoadedRows());
    System.out.println("Load bytes: " + resp.getLoadBytes());
    System.out.println("Load time (ms): " + resp.getLoadTimeMs());
    System.out.println("Label: " + resp.getLabel());
} else {
    System.out.println("Load failed: " + response.getErrorMessage());

    // Get detailed error info
    if (response.getRespContent().getErrorUrl() != null) {
        System.out.println("Error detail: " + response.getRespContent().getErrorUrl());
    }
}
```

## 🛠️ Utilities

### Stream Conversion Helpers

```java
// String to InputStream
InputStream stream = DorisClient.stringStream("1,Alice,25\n2,Bob,30");

// byte array to InputStream
byte[] data = ...;
InputStream stream = DorisClient.bytesStream(data);

// Serialize objects to JSON InputStream (uses Jackson)
List<User> users = Arrays.asList(new User(1, "Alice"), new User(2, "Bob"));
InputStream stream = DorisClient.jsonStream(users);
```

### Default Config Builders

```java
DorisConfig.defaultRetry()      // 6 retries, 60s total time
DorisConfig.defaultJsonFormat() // JSON Lines format
DorisConfig.defaultCsvFormat()  // standard CSV format

RetryConfig.builder()
        .maxRetryTimes(3)
        .baseIntervalMs(1000)
        .maxTotalTimeMs(30000)
        .build()
```

## 📈 Production Examples

Full production-grade examples are available under `src/main/java/org/apache/doris/sdk/examples/`:

```bash
# Build
mvn package -DskipTests

# Run all examples
java -cp target/java-doris-sdk-1.0.0.jar org.apache.doris.sdk.examples.ExamplesMain all

# Run individual examples
java -cp target/java-doris-sdk-1.0.0.jar org.apache.doris.sdk.examples.ExamplesMain simple      # basic JSON load
java -cp target/java-doris-sdk-1.0.0.jar org.apache.doris.sdk.examples.ExamplesMain single      # large batch load (100k records)
java -cp target/java-doris-sdk-1.0.0.jar org.apache.doris.sdk.examples.ExamplesMain json        # production JSON load (50k records)
java -cp target/java-doris-sdk-1.0.0.jar org.apache.doris.sdk.examples.ExamplesMain concurrent  # concurrent load (1M records, 10 threads)
java -cp target/java-doris-sdk-1.0.0.jar org.apache.doris.sdk.examples.ExamplesMain gzip        # gzip compressed load
```

## 🤝 Contributing

Pull requests are welcome!

## 🙏 Acknowledgements

Maintained by the Apache Doris core contributor team.
