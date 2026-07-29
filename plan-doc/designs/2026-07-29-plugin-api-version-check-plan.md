# 四族插件 API 版本检查 — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让四个插件族（CONNECTOR / FILESYSTEM / AUTHENTICATION / LINEAGE）在加载目录插件时，依据插件 jar MANIFEST 中声明的 API 版本拒绝与本 FE 不兼容的插件。

**Architecture:** 版本号来自各族父 pom 的一个 maven property，同时流向 (a) 该族 SPI 模块的 filtered resource（内核期望值）与 (b) 各插件 jar 的 MANIFEST（插件声明值），因此一次构建内两者不可能不一致。`fe-extension-loader` 新增族中立的 `ApiVersion` + `ApiVersionGate`，由 `DirectoryPluginRuntimeManager.loadAll` 在工厂实例化后、`PluginHandle` 发布前调用。四族各自构造自己的 gate 接线。

**Tech Stack:** Java 8 兼容语法、Maven（resource filtering + `maven-jar-plugin` `manifestEntries`）、JUnit 5（`org.junit.jupiter.api`）。

**设计文档：** `plan-doc/designs/2026-07-29-plugin-api-version-check-design.md`（本计划的唯一依据；下称"spec"）

## Global Constraints

- **兼容规则**：`P.major != K.major` → 拒绝；minor / patch 完全忽略。（spec §3.1）
- **版本格式**：接受 `1`、`1.0`、`1.0.3`；缺失段按 0 补。解析失败等同未声明。（spec §3.4）
- **缺失策略**：目录加载缺声明即拒绝（fail-closed）；classpath / ServiceLoader 路径无条件豁免，绝不校验。（spec §5.2）
- **内核期望值读不到**（filtered resource 缺失/损坏）→ 抛异常、启动即失败，不得降级放行。（spec §5.4）
- **四族版本彼此独立**，四个 property、四个 manifest 属性名、四个 filtered resource。（spec §3.3）
- **起始值四族均为 `1.0`。**（spec §3）
- **fe-core 源相关代码只出不进**：本计划不向 fe-core 新增任何属性解析/通用工具；新增类一律落 `fe-extension-loader`。
- **不改** `information_schema.extensions` 的表结构。（spec §2 非目标）
- **明确不做**：spec §8 提到的顺带补 `Implementation-Version`（owner 未拍板，保持默认不做）。
- **Java 8 语法**：不用 `var`、不用 `List.of`、不用 text block。
- **构建命令一律用绝对 `-f` 路径**（cwd 跨调用持久，`cd` 会破相对路径）。全反应堆构建必须带 `-Dcheckstyle.skip=true`（checkstyle 扫 generated-sources 会退化成平方级）。

---

## File Structure

**新建（fe-extension-loader）**
- `fe/fe-extension-loader/src/main/java/org/apache/doris/extension/loader/ApiVersion.java` — 版本值类型与解析，无 I/O，纯函数
- `fe/fe-extension-loader/src/main/java/org/apache/doris/extension/loader/ApiVersionGate.java` — 判定逻辑 + 从 classpath 读内核期望值
- `fe/fe-extension-loader/src/test/java/org/apache/doris/extension/loader/ApiVersionTest.java`
- `fe/fe-extension-loader/src/test/java/org/apache/doris/extension/loader/ApiVersionGateTest.java`
- `fe/fe-extension-loader/src/test/java/org/apache/doris/extension/loader/DirectoryPluginRuntimeManagerApiVersionTest.java`

**修改（fe-extension-loader）**
- `ManifestVersions.java` — `fromManifest` 泛化到任意属性名
- `LoadFailure.java` — 新增 `STAGE_API_VERSION`
- `DirectoryPluginRuntimeManager.java` — `loadAll` 增加 gate 重载；`readImplementationVersion` 泛化；`loadFromPluginDir` 接入 gate

**每族四件套**（族名 `<f>` ∈ connector / filesystem / authentication / lineage）
- 父 pom：`<<f>.plugin.api.version>1.0</...>` + `maven-jar-plugin` `manifestEntries`
- SPI 模块 pom：resource filtering
- SPI 模块 resource：`META-INF/doris/<f>-plugin-api-version.properties`
- 管理器类：构造 gate 并传入 `loadAll`

**删除**
- `ConnectorProvider#apiVersion()`
- `ConnectorPluginManager.CURRENT_API_VERSION` 及三处比较

**基线测试（Task 8）** — 每族一个自包含测试 + 一个 `.txt` 基线，共 4 组。

---

## Task 1: ApiVersion 值类型

**Files:**
- Create: `fe/fe-extension-loader/src/main/java/org/apache/doris/extension/loader/ApiVersion.java`
- Test: `fe/fe-extension-loader/src/test/java/org/apache/doris/extension/loader/ApiVersionTest.java`

**Interfaces:**
- Consumes: 无
- Produces: `public final class ApiVersion`，`public static ApiVersion parse(String)`（非法输入抛 `IllegalArgumentException`）、`public int getMajor()` / `getMinor()` / `getPatch()`、`public String toString()`（patch 为 0 时输出 `"1.0"`，否则 `"1.0.3"`）

- [ ] **Step 1: 写失败测试**

```java
// fe/fe-extension-loader/src/test/java/org/apache/doris/extension/loader/ApiVersionTest.java
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.extension.loader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link ApiVersion} parsing.
 *
 * <p><b>Why the lenient forms matter:</b> a plugin author may legitimately omit patch (the gate
 * ignores it) or even minor, so "1" and "1.0" must both mean major 1. Rejecting them would push
 * authors toward guessing a patch number they have no basis for.
 *
 * <p><b>Why the strict forms matter:</b> anything unparsable is treated by the gate as an absent
 * declaration, i.e. the plugin is refused. Parsing must therefore be decisive rather than lossy —
 * silently reading "1.x" as major 1 would let a malformed declaration pass as a valid one.
 */
public class ApiVersionTest {

    @Test
    void parsesMajorOnly() {
        ApiVersion v = ApiVersion.parse("2");
        Assertions.assertEquals(2, v.getMajor());
        Assertions.assertEquals(0, v.getMinor());
        Assertions.assertEquals(0, v.getPatch());
    }

    @Test
    void parsesMajorMinor() {
        ApiVersion v = ApiVersion.parse("1.4");
        Assertions.assertEquals(1, v.getMajor());
        Assertions.assertEquals(4, v.getMinor());
        Assertions.assertEquals(0, v.getPatch());
    }

    @Test
    void parsesMajorMinorPatch() {
        ApiVersion v = ApiVersion.parse("1.4.7");
        Assertions.assertEquals(1, v.getMajor());
        Assertions.assertEquals(4, v.getMinor());
        Assertions.assertEquals(7, v.getPatch());
    }

    @Test
    void trimsSurroundingWhitespace() {
        // Manifest values routinely arrive padded; treating " 1.0 " as malformed would reject a
        // perfectly well-formed plugin over whitespace.
        Assertions.assertEquals(1, ApiVersion.parse("  1.0  ").getMajor());
    }

    @Test
    void toStringOmitsZeroPatch() {
        // The string form appears in rejection messages; "1.0" is what the author wrote in the pom,
        // so echoing "1.0.0" back at them would not match anything they can search for.
        Assertions.assertEquals("1.0", ApiVersion.parse("1.0").toString());
        Assertions.assertEquals("1.0", ApiVersion.parse("1").toString());
        Assertions.assertEquals("1.0.3", ApiVersion.parse("1.0.3").toString());
    }

    @Test
    void rejectsBlankAndNull() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ApiVersion.parse(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ApiVersion.parse(""));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ApiVersion.parse("   "));
    }

    @Test
    void rejectsNonNumericSegment() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ApiVersion.parse("1.x"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ApiVersion.parse("1.0-SNAPSHOT"));
    }

    @Test
    void rejectsEmptySegment() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ApiVersion.parse("1."));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ApiVersion.parse("1..2"));
    }

    @Test
    void rejectsMoreThanThreeSegments() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ApiVersion.parse("1.0.0.1"));
    }

    @Test
    void rejectsNegativeAndOversizedSegment() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ApiVersion.parse("-1.0"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> ApiVersion.parse("99999999999.0"));
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-extension-loader -am \
    test -Dtest=ApiVersionTest -DfailIfNoTests=false
```

Expected: 编译失败，`cannot find symbol: class ApiVersion`

- [ ] **Step 3: 实现**

```java
// fe/fe-extension-loader/src/main/java/org/apache/doris/extension/loader/ApiVersion.java
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.extension.loader;

/**
 * A plugin API version, {@code major.minor[.patch]}.
 *
 * <p>Only {@link #getMajor()} takes part in the compatibility decision (see {@link ApiVersionGate});
 * minor and patch are carried for diagnostics and for the extension inventory. That asymmetry is
 * deliberate: every change to an SPI surface is by definition a major change, so two builds sharing
 * a major expose an identical API surface to plugins and a minor difference cannot break either
 * direction.
 */
public final class ApiVersion {

    private final int major;
    private final int minor;
    private final int patch;

    private ApiVersion(int major, int minor, int patch) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
    }

    /**
     * Parses {@code "1"}, {@code "1.0"} or {@code "1.0.3"}; omitted segments default to 0.
     *
     * <p>Deliberately strict. The gate treats an unparsable declaration as an absent one and refuses
     * the plugin, so a lossy parse would turn a malformed declaration into a passing one.
     *
     * @throws IllegalArgumentException if the text is null, blank, has more than three segments, or
     *                                  has any segment that is not a non-negative {@code int}
     */
    public static ApiVersion parse(String text) {
        if (text == null) {
            throw new IllegalArgumentException("API version is null");
        }
        String trimmed = text.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("API version is blank");
        }
        // Limit -1 keeps trailing empty segments ("1." -> ["1", ""]) so they can be rejected
        // rather than silently dropped.
        String[] parts = trimmed.split("\\.", -1);
        if (parts.length > 3) {
            throw new IllegalArgumentException("API version has more than three segments: " + text);
        }
        int[] segments = new int[3];
        for (int i = 0; i < parts.length; i++) {
            segments[i] = parseSegment(parts[i], text);
        }
        return new ApiVersion(segments[0], segments[1], segments[2]);
    }

    private static int parseSegment(String segment, String whole) {
        if (segment.isEmpty()) {
            throw new IllegalArgumentException("API version has an empty segment: " + whole);
        }
        for (int i = 0; i < segment.length(); i++) {
            char c = segment.charAt(i);
            // ASCII-only on purpose: Character.isDigit accepts other scripts' digits, which
            // Integer.parseInt would then happily convert, admitting a version string no build
            // tool would ever emit.
            if (c < '0' || c > '9') {
                throw new IllegalArgumentException("API version segment is not a number: " + whole);
            }
        }
        try {
            return Integer.parseInt(segment);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("API version segment is out of range: " + whole, e);
        }
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public int getPatch() {
        return patch;
    }

    @Override
    public String toString() {
        if (patch == 0) {
            return major + "." + minor;
        }
        return major + "." + minor + "." + patch;
    }
}
```

- [ ] **Step 4: 跑测试确认通过**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-extension-loader -am \
    test -Dtest=ApiVersionTest -DfailIfNoTests=false
```

Expected: `Tests run: 10, Failures: 0, Errors: 0`

- [ ] **Step 5: 提交**

```bash
git add fe/fe-extension-loader/src/main/java/org/apache/doris/extension/loader/ApiVersion.java \
        fe/fe-extension-loader/src/test/java/org/apache/doris/extension/loader/ApiVersionTest.java
git commit -m "[feat](plugin) add the ApiVersion value type for plugin compatibility checks

Parsing is strict because the gate treats an unparsable declaration as an
absent one and refuses the plugin: a lossy parse would turn a malformed
declaration into a passing one."
```

---

## Task 2: ApiVersionGate 判定逻辑

**Files:**
- Create: `fe/fe-extension-loader/src/main/java/org/apache/doris/extension/loader/ApiVersionGate.java`
- Create: `fe/fe-extension-loader/src/test/resources/META-INF/doris/test-plugin-api-version.properties`
- Test: `fe/fe-extension-loader/src/test/java/org/apache/doris/extension/loader/ApiVersionGateTest.java`

**Interfaces:**
- Consumes: Task 1 的 `ApiVersion.parse(String)` / `getMajor()` / `toString()`
- Produces:
  - `public ApiVersionGate(String manifestAttribute, ApiVersion expected)`
  - `public static ApiVersion readExpectedFromClasspath(Class<?> anchor, String resourcePath, String key)` — 缺失/损坏抛 `IllegalStateException`
  - `public String getManifestAttribute()`
  - `public ApiVersion getExpected()`
  - `public String checkDeclared(String declared)` — 兼容返回 `null`，否则返回拒绝原因

- [ ] **Step 1: 建测试用的 filtered resource 替身**

这个文件**不**参与 filtering，是一份固定内容的测试夹具，用来验证 `readExpectedFromClasspath` 的读取路径。

```properties
# fe/fe-extension-loader/src/test/resources/META-INF/doris/test-plugin-api-version.properties
version=3.7
```

- [ ] **Step 2: 写失败测试**

```java
// fe/fe-extension-loader/src/test/java/org/apache/doris/extension/loader/ApiVersionGateTest.java
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.extension.loader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link ApiVersionGate}'s compatibility decision.
 *
 * <p><b>Why minor and patch must be ignored in BOTH directions:</b> the version scheme defines every
 * SPI surface change as a major change, so two builds sharing a major expose the identical API
 * surface. A newer-minor plugin therefore cannot be calling a method an older-minor kernel lacks,
 * and refusing it would reject a plugin that provably works.
 *
 * <p><b>Why an absent declaration must be refused:</b> the declaration is the only part of the
 * compatibility contract that physically travels inside the plugin artifact. Accepting an absent one
 * would let any plugin opt out of the check by writing nothing — which is exactly how the previous
 * {@code ConnectorProvider.apiVersion()} default method failed.
 */
public class ApiVersionGateTest {

    private static final String ATTR = "Doris-Test-Plugin-Api-Version";

    private static ApiVersionGate gateExpecting(String expected) {
        return new ApiVersionGate(ATTR, ApiVersion.parse(expected));
    }

    @Test
    void acceptsSameMajor() {
        Assertions.assertNull(gateExpecting("2.3").checkDeclared("2.3"));
    }

    @Test
    void acceptsOlderMinor() {
        Assertions.assertNull(gateExpecting("2.9").checkDeclared("2.1"),
                "an older-minor plugin sees the same API surface and must load");
    }

    @Test
    void acceptsNewerMinor() {
        Assertions.assertNull(gateExpecting("2.1").checkDeclared("2.9"),
                "minor is compatible in both directions: a minor bump never changes the SPI surface");
    }

    @Test
    void acceptsDifferingPatchAndOmittedPatch() {
        Assertions.assertNull(gateExpecting("2.3.1").checkDeclared("2.3.9"));
        Assertions.assertNull(gateExpecting("2.3.1").checkDeclared("2"));
    }

    @Test
    void rejectsDifferentMajor() {
        String reason = gateExpecting("2.0").checkDeclared("1.9");
        Assertions.assertNotNull(reason);
        Assertions.assertTrue(reason.contains("1.9") && reason.contains("2.0"),
                "the message must name both versions so an operator can act on it: " + reason);
    }

    @Test
    void rejectsNewerMajor() {
        Assertions.assertNotNull(gateExpecting("2.0").checkDeclared("3.0"));
    }

    @Test
    void rejectsAbsentDeclaration() {
        String reason = gateExpecting("1.0").checkDeclared(null);
        Assertions.assertNotNull(reason);
        Assertions.assertTrue(reason.contains(ATTR),
                "the message must name the manifest attribute the author has to add: " + reason);
    }

    @Test
    void rejectsBlankDeclaration() {
        Assertions.assertNotNull(gateExpecting("1.0").checkDeclared("   "));
    }

    @Test
    void rejectsUnparsableDeclaration() {
        String reason = gateExpecting("1.0").checkDeclared("1.0-SNAPSHOT");
        Assertions.assertNotNull(reason);
        Assertions.assertTrue(reason.contains("1.0-SNAPSHOT"),
                "echo the bad value back so the author can find it in their pom: " + reason);
    }

    @Test
    void readsExpectedVersionFromClasspath() {
        ApiVersion v = ApiVersionGate.readExpectedFromClasspath(
                ApiVersionGateTest.class, "/META-INF/doris/test-plugin-api-version.properties", "version");
        Assertions.assertEquals(3, v.getMajor());
        Assertions.assertEquals(7, v.getMinor());
    }

    @Test
    void missingResourceFailsLoud() {
        // A missing resource means the build did not filter it in. That is a build defect, not a
        // deployment one, so it must stop startup rather than silently disable the whole gate.
        Assertions.assertThrows(IllegalStateException.class, () ->
                ApiVersionGate.readExpectedFromClasspath(
                        ApiVersionGateTest.class, "/META-INF/doris/absent.properties", "version"));
    }

    @Test
    void missingKeyFailsLoud() {
        Assertions.assertThrows(IllegalStateException.class, () ->
                ApiVersionGate.readExpectedFromClasspath(
                        ApiVersionGateTest.class,
                        "/META-INF/doris/test-plugin-api-version.properties",
                        "no-such-key"));
    }
}
```

- [ ] **Step 3: 跑测试确认失败**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-extension-loader -am \
    test -Dtest=ApiVersionGateTest -DfailIfNoTests=false
```

Expected: 编译失败，`cannot find symbol: class ApiVersionGate`

- [ ] **Step 4: 实现**

```java
// fe/fe-extension-loader/src/main/java/org/apache/doris/extension/loader/ApiVersionGate.java
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.extension.loader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * Refuses a directory-loaded plugin whose declared API version is incompatible with this FE build.
 *
 * <p>Family-neutral by construction: a family supplies its own manifest attribute name and its own
 * expected version, so this class holds no knowledge of connectors, filesystems, authentication or
 * lineage. {@link DirectoryPluginRuntimeManager} therefore stays free of family-specific branches.
 *
 * <p><b>Compatibility rule.</b> Major must match exactly; minor and patch are ignored. Every change
 * to an SPI surface is by definition a major change, so two builds sharing a major expose an
 * identical API surface to plugins and a minor difference is safe in both directions.
 *
 * <p><b>Fail-closed.</b> A plugin declaring nothing is refused. The declaration is the only part of
 * the contract that physically travels inside the plugin artifact; accepting an absent one would let
 * a plugin opt out of the check by writing nothing.
 *
 * <p><b>Where the two numbers come from.</b> Both the expected version (via
 * {@link #readExpectedFromClasspath}, reading a resource filtered at build time) and the declared
 * version (read from the plugin jar's manifest) originate in one maven property per family. Within a
 * single build they cannot disagree; across builds a disagreement is exactly what this gate detects.
 */
public final class ApiVersionGate {

    private final String manifestAttribute;
    private final ApiVersion expected;

    public ApiVersionGate(String manifestAttribute, ApiVersion expected) {
        this.manifestAttribute = Objects.requireNonNull(manifestAttribute, "manifestAttribute");
        this.expected = Objects.requireNonNull(expected, "expected");
    }

    /**
     * Reads this FE build's expected API version from a build-filtered classpath resource.
     *
     * @param anchor       a class from the module that owns the resource, so it resolves on that
     *                     module's classloader
     * @param resourcePath absolute resource path, e.g.
     *                     {@code /META-INF/doris/connector-plugin-api-version.properties}
     * @param key          property key inside that resource, {@code version}
     * @throws IllegalStateException if the resource is missing, unreadable, or unparsable. That is a
     *                               build defect rather than a deployment one — degrading to "no
     *                               check" would silently disable the gate for the whole family, so
     *                               it must stop startup instead.
     */
    public static ApiVersion readExpectedFromClasspath(Class<?> anchor, String resourcePath, String key) {
        try (InputStream in = anchor.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("Plugin API version resource not found on the classpath: "
                        + resourcePath + " (anchor " + anchor.getName() + "). This is a build defect: "
                        + "the resource must be generated by maven resource filtering.");
            }
            Properties props = new Properties();
            props.load(in);
            String raw = props.getProperty(key);
            if (raw == null) {
                throw new IllegalStateException("Plugin API version resource " + resourcePath
                        + " has no '" + key + "' entry");
            }
            return ApiVersion.parse(raw);
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalStateException(
                    "Failed to read the plugin API version from " + resourcePath, e);
        }
    }

    public String getManifestAttribute() {
        return manifestAttribute;
    }

    public ApiVersion getExpected() {
        return expected;
    }

    /**
     * Decides whether a plugin may load.
     *
     * @param declared raw manifest attribute value read from the plugin jar, or null when absent
     * @return {@code null} when the plugin is compatible; otherwise a message naming both versions,
     *         suitable for a {@code LoadFailure} and for an operator-facing log line
     */
    public String checkDeclared(String declared) {
        if (declared == null || declared.trim().isEmpty()) {
            return "plugin jar manifest does not declare " + manifestAttribute
                    + "; this FE provides API version " + expected
                    + " (add the attribute to the plugin's maven-jar-plugin manifestEntries)";
        }
        ApiVersion parsed;
        try {
            parsed = ApiVersion.parse(declared);
        } catch (IllegalArgumentException e) {
            return "plugin declares an unparsable " + manifestAttribute + " '" + declared
                    + "'; this FE provides API version " + expected;
        }
        if (parsed.getMajor() != expected.getMajor()) {
            return "plugin was built against API version " + parsed
                    + " but this FE provides " + expected
                    + "; major must match, so the plugin has to be rebuilt against this Doris release";
        }
        return null;
    }
}
```

- [ ] **Step 5: 跑测试确认通过**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-extension-loader -am \
    test -Dtest=ApiVersionGateTest -DfailIfNoTests=false
```

Expected: `Tests run: 12, Failures: 0, Errors: 0`

- [ ] **Step 6: 提交**

```bash
git add fe/fe-extension-loader/src/main/java/org/apache/doris/extension/loader/ApiVersionGate.java \
        fe/fe-extension-loader/src/test/java/org/apache/doris/extension/loader/ApiVersionGateTest.java \
        fe/fe-extension-loader/src/test/resources/META-INF/doris/test-plugin-api-version.properties
git commit -m "[feat](plugin) add the family-neutral ApiVersionGate

Major must match; minor and patch are ignored in both directions, which is
safe because every SPI surface change is a major change by definition, so two
builds sharing a major expose an identical surface to plugins.

An absent declaration is refused. The declaration is the only part of the
contract that travels inside the plugin artifact, so accepting an absent one
would let a plugin opt out by writing nothing."
```

---

## Task 3: 接入 DirectoryPluginRuntimeManager

**Files:**
- Modify: `fe/fe-extension-loader/src/main/java/org/apache/doris/extension/loader/LoadFailure.java:33`（`STAGE_CONFLICT` 之后加常量）
- Modify: `fe/fe-extension-loader/src/main/java/org/apache/doris/extension/loader/ManifestVersions.java:56`（`fromManifest` 泛化）
- Modify: `fe/fe-extension-loader/src/main/java/org/apache/doris/extension/loader/DirectoryPluginRuntimeManager.java:108`（`loadAll` 重载）、`:210`（`loadFromPluginDir` 增参）、`:333`（读属性泛化）
- Test: `fe/fe-extension-loader/src/test/java/org/apache/doris/extension/loader/DirectoryPluginRuntimeManagerApiVersionTest.java`

**Interfaces:**
- Consumes: Task 2 的 `ApiVersionGate#getManifestAttribute()` / `#checkDeclared(String)`；`ApiVersion.parse(String)`
- Produces:
  - `LoadFailure.STAGE_API_VERSION`（值 `"apiVersion"`）
  - `public LoadReport<F> loadAll(List<Path>, ClassLoader, Class<F>, ClassLoadingPolicy, ApiVersionGate)` — 新 5 参重载；gate 为 `null` 表示不校验
  - 原 4 参 `loadAll` 保留，委派给新重载并传 `null`
  - `static String ManifestVersions.fromManifest(JarFile, String packagePath, String attributeName)`

- [ ] **Step 1: 写失败测试**

测试要造真实 jar，因为这一层的价值正是"从 jar 的 MANIFEST 里把值读出来"——用 mock 会把唯一要验的东西验没了。

```java
// fe/fe-extension-loader/src/test/java/org/apache/doris/extension/loader/DirectoryPluginRuntimeManagerApiVersionTest.java
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.extension.loader;

import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginContext;
import org.apache.doris.extension.spi.PluginFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Tests that {@link DirectoryPluginRuntimeManager} enforces the API version declared in a plugin
 * jar's MANIFEST.
 *
 * <p><b>Why these tests build real jars:</b> the whole point of the mechanism is that the version
 * physically travels inside the plugin artifact. The previous {@code ConnectorProvider.apiVersion()}
 * default method looked correct in every unit test and still could not reject anything, because the
 * bytecode being executed came from the kernel rather than the plugin. Only a test that reads an
 * actual jar manifest can tell the two apart.
 */
public class DirectoryPluginRuntimeManagerApiVersionTest {

    private static final String ATTR = "Doris-Test-Plugin-Api-Version";

    /** A factory compiled into the test classes; the jar under test only carries its manifest. */
    public static final class TestFactory implements PluginFactory {
        @Override
        public String name() {
            return "versioned";
        }

        @Override
        public Plugin create(PluginContext context) {
            return new Plugin() {
            };
        }
    }

    /**
     * Writes a plugin directory containing one jar that declares {@code declaredVersion}
     * (or no attribute at all when null) and registers {@link TestFactory} via ServiceLoader.
     */
    private Path writePluginDir(Path root, String dirName, String declaredVersion) throws IOException {
        Path pluginDir = Files.createDirectories(root.resolve(dirName));
        Path jar = pluginDir.resolve("plugin.jar");

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (declaredVersion != null) {
            manifest.getMainAttributes().putValue(ATTR, declaredVersion);
        }

        try (OutputStream fileOut = Files.newOutputStream(jar);
                JarOutputStream jarOut = new JarOutputStream(fileOut, manifest)) {
            jarOut.putNextEntry(new JarEntry("META-INF/services/" + PluginFactory.class.getName()));
            jarOut.write(TestFactory.class.getName().getBytes("UTF-8"));
            jarOut.closeEntry();
        }
        return pluginDir;
    }

    private LoadReport<PluginFactory> load(Path root, ApiVersionGate gate) {
        return new DirectoryPluginRuntimeManager<PluginFactory>().loadAll(
                Collections.singletonList(root),
                getClass().getClassLoader(),
                PluginFactory.class,
                new ClassLoadingPolicy(Collections.singletonList("org.apache.doris.extension.")),
                gate);
    }

    @Test
    void loadsPluginDeclaringMatchingMajor(@TempDir Path root) throws IOException {
        writePluginDir(root, "ok", "1.4");

        LoadReport<PluginFactory> report = load(root, new ApiVersionGate(ATTR, ApiVersion.parse("1.0")));

        Assertions.assertEquals(1, report.getSuccesses().size(),
                "minor differences must not block a load: " + report.getFailures());
        Assertions.assertEquals(0, report.getFailures().size());
    }

    @Test
    void rejectsPluginDeclaringDifferentMajor(@TempDir Path root) throws IOException {
        writePluginDir(root, "wrong-major", "2.0");

        LoadReport<PluginFactory> report = load(root, new ApiVersionGate(ATTR, ApiVersion.parse("1.0")));

        Assertions.assertEquals(0, report.getSuccesses().size());
        Assertions.assertEquals(1, report.getFailures().size());
        LoadFailure failure = report.getFailures().get(0);
        Assertions.assertEquals(LoadFailure.STAGE_API_VERSION, failure.getStage());
        Assertions.assertTrue(failure.getMessage().contains("2.0"),
                "the failure must name the declared version: " + failure.getMessage());
    }

    @Test
    void rejectsPluginDeclaringNothing(@TempDir Path root) throws IOException {
        // Fail-closed: this is the case a third party would hit by simply not configuring the
        // manifest entry, and it must not become a free pass.
        writePluginDir(root, "undeclared", null);

        LoadReport<PluginFactory> report = load(root, new ApiVersionGate(ATTR, ApiVersion.parse("1.0")));

        Assertions.assertEquals(0, report.getSuccesses().size());
        Assertions.assertEquals(1, report.getFailures().size());
        Assertions.assertEquals(LoadFailure.STAGE_API_VERSION, report.getFailures().get(0).getStage());
    }

    @Test
    void skipsCheckWhenNoGateSupplied(@TempDir Path root) throws IOException {
        // The 4-arg loadAll must keep behaving exactly as before, so a family that has not been
        // wired yet is unaffected.
        writePluginDir(root, "ungated", null);

        LoadReport<PluginFactory> report = load(root, null);

        Assertions.assertEquals(1, report.getSuccesses().size(),
                "a null gate must disable the check entirely: " + report.getFailures());
    }

    @Test
    void rejectedPluginIsNotRetained(@TempDir Path root) throws IOException {
        // A refused plugin must not keep its classloader alive, mirroring how the duplicate-name
        // rejection path discards its handle.
        writePluginDir(root, "wrong-major", "2.0");

        DirectoryPluginRuntimeManager<PluginFactory> manager = new DirectoryPluginRuntimeManager<>();
        manager.loadAll(
                Collections.singletonList(root),
                getClass().getClassLoader(),
                PluginFactory.class,
                new ClassLoadingPolicy(Collections.singletonList("org.apache.doris.extension.")),
                new ApiVersionGate(ATTR, ApiVersion.parse("1.0")));

        Assertions.assertTrue(manager.list().isEmpty(),
                "a version-rejected plugin must not appear in the runtime manager's inventory");
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-extension-loader -am \
    test -Dtest=DirectoryPluginRuntimeManagerApiVersionTest -DfailIfNoTests=false
```

Expected: 编译失败，`cannot find symbol: STAGE_API_VERSION` 及 5 参 `loadAll` 不存在

- [ ] **Step 3: 加 `LoadFailure.STAGE_API_VERSION`**

在 `LoadFailure.java:33`（`STAGE_CONFLICT` 那行）之后插入：

```java
    /**
     * The plugin's declared API version is incompatible with this FE build, or it declared none.
     * Distinct from {@link #STAGE_INSTANTIATE} because the plugin is well-formed — it simply
     * belongs to a different Doris release.
     */
    public static final String STAGE_API_VERSION = "apiVersion";
```

- [ ] **Step 4: 泛化 `ManifestVersions.fromManifest`**

把 `ManifestVersions.java` 中现有的 `fromManifest(JarFile, String)` 整个方法体替换为下面两个方法（保留原 javadoc 于 2 参重载上）：

```java
    static String fromManifest(JarFile jarFile, String packagePath) throws IOException {
        return fromManifest(jarFile, packagePath, Attributes.Name.IMPLEMENTATION_VERSION.toString());
    }

    /**
     * Reads one manifest attribute from a jar, honoring the class's package section first.
     *
     * <p>Per the jar spec a package section ("Name: com/acme/plugin/") overrides the main attributes
     * for classes in that package, mirroring {@code Package.getImplementationVersion()}.
     *
     * @param packagePath   manifest section name from {@link #packagePathOf}, may be null
     * @param attributeName attribute to read, e.g. {@code Implementation-Version} or a plugin
     *                      family's API version attribute
     * @return the value, or null when the manifest or the attribute is absent
     */
    static String fromManifest(JarFile jarFile, String packagePath, String attributeName)
            throws IOException {
        Manifest manifest = jarFile.getManifest();
        if (manifest == null) {
            return null;
        }
        if (packagePath != null) {
            Attributes packageAttributes = manifest.getAttributes(packagePath);
            if (packageAttributes != null) {
                String value = packageAttributes.getValue(attributeName);
                if (value != null) {
                    return value;
                }
            }
        }
        return manifest.getMainAttributes().getValue(attributeName);
    }
```

- [ ] **Step 5: 改 `DirectoryPluginRuntimeManager`**

**5a.** 加 import：`import java.util.jar.Attributes;` 已由 ManifestVersions 用到，此文件只需确认 `ApiVersionGate` 同包无需 import。

**5b.** `loadAll`（原 `:108`）拆成重载。把原签名行改为委派，并把原方法体移到新的 5 参版本：

```java
    /**
     * Loads every plugin directory under the given roots, with no API version check.
     *
     * <p>Retained for families that have not been wired to a gate yet; behaviour is unchanged.
     */
    public LoadReport<F> loadAll(List<Path> pluginRoots, ClassLoader parent, Class<F> factoryType,
            ClassLoadingPolicy policy) {
        return loadAll(pluginRoots, parent, factoryType, policy, null);
    }

    /**
     * Loads every plugin directory under the given roots, refusing any plugin the gate rejects.
     *
     * @param apiVersionGate the family's version gate, or null to skip the check entirely
     */
    public LoadReport<F> loadAll(List<Path> pluginRoots, ClassLoader parent, Class<F> factoryType,
            ClassLoadingPolicy policy, ApiVersionGate apiVersionGate) {
        // ... 原有方法体保持不变，只把下面这一行的调用改成传 gate ...
    }
```

在 5 参版本的方法体里，把

```java
                    PluginHandle<F> handle = loadFromPluginDir(pluginDir, parent, factoryType, effectivePolicy);
```

改为

```java
                    PluginHandle<F> handle = loadFromPluginDir(
                            pluginDir, parent, factoryType, effectivePolicy, apiVersionGate);
```

**5c.** `loadFromPluginDir`（原 `:210`）签名加参数：

```java
    private PluginHandle<F> loadFromPluginDir(Path pluginDir, ClassLoader parent, Class<F> factoryType,
            ClassLoadingPolicy policy, ApiVersionGate apiVersionGate) throws PluginLoadException {
```

**5d.** 在该方法内，把

```java
        String version = readImplementationVersion(factory.getClass(), allJars);
```

替换为

```java
        if (apiVersionGate != null) {
            String declared = readManifestAttribute(
                    factory.getClass(), allJars, apiVersionGate.getManifestAttribute());
            String problem = apiVersionGate.checkDeclared(declared);
            if (problem != null) {
                closeClassLoader(classLoader);
                throw new PluginLoadException(
                        normalizedDir,
                        LoadFailure.STAGE_API_VERSION,
                        "Refused plugin in " + normalizedDir + ": " + problem,
                        null);
            }
        }

        String version = readManifestAttribute(
                factory.getClass(), allJars, Attributes.Name.IMPLEMENTATION_VERSION.toString());
```

并在文件顶部 import 中补 `import java.util.jar.Attributes;`（若尚未存在）。

**5e.** 把 `readImplementationVersion`（原 `:333`）改名并加参数。方法签名与两处 `fromManifest` 调用改为：

```java
    /**
     * Reads one manifest attribute from the jar that defined the factory class: the class's code
     * source when available (covers layouts where the service descriptor sits in a root jar but the
     * implementation lives in lib/), otherwise the first candidate jar containing the class entry.
     *
     * <p>Returns null when it cannot be determined. Callers decide what that means: it is merely
     * absent display metadata for {@code Implementation-Version}, but it is a refusal for an API
     * version attribute (see {@link ApiVersionGate#checkDeclared}).
     */
    private String readManifestAttribute(Class<?> factoryClass, List<Path> candidateJars,
            String attributeName) {
        String packagePath = ManifestVersions.packagePathOf(factoryClass);
        Path definingJar = ManifestVersions.jarOf(factoryClass);
        if (definingJar != null) {
            try (JarFile jarFile = new JarFile(definingJar.toFile())) {
                return ManifestVersions.fromManifest(jarFile, packagePath, attributeName);
            } catch (IOException ignored) {
                // Fall through to scanning the candidate jars.
            }
        }
        String classEntry = factoryClass.getName().replace('.', '/') + ".class";
        for (Path jar : candidateJars) {
            try (JarFile jarFile = new JarFile(jar.toFile())) {
                if (jarFile.getEntry(classEntry) == null) {
                    continue;
                }
                return ManifestVersions.fromManifest(jarFile, packagePath, attributeName);
            } catch (IOException ignored) {
                // Fall through to the next candidate jar.
            }
        }
        return null;
    }
```

> **注意**：测试里 `TestFactory` 编译进 `target/test-classes`（目录，非 jar），因此
> `ManifestVersions.jarOf` 返回 null，读取会走"扫描 candidateJars"分支并命中临时 jar
> —— 这正是本任务要验证的路径。

- [ ] **Step 6: 跑测试确认通过**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-extension-loader -am \
    test -Dtest='DirectoryPluginRuntimeManagerApiVersionTest+ApiVersionTest+ApiVersionGateTest' \
    -DfailIfNoTests=false
```

Expected: 全部通过，`Failures: 0, Errors: 0`

- [ ] **Step 7: 确认既有 loader 测试未回归**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-extension-loader -am test
```

Expected: `PluginRegistryTest`、`DirectoryPluginRuntimeManagerMetadataTest` 等全绿（4 参 `loadAll` 行为未变）

- [ ] **Step 8: 提交**

```bash
git add fe/fe-extension-loader/
git commit -m "[feat](plugin) enforce the declared API version when loading directory plugins

The gate runs after the factory is instantiated and before the PluginHandle is
published, so a refused plugin never reaches any family's registry, and its
classloader is closed on the way out - the same pairing the duplicate-name
rejection path already follows.

The 4-arg loadAll is kept and delegates with a null gate, so a family that is
not wired yet behaves exactly as before."
```

---

## Task 4: CONNECTOR 族端到端接线（含删除旧机制）

**Files:**
- Modify: `fe/fe-connector/pom.xml`（加 `<properties>`；`<build><plugins>` 加 `maven-jar-plugin`）
- Modify: `fe/fe-connector/fe-connector-spi/pom.xml`（加 `<build><resources>`）
- Create: `fe/fe-connector/fe-connector-spi/src/main/resources/META-INF/doris/connector-plugin-api-version.properties`
- Modify: `fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorProvider.java:178-182`（删 `apiVersion()`）
- Modify: `fe/fe-core/src/main/java/org/apache/doris/connector/ConnectorPluginManager.java`（`:69` 删常量、`:201` 传 gate、`:297`/`:324`/`:366` 删三处比较）
- Modify: `fe/fe-core/src/test/java/org/apache/doris/connector/ConnectorPluginManagerTest.java`

**Interfaces:**
- Consumes: Task 2 的 `ApiVersionGate(String, ApiVersion)` 与 `ApiVersionGate.readExpectedFromClasspath(Class, String, String)`；Task 3 的 5 参 `loadAll`
- Produces: manifest 属性名 `Doris-Connector-Plugin-Api-Version`；resource 路径 `/META-INF/doris/connector-plugin-api-version.properties`；property 名 `connector.plugin.api.version`。后续三族严格照此四件套命名。

- [ ] **Step 1: 加 property 与 manifest 配置**

在 `fe/fe-connector/pom.xml` 的 `</modules>` 之后、`<build>` 之前插入：

```xml
    <properties>
        <!--
          The CONNECTOR family's plugin API version. Single source of truth: it stamps both the
          filtered resource in fe-connector-spi (what this FE expects) and every connector jar's
          MANIFEST (what the plugin declares), so the two cannot disagree within one build.

          Bump the MAJOR whenever the SPI surface changes at all - a type or method added, removed,
          or given a different signature. Bump the MINOR when only an implementation changes behind
          an unchanged surface. See plan-doc/designs/2026-07-29-plugin-api-version-check-design.md
          section 12.

          The connector contract spans fe-connector-api, fe-connector-spi, fe-extension-spi and
          fe-filesystem-api; the last two live under other parent poms, so changing them must bump
          this property too.
        -->
        <connector.plugin.api.version>1.0</connector.plugin.api.version>
    </properties>
```

在同文件 `<build><plugins>` 内，**现有 exec-maven-plugin 之后**追加（注意此插件**不加** `<inherited>false</inherited>`，正是要让 11 个子模块继承）：

```xml
            <!--
              Stamp every connector jar with the API version it was built against. Inherited by all
              child modules on purpose: the number then appears in exactly one place in the tree,
              and adding a connector requires no pom change to pick it up.
            -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <configuration>
                    <archive>
                        <manifestEntries>
                            <Doris-Connector-Plugin-Api-Version>${connector.plugin.api.version}</Doris-Connector-Plugin-Api-Version>
                        </manifestEntries>
                    </archive>
                </configuration>
            </plugin>
```

- [ ] **Step 2: 建 filtered resource**

```properties
# fe/fe-connector/fe-connector-spi/src/main/resources/META-INF/doris/connector-plugin-api-version.properties
# Generated from the connector.plugin.api.version property in fe/fe-connector/pom.xml.
# Do not edit the value here - edit the property.
version=${connector.plugin.api.version}
```

`fe/fe-connector/fe-connector-spi/pom.xml` 已有 `<build>`（`:69`）且其首个子元素是 `<plugins>`（`:71`）。
把下面的 `<resources>` 块插在 `<build>` 之后、`<plugins>` 之前（**不要**新建 `<build>`）：

```xml
        <resources>
            <!--
              Two resource entries on purpose: filtering is enabled ONLY for the API version file.
              Filtering the whole directory would also rewrite META-INF/services descriptors, where a
              stray ${...} would be silently substituted.
            -->
            <resource>
                <directory>src/main/resources</directory>
                <filtering>false</filtering>
                <excludes>
                    <exclude>META-INF/doris/connector-plugin-api-version.properties</exclude>
                </excludes>
            </resource>
            <resource>
                <directory>src/main/resources</directory>
                <filtering>true</filtering>
                <includes>
                    <include>META-INF/doris/connector-plugin-api-version.properties</include>
                </includes>
            </resource>
        </resources>
```

- [ ] **Step 3: 验证 filtering 与 manifest 真的生效**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-connector/fe-connector-spi -am \
    package -DskipTests -Dcheckstyle.skip=true
unzip -p /mnt/disk1/yy/git/wt-catalog-spi/fe/fe-connector/fe-connector-spi/target/fe-connector-spi-*.jar \
    META-INF/doris/connector-plugin-api-version.properties
unzip -p /mnt/disk1/yy/git/wt-catalog-spi/fe/fe-connector/fe-connector-spi/target/fe-connector-spi-*.jar \
    META-INF/MANIFEST.MF | grep -i 'Doris-Connector-Plugin-Api-Version'
```

Expected: 第一条输出含 `version=1.0`（**不是** `${connector.plugin.api.version}`）；第二条输出 `Doris-Connector-Plugin-Api-Version: 1.0`

若第一条仍是字面 `${...}`，说明 filtering 未生效，先修 Step 2 再继续。

- [ ] **Step 4: 删除 `ConnectorProvider#apiVersion()`**

删掉 `ConnectorProvider.java` 中这四行（含其上的 javadoc）：

```java
    /** API version for compatibility checking. Major version change = incompatible. */
    default int apiVersion() {
        return 1;
    }
```

- [ ] **Step 5: 改 `ConnectorPluginManager`**

**5a.** 删掉 `:68-69` 的常量及其注释：

```java
    /** The API version that this FE build supports. Increment on breaking SPI changes. */
    static final int CURRENT_API_VERSION = 1;
```

**5b.** 在 `PLUGIN_FAMILY` 常量之后加入 gate 定义：

```java
    /** Manifest attribute a connector plugin uses to declare the API version it was built against. */
    private static final String API_VERSION_ATTRIBUTE = "Doris-Connector-Plugin-Api-Version";

    /** Build-filtered resource in fe-connector-spi carrying this FE's expected API version. */
    private static final String API_VERSION_RESOURCE =
            "/META-INF/doris/connector-plugin-api-version.properties";
```

并在字段区（`runtimeManager` 附近）加入：

```java
    /**
     * Refuses a directory plugin built against an incompatible connector SPI.
     *
     * <p>Anchored on {@link ConnectorProvider} so the resource resolves on the classloader that
     * actually has fe-connector-spi. Constructed eagerly: a missing resource is a build defect and
     * must stop FE startup rather than silently disable the check.
     */
    private final ApiVersionGate apiVersionGate = new ApiVersionGate(
            API_VERSION_ATTRIBUTE,
            ApiVersionGate.readExpectedFromClasspath(
                    ConnectorProvider.class, API_VERSION_RESOURCE, "version"));
```

补 import：`import org.apache.doris.extension.loader.ApiVersionGate;`

**5c.** `loadPlugins`（`:201`）把 `runtimeManager.loadAll(...)` 改为 5 参：

```java
        LoadReport<ConnectorProvider> report = runtimeManager.loadAll(
                pluginRoots,
                ConnectorPluginManager.class.getClassLoader(),
                ConnectorProvider.class,
                classLoadingPolicy,
                apiVersionGate);
```

**5d.** 删除三处旧比较。

`createConnector`（原 `:296-302`）中删掉：

```java
                int providerVersion = provider.apiVersion();
                if (providerVersion != CURRENT_API_VERSION) {
                    LOG.warn("Skipping connector provider '{}': apiVersion={} (expected {})",
                            provider.getType(), providerVersion, CURRENT_API_VERSION);
                    continue;
                }
```

`findProvider`（原 `:323-326`）中删掉：

```java
                if (provider.apiVersion() != CURRENT_API_VERSION) {
                    continue;
                }
```

同时把 `findProvider` 的 javadoc 末句 `Same selection as {@link #createConnector}: first provider that supports the type with a compatible API version.` 改为 `Same selection as {@link #createConnector}: the first provider that supports the type.`

`validateProperties`（原 `:365-372`）中删掉：

```java
                if (provider.apiVersion() != CURRENT_API_VERSION) {
                    throw new IllegalArgumentException(
                            "Connector provider '" + provider.getType()
                                    + "' has incompatible API version " + provider.apiVersion()
                                    + " (expected " + CURRENT_API_VERSION + ")");
                }
```

**5e.** 类 javadoc（`:47-62`）中 "Classpath providers have higher priority than directory-loaded providers." 之后补一句：

```java
 * <p>Directory-loaded providers additionally pass an API version gate (see {@link ApiVersionGate}):
 * a plugin that declares an incompatible major, or declares nothing, is refused at load time and
 * never reaches {@link #registerDiscovered}. Classpath providers are exempt — they are compiled in
 * the same build as this class, so there is no version to disagree about.
```

- [ ] **Step 6: 改 `ConnectorPluginManagerTest`**

**6a.** 删除 `:43` 的常量：

```java
    private static final int CURRENT = ConnectorPluginManager.CURRENT_API_VERSION;
```

**6b.** 删除四个测试方法（连同其 `@Test` 注解与 javadoc）：
`testCompatibleApiVersionCreatesConnector`、`testIncompatibleApiVersionReturnsNull`、
`testIncompatibleApiVersionValidateThrows`、`testFallsBackToCompatibleProvider`。

> 这四个测的是被本任务删掉的机制。新机制的等价覆盖在 Task 3 的
> `DirectoryPluginRuntimeManagerApiVersionTest` —— 那里用真实 jar 验证，而这四个用不到 jar
> 的桩恰恰是旧机制"看起来能测、实际拦不住"的原因。

**6c.** 新增一个替代用例，钉住"兼容 provider 能建出 connector"这一仍然有效的行为：

```java
    @Test
    void createsConnectorFromRegisteredProvider() {
        manager.registerProvider(createProvider("test_type"));

        Connector connector = manager.createConnector("test_type",
                Collections.emptyMap(), testContext);
        Assertions.assertNotNull(connector,
                "a registered provider claiming the type must produce a connector");
    }
```

**6d.** 两个工厂方法去掉 `apiVersion` 参数：

```java
    private static ConnectorProvider createProvider(String type) {
        return createProvider(type, true, "");
    }

    private static ConnectorProvider createProvider(String type, boolean standalone, String tag) {
        return new ConnectorProvider() {
            @Override
            public String getType() {
                return type;
            }

            @Override
            public boolean isStandaloneCatalogType() {
                return standalone;
            }

            @Override
            public Connector create(Map<String, String> properties, ConnectorContext context) {
                return new TaggedConnector(tag);
            }
        };
    }
```

**6e.** 全文件把 `createProvider(X, CURRENT, ` 替换为 `createProvider(X, `（`:129`、`:143`、`:156`、`:169`、`:172`、`:182`、`:183`、`:200`、`:203`、`:215`、`:224`、`:225` 共 12 处）。

**6f.** 类 javadoc（`:36-39`）把 "API version compatibility, the type-name contract..." 改为 "the type-name contract enforced when a provider is discovered, and the split between sibling lookup and building a standalone catalog."

- [ ] **Step 7: 确认全仓无残留引用**

```bash
grep -rn "apiVersion\|CURRENT_API_VERSION" \
    /mnt/disk1/yy/git/wt-catalog-spi/fe --include=*.java | grep -v "/target/"
```

Expected: 无输出。若有命中，逐个清掉再继续（删除类改动不能只靠 test-compile，增量编译会跳过未改模块）。

- [ ] **Step 8: 跑测试**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am \
    test -Dtest=ConnectorPluginManagerTest -DfailIfNoTests=false -Dcheckstyle.skip=true
```

Expected: 全绿

- [ ] **Step 9: 验证 8 个连接器 jar 都带上了属性**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-connector/fe-connector-iceberg -am \
    package -DskipTests -Dcheckstyle.skip=true
unzip -p /mnt/disk1/yy/git/wt-catalog-spi/fe/fe-connector/fe-connector-iceberg/target/fe-connector-iceberg-*.jar \
    META-INF/MANIFEST.MF | grep -i 'Doris-Connector-Plugin-Api-Version'
```

Expected: `Doris-Connector-Plugin-Api-Version: 1.0`（证明父 pom 继承生效，连接器 pom 未改一行）

- [ ] **Step 10: 提交**

```bash
git add fe/fe-connector/ fe/fe-core/src/main/java/org/apache/doris/connector/ConnectorPluginManager.java \
        fe/fe-core/src/test/java/org/apache/doris/connector/ConnectorPluginManagerTest.java
git commit -m "[feat](plugin) wire the CONNECTOR family to the API version gate

Replaces ConnectorProvider.apiVersion(), which could never reject anything: no
provider overrode it, and the SPI interface is excluded from every plugin zip
and loaded parent-first, so the default body executing at runtime came from the
kernel. The number never left the kernel.

The version now lives in one maven property that stamps both the filtered
resource fe-core reads and every connector jar's manifest. The eight connector
poms are untouched - they inherit it.

Drops the four tests that covered the old mechanism; their replacement is
DirectoryPluginRuntimeManagerApiVersionTest, which uses real jars. Stubs that
never touch a jar are precisely why the old gate looked testable while being
unable to reject anything."
```

---

## Task 5: FILESYSTEM 族接线

**Files:**
- Modify: `fe/fe-filesystem/pom.xml`
- Modify: `fe/fe-filesystem/fe-filesystem-spi/pom.xml`
- Create: `fe/fe-filesystem/fe-filesystem-spi/src/main/resources/META-INF/doris/filesystem-plugin-api-version.properties`
- Modify: `fe/fe-core/src/main/java/org/apache/doris/fs/FileSystemPluginManager.java`

**Interfaces:**
- Consumes: Task 2 的 `ApiVersionGate`；Task 3 的 5 参 `loadAll`；Task 4 确立的四件套命名范式
- Produces: manifest 属性 `Doris-Filesystem-Plugin-Api-Version`；resource `/META-INF/doris/filesystem-plugin-api-version.properties`；property `filesystem.plugin.api.version`

- [ ] **Step 1: 加 property 与 manifest 配置**

在 `fe/fe-filesystem/pom.xml` 的 `</modules>` 之后插入：

```xml
    <properties>
        <!--
          The FILESYSTEM family's plugin API version. Single source of truth: it stamps both the
          filtered resource in fe-filesystem-spi and every filesystem jar's MANIFEST.

          Bump the MAJOR on any SPI surface change; the MINOR when only an implementation changes.
          The contract spans fe-filesystem-api, fe-filesystem-spi and fe-extension-spi; the last
          lives under another parent pom, so changing it must bump this property too.
        -->
        <filesystem.plugin.api.version>1.0</filesystem.plugin.api.version>
    </properties>
```

在同文件既有 `<build>` 内，`</pluginManagement>` 之后、`</build>` 之前追加：

```xml
        <plugins>
            <!--
              Stamp every filesystem jar with the API version it was built against. Inherited by all
              child modules so the number appears in exactly one place in the tree.
            -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <configuration>
                    <archive>
                        <manifestEntries>
                            <Doris-Filesystem-Plugin-Api-Version>${filesystem.plugin.api.version}</Doris-Filesystem-Plugin-Api-Version>
                        </manifestEntries>
                    </archive>
                </configuration>
            </plugin>
        </plugins>
```

- [ ] **Step 2: 建 filtered resource**

```properties
# fe/fe-filesystem/fe-filesystem-spi/src/main/resources/META-INF/doris/filesystem-plugin-api-version.properties
# Generated from the filesystem.plugin.api.version property in fe/fe-filesystem/pom.xml.
# Do not edit the value here - edit the property.
version=${filesystem.plugin.api.version}
```

`fe/fe-filesystem/fe-filesystem-spi/pom.xml` 已有 `<build>`（`:74`）且其首个子元素是 `<plugins>`（`:76`）。
把下面的 `<resources>` 块插在 `<build>` 之后、`<plugins>` 之前（**不要**新建 `<build>`）：

```xml
        <resources>
            <!-- Filtering is enabled ONLY for the API version file; filtering the whole directory
                 would also rewrite META-INF/services descriptors. -->
            <resource>
                <directory>src/main/resources</directory>
                <filtering>false</filtering>
                <excludes>
                    <exclude>META-INF/doris/filesystem-plugin-api-version.properties</exclude>
                </excludes>
            </resource>
            <resource>
                <directory>src/main/resources</directory>
                <filtering>true</filtering>
                <includes>
                    <include>META-INF/doris/filesystem-plugin-api-version.properties</include>
                </includes>
            </resource>
        </resources>
```

- [ ] **Step 3: 验证 filtering 生效**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-filesystem/fe-filesystem-spi -am \
    package -DskipTests -Dcheckstyle.skip=true
unzip -p /mnt/disk1/yy/git/wt-catalog-spi/fe/fe-filesystem/fe-filesystem-spi/target/fe-filesystem-spi-*.jar \
    META-INF/doris/filesystem-plugin-api-version.properties
```

Expected: `version=1.0`

- [ ] **Step 4: 接线 `FileSystemPluginManager`**

在 `PLUGIN_FAMILY` 常量（`:93`）之后加入：

```java
    /** Manifest attribute a filesystem plugin uses to declare the API version it was built against. */
    private static final String API_VERSION_ATTRIBUTE = "Doris-Filesystem-Plugin-Api-Version";

    /** Build-filtered resource in fe-filesystem-spi carrying this FE's expected API version. */
    private static final String API_VERSION_RESOURCE =
            "/META-INF/doris/filesystem-plugin-api-version.properties";
```

在 `classLoadingPolicy` 字段之后加入：

```java
    /**
     * Refuses a directory plugin built against an incompatible filesystem SPI.
     *
     * <p>Anchored on {@link FileSystemProvider} so the resource resolves on the classloader that has
     * fe-filesystem-spi. Constructed eagerly: a missing resource is a build defect and must stop FE
     * startup rather than silently disable the check.
     */
    private final ApiVersionGate apiVersionGate = new ApiVersionGate(
            API_VERSION_ATTRIBUTE,
            ApiVersionGate.readExpectedFromClasspath(
                    FileSystemProvider.class, API_VERSION_RESOURCE, "version"));
```

补 import：`import org.apache.doris.extension.loader.ApiVersionGate;`

把该类中 `runtimeManager.loadAll(...)` 的调用改为 5 参、末位传 `apiVersionGate`（与 Task 4 Step 5c 同形）。

- [ ] **Step 5: 编译验证**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am \
    test-compile -Dcheckstyle.skip=true
```

Expected: `BUILD SUCCESS`

- [ ] **Step 6: 验证 filesystem 插件 jar 带上属性**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-filesystem/fe-filesystem-s3 -am \
    package -DskipTests -Dcheckstyle.skip=true
unzip -p /mnt/disk1/yy/git/wt-catalog-spi/fe/fe-filesystem/fe-filesystem-s3/target/fe-filesystem-s3-*.jar \
    META-INF/MANIFEST.MF | grep -i 'Doris-Filesystem-Plugin-Api-Version'
```

Expected: `Doris-Filesystem-Plugin-Api-Version: 1.0`

- [ ] **Step 7: 提交**

```bash
git add fe/fe-filesystem/ fe/fe-core/src/main/java/org/apache/doris/fs/FileSystemPluginManager.java
git commit -m "[feat](plugin) wire the FILESYSTEM family to the API version gate

The family had no version check at all. Same four-part shape as CONNECTOR: one
property in the family parent pom, a filtered resource in fe-filesystem-spi, a
manifest entry inherited by all 14 plugin modules, and a gate passed to loadAll.
The version is independent of the other families - bumping it rebuilds nothing
outside fe-filesystem."
```

---

## Task 6: AUTHENTICATION 族接线

**Files:**
- Modify: `fe/fe-authentication/pom.xml`
- Modify: `fe/fe-authentication/fe-authentication-spi/pom.xml`
- Create: `fe/fe-authentication/fe-authentication-spi/src/main/resources/META-INF/doris/authentication-plugin-api-version.properties`
- Modify: `fe/fe-authentication/fe-authentication-handler/src/main/java/org/apache/doris/authentication/handler/AuthenticationPluginManager.java`
- Modify: `fe/fe-core/src/main/java/org/apache/doris/authentication/AuthenticationIntegrationRuntime.java:293-314`

**Interfaces:**
- Consumes: 同 Task 5
- Produces: manifest 属性 `Doris-Authentication-Plugin-Api-Version`；resource `/META-INF/doris/authentication-plugin-api-version.properties`；property `authentication.plugin.api.version`

> **本族与另外三族的差别**：加载是**懒的**（首次用到某认证类型时才 `loadAll`），失败要经
> `AuthenticationException` 冒泡。被拒插件不进 `factories`，若不额外处理，用户只会看到
> "No authentication plugin factory found for type"，无从诊断——Step 5 就是为此。

- [ ] **Step 1: 加 property 与 manifest 配置**

在 `fe/fe-authentication/pom.xml` 的 `</modules>` 之后插入：

```xml
    <properties>
        <!--
          The AUTHENTICATION family's plugin API version. Single source of truth: it stamps both the
          filtered resource in fe-authentication-spi and every authentication jar's MANIFEST.

          Bump the MAJOR on any SPI surface change; the MINOR when only an implementation changes.
          The contract spans fe-authentication-api, fe-authentication-spi and fe-extension-spi; the
          last lives under another parent pom, so changing it must bump this property too.
        -->
        <authentication.plugin.api.version>1.0</authentication.plugin.api.version>
    </properties>
```

`fe/fe-authentication/pom.xml` **没有** `<build>` 节，整段新建，置于上面刚加的 `</properties>` 之后：

```xml
    <build>
        <plugins>
            <!-- Stamp every authentication jar with the API version it was built against. -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <configuration>
                    <archive>
                        <manifestEntries>
                            <Doris-Authentication-Plugin-Api-Version>${authentication.plugin.api.version}</Doris-Authentication-Plugin-Api-Version>
                        </manifestEntries>
                    </archive>
                </configuration>
            </plugin>
        </plugins>
    </build>
```

- [ ] **Step 2: 建 filtered resource**

```properties
# fe/fe-authentication/fe-authentication-spi/src/main/resources/META-INF/doris/authentication-plugin-api-version.properties
# Generated from the authentication.plugin.api.version property in fe/fe-authentication/pom.xml.
# Do not edit the value here - edit the property.
version=${authentication.plugin.api.version}
```

`fe/fe-authentication/fe-authentication-spi/pom.xml` **没有** `<build>` 节，整段新建：

```xml
    <build>
        <resources>
            <!-- Filtering is enabled ONLY for the API version file; filtering the whole directory
                 would also rewrite META-INF/services descriptors. -->
            <resource>
                <directory>src/main/resources</directory>
                <filtering>false</filtering>
                <excludes>
                    <exclude>META-INF/doris/authentication-plugin-api-version.properties</exclude>
                </excludes>
            </resource>
            <resource>
                <directory>src/main/resources</directory>
                <filtering>true</filtering>
                <includes>
                    <include>META-INF/doris/authentication-plugin-api-version.properties</include>
                </includes>
            </resource>
        </resources>
    </build>
```

- [ ] **Step 3: 验证 filtering 生效**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-authentication/fe-authentication-spi -am \
    package -DskipTests -Dcheckstyle.skip=true
unzip -p /mnt/disk1/yy/git/wt-catalog-spi/fe/fe-authentication/fe-authentication-spi/target/fe-authentication-spi-*.jar \
    META-INF/doris/authentication-plugin-api-version.properties
```

Expected: `version=1.0`

- [ ] **Step 4: 接线 `AuthenticationPluginManager`**

在 `AUTH_PARENT_FIRST_PREFIXES`（`:62`）之后加入：

```java
    /** Manifest attribute an authentication plugin uses to declare its API version. */
    private static final String API_VERSION_ATTRIBUTE = "Doris-Authentication-Plugin-Api-Version";

    /** Build-filtered resource in fe-authentication-spi carrying this FE's expected API version. */
    private static final String API_VERSION_RESOURCE =
            "/META-INF/doris/authentication-plugin-api-version.properties";

    /**
     * Refuses a directory plugin built against an incompatible authentication SPI.
     *
     * <p>Unlike the other families this manager loads lazily, on the login path. A refused plugin
     * therefore surfaces as "no factory for type" rather than as a startup log line, which is why
     * {@code AuthenticationIntegrationRuntime} folds the rejection reasons into its exception.
     */
    private static final ApiVersionGate API_VERSION_GATE = new ApiVersionGate(
            API_VERSION_ATTRIBUTE,
            ApiVersionGate.readExpectedFromClasspath(
                    AuthenticationPluginFactory.class, API_VERSION_RESOURCE, "version"));
```

补 import：`import org.apache.doris.extension.loader.ApiVersionGate;`

把 `loadAll` 内的 `runtimeManager.loadAll(...)`（`:141`）改为 5 参、末位传 `API_VERSION_GATE`。

- [ ] **Step 5: 让拒绝原因进到异常消息**

`AuthenticationPluginManager.loadAll` 需要把版本类失败暴露给调用方。在该方法内、处理
`report.getFailures()` 的位置（若无则在 `LoadReport` 取回后）加入收集逻辑，并新增一个 getter：

```java
    /** Version-rejection reasons from the most recent {@link #loadAll}, newest call wins. */
    private volatile List<String> lastApiVersionRejections = Collections.emptyList();

    /**
     * Reasons plugins were refused for an incompatible API version during the last load.
     *
     * <p>Exposed because this family loads lazily: without it a refused plugin is indistinguishable
     * from one that was never installed, and the operator has no way to tell an upgrade mistake from
     * a missing deployment.
     */
    public List<String> getLastApiVersionRejections() {
        return lastApiVersionRejections;
    }
```

在 `loadAll` 内取回 `report` 之后加入：

```java
        List<String> rejections = new ArrayList<>();
        for (LoadFailure failure : report.getFailures()) {
            if (LoadFailure.STAGE_API_VERSION.equals(failure.getStage())) {
                rejections.add(failure.getMessage());
            }
        }
        lastApiVersionRejections = Collections.unmodifiableList(rejections);
```

补 import：`java.util.ArrayList`、`java.util.List`、`java.util.Collections`、
`org.apache.doris.extension.loader.LoadFailure`（按已存在情况增补）。

- [ ] **Step 6: 改 `AuthenticationIntegrationRuntime.ensurePluginFactoryLoaded`**

把 `:308-313` 的末段：

```java
        if (!pluginManager.hasFactory(pluginType)) {
            throw new AuthenticationException(
                    "No authentication plugin factory found for type: " + pluginType,
                    AuthenticationFailureType.MISCONFIGURED);
        }
```

改为：

```java
        if (!pluginManager.hasFactory(pluginType)) {
            // A plugin refused for an incompatible API version is absent from the factory map, so
            // without this the operator sees "not found" for a plugin that is installed and merely
            // built against another Doris release.
            List<String> rejections = pluginManager.getLastApiVersionRejections();
            String detail = rejections.isEmpty()
                    ? ""
                    : " Some plugins were refused for an incompatible API version: "
                            + String.join("; ", rejections);
            throw new AuthenticationException(
                    "No authentication plugin factory found for type: " + pluginType + "." + detail,
                    AuthenticationFailureType.MISCONFIGURED);
        }
```

补 import `java.util.List`（若缺）。

- [ ] **Step 7: 编译验证**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am \
    test-compile -Dcheckstyle.skip=true
```

Expected: `BUILD SUCCESS`

- [ ] **Step 8: 跑既有认证测试**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-authentication/fe-authentication-handler -am \
    test -Dcheckstyle.skip=true
```

Expected: 全绿（ldap / password 走 ServiceLoader 内建路径，按 spec §5.2 豁免，不受影响）

- [ ] **Step 9: 提交**

```bash
git add fe/fe-authentication/ \
        fe/fe-core/src/main/java/org/apache/doris/authentication/AuthenticationIntegrationRuntime.java
git commit -m "[feat](plugin) wire the AUTHENTICATION family to the API version gate

This family loads lazily on the login path, so a refused plugin surfaces as
'no factory for type' rather than a startup log line. Carry the rejection
reasons through to that exception: otherwise an installed plugin built against
another Doris release is indistinguishable from one that was never deployed.

The in-tree ldap and password plugins go through ServiceLoader and stay exempt."
```

---

## Task 7: LINEAGE 族接线

**Files:**
- Modify: `fe/fe-core/pom.xml`（加 property + `<resources>` filtering）
- Create: `fe/fe-core/src/main/resources/META-INF/doris/lineage-plugin-api-version.properties`
- Modify: `fe/fe-core/src/main/java/org/apache/doris/nereids/lineage/LineageEventProcessor.java:70`、`:159-162`

**Interfaces:**
- Consumes: 同 Task 5
- Produces: manifest 属性 `Doris-Lineage-Plugin-Api-Version`；resource `/META-INF/doris/lineage-plugin-api-version.properties`；property `lineage.plugin.api.version`

> **本族的差别**：SPI（`LineagePluginFactory` / `LineagePlugin`）就在 fe-core 里，没有独立
> artifact；树内零实现，目录路径只服务第三方。因此**不需要** `maven-jar-plugin`
> `manifestEntries`（fe-core 自己不是插件），只需 property + filtered resource。

- [ ] **Step 1: 加 property**

在 `fe/fe-core/pom.xml` 的 `<properties>` 内加入（若无该节则新建）：

```xml
        <!--
          The LINEAGE family's plugin API version. Unlike the other three families the SPI
          (LineagePluginFactory / LineagePlugin) lives inside fe-core itself, so this property feeds
          only the filtered resource this module reads; there is no in-tree lineage plugin to stamp.
          Third-party lineage plugins declare Doris-Lineage-Plugin-Api-Version in their own jars.

          Bump the MAJOR on any change to the lineage SPI surface, or to fe-extension-spi.
        -->
        <lineage.plugin.api.version>1.0</lineage.plugin.api.version>
```

- [ ] **Step 2: 建 filtered resource**

```properties
# fe/fe-core/src/main/resources/META-INF/doris/lineage-plugin-api-version.properties
# Generated from the lineage.plugin.api.version property in fe/fe-core/pom.xml.
# Do not edit the value here - edit the property.
version=${lineage.plugin.api.version}
```

`fe/fe-core/pom.xml` **已有** `<resources>`（`:805-815`），必须**改**它而不是新建。现状是：

```xml
        <resources>
            <resource>
                <directory>target/generated-sources</directory>
            </resource>
            <resource>
                <directory>src/main/resources</directory>
                <includes>
                    <include>**/*.*</include>
                </includes>
            </resource>
        </resources>
```

整块替换为（给既有 `src/main/resources` entry 加一条 exclude，再追加一条只作用于该文件的
filtered entry；`target/generated-sources` entry 逐字不动）：

```xml
        <resources>
            <resource>
                <directory>target/generated-sources</directory>
            </resource>
            <!--
              The API version file is excluded here and re-added below with filtering on. Filtering
              this whole directory instead would rewrite fe-core's service descriptors and config
              templates, where a stray ${...} would be silently substituted.
            -->
            <resource>
                <directory>src/main/resources</directory>
                <includes>
                    <include>**/*.*</include>
                </includes>
                <excludes>
                    <exclude>META-INF/doris/lineage-plugin-api-version.properties</exclude>
                </excludes>
            </resource>
            <resource>
                <directory>src/main/resources</directory>
                <filtering>true</filtering>
                <includes>
                    <include>META-INF/doris/lineage-plugin-api-version.properties</include>
                </includes>
            </resource>
        </resources>
```

- [ ] **Step 3: 验证 filtering 生效**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am \
    process-resources -Dcheckstyle.skip=true
cat /mnt/disk1/yy/git/wt-catalog-spi/fe/fe-core/target/classes/META-INF/doris/lineage-plugin-api-version.properties
```

Expected: `version=1.0`

- [ ] **Step 4: 接线 `LineageEventProcessor`**

在 `PLUGIN_FAMILY`（`:70`）之后加入：

```java
    /** Manifest attribute a lineage plugin uses to declare the API version it was built against. */
    private static final String API_VERSION_ATTRIBUTE = "Doris-Lineage-Plugin-Api-Version";

    /** Build-filtered resource carrying this FE's expected lineage plugin API version. */
    private static final String API_VERSION_RESOURCE =
            "/META-INF/doris/lineage-plugin-api-version.properties";

    /**
     * Refuses a directory plugin built against an incompatible lineage SPI.
     *
     * <p>The lineage SPI lives in fe-core itself, so the anchor class and the resource come from the
     * same module. There is no in-tree lineage plugin; this gate exists for third-party ones.
     */
    private static final ApiVersionGate API_VERSION_GATE = new ApiVersionGate(
            API_VERSION_ATTRIBUTE,
            ApiVersionGate.readExpectedFromClasspath(
                    LineagePluginFactory.class, API_VERSION_RESOURCE, "version"));
```

补 import：`import org.apache.doris.extension.loader.ApiVersionGate;`

把 `:159-162` 的调用改为 5 参：

```java
            LoadReport<LineagePluginFactory> report = runtimeManager.loadAll(
                    pluginRoots, getClass().getClassLoader(),
                    LineagePluginFactory.class, policy, API_VERSION_GATE);
```

- [ ] **Step 5: 编译并跑 fe-core lineage 测试**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am \
    test -Dtest='*Lineage*' -DfailIfNoTests=false -Dcheckstyle.skip=true
```

Expected: 全绿（无 lineage 测试时输出 "No tests to run"，也可接受）

- [ ] **Step 6: 全反应堆编译（含测试源）**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test-compile -Dcheckstyle.skip=true
```

Expected: `BUILD SUCCESS`。这是四族接线完成后最强的单一信号——它会暴露任何遗漏的 `loadAll` 调用点或残留的 `apiVersion` 引用。

- [ ] **Step 7: 提交**

```bash
git add fe/fe-core/pom.xml fe/fe-core/src/main/resources/META-INF/doris/ \
        fe/fe-core/src/main/java/org/apache/doris/nereids/lineage/LineageEventProcessor.java
git commit -m "[feat](plugin) wire the LINEAGE family to the API version gate

The lineage SPI lives in fe-core itself rather than a separate artifact, so the
property feeds only the filtered resource this module reads - there is no
in-tree lineage plugin to stamp. The gate exists for third-party plugins, which
declare the attribute in their own jars.

All four families are now gated, each with an independent version."
```

---

## Task 8: 四族顶层契约表面基线

**Files:**
- Create: `fe/fe-connector/fe-connector-spi/src/test/java/org/apache/doris/connector/spi/ConnectorPluginContractSurfaceTest.java`
- Create: `fe/fe-connector/fe-connector-spi/src/test/resources/connector-plugin-contract-surface.txt`
- Create: `fe/fe-filesystem/fe-filesystem-spi/src/test/java/org/apache/doris/filesystem/spi/FilesystemPluginContractSurfaceTest.java`
- Create: `fe/fe-filesystem/fe-filesystem-spi/src/test/resources/filesystem-plugin-contract-surface.txt`
- Create: `fe/fe-authentication/fe-authentication-spi/src/test/java/org/apache/doris/authentication/spi/AuthenticationPluginContractSurfaceTest.java`
- Create: `fe/fe-authentication/fe-authentication-spi/src/test/resources/authentication-plugin-contract-surface.txt`
- Create: `fe/fe-core/src/test/java/org/apache/doris/nereids/lineage/LineagePluginContractSurfaceTest.java`
- Create: `fe/fe-core/src/test/resources/lineage-plugin-contract-surface.txt`

**Interfaces:**
- Consumes: 无（纯反射，不依赖前序任务的类型）
- Produces: 四份录制基线。任何 SPI 表面变化都会让对应基线变红，失败信息指向要 bump 的 property。

> **关于四份重复的反射 helper**：这四个模块分属不同 maven 树、依赖互不相交，为 25 行 helper
> 建 test-jar 依赖网不划算。四份自包含副本是**有意为之**，不是疏漏。

- [ ] **Step 1: 写 CONNECTOR 基线测试（先不建 .txt，让它红）**

```java
// fe/fe-connector/fe-connector-spi/src/test/java/org/apache/doris/connector/spi/ConnectorPluginContractSurfaceTest.java
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.connector.spi;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginContext;
import org.apache.doris.extension.spi.PluginFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

/**
 * Freezes the surface of the contract a CONNECTOR plugin implements or calls.
 *
 * <p><b>Why this exists:</b> the version scheme defines every SPI surface change as a MAJOR change,
 * which obliges whoever changes the surface to bump
 * {@code connector.plugin.api.version} in fe/fe-connector/pom.xml. Nothing else enforces that —
 * these interfaces are full of default methods, so the compiler stays silent when one is added,
 * removed, or given a different signature. This test is the signal.
 *
 * <p><b>What it deliberately does NOT freeze:</b> the rest of fe-connector-api. Freezing every
 * public type would turn internal refactors red, and a baseline that cries wolf stops being read.
 * What is frozen here is the contract itself — what a plugin implements and what it calls.
 *
 * <p><b>It cannot close the loop.</b> A unit test sees only the current state, never "what changed
 * since last time": refreshing the baseline without bumping the property leaves this test green. The
 * gap is closed by review, which is why the failure message names the property explicitly.
 *
 * <p><b>To regenerate:</b> run this test and copy the "actual" set from the failure message.
 */
public class ConnectorPluginContractSurfaceTest {

    private static final String BASELINE_RESOURCE = "/connector-plugin-contract-surface.txt";

    private static final String BUMP_REMINDER =
            "The CONNECTOR plugin contract surface changed. Any surface change is a MAJOR change: "
            + "bump the major of <connector.plugin.api.version> in fe/fe-connector/pom.xml in THIS "
            + "SAME commit, then refresh this baseline from the actual set below.";

    /**
     * The types a connector plugin implements or calls. fe-extension-spi's three types are included
     * in all four family baselines on purpose: they are shared, so changing them is a major change
     * for every family at once, and each family's baseline has to say so.
     */
    private static final List<Class<?>> CONTRACT = Arrays.asList(
            ConnectorProvider.class,
            ConnectorContext.class,
            Connector.class,
            Plugin.class,
            PluginFactory.class,
            PluginContext.class);

    @Test
    void contractSurfaceMatchesBaseline() throws IOException {
        TreeSet<String> actual = new TreeSet<>();
        for (Class<?> type : CONTRACT) {
            for (Method m : type.getDeclaredMethods()) {
                if (m.isSynthetic() || m.isBridge()) {
                    continue;
                }
                actual.add(signatureOf(type, m));
            }
        }

        TreeSet<String> baseline = readBaseline();

        Assertions.assertEquals(baseline, actual,
                BUMP_REMINDER + "\n\nactual:\n" + String.join("\n", actual));
    }

    private static String signatureOf(Class<?> owner, Method m) {
        StringBuilder sb = new StringBuilder();
        sb.append(owner.getSimpleName()).append('#').append(m.getName()).append('(');
        Class<?>[] params = m.getParameterTypes();
        for (int i = 0; i < params.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(params[i].getName());
        }
        sb.append(')').append(':').append(m.getReturnType().getName());
        return sb.toString();
    }

    private static TreeSet<String> readBaseline() throws IOException {
        TreeSet<String> baseline = new TreeSet<>();
        try (InputStream in = ConnectorPluginContractSurfaceTest.class
                .getResourceAsStream(BASELINE_RESOURCE)) {
            Assertions.assertNotNull(in, "baseline resource missing: " + BASELINE_RESOURCE);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                    baseline.add(trimmed);
                }
            }
        }
        return baseline;
    }
}
```

- [ ] **Step 2: 建空基线并跑测试取实际集合**

```bash
printf '# CONNECTOR plugin contract surface. Regenerate from the test failure message.\n' \
  > /mnt/disk1/yy/git/wt-catalog-spi/fe/fe-connector/fe-connector-spi/src/test/resources/connector-plugin-contract-surface.txt

mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-connector/fe-connector-spi -am \
    test -Dtest=ConnectorPluginContractSurfaceTest -DfailIfNoTests=false -Dcheckstyle.skip=true
```

Expected: FAIL，失败信息里 `actual:` 之后是完整签名集合

- [ ] **Step 3: 把 actual 集合写进基线，重跑至绿**

把失败信息中 `actual:` 之后的每一行（逐字，不改顺序不改空白）追加到
`connector-plugin-contract-surface.txt`，然后：

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-connector/fe-connector-spi -am \
    test -Dtest=ConnectorPluginContractSurfaceTest -DfailIfNoTests=false -Dcheckstyle.skip=true
```

Expected: PASS

- [ ] **Step 4: FILESYSTEM 基线**

复制 Step 1 的测试到
`fe/fe-filesystem/fe-filesystem-spi/src/test/java/org/apache/doris/filesystem/spi/FilesystemPluginContractSurfaceTest.java`，
并作如下修改（其余逐字保留）：

- `package org.apache.doris.filesystem.spi;`
- 类名 `FilesystemPluginContractSurfaceTest`（含 `readBaseline` 内的类字面量）
- `BASELINE_RESOURCE = "/filesystem-plugin-contract-surface.txt"`
- `BUMP_REMINDER` 中的 `CONNECTOR` → `FILESYSTEM`、
  `<connector.plugin.api.version> in fe/fe-connector/pom.xml` →
  `<filesystem.plugin.api.version> in fe/fe-filesystem/pom.xml`
- import 去掉 `org.apache.doris.connector.api.Connector` 与 `ConnectorProvider`/`ConnectorContext` 相关
- `CONTRACT` 改为：

```java
    private static final List<Class<?>> CONTRACT = Arrays.asList(
            FileSystemProvider.class,
            ObjFileSystem.class,
            ObjStorage.class,
            Plugin.class,
            PluginFactory.class,
            PluginContext.class);
```

- 类 javadoc 首句改为 `Freezes the surface of the contract a FILESYSTEM plugin implements or calls.`

然后照 Step 2–3 生成 `filesystem-plugin-contract-surface.txt`：

```bash
printf '# FILESYSTEM plugin contract surface. Regenerate from the test failure message.\n' \
  > /mnt/disk1/yy/git/wt-catalog-spi/fe/fe-filesystem/fe-filesystem-spi/src/test/resources/filesystem-plugin-contract-surface.txt

mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-filesystem/fe-filesystem-spi -am \
    test -Dtest=FilesystemPluginContractSurfaceTest -DfailIfNoTests=false -Dcheckstyle.skip=true
```

把 actual 写入后重跑至 PASS。

- [ ] **Step 5: AUTHENTICATION 基线**

同样复制到
`fe/fe-authentication/fe-authentication-spi/src/test/java/org/apache/doris/authentication/spi/AuthenticationPluginContractSurfaceTest.java`，改：

- `package org.apache.doris.authentication.spi;`
- 类名 `AuthenticationPluginContractSurfaceTest`
- `BASELINE_RESOURCE = "/authentication-plugin-contract-surface.txt"`
- `BUMP_REMINDER` 指向 `<authentication.plugin.api.version> in fe/fe-authentication/pom.xml`
- `CONTRACT`：

```java
    private static final List<Class<?>> CONTRACT = Arrays.asList(
            AuthenticationPluginFactory.class,
            AuthenticationPlugin.class,
            Plugin.class,
            PluginFactory.class,
            PluginContext.class);
```

```bash
printf '# AUTHENTICATION plugin contract surface. Regenerate from the test failure message.\n' \
  > /mnt/disk1/yy/git/wt-catalog-spi/fe/fe-authentication/fe-authentication-spi/src/test/resources/authentication-plugin-contract-surface.txt

mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-authentication/fe-authentication-spi -am \
    test -Dtest=AuthenticationPluginContractSurfaceTest -DfailIfNoTests=false -Dcheckstyle.skip=true
```

把 actual 写入后重跑至 PASS。

- [ ] **Step 6: LINEAGE 基线**

复制到 `fe/fe-core/src/test/java/org/apache/doris/nereids/lineage/LineagePluginContractSurfaceTest.java`，改：

- `package org.apache.doris.nereids.lineage;`
- 类名 `LineagePluginContractSurfaceTest`
- `BASELINE_RESOURCE = "/lineage-plugin-contract-surface.txt"`
- `BUMP_REMINDER` 指向 `<lineage.plugin.api.version> in fe/fe-core/pom.xml`
- `CONTRACT`：

```java
    private static final List<Class<?>> CONTRACT = Arrays.asList(
            LineagePluginFactory.class,
            LineagePlugin.class,
            Plugin.class,
            PluginFactory.class,
            PluginContext.class);
```

```bash
printf '# LINEAGE plugin contract surface. Regenerate from the test failure message.\n' \
  > /mnt/disk1/yy/git/wt-catalog-spi/fe/fe-core/src/test/resources/lineage-plugin-contract-surface.txt

mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am \
    test -Dtest=LineagePluginContractSurfaceTest -DfailIfNoTests=false -Dcheckstyle.skip=true
```

把 actual 写入后重跑至 PASS。

- [ ] **Step 7: 验证基线真的会红（变异验证）**

这是本任务唯一值得做变异的地方——基线测试是"改 SPI 表面必须 bump"的**唯一护栏**，
若它其实抓不住变化，整个 §7 的防漂移就是空的。

```bash
# 临时给 ConnectorProvider 加一个 default 方法
python3 - <<'PY'
import re
p = "/mnt/disk1/yy/git/wt-catalog-spi/fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorProvider.java"
s = open(p).read()
s = s.replace("    @Override\n    default String name() {",
              "    default boolean mutationProbe() {\n        return false;\n    }\n\n    @Override\n    default String name() {", 1)
open(p, "w").write(s)
PY

mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-connector/fe-connector-spi -am \
    test -Dtest=ConnectorPluginContractSurfaceTest -DfailIfNoTests=false -Dcheckstyle.skip=true
```

Expected: **FAIL**，且失败信息含 "bump the major of &lt;connector.plugin.api.version&gt;"

```bash
git checkout -- fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorProvider.java
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-connector/fe-connector-spi -am \
    test -Dtest=ConnectorPluginContractSurfaceTest -DfailIfNoTests=false -Dcheckstyle.skip=true
```

Expected: PASS（还原后恢复绿）

- [ ] **Step 8: 全反应堆验证 + checkstyle**

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml test-compile -Dcheckstyle.skip=true
```

Expected: `BUILD SUCCESS`

对本次改动过的模块单独跑 checkstyle（不要全反应堆跑，会卡死）：

```bash
for m in fe-extension-loader fe-connector/fe-connector-spi fe-filesystem/fe-filesystem-spi \
         fe-authentication/fe-authentication-spi fe-core; do
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl "$m" checkstyle:check
done
```

Expected: 每个模块均 `BUILD SUCCESS`

- [ ] **Step 9: 提交**

```bash
git add fe/fe-connector/fe-connector-spi/src/test/ \
        fe/fe-filesystem/fe-filesystem-spi/src/test/ \
        fe/fe-authentication/fe-authentication-spi/src/test/ \
        fe/fe-core/src/test/java/org/apache/doris/nereids/lineage/LineagePluginContractSurfaceTest.java \
        fe/fe-core/src/test/resources/lineage-plugin-contract-surface.txt
git commit -m "[test](plugin) freeze each family's plugin contract surface

Every SPI surface change is a MAJOR change under the version scheme, which
obliges whoever changes it to bump that family's maven property. Nothing else
enforces this: these interfaces are full of default methods, so the compiler
stays silent when one is added, removed, or resigned.

These baselines cannot close the loop - a unit test sees only the current
state, never what changed since last time, so refreshing a baseline without
bumping the property still leaves them green. The remaining gap is review,
which is why each failure message names the exact property.

fe-extension-spi's three types are frozen in all four baselines because they
are shared: changing them is a major change for every family at once."
```

---

## Self-Review

**1. Spec coverage**

| spec 节 | 覆盖任务 |
| --- | --- |
| §3.1 判定规则 | Task 2（`checkDeclared`）+ Task 2 测试双向 minor |
| §3.2 bump 纪律 | Task 4/5/6/7 的 pom 注释 + Task 8 的 `BUMP_REMINDER` |
| §3.3 四族独立 + 共享耦合 | Task 4–7 四个独立 property；Task 8 四份基线均冻结 fe-extension-spi 三类型 |
| §3.4 解析规则 | Task 1 |
| §4 单一来源 | Task 4–7 的 property → filtered resource + manifestEntries；Task 4 Step 3 / 5 Step 3 / 6 Step 3 / 7 Step 3 实测 filtering 生效 |
| §5.1 校验点 | Task 3 Step 5d（工厂实例化后、`PluginHandle` 前）+ `closeClassLoader` |
| §5.2 缺失策略分路径 | Task 3 测试 `rejectsPluginDeclaringNothing`；classpath 豁免体现为 `loadBuiltins` 完全不接 gate |
| §5.3 各族拒绝行为 | Task 4/5/7 走 partial-success；Task 6 Step 5–6 处理懒加载与异常消息 |
| §5.4 期望值读不到即失败 | Task 2 `readExpectedFromClasspath` + `missingResourceFailsLoud` / `missingKeyFailsLoud` |
| §6 删除旧机制 | Task 4 Step 4–7（含全仓 grep 兜底） |
| §7 防漂移 | Task 8（含 Step 7 变异验证基线真的会红） |
| §8 可见性 | Task 3 拒绝消息带双方版本；不改表结构（Global Constraints 已声明） |
| §9 测试策略 | Task 1/2/3/8 逐条对应 |
| §10 改动清单 | Task 4–7 逐项 |

无未覆盖项。§12 runbook 为文档，不产生代码任务。

**2. Placeholder scan** — 无 TBD/TODO；无"similar to Task N"（Task 8 Step 4–6 明确列出每处差异而非泛指）；每个代码步骤均有可直接粘贴的代码块。

**3. Type consistency** — 已核对：`ApiVersion.parse` / `getMajor` / `toString`（Task 1 定义，Task 2 使用）；`ApiVersionGate` 构造器与 `getManifestAttribute` / `checkDeclared`（Task 2 定义，Task 3 使用）；`LoadFailure.STAGE_API_VERSION`（Task 3 定义，Task 3 测试与 Task 6 Step 5 使用）；5 参 `loadAll`（Task 3 定义，Task 4/5/6/7 使用）；`readManifestAttribute` 三参形（Task 3 内自洽）；`getLastApiVersionRejections`（Task 6 Step 5 定义，Step 6 使用）。四族 property / resource / 属性名三元组命名一致。

---

## 已知风险与验证点

1. **filtering 未生效会静默变成"字面 `${...}`"** → 四族各有一个 Step 显式 `unzip -p` / `cat` 验证真实值，不靠猜。
2. **`maven-jar-plugin` 继承范围**大于 8 个插件模块（`fe-connector-api`、`fe-connector-spi` 等也会被打上属性）→ 无害，且给 SPI jar 自己打上属性反而是一层交叉验证。
3. **shade 模块**（`fe-connector-hms-hive-shade` / `fe-connector-paimon-hive-shade`）用 `maven-shade-plugin` 重建 MANIFEST，属性可能不落地 → 无影响，它们不是 provider jar，加载器只读定义 provider 类的那个 jar。
4. **删除类改动不能只信 `test-compile`**（增量编译会跳过未改模块，陈旧 class 留到运行期 `NoSuchMethodError`）→ Task 4 Step 7 用全仓 grep 兜底。
5. **`fe-core` 已有 `<resources>`**（`fe/fe-core/pom.xml:805-815`，含一条 `target/generated-sources`
   与一条 `src/main/resources`）→ Task 7 Step 2 给的是**整块替换后的完整内容**，照抄即可；
   切勿只追加新 entry 而丢掉 `target/generated-sources`（antlr 生成物走那条，丢了 FE 起不来）。
6. **四族 pom 起点不一**，已在各 Step 写死，不要凭印象：`fe-connector-spi` / `fe-filesystem-spi`
   有 `<build>` 无 `<resources>`（插在 `<plugins>` 前）；`fe-authentication/pom.xml` 与
   `fe-authentication-spi/pom.xml` 两者都**没有** `<build>`（整段新建）。
