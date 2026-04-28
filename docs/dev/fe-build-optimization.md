# Doris FE 编译优化方案

## 1. 目标

修改 `fe-core` 任意一个 `.java` 文件后，`./build.sh --fe` 不应重新清空并全量重建 `fe-core/target/`。本优化让 Doris FE 构建回到 Maven 的标准模型：

- `clean` 只由显式 `mvn clean` 或 `./build.sh --clean` 触发
- `compile/package` 允许 Maven 复用 `target/` 做增量编译
- FE 运行时依赖由可重复、可缓存、无 stale 文件的 assembly 归档生成
- 本地开发构建只运行必要质量门禁，发布、源码包、coverage 等动作放入显式 profile
- `build.sh --fe` 仍是 Doris FE 的统一入口，输出目录结构保持 `output/fe/lib`

## 2. 优化前问题

### 2.1 `fe-core` 在 `initialize` 阶段自动清空 `target/`

[fe/fe-core/pom.xml](../../fe/fe-core/pom.xml) 曾通过 `maven-clean-plugin` 在默认生命周期的 `initialize` 阶段执行 `auto-clean`：

```xml
<execution>
    <id>auto-clean</id>
    <phase>initialize</phase>
    <goals><goal>clean</goal></goals>
</execution>
```

这意味着 `mvn install/package/compile -pl fe-core` 都会先清空 `fe-core/target/`，直接废掉 Maven 编译器基于 `target/classes` 和时间戳的增量能力。`maven-build-cache-extension` 未命中时也会因为 `target/` 被清空而被迫全量 javac。

该机制的历史目的不是编译正确性，而是避免 `target/lib/` 中残留已删除或已升级的运行时 jar。这个问题应该由打包方式解决，而不是在每次普通构建前清空整个模块输出。

### 2.2 `copy-dependencies` 生成运行时 lib 目录不可重复

[fe/fe-core/pom.xml](../../fe/fe-core/pom.xml) 曾用 `maven-dependency-plugin:copy-dependencies` 将运行时依赖复制到 `target/lib/`：

```xml
<outputDirectory>${project.build.directory}/lib</outputDirectory>
<overWriteReleases>false</overWriteReleases>
<overWriteSnapshots>false</overWriteSnapshots>
<overWriteIfNewer>true</overWriteIfNewer>
<includeScope>runtime</includeScope>
```

`copy-dependencies` 只向目标目录追加或覆盖同名文件，不会根据当前依赖图删除目标目录里已经不再需要的 jar。依赖升级或删除后，旧 jar 会继续留在 `target/lib/`，再被 [build.sh](../../build.sh) 拷到 `output/fe/lib`，造成运行时 classpath 污染。

这里的真实风险是运行时污染，而不是编译期遗漏：javac 的 classpath 来自 Maven 当前依赖图，不来自 `target/lib/`。如果代码仍引用被移除的依赖，正常情况下编译会失败；如果旧 jar 残留在 `output/fe/lib`，则可能在运行时掩盖依赖删除、引入版本冲突或改变类加载顺序。

### 2.3 build cache 被迫为 `copy-dependencies` 打补丁

[fe/.mvn/maven-build-cache-config.xml](../../fe/.mvn/maven-build-cache-config.xml) 曾将 `copy-dependencies` 配置为 `runAlways`。原因是 build cache 命中后 Maven 会跳过模块构建，`target/lib/` 这类普通目录不会自动恢复；如果刚执行过 clean，`build.sh` 会找不到 `target/lib/*`。

这形成了错误的职责关系：

- `auto-clean` 用清空 `target/` 规避 stale jar
- `copy-dependencies` 每次重新复制大量 jar
- build cache 又强制 `copy-dependencies` 在 cache hit 时也运行

最终结果是普通增量构建仍要做大量不必要 IO，而且 cache hit 仍可能把新 jar 叠加到旧目录上。

### 2.4 默认生命周期绑定了非日常构建动作

[fe/pom.xml](../../fe/pom.xml) 和部分子模块曾默认绑定若干不属于本地日常编译闭环的插件：

| 插件 | 优化前问题 | 本分支位置 |
|---|---|---|
| `license-maven-plugin:add-third-party` | 每次本地构建都执行依赖许可证收集 | `release` profile |
| `jacoco-maven-plugin:prepare-agent` | 每次本地构建都注入 coverage agent 参数 | `coverage` profile |
| `maven-source-plugin:jar-no-fork` | 普通构建生成源码包 | `release` profile |
| `flatten-maven-plugin` | 发布需要，普通本地编译不应成为关键路径 | `release` profile，或仅保留发布必需 execution |
| `maven-checkstyle-plugin:check` | 质量门禁，不能默认移除 | 继续默认执行，可由 `DISABLE_JAVA_CHECK_STYLE=ON` 显式跳过 |

Checkstyle 是 Doris FE 构建的质量门禁，不能为了优化日常构建直接移到 profile 或默认关闭。优化目标是移除非编译、非质量门禁动作，而不是降低默认质量要求。

## 3. 当前构建形态

### 3.1 Maven 生命周期职责

当前 FE Maven 构建遵循标准生命周期：

- `validate`：执行 enforcer、checkstyle 等质量门禁
- `generate-sources`：只执行真正需要的代码生成
- `compile`：增量 javac
- `test-compile`：仅在需要测试类或 test-jar 时执行
- `package`：生成模块 jar 和可重复的 FE runtime lib 归档
- `install`：仅在需要把当前 reactor 产物安装到本地 Maven 仓库、供 reactor 外构建解析时显式使用
- `clean`：只在显式 clean 时删除输出

`fe-core` 不再在 `initialize` 或其它默认阶段执行 `maven-clean-plugin`。如果需要彻底清理，使用：

```bash
./build.sh --fe --clean
```

或在 `fe/` 下显式运行：

```bash
mvn clean
```

### 3.2 FE runtime lib 使用 assembly 归档生成

`fe-core` 的运行时依赖改由 `maven-assembly-plugin` 生成 zip 归档，替代 `maven-dependency-plugin:copy-dependencies` 直接写 `target/lib/`。assembly 产物是单个 Maven artifact，build cache 可以按归档恢复；`build.sh` 每次清空 `output/fe/lib` 后解压该归档，避免目标运行目录残留 stale jar。

新增的 descriptor：

```xml
<!-- fe/fe-core/src/main/assembly/fe-lib.xml -->
<assembly xmlns="http://maven.apache.org/ASSEMBLY/2.2.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/ASSEMBLY/2.2.0 https://maven.apache.org/xsd/assembly-2.2.0.xsd">
    <id>lib</id>
    <formats>
        <format>zip</format>
    </formats>
    <includeBaseDirectory>false</includeBaseDirectory>
    <dependencySets>
        <dependencySet>
            <outputDirectory>/</outputDirectory>
            <scope>runtime</scope>
            <useProjectArtifact>false</useProjectArtifact>
            <unpack>false</unpack>
        </dependencySet>
    </dependencySets>
</assembly>
```

`fe-core/pom.xml` 中使用 Maven 默认 assembly 命名规则，把 runtime lib zip 作为附加 artifact 参与 package/cache：

```xml
<plugin>
    <artifactId>maven-assembly-plugin</artifactId>
    <configuration>
        <appendAssemblyId>true</appendAssemblyId>
        <outputDirectory>${project.build.directory}</outputDirectory>
        <descriptors>
            <descriptor>src/main/assembly/fe-lib.xml</descriptor>
        </descriptors>
    </configuration>
    <executions>
        <execution>
            <id>make-fe-lib</id>
            <phase>package</phase>
            <goals>
                <goal>single</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```

assembly 输出归档使用 Maven 默认命名规则，由 `build.finalName=doris-fe`、descriptor id `lib` 和 format `zip` 组成，稳定为 `fe/fe-core/target/doris-fe-lib.zip`。[build.sh](../../build.sh) 解压该归档到运行时 lib 目录：

```bash
unzip -q -o "${DORIS_HOME}/fe/fe-core/target/doris-fe-lib.zip" -d "${DORIS_OUTPUT}/fe/lib"
cp -r -p "${DORIS_HOME}/fe/fe-core/target/doris-fe.jar" "${DORIS_OUTPUT}/fe/lib"/
```

如果后续希望改名，必须同时修改 assembly descriptor id 和 `build.sh`，不能只改其中一处。

### 3.3 build cache 缓存 runtime lib 归档

`doris-fe-lib.zip` 是 assembly 附加 artifact，不是普通临时目录。移除 `copy-dependencies` 的 `runAlways` 后，build cache 会随模块产物恢复该 zip；不需要依赖目录型 `attachedOutputs`，也不会出现 cache hit 时旧目录文件未清理的问题。

当前 [fe/.mvn/maven-build-cache-config.xml](../../fe/.mvn/maven-build-cache-config.xml) 满足：

- 删除 `maven-dependency-plugin:copy-dependencies` 的 `runAlways`
- 保留 `maven-checkstyle-plugin` 的 `runAlways`
- 保持 generated sources 恢复能力，避免 cache hit 后缺少生成源码

这样 cache hit 时，`doris-fe.jar` 和 `target/doris-fe-lib.zip` 都来自同一个缓存结果；cache miss 时，assembly 根据当前依赖图重新生成归档。

### 3.4 `build.sh --fe` 的当前行为

`build.sh --fe` 继续作为统一入口，Maven 参数符合日常开发构建：

```bash
mvn package \
    -pl "${FE_MODULES}" \
    -am \
    -Dskip.doc=true \
    -DskipTests \
    -T "${FE_MAVEN_THREADS:-1C}"
```

默认行为：

- 不隐式 clean
- 不写入本地 Maven 仓库
- 不传 `maven.test.skip=true`
- 不默认跳过 checkstyle
- 继续支持 `DISABLE_JAVA_CHECK_STYLE=ON` 用于本地快速实验
- 继续支持 `DISABLE_BUILD_UI=ON` 跳过 UI
- 输出仍拷贝到 `output/fe/lib`

显式 clean 行为：

```bash
./build.sh --fe --clean
```

该命令先执行 `clean_fe()`，再进行正常 FE 构建。无需新增 `--full-clean`；现有 `--clean` 已经表达了“先清理再构建”。

### 3.5 非日常构建动作已进入 profile

Parent POM 保留日常构建必需插件，把发布和覆盖率相关动作放入显式 profile。

`release` profile：

- `flatten-maven-plugin`
- `license-maven-plugin:add-third-party`
- `maven-source-plugin:jar-no-fork`
- 发布所需的其它归档动作

`coverage` profile：

- `jacoco-maven-plugin:prepare-agent`
- coverage report 所需配置

日常构建：

```bash
./build.sh --fe
```

发布构建：

```bash
MVN_OPT="-Prelease" ./build.sh --fe
```

覆盖率构建：

```bash
MVN_OPT="-Pcoverage" ./build.sh --fe
```

### 3.6 test-jar 本次保留默认构建

本分支没有移动 test-jar 绑定。[fe/fe-common/pom.xml](../../fe/fe-common/pom.xml)、[fe/fe-catalog/pom.xml](../../fe/fe-catalog/pom.xml)、[fe/fe-foundation/pom.xml](../../fe/fe-foundation/pom.xml) 和 [fe/fe-type/pom.xml](../../fe/fe-type/pom.xml) 仍在 `test-compile` 阶段默认生成 test-jar，因为这些产物可能被其它模块作为测试工具依赖。

`build.sh --fe` 只传 `-DskipTests`，不传 `maven.test.skip=true`，因此会跳过测试执行但保留测试编译和 test-jar 产物。后续如果要继续优化 test-jar，应先完整梳理哪些模块真实依赖这些 test artifacts：

- 如果只有测试代码依赖，可把 test-jar 绑定放入 `tests` 或 `test-artifacts` profile
- 如果主构建确实依赖 test-jar，应保留默认绑定

这个调整不属于本分支改动，不能只为了缩短构建时间盲目删除。

### 3.7 注解处理器保持构建期生成，后续单独评估

`fe-core` 当前有三段式 annotation processor 编译，用于生成 Nereids pattern 相关代码。把生成结果提交入库可以减少构建动作，但会引入“源注解和生成代码不同步”的维护成本。

本次 Maven 编译优化不改变该机制。只有在完成 runtime lib、auto-clean、profile、cache 修正后，仍确认该处理器是主要瓶颈，才单独设计：

- 生成代码入库
- CI 校验生成结果与源注解一致
- 清晰的开发者再生成命令

## 4. 本分支落地内容

1. 删除 `fe/fe-core/pom.xml` 中 `maven-clean-plugin:auto-clean` execution。
2. 删除 `fe/fe-core/pom.xml` 中 `maven-dependency-plugin:copy-dependencies` execution。
3. 新增 `fe/fe-core/src/main/assembly/fe-lib.xml`。
4. 在 `fe/fe-core/pom.xml` 中新增 `maven-assembly-plugin:single`，明确输出 `target/doris-fe-lib.zip`。
5. 更新 `fe/.mvn/maven-build-cache-config.xml`：
   - 删除 `copy-dependencies` 的 `runAlways`
   - 保留 checkstyle `runAlways`
6. 将 `license`、`source jar`、`jacoco`、发布所需 `flatten` 动作移入显式 profile。
7. 保持 `maven-checkstyle-plugin:check` 默认运行。
8. 保留 test-jar 默认绑定，避免在未完成依赖分析前破坏 test artifacts。
9. 保持 `output/fe/lib` 输出契约不变：从 `fe/fe-core/target/doris-fe-lib.zip` 和 `target/doris-fe.jar` 组装 `output/fe/lib`。

## 5. 验收标准

### 5.1 产物正确性

修改前后分别执行一次干净构建：

```bash
DISABLE_BUILD_UI=ON ./build.sh --fe --clean
```

对比：

- `output/fe/lib/doris-fe.jar` 存在
- `output/fe/lib/` 中 runtime jar 清单符合当前 Maven 依赖图
- 删除或升级一个依赖后，旧版本 jar 不再残留
- `fe/fe-core/target/doris-fe-lib.zip` 解压后的第三方 jar 清单与 `output/fe/lib/` 一致

### 5.2 增量性能

在已有 `target/` 的情况下修改一个 `fe-core` Java 文件：

```bash
DISABLE_BUILD_UI=ON ./build.sh --fe
```

目标：

| 场景 | 目标 |
|---|---|
| 修改一个 `fe-core` `.java` 文件后重 build | 30 秒级 |
| 修改一个上游 FE 子模块后重 build | 1 分钟级 |
| 全新 clean build | 不劣化 |

### 5.3 build cache 行为

必须覆盖 cache hit 路径：

1. 执行一次 clean build，保存 cache。
2. 删除 `fe/fe-core/target/`。
3. 再次执行 `DISABLE_BUILD_UI=ON ./build.sh --fe`。
4. 确认 `fe/fe-core/target/doris-fe.jar` 和 `fe/fe-core/target/doris-fe-lib.zip` 都被恢复或重新生成。
5. 确认 `build.sh` 不再依赖 `copy-dependencies runAlways`。

### 5.4 质量门禁

至少执行：

```bash
DISABLE_BUILD_UI=ON ./build.sh --fe
```

并确认 checkstyle 默认仍然运行。只有显式设置 `DISABLE_JAVA_CHECK_STYLE=ON` 时才允许跳过。

### 5.5 启动烟测

使用新产物启动 FE，确认 classpath 没有因 runtime lib 目录生成方式变化而缺 jar 或加载旧 jar：

```bash
output/fe/bin/start_fe.sh --daemon
```

启动成功后检查 `output/fe/log/fe.log`，确认无 `ClassNotFoundException`、`NoClassDefFoundError`、`NoSuchMethodError` 等依赖相关错误。

## 6. 非目标

- 不优化 BE 或 Cloud 编译。
- 不在本方案中拆分 `fe-core` 巨型模块。
- 不默认关闭 checkstyle。
- 不用 `maven.test.skip=true` 作为日常构建优化。
- 不把 annotation processor 生成代码入库作为本次必需改动。
- 不改变 `output/fe` 的最终发布目录结构。

## 7. 参考

- [Maven Build Cache Extension](https://maven.apache.org/extensions/maven-build-cache-extension/)
- [maven-assembly-plugin: single](https://maven.apache.org/plugins/maven-assembly-plugin/single-mojo.html)
- [maven-dependency-plugin: copy-dependencies](https://maven.apache.org/plugins/maven-dependency-plugin/copy-dependencies-mojo.html)
- 历史 commit：`6c2767c65e6f` Clean the fe/target directory before building (PR #2173)
- 历史 commit：`ad17afef913` Make FE multi module (PR #4099)
