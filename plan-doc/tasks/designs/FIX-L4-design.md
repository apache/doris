# FIX-L4 — trino plugin.dir 首胜单例静默用旧 → fail-loud

> 来源：reverify §1 表 L4（原 P2-2）。🟡 低（多目录不同 plugin.dir 时静默用错插件）。范围：`TrinoBootstrap` 单例。
> HEAD 复核基线：`4cd63c6911a`。

## Problem / Root Cause

`TrinoBootstrap` 是**进程级单例**（:99 `instance`，一次加载所有插件）。`getInstance(pluginDir)`（:136-145）是
**first-wins**：第一个建的 trino 目录用它的 `pluginDir` 初始化单例；之后目录若传**不同** `plugin.dir`，被**静默忽略**
（返回旧 instance,用第一个 dir 的插件）→ 后建目录可能找不到自己的 connector factory,或悄悄用错插件集。

## Design（存 pluginDir + 不匹配 fail-loud）

- `TrinoBootstrap` 加 `private final String pluginDir;`,构造函数存下。
- `getInstance(pluginDir)`：保持 first-wins 初始化不变;返回前若 `!Objects.equals(instance.pluginDir, pluginDir)`
  → 抛 `IllegalStateException`（响亮报「已用 dir A 初始化,不能再用 dir B」+ 提示同 FE 内 trino 目录须共享单一 plugin dir）。
```java
public static TrinoBootstrap getInstance(String pluginDir) {
    if (instance == null) {
        synchronized (INIT_LOCK) {
            if (instance == null) {
                instance = new TrinoBootstrap(pluginDir);
            }
        }
    }
    if (!java.util.Objects.equals(instance.pluginDir, pluginDir)) {
        throw new IllegalStateException(String.format(
            "TrinoBootstrap already initialized with plugin dir '%s'; cannot reuse for a different "
            + "plugin dir '%s'. All trino-connector catalogs in one FE must share one plugin dir.",
            instance.pluginDir, pluginDir));
    }
    return instance;
}
```

**选 fail-loud（task-list 选项 A）而非删 per-catalog 分支（选项 B）**：A 最小、保留现「first-wins」语义、只把静默变响亮;
B 改功能面（移除 per-catalog `trino.plugin.dir`）属更大设计改动,本条不做。`Objects.equals` 兜 null（虽构造用 `new
File(pluginDir)` 已隐含非空,首次前的比较仍走 null-safe）。

**并发对抗复审 `agent a28dc47095` = SOUND，折入其 1 minor**：裸 `String.equals` 会把**同一物理目录的不同拼写**
（尾斜杠 / 相对 vs 绝对 / symlink）误判为不同 → 良性同目录配置被误抛（false positive）。→ 比较前经 `canonicalize(dir)`
（`new File(dir).getCanonicalPath()`，best-effort，IOException 回退原串,null 直通）。仍只对**真不同**目录抛。另修 nit:
`Objects` 已 import → 去掉全限定。

## Risk

- 单目录 / 多目录同 dir：`equals` 恒 true → 行为完全不变。
- 只有「多目录不同 plugin.dir」这一**本就是误配**的场景从「静默用错」变「启动/建目录即响亮抛」——Rule 12 fail-loud,正向。
- 不碰 fe-core。

## Test Plan

- build-compile。
- 行为 UT 受阻：`getInstance` 走真 `new TrinoBootstrap(pluginDir)`（`pluginManager.loadPlugins()` + attach-self,重副作用,无既有夹具;
  现 `TrinoBootstrapTest` 只测纯 `resolvePluginDir`）→ 不加 getInstance 行为 UT,登记 UT-wall（同 FIX-L3 模式）。
- e2e：两个 trino 目录配不同 `trino.plugin.dir` → 第二个建目录时响亮抛（live）。

## 备注

与 L6 合并一次并发/单例对抗复审。
