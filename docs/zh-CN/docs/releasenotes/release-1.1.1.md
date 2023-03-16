---
{
    "title": "Release 1.1.1",
    "language": "zh-CN"
}
---

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

## 新增功能

### 向量化执行引擎支持 ODBC Sink。

在 1.1.0 版本的向量化执行引擎中 ODBC Sink 是不支持的，而这一功能在之前版本的行存引擎是支持的，因此在 1.1.1 版本中我们重新完善了这一功能。

### 增加简易版 MemTracker

MemTracker 是一个用于分析内存使用情况的统计工具，在 1.1.0 版本的向量化执行引擎中，由于 BE 侧没有 MemTracker，可能出现因内存失控导致的 OOM 问题。在 1.1.1 版本中，BE 侧增加了一个简易版 MemTracker，可以帮助控制内存，并在内存超出时取消查询。

完整版 MemTracker 将在 1.1.2 版本中正式发布。


## 改进

### 支持在 Page Cache 中缓存解压后数据。

在 Page Cache 中有些数据是用 bitshuffle 编码方式压缩的，在查询过程中需要花费大量的时间来解压。在 1.1.1 版本中，Doris 将缓存解压由 bitshuffle 编码的数据以加速查询，我们发现在 ssb-flat 的一些查询中，可以减少 30% 的延时。

## Bug 修复

### 修复无法从 1.0 版本进行滚动升级的问题。

这个问题是在 1.1.0 版本中出现的，当升级 BE 而不升级 FE 时，可能会导致 BE Core。

如果你遇到这个问题，你可以尝试用 [#10833](https://github.com/apache/doris/pull/10833) 来修复它。

### 修复某些查询不能回退到非向量化引擎的问题，并导致 BE Core。

目前，向量化执行引擎不能处理所有的 SQL 查询，一些查询（如 left outer join）将使用非向量化引擎来运行。但部分场景在 1.1.0 版本中未被覆盖到，这可能导致 BE 挂掉。

### 修复 Compaction 不能正常工作导致的 -235 错误。

在 Unique Key 模型中，当一个 Rowset 有多个 Segment 时，在做 Compaction 过程中由于没有正确的统计行数，会导致Compaction 失败并且产生 Tablet 版本过多而导致的 -235 错误。

### 修复查询过程中出现的部分 Segment fault。

[#10961](https://github.com/apache/doris/pull/10961) 
[#10954](https://github.com/apache/doris/pull/10954) 
[#10962](https://github.com/apache/doris/pull/10962)

# 致谢

感谢所有参与贡献 1.1.1 版本的开发者:

```
@jacktengg
@mrhhsg
@xinyiZzz
@yixiutt
@starocean999
@morrySnow
@morningman
@HappenLee
```