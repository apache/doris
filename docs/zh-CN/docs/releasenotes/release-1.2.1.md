---
{
    "title": "Release 1.2.1",
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

在 1.2.1 版本中，Doris 团队已经修复了自 1.2.0 版本发布以来约 200 个问题或性能改进项。同时，1.2.1 版本也作为 1.2 LTS 的第一个迭代版本，具备更高的稳定性，建议所有用户升级到这个版本。


# 优化改进

### 支持高精度小数 DecimalV3

支持精度更高和性能更好的 DecimalV3，相较于过去版本具有以下优势：

- 可表示范围更大，取值范围都进行了明显扩充，有效数字范围 [1,38]。

- 性能更高，根据不同精度，占用存储空间可自适应调整。

- 支持更完备的精度推演，对于不同的表达式，应用不同的精度推演规则对结果的精度进行推演。

[DecimalV3](https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-reference/Data-Types/DECIMALV3)

### 支持 Iceberg V2

支持 Iceberg V2 (仅支持 Position Delete， Equality Delete 会在后续版本支持)，可以通过 Multi-Catalog 功能访问 Iceberg V2 格式的表。


### 支持 OR 条件转 IN 

支持将 where 条件表达式后的 or 条件转换成 in 条件，在部分场景中可以提升执行效率。 [#15437](https://github.com/apache/doris/pull/15437) [#12872](https://github.com/apache/doris/pull/12872)


### 优化 JSONB 类型的导入和查询性能

优化 JSONB 类型的导入和查询性能，在测试数据上约有 70% 的性能提升。  [#15219](https://github.com/apache/doris/pull/15219)  [#15219](https://github.com/apache/doris/pull/15219)

### Stream load 支持带引号的 CSV 数据 

通过导入任务参数 `trim_double_quotes` 来控制，默认值为 false，为 true 时表示裁剪掉 CSV 文件每个字段最外层的双引号。  [#15241](https://github.com/apache/doris/pull/15241)

### Broker 支持腾讯云 CHDFS 和 百度云 BOS 、AFS 

可以通过 Broker 访问存储在腾讯云 CHDFS 和 百度智能云 BOS、AFS 上的数据。 [#15297](https://github.com/apache/doris/pull/15297) [#15448](https://github.com/apache/doris/pull/15448)

### 新增函数

新增函数 `substring_index`。 [#15373](https://github.com/apache/doris/pull/15373)



# 问题修复

- 修复部分情况下，从 1.1.x 版本升级到 1.2.0 版本后，用户权限信息丢失的问题。 [#15144](https://github.com/apache/doris/pull/15144)

- 修复使用 date/datetimev2 类型进行分区时，分区值错误的问题。 [#15094](https://github.com/apache/doris/pull/15094)

- 修复部分已发布功能的 Bug，具体列表可参阅：[PR List](https://github.com/apache/doris/pulls?q=is%3Apr+label%3Adev%2F1.2.1-merged+is%3Aclosed)


# 升级注意事项

### 已知问题

- 请勿使用 JDK11 作为 BE 的运行时 JDK，会导致 BE Crash。

- 该版本对csv格式的读取性能有下降，会影响csv格式的导入和读取效率，我们会在下一个三位版本尽快修复

### 行为改变

- BE 配置项 `high_priority_flush_thread_num_per_store` 默认值由 1 改成 6 ，以提升 Routine Load 的写入效率。[#14775](https://github.com/apache/doris/pull/14775)

- FE 配置项 `enable_new_load_scan_node` 默认值改为 true ，将使用新的 File Scan Node 执行导入任务，对用户无影响。 [#14808](https://github.com/apache/doris/pull/14808)

- 删除 FE 配置项 `enable_multi_catalog`，默认开启 Multi-Catalog 功能。

- 默认强制开启向量化执行引擎。会话变量 `enable_vectorized_engine` 将不再生效，如需重新生效，需将 FE 配置项 `disable_enable_vectorized_engine` 设为 false，并重启 FE。 [#15213](https://github.com/apache/doris/pull/15213)

# 致谢

有 45 位贡献者参与到 1.2.1 版本的开发与完善中，感谢他们的付出，他们分别是：

@adonis0147

@AshinGau

@BePPPower

@BiteTheDDDDt

@ByteYue

@caiconghui

@cambyzju

@chenlinzhong

@dataroaring

@Doris-Extras

@dutyu

@eldenmoon

@englefly

@freemandealer

@Gabriel39

@HappenLee

@Henry2SS

@hf200012

@jacktengg

@Jibing-Li

@Kikyou1997

@liaoxin01

@luozenglin

@morningman

@morrySnow

@mrhhsg

@nextdreamblue

@qidaye

@spaces-X

@starocean999

@wangshuo128

@weizuo93

@wsjz

@xiaokang

@xinyiZzz

@xutaoustc

@yangzhg

@yiguolei

@yixiutt

@Yulei-Yang

@yuxuan-luo

@zenoyang

@zhangstar333

@zhannngchen

@zhengshengjun


