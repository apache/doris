---
{
    "title": "Release 2.0.2",
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

亲爱的社区小伙伴们，Apache Doris 2.0.2  版本已于 2023 年 10 月 6 日正式发布，该版本对多个功能进行了更新优化，旨在更好地满足用户的需求。有 92 位贡献者为 Apache Doris 2.0.2 版本提交了功能优化项以及问题修复，进一步提升了系统的稳定性和性能，欢迎大家下载体验。

**GitHub下载**：https://github.com/apache/doris/releases/tag/2.0.2-rc05

**官网下载页**：https://doris.apache.org/download/

## Behavior Changes

- https://github.com/apache/doris/pull/24679 

 删除与 lambda 函数语法冲突的  json“->”运算符，可以使用函数 json_extract 代替。

- https://github.com/apache/doris/pull/24308 

将 `metadata_failure_recovery` 从 fe.conf 移动到 start_fe.sh 参数，以避免异常操作。

- https://github.com/apache/doris/pull/24207 

对于普通类型中的 null 值使用 \n 来表示，对于复杂类型或嵌套类型的 null 值，跟 JSON 类型保持一致、采取 null 来表示。

- https://github.com/apache/doris/pull/23795 https://github.com/apache/doris/pull/23784 

优化 BE 节点 priority_network 配置项的绑定策略，如果用户配置了错误的 priority_network 则直接启动失败，以避免用户错误地认为配置是正确的。如果用户没有配置 priority_network，则仅从 IPv4 列表中选择第一个 IP，而不是从所有 IP 中选择，以避免用户的服务器不支持 IPv4。

- https://github.com/apache/doris/pull/17730 

支持取消正在重试的导入任务，修复取消加载失败的问题。

## 功能优化

### 易用性提升

- https://github.com/apache/doris/pull/23887 

某些场景下，用户需要向集群中添加一些自定义的库，如 lzo.jar、orai18n.jar 等。在过去的版本中，这些 lib 文件位于 fe/lib 或 be/lib 中，但在升级集群时，lib 库将被新的 lib 库替换，导致所有自定义的 lib 库都会丢失。

在新版本中，为 FE 和 BE 添加了新的自定义目录 custom_lib，用户可以在其中放置自定义 lib 文件。

- https://github.com/apache/doris/pull/23784 optimize priority_network logic to avoid error when this config is wrong or not configured.
- https://github.com/apache/doris/pull/23022 

支持基于用户角色的权限访问控制，实现了行级细粒度的权限控制策略。

### 改进查询优化器 Nereids 统计信息收集

- https://github.com/apache/doris/pull/23663

在运行 Analysis 任务时禁用 File Cache，Analysis 任务是后台任务，不应影响用户本地 File Cache 数据。

- https://github.com/apache/doris/pull/23703

在过去版本中，查看列的统计信息时将忽略出现错误的列。

在新版本中，当 min 或 max 值未能反序列化时，查看列的统计信息时将使用 N/A 作为 min 或 max 的值并仍显示其余的统计信息，包括 count、null_count、ndv 等。

- https://github.com/apache/doris/pull/23965

支持 JDBC 外部表的统计信息收集。

- https://github.com/apache/doris/pull/24625

跳过 `__internal_schema` 和 `information_schema` 上未知列的统计信息检查。

### Multi-Catalog 功能优化

- https://github.com/apache/doris/pull/24168

支持 Hadoop viewfs；

- https://github.com/apache/doris/pull/22369 

优化 JDBC Catalog Checksum Replay 和 Range 相关问题；

- https://github.com/apache/doris/pull/23868 

优化了 JDBC Catalog 的 Property 检查和错误消息提示。

- https://github.com/apache/doris/pull/24242 

修复了 MaxCompute Catalog Decimal 类型解析问题以及使用对象存储地址错误的问题。

- https://github.com/apache/doris/pull/23391 

支持 Hive Metastore Catalog 的 SQL Cache。

- https://github.com/apache/doris/pull/22869 

提高了 Hive Metastore Catalog 的元数据同步性能。

- https://github.com/apache/doris/pull/22702 

添加 metadata_name_ids 以快速获取 Catalogs、DB、Table，在创建或删除 Catalog 和 Table 时无需 Refresh Catalog， 并添加 Profiling 表从而与 MySQL 兼容。

### 倒排索引性能优化

- https://github.com/apache/doris/pull/23952

增加 bkd 索引的查询缓存，通过缓存可以加速在命中 bkd 索引时的查询性能，在高并发场景中效果更为明显；

- https://github.com/apache/doris/pull/24678

提升倒排索引在 Count 算子上的查询性能；

- https://github.com/apache/doris/pull/24751

提升了 Match 算子在未命中索引时的效率，在测试表现中性能最高提升 60 倍； 

- https://github.com/apache/doris/pull/23871 
- https://github.com/apache/doris/pull/24389 

提升了 MATCH 和 MATCH_ALL 在倒排索引上的查询性能；

### Array 函数优化

- https://github.com/apache/doris/pull/23630

优化了老版本查询优化器 Array 函数无法处理 Decimal 类型的问题；

- https://github.com/apache/doris/pull/24327

优化了 `array_union` 数组函数对多个参数的支持；

- https://github.com/apache/doris/pull/24455

支持通过 explode 函数来处理数组嵌套复杂类型；

## Bug修复

 修复了之前版本存在的部分 Bug，使系统整体稳定性表现得到大幅提升，完整 BugFix 列表请参考 GitHub Commits 记录；

- https://github.com/apache/doris/pull/23601
- https://github.com/apache/doris/pull/23630
- https://github.com/apache/doris/pull/23555
- https://github.com/apache/doris/pull/17644
- https://github.com/apache/doris/pull/23779
- https://github.com/apache/doris/pull/23940
- https://github.com/apache/doris/pull/23860
- https://github.com/apache/doris/pull/23973
- https://github.com/apache/doris/pull/24020
- https://github.com/apache/doris/pull/24039
- https://github.com/apache/doris/pull/23958
- https://github.com/apache/doris/pull/24104
- https://github.com/apache/doris/pull/24097
- https://github.com/apache/doris/pull/23852
- https://github.com/apache/doris/pull/24139
- https://github.com/apache/doris/pull/24165
- https://github.com/apache/doris/pull/24164
- https://github.com/apache/doris/pull/24369
- https://github.com/apache/doris/pull/24372
- https://github.com/apache/doris/pull/24381
- https://github.com/apache/doris/pull/24385
- https://github.com/apache/doris/pull/24290
- https://github.com/apache/doris/pull/24207
- https://github.com/apache/doris/pull/24521
- https://github.com/apache/doris/pull/24460
- https://github.com/apache/doris/pull/24568
- https://github.com/apache/doris/pull/24610
- https://github.com/apache/doris/pull/24595
- https://github.com/apache/doris/pull/24616
- https://github.com/apache/doris/pull/24635
- https://github.com/apache/doris/pull/24625
- https://github.com/apache/doris/pull/24572
- https://github.com/apache/doris/pull/24578
- https://github.com/apache/doris/pull/23943
- https://github.com/apache/doris/pull/24697
- https://github.com/apache/doris/pull/24681
- https://github.com/apache/doris/pull/24617
- https://github.com/apache/doris/pull/24692
- https://github.com/apache/doris/pull/24700
- https://github.com/apache/doris/pull/24389
- https://github.com/apache/doris/pull/24698
- https://github.com/apache/doris/pull/24778
- https://github.com/apache/doris/pull/24782
- https://github.com/apache/doris/pull/24800
- https://github.com/apache/doris/pull/24808
- https://github.com/apache/doris/pull/24636
- https://github.com/apache/doris/pull/24981
- https://github.com/apache/doris/pull/24949

## 致谢

感谢所有在 2.0.2 版本中参与功能开发与优化以及问题修复的所有贡献者，他们分别是：

[@adonis0147](https://github.com/adonis0147) [@airborne12](https://github.com/airborne12) [@amorynan](https://github.com/amorynan) [@AshinGau](https://github.com/AshinGau) [@BePPPower](https://github.com/BePPPower) [@BiteTheDDDDt](https://github.com/BiteTheDDDDt) [@bobhan1](https://github.com/bobhan1) [@ByteYue](https://github.com/ByteYue) [@caiconghui](https://github.com/caiconghui) [@CalvinKirs](https://github.com/CalvinKirs) [@cambyzju](https://github.com/cambyzju) [@ChengDaqi2023](https://github.com/ChengDaqi2023) [@ChinaYiGuan](https://github.com/ChinaYiGuan) [@CodeCooker17](https://github.com/CodeCooker17) [@csun5285](https://github.com/csun5285) [@dataroaring](https://github.com/dataroaring) [@deadlinefen](https://github.com/deadlinefen) [@DongLiang-0](https://github.com/DongLiang-0) [@Doris-Extras](https://github.com/Doris-Extras) [@dutyu](https://github.com/dutyu) [@eldenmoon](https://github.com/eldenmoon) [@englefly](https://github.com/englefly) [@freemandealer](https://github.com/freemandealer) [@Gabriel39](https://github.com/Gabriel39) [@gnehil](https://github.com/gnehil) [@GoGoWen](https://github.com/GoGoWen) [@gohalo](https://github.com/gohalo) [@HappenLee](https://github.com/HappenLee) [@hello-stephen](https://github.com/hello-stephen) [@HHoflittlefish777](https://github.com/HHoflittlefish777) [@hubgeter](https://github.com/hubgeter) [@hust-hhb](https://github.com/hust-hhb) [@ixzc](https://github.com/ixzc) [@JackDrogon](https://github.com/JackDrogon) [@jacktengg](https://github.com/jacktengg) [@jackwener](https://github.com/jackwener) [@Jibing-Li](https://github.com/Jibing-Li) [@JNSimba](https://github.com/JNSimba) [@kaijchen](https://github.com/kaijchen) [@kaka11chen](https://github.com/kaka11chen) [@Kikyou1997](https://github.com/Kikyou1997) [@Lchangliang](https://github.com/Lchangliang) [@LemonLiTree](https://github.com/LemonLiTree) [@liaoxin01](https://github.com/liaoxin01) [@LiBinfeng-01](https://github.com/LiBinfeng-01) [@liugddx](https://github.com/liugddx) [@luwei16](https://github.com/luwei16) [@mongo360](https://github.com/mongo360) [@morningman](https://github.com/morningman) [@morrySnow](https://github.com/morrySnow) @mrhhsg @Mryange @mymeiyi @neuyilan @pingchunzhang @platoneko @qidaye @realize096 @RYH61 @shuke987 @sohardforaname @starocean999 @SWJTU-ZhangLei @TangSiyang2001 @Tech-Circle-48 @w41ter @wangbo @wsjz @wuwenchi @wyx123654 @xiaokang @XieJiann @xinyiZzz @XuJianxu @xutaoustc @xy720 @xyfsjq @xzj7019 @yiguolei @yujun777 @Yukang-Lian @Yulei-Yang @zclllyybb @zddr @zhangguoqiang666 @zhangstar333 @ZhangYu0123 @zhannngchen @zxealous @zy-kkk @zzzxl1993 @zzzzzzzs
