---
{
    "title": "Release 2.0.5",
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


亲爱的社区小伙伴们，[Apache Doris 2.0.5](https://doris.apache.org/download/) 版本已于 2024 年 2 月 27 日正式与大家见面。这次更新带来一系列行为变更和功能更新，并进行了若干的改进与优化，旨在为用户提供更为稳定高效的数据查询与分析体验。新版本已经上线，欢迎大家下载体验！

**官网下载：** [https://doris.apache.org/download/](https://doris.apache.org/download/)

**GitHub 下载：** [https://github.com/apache/doris/releases](https://github.com/apache/doris/releases)


## 行为变更
- `select char(0) = '\0'`  返回 true，跟 MySQL 的行为保持一致
  - https://github.com/apache/doris/pull/30034
- Export 导出数据支持空表
  - https://github.com/apache/doris/pull/30703

## 新功能
- 利用过滤条件中的 `is null` 谓词，将 OUTER JOIN 转换为 ANTI JOIN
- 增加 `SHOW TABLETS BELONG` 语法用于获取 tablet 属于哪个 table
- InferPredicates 支持 `IN`，例如：`a = b & a in [1, 2] -> b in [1, 2]`
- 支持对物化视图收集统计信息
- `SHOW PROCESSLIST` 支持输出连接对应的 FE
- Export 导出 CSV 文件支持通过 `with_bom` 参数控制是否带有 Windows BOM

## 改进和优化
- 在无统计信息时优化 Query Plan
- 基于 Rollup 的统计信息优化 Query Plan
- 用户停止 Auto Analyze 后尽快停止统计信息收集任务
- 缓存统计信息收集异常，避免大约太多异常栈
- 支持在 SQL 中自定使用某个物化视图
- JDBC Catalog 谓词下推列名字符转义
- 修复 MySQL Catalog 中 `to_date` 函数下推的问题
- 优化 JDBC 客户端连接关闭的逻辑，在异常时正常取消查询
- 优化 JDBC 连接池的参数
- 通过 HMS API 获取 Hudi 外表的分区信息
- 优化 Routine Load 的内存占用和错误信息
- 如果 `max_backup_restore_job_num_per_db` 参数为 0，跳过所有备份恢复任务


## 致谢
最后，衷心感谢 59 位开发者为 Apache Doris 2.0.5 版本做出了重要贡献:

airborne12, alexxing662, amorynan, AshinGau, BePPPower, bingquanzhao, BiteTheDDDDt, ByteYue, caiconghui, cambyzju, catpineapple, dataroaring, eldenmoon, Emor-nj, englefly, felixwluo, GoGoWen, HappenLee, hello-stephen, HHoflittlefish777, HowardQin, JackDrogon, jacktengg, jackwener, Jibing-Li, KassieZ, LemonLiTree, liaoxin01, liugddx, LuGuangming, morningman, morrySnow, mrhhsg, Mryange, mymeiyi, nextdreamblue, qidaye, ryanzryu, seawinde,starocean999, TangSiyang2001, vinlee19, w41ter, wangbo, wsjz, wuwenchi, xiaokang, XieJiann, xingyingone, xy720,xzj7019, yujun777, zclllyybb, zhangstar333, zhannngchen, zhiqiang-hhhh, zxealous, zy-kkk, zzzxl1993

