---
{
    "title": "Release 2.0.1",
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

亲爱的社区小伙伴们，我们很高兴地向大家宣布，Apache Doris 2.0.1 Release 版本已于 2023 年 9 月 4 日正式发布，有超过 71 位贡献者为 Apache Doris 提交了超过 380 个优化与修复。

# 行为变更
- 将varchar默认长度1修改为65533

# 功能改进

### Array 和 Map 数据类型的功能优化及稳定性改进

- [https://github.com/apache/doris/pull/22793](https://github.com/apache/doris/pull/22793)
- [https://github.com/apache/doris/pull/22927](https://github.com/apache/doris/pull/22927)
- [https://github.com/apache/doris/pull/22738](https://github.com/apache/doris/pull/22738)
- [https://github.com/apache/doris/pull/22347](https://github.com/apache/doris/pull/22347)
- [https://github.com/apache/doris/pull/23250](https://github.com/apache/doris/pull/23250)
- [https://github.com/apache/doris/pull/22300](https://github.com/apache/doris/pull/22300)

### 倒排索引的查询性能优化

- [https://github.com/apache/doris/pull/22836](https://github.com/apache/doris/pull/22836)
- [https://github.com/apache/doris/pull/23381](https://github.com/apache/doris/pull/23381)
- [https://github.com/apache/doris/pull/23389](https://github.com/apache/doris/pull/23389)
- [https://github.com/apache/doris/pull/22570](https://github.com/apache/doris/pull/22570)

### bitmap、like、scan、agg 等执行性能优化

- [https://github.com/apache/doris/pull/23172](https://github.com/apache/doris/pull/23172)
- [https://github.com/apache/doris/pull/23495](https://github.com/apache/doris/pull/23495)
- [https://github.com/apache/doris/pull/23476](https://github.com/apache/doris/pull/23476)
- [https://github.com/apache/doris/pull/23396](https://github.com/apache/doris/pull/23396)
- [https://github.com/apache/doris/pull/23182](https://github.com/apache/doris/pull/23182)
- [https://github.com/apache/doris/pull/22216](https://github.com/apache/doris/pull/22216)

### CCR 的功能优化与稳定性提升

- [https://github.com/apache/doris/pull/22447](https://github.com/apache/doris/pull/22447)
- [https://github.com/apache/doris/pull/22559](https://github.com/apache/doris/pull/22559)
- [https://github.com/apache/doris/pull/22173](https://github.com/apache/doris/pull/22173)
- [https://github.com/apache/doris/pull/22678](https://github.com/apache/doris/pull/22678)

### Merge-on-Write 主键表的能力增强

- [https://github.com/apache/doris/pull/22282](https://github.com/apache/doris/pull/22282)
- [https://github.com/apache/doris/pull/22984](https://github.com/apache/doris/pull/22984)
- [https://github.com/apache/doris/pull/21933](https://github.com/apache/doris/pull/21933)
- [https://github.com/apache/doris/pull/22874](https://github.com/apache/doris/pull/22874)


### 表状态和统计信息的功能优化

- [https://github.com/apache/doris/pull/22658](https://github.com/apache/doris/pull/22658)
- [https://github.com/apache/doris/pull/22211](https://github.com/apache/doris/pull/22211)
- [https://github.com/apache/doris/pull/22775](https://github.com/apache/doris/pull/22775)
- [https://github.com/apache/doris/pull/22896](https://github.com/apache/doris/pull/22896)
- [https://github.com/apache/doris/pull/22788](https://github.com/apache/doris/pull/22788)
- [https://github.com/apache/doris/pull/22882](https://github.com/apache/doris/pull/22882)


### Multi-Catalog 的功能优化及稳定性改进

- [https://github.com/apache/doris/pull/22949](https://github.com/apache/doris/pull/22949)
- [https://github.com/apache/doris/pull/22923](https://github.com/apache/doris/pull/22923)
- [https://github.com/apache/doris/pull/22336](https://github.com/apache/doris/pull/22336)
- [https://github.com/apache/doris/pull/22915](https://github.com/apache/doris/pull/22915)
- [https://github.com/apache/doris/pull/23056](https://github.com/apache/doris/pull/23056)
- [https://github.com/apache/doris/pull/23297](https://github.com/apache/doris/pull/23297)
- [https://github.com/apache/doris/pull/23279](https://github.com/apache/doris/pull/23279)


# 问题修复

修复了若干个 2.0.0 版本中的问题，使系统稳定性得到进一步提升

- [https://github.com/apache/doris/pull/22673](https://github.com/apache/doris/pull/22673)
- [https://github.com/apache/doris/pull/22656](https://github.com/apache/doris/pull/22656)
- [https://github.com/apache/doris/pull/22892](https://github.com/apache/doris/pull/22892)
- [https://github.com/apache/doris/pull/22959](https://github.com/apache/doris/pull/22959)
- [https://github.com/apache/doris/pull/22902](https://github.com/apache/doris/pull/22902)
- [https://github.com/apache/doris/pull/22976](https://github.com/apache/doris/pull/22976)
- [https://github.com/apache/doris/pull/22734](https://github.com/apache/doris/pull/22734)
- [https://github.com/apache/doris/pull/22840](https://github.com/apache/doris/pull/22840)
- [https://github.com/apache/doris/pull/23008](https://github.com/apache/doris/pull/23008)
- [https://github.com/apache/doris/pull/23003](https://github.com/apache/doris/pull/23003)
- [https://github.com/apache/doris/pull/22966](https://github.com/apache/doris/pull/22966)
- [https://github.com/apache/doris/pull/22965](https://github.com/apache/doris/pull/22965)
- [https://github.com/apache/doris/pull/22784](https://github.com/apache/doris/pull/22784)
- [https://github.com/apache/doris/pull/23049](https://github.com/apache/doris/pull/23049)
- [https://github.com/apache/doris/pull/23084](https://github.com/apache/doris/pull/23084)
- [https://github.com/apache/doris/pull/22947](https://github.com/apache/doris/pull/22947)
- [https://github.com/apache/doris/pull/22919](https://github.com/apache/doris/pull/22919)
- [https://github.com/apache/doris/pull/22979](https://github.com/apache/doris/pull/22979)
- [https://github.com/apache/doris/pull/23096](https://github.com/apache/doris/pull/23096)
- [https://github.com/apache/doris/pull/23113](https://github.com/apache/doris/pull/23113)
- [https://github.com/apache/doris/pull/23062](https://github.com/apache/doris/pull/23062)
- [https://github.com/apache/doris/pull/22918](https://github.com/apache/doris/pull/22918)
- [https://github.com/apache/doris/pull/23026](https://github.com/apache/doris/pull/23026)
- [https://github.com/apache/doris/pull/23175](https://github.com/apache/doris/pull/23175)
- [https://github.com/apache/doris/pull/23167](https://github.com/apache/doris/pull/23167)
- [https://github.com/apache/doris/pull/23015](https://github.com/apache/doris/pull/23015)
- [https://github.com/apache/doris/pull/23165](https://github.com/apache/doris/pull/23165)
- [https://github.com/apache/doris/pull/23264](https://github.com/apache/doris/pull/23264)
- [https://github.com/apache/doris/pull/23246](https://github.com/apache/doris/pull/23246)
- [https://github.com/apache/doris/pull/23198](https://github.com/apache/doris/pull/23198)
- [https://github.com/apache/doris/pull/23221](https://github.com/apache/doris/pull/23221)
- [https://github.com/apache/doris/pull/23277](https://github.com/apache/doris/pull/23277)
- [https://github.com/apache/doris/pull/23249](https://github.com/apache/doris/pull/23249)
- [https://github.com/apache/doris/pull/23272](https://github.com/apache/doris/pull/23272)
- [https://github.com/apache/doris/pull/23383](https://github.com/apache/doris/pull/23383)
- [https://github.com/apache/doris/pull/23372](https://github.com/apache/doris/pull/23372)
- [https://github.com/apache/doris/pull/23399](https://github.com/apache/doris/pull/23399)
- [https://github.com/apache/doris/pull/23295](https://github.com/apache/doris/pull/23295)
- [https://github.com/apache/doris/pull/23446](https://github.com/apache/doris/pull/23446)
- [https://github.com/apache/doris/pull/23406](https://github.com/apache/doris/pull/23406)
- [https://github.com/apache/doris/pull/23387](https://github.com/apache/doris/pull/23387)
- [https://github.com/apache/doris/pull/23421](https://github.com/apache/doris/pull/23421)
- [https://github.com/apache/doris/pull/23456](https://github.com/apache/doris/pull/23456)
- [https://github.com/apache/doris/pull/23361](https://github.com/apache/doris/pull/23361)
- [https://github.com/apache/doris/pull/23402](https://github.com/apache/doris/pull/23402)
- [https://github.com/apache/doris/pull/23369](https://github.com/apache/doris/pull/23369)
- [https://github.com/apache/doris/pull/23245](https://github.com/apache/doris/pull/23245)
- [https://github.com/apache/doris/pull/23532](https://github.com/apache/doris/pull/23532)
- [https://github.com/apache/doris/pull/23529](https://github.com/apache/doris/pull/23529)
- [https://github.com/apache/doris/pull/23601](https://github.com/apache/doris/pull/23601)

优化改进及修复问题的完整列表请在 GitHub 按照标签 dev/2.0.1-merged 进行筛选即可。


# 致谢

向所有参与 Apache Doris 2.0.1 版本开发和测试的贡献者们表示最衷心的感谢，他们分别是：

adonis0147、airborne12、amorynan、AshinGau、BePPPower、BiteTheDDDDt、bobhan1、ByteYue、caiconghui、CalvinKirs、csun5285、DarvenDuan、deadlinefen、DongLiang-0、Doris-Extras、dutyu、englefly、freemandealer、Gabriel39、GoGoWen、HappenLee、hello-stephen、HHoflittlefish777、hubgeter、hust-hhb、JackDrogon、jacktengg、jackwener、Jibing-Li、kaijchen、kaka11chen、Kikyou1997、Lchangliang、LemonLiTree、liaoxin01、LiBinfeng-01、lsy3993、luozenglin、morningman、morrySnow、mrhhsg、Mryange、mymeiyi、shuke987、sohardforaname、starocean999、TangSiyang2001、Tanya-W、ucasfl、vinlee19、wangbo
wsjz、wuwenchi、xiaokang、XieJiann、xinyiZzz、yujun777、Yukang-Lian、Yulei-Yang、zclllyybb、zddr、zenoyang、zgxme、zhangguoqiang666、zhangstar333、zhannngchen、zhiqiang-hhhh、zxealous、zy-kkk、zzzxl1993、zzzzzzzs