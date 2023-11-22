---
{
    "title": "IS_BEING_SYNCED 属性",
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

# 背景

<version since="2.0">

"is_being_synced" = "true"

</version>

CCR功能在建立同步时，会在目标集群中创建源集群同步范围中表（后称源表，位于源集群）的副本表（后称目标表，位于目标集群），但是在创建副本表时需要失效或者擦除一些功能和属性以保证同步过程中的正确性。  

如：  
- 源表中包含了可能没有被同步到目标集群的信息，如`storage_policy`等，可能会导致目标表创建失败或者行为异常。
- 源表中可能包含一些动态功能，如动态分区等，可能导致目标表的行为不受syncer控制导致partition不一致。

在被复制时因失效而需要擦除的属性有：
- `storage_policy`
- `colocate_with`

在被同步时需要失效的功能有：
- 自动分桶
- 动态分区

# 实现
在创建目标表时，这条属性将会由syncer控制添加或者删除，在CCR功能中，创建一个目标表有两个途径：
1. 在表同步时，syncer通过backup/restore的方式对源表进行全量复制来得到目标表。
2. 在库同步时，对于存量表而言，syncer同样通过backup/restore的方式来得到目标表，对于增量表而言，syncer会通过携带有CreateTableRecord的binlog来创建目标表。  

综上，对于插入`is_being_synced`属性有两个切入点：全量同步中的restore过程和增量同步时的getDdlStmt。  

在全量同步的restore过程中，syncer会通过rpc发起对原集群中snapshot的restore，在这个过程中为会为RestoreStmt添加`is_being_synced`属性，并在最终的restoreJob中生效，执行`isBeingSynced`的相关逻辑。  
在增量同步时的getDdlStmt中，为getDdlStmt方法添加参数`boolean getDdlForSync`，以区分是否为受控转化为目标表ddl的操作，并在创建目标表时执行`isBeingSynced`的相关逻辑。
  
对于失效属性的擦除无需多言，对于上述功能的失效需要进行说明：
1. 自动分桶  
    自动分桶会在创建表时生效，计算当前合适的bucket数量，这就可能导致源表和目的表的bucket数目不一致。因此在同步时需要获得源表的bucket数目，并且也要获得源表是否为自动分桶表的信息以便结束同步后恢复功能。当前的做法是在获取distribution信息时默认autobucket为false，在恢复表时通过检查`_auto_bucket`属性来判断源表是否为自动分桶表，如是则将目标表的autobucket字段设置为true，以此来达到跳过计算bucket数量，直接应用源表bucket数量的目的。
2. 动态分区  
    动态分区则是通过将`olapTable.isBeingSynced()`添加到是否执行add/drop partition的判断中来实现的，这样目标表在被同步的过程中就不会周期性的执行add/drop partition操作。
# 注意
在未出现异常时，`is_being_synced`属性应该完全由syncer控制开启或关闭，用户不要自行修改该属性。