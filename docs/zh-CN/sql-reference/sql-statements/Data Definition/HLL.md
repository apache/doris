---
{
    "title": "HLL",
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

# HLL
## description
    HLL是基于HyperLogLog算法的工程实现，用于保存HyperLogLog计算过程的中间结果，它只能作为表的value列类型
    通过聚合来不断的减少数据量，以此来实现加快查询的目的，基于它到的是一个估算结果，误差大概在1%左右
    hll列是通过其它列或者导入数据里面的数据生成的，导入的时候通过hll_hash函数来指定数据中哪一列用于生成hll列
    它常用于替代count distinct，通过结合rollup在业务上用于快速计算uv等
    
      相关函数:
    
      HLL_UNION_AGG(hll)
      此函数为聚合函数，用于计算满足条件的所有数据的基数估算。此函数还可用于分析函数，只支持默认窗口，不支持window从句。
    
      HLL_RAW_AGG(hll)
      此函数为聚合函数，用于聚合hll类型字段，并且返回的还是hll类型。

      HLL_CARDINALITY(hll)
      此函数用于计算单条hll列的基数估算
    
      HLL_HASH(column_name)
      生成HLL列类型，用于insert或导入的时候，导入的使用见相关说明
      
      EMPTY_HLL()
      生成空HLL列，用于insert或导入的时候补充默认值，导入的使用见相关说明
    
## example
    1. 首先创建一张含有hll列的表
        create table test(
        dt date,
        id int, 
        name char(10), 
        province char(10),
        os char(1),
        set1 hll hll_union, 
        set2 hll hll_union) 
        distributed by hash(id) buckets 32;
        
    2. 导入数据，导入的方式见相关help curl

      a. 使用表中的列生成hll列
        curl --location-trusted -uname:password -T data -H "label:load_1" -H "columns:dt, id, name, province, os, set1=hll_hash(id), set2=hll_hash(name)"
            http://host/api/test_db/test/_stream_load
      b. 使用数据中的某一列生成hll列
        curl --location-trusted -uname:password -T data -H "label:load_1" -H "columns:dt, id, name, province, sex, cuid, os, set1=hll_hash(cuid), set2=hll_hash(os)"
            http://host/api/test_db/test/_stream_load

    3. 聚合数据，常用方式3种：（如果不聚合直接对base表查询，速度可能跟直接使用approx_count_distinct速度差不多）

      a. 创建一个rollup，让hll列产生聚合，
        alter table test add rollup test_rollup(dt, set1);
        
      b. 创建另外一张专门计算uv的表，然后insert数据）
    
        create table test_uv(
        dt date,
        uv_set hll hll_union)
        distributed by hash(dt) buckets 32;

        insert into test_uv select dt, set1 from test;
        
      c. 创建另外一张专门计算uv的表，然后insert并通过hll_hash根据test其它非hll列生成hll列
      
        create table test_uv(
        dt date,
        id_set hll hll_union)
        distributed by hash(dt) buckets 32;
        
        insert into test_uv select dt, hll_hash(id) from test;
            
    4. 查询，hll列不允许直接查询它的原始值，可以通过配套的函数进行查询
    
      a. 求总uv
        select HLL_UNION_AGG(uv_set) from test_uv;
            
      b. 求每一天的uv
        select dt, HLL_CARDINALITY(uv_set) from test_uv;

      c. 求test表中set1的聚合值
        select dt, HLL_CARDINALITY(uv) from (select dt, HLL_RAW_AGG(set1) as uv from test group by dt) tmp;
        select dt, HLL_UNION_AGG(set1) as uv from test group by dt;

## keyword
    HLL

