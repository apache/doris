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

suite("test_dup_tab_basic_varchar_nullable") {

    def table1 = "test_dup_tab_basic_varchar_nullable_tab"

    sql "drop table if exists ${table1}"

    sql """
       CREATE TABLE `${table1}` (
      `city` varchar(20) NULL COMMENT "",
      `name` varchar(20) NULL COMMENT "",
      `addr` varchar(20) NULL COMMENT "",
      `compy` varchar(20) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`city`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`city`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    )
    """
    sql """insert into ${table1} values(null,'qie3','yy','lj'),
        (null,'hehe',null,'lala'),
        ('beijing','xuanwu','wugui',null),
        ('beijing','fengtai','fengtai1','fengtai2'),
        ('beijing','chaoyang','wangjing','donghuqu'),
        ('shanghai','hehe',null,'haha'),
        ('tengxun','qie','gg','lj'),
        ('tengxun2','qie',null,'lj')
"""

    // read single column
    test {
        sql "select city from ${table1} order by city"
        result([[null],[null],['beijing'],['beijing'],['beijing'],['shanghai'],['tengxun'],['tengxun2']])
    }

    test {
        sql "select addr from ${table1} order by addr"
        result([[null],[null],[null],['fengtai1'],['gg'],['wangjing'],['wugui'],['yy']])
    }

    test {
        sql "select city,addr from ${table1} order by city,addr"
        result([[null,null],[null,'yy'],['beijing','fengtai1'],['beijing','wangjing'],['beijing','wugui'],['shanghai',null],['tengxun','gg'],['tengxun2', null]])
    }

    // query with pred
    test {
        sql "select city from ${table1} where city='shanghai' order by city"
        result([['shanghai']])
    }

    test {
        sql "select city from ${table1} where city!='shanghai' order by city"
        result([['beijing'],['beijing'],['beijing'],['tengxun'],['tengxun2']])
    }

    test {
        sql "select addr from ${table1} where addr='fengtai1'"
        result([['fengtai1']])
    }

    test {
        sql "select addr from ${table1} where addr!='fengtai1' order by addr"
        result([['gg'],['wangjing'],['wugui'],['yy']])
    }

    // query with multiple column
    test {
        sql "select city,addr from ${table1} where city='tengxun'"
        result([['tengxun', 'gg']])
    }

    test {
        sql "select city,addr from ${table1} where city!='tengxun' order by city,addr"
        result([['beijing','fengtai1'],['beijing','wangjing'],['beijing','wugui'],['shanghai',null],['tengxun2',null]])
    }

    test {
        sql "select city from ${table1} where addr='wugui'"
        result([['beijing']])
    }

    test {
        sql "select city from ${table1} where addr!='wugui' order by city"
        result([[null],['beijing'],['beijing'],['tengxun']])
    }

    sql "drop table if exists ${table1}"

}

