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

suite("test_dynamic_partition", "partition") {
    // todo: test dynamic partition
    sql "drop table if exists dy_par"
    sql """
        CREATE TABLE dy_par ( k1 date NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL ) 
        AGGREGATE KEY(k1,k2) 
        PARTITION BY RANGE(k1) ( ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 3 
        PROPERTIES (  
            "dynamic_partition.enable"="true", 
            "dynamic_partition.end"="3", 
            "dynamic_partition.buckets"="10", 
            "dynamic_partition.start"="-3", 
            "dynamic_partition.prefix"="p", 
            "dynamic_partition.time_unit"="DAY", 
            "dynamic_partition.create_history_partition"="true" )
        """
    List<List<Object>> result  = sql "show tables like 'dy_par'"
    logger.info("${result}")
    assertEquals(result.size(), 1)
    sql "drop table dy_par"    
}
