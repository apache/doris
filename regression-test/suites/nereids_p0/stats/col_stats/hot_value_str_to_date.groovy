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

suite('hot_value_str_to_date') {

    sql """
        drop table if exists t22;
        create table t22(
            `data_date` varchar(32) NULL,
            `spend` decimal(19,6) NULL) 
            properties('replication_num'='1');

        insert into t22 values ('2023-09-01', 10), ('2023-09-08',10);


        alter table t22 modify column data_date set stats ('row_count'='1000', 'ndv'='8', 'min_value'='2023-09-01', 'max_value'='2023-09-08', 'avg_size'='10', 'num_nulls'='100', 'hot_values'='2023-09-08T00:00:00Z :0.5');

        """
    qt_cached_col_stats "show column cached stats t22;"
    
    explain {
        sql """ physical plan
                select * from t22
                where data_date >= CURDATE();
            """
            // verify that hot value is converted to date literal before derive comparison
            // | PhysicalResultSink[75] ( outputExprs=[data_date#0, spend#1] )
            // | +--PhysicalFilter[71]@1 ( stats=0, predicates=(data_date#0 >= '2025-09-08') )
            // |    +--PhysicalOlapScan[t22]@0 ( stats=1,000, operativeSlots=[data_date#0], virtualColumns=[] )
        contains "stats=0"
    }

    explain {
        sql """ physical plan
                select * from t22
                where data_date >= '2025-09-08';
            """
        // verify that org.apache.doris.nereids.stats.ExpressionEstimation#castMinMax convert hot value from string literal to date literal 
        // | cost = 1000.0004                                                                               |
        // | PhysicalResultSink[75] ( outputExprs=[data_date#0, spend#1] )                                  |
        // | +--PhysicalFilter[71]@1 ( stats=0, predicates=(data_date#0 >= '2025-09-08') )                  |
        // |    +--PhysicalOlapScan[t22]@0 ( stats=1,000, operativeSlots=[data_date#0], virtualColumns=[] ) |
        // +------------------------------------------------------------------------------------------------+
        contains "stats=0"
    }
}