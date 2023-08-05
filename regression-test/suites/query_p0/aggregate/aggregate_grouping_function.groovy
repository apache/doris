      
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

suite("aggregate_grouping_function") {
    sql "DROP TABLE IF EXISTS test_aggregate_grouping_function"

    sql """
        CREATE TABLE IF NOT EXISTS `test_aggregate_grouping_function` (
        `dt_date` varchar(1000) NULL COMMENT "",
        `name` varchar(1000) NULL COMMENT "",
        `num1` bigint(20) SUM NOT NULL COMMENT "",
        `num2` bigint(20) SUM NOT NULL COMMENT ""
        ) ENGINE=OLAP
        AGGREGATE KEY(`dt_date`, `name`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`dt_date`) BUCKETS 32
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
        ;
        """

    sql """INSERT INTO test_aggregate_grouping_function values ('2022-08-01', "aaa", 1,2),('2022-08-01', "bbb", 1,2),('2022-08-01', "ccc", 1,2),
            ('2022-08-02', "aaa", 1,2),('2022-08-02', "bbb", 1,2),('2022-08-02', "ccc", 1,2),('2022-08-03', "aaa", 1,2),
            ('2022-08-03', "bbb", 1,2),('2022-08-03', "ccc", 1,2);"""

    qt_select """
        WITH base_table AS (
		SELECT dt_date, name, sum(num1) AS sum1
			, sum(num2) AS sum2
			, sum(num1) / sum(num2) AS ratio
		FROM test_aggregate_grouping_function
		GROUP BY dt_date, name
            )
        SELECT grouping_id(sum1), dt_date, name
            , sum(sum2)
        FROM base_table
        GROUP BY GROUPING SETS ((dt_date), (dt_date, name, sum1))
        ORDER BY dt_date, name;
    """

    qt_select """
        WITH base_table AS (
		SELECT dt_date, name, sum(num1) AS sum1
			, sum(num2) AS sum2
			, sum(num1) / sum(num2) AS ratio
		FROM test_aggregate_grouping_function
                GROUP BY dt_date, name
            )
        SELECT grouping_id(ratio), dt_date, name
            , sum(sum2)
        FROM base_table
        GROUP BY GROUPING SETS ((dt_date), (dt_date, name, ratio))
        ORDER BY dt_date, name;
    """

    qt_select """
        WITH base_table AS (
		SELECT dt_date, name, sum(num1) AS sum1
			, sum(num2) AS sum2
			, sum(num1) / sum(num2) AS ratio
		FROM test_aggregate_grouping_function
		GROUP BY dt_date, name
        ), 
        base_table2 AS (
            SELECT *
            FROM base_table
        )
        SELECT grouping_id(ratio), dt_date, name
            , sum(sum2)
        FROM base_table2
        GROUP BY GROUPING SETS ((dt_date), (dt_date, name, ratio))
        ORDER BY dt_date, name;
    """

    sql "DROP TABLE test_aggregate_grouping_function"

    sql "DROP TABLE IF EXISTS test_aggregate_collect_set"
 
    sql """
        CREATE table test_aggregate_collect_set (
		custr_nbr varchar(30),   
		bar_code varchar(36),
       		audit_id varchar(255),
		case_id text,
		into_time text,
		audit_time text,
		apply_type text,
		node_code text,
		audit_code1 text,
		audit_code2 text,
		audit_code3 text
	) UNIQUE KEY (custr_nbr,bar_code,audit_id )
	distributed by hash (custr_nbr,bar_code,audit_id ) buckets 10
	properties(
		  "replication_num" = "1"
	);"""

    sql """ INSERT into test_aggregate_collect_set values ('custr_nbr', 'barcode', 'audit_id', 'case_id', '2007-11-30 10:30:19', '2007-11-30 10:30:19', '2', 'Decline', 'c1', 'c2', 'c3');"""

    sql """ 
	SELECT
	    tt.tag_value
	FROM (
	    SELECT
	        d.custr_nbr,
	        concat_ws('|', collect_set(d.is_code1)) as tag_value
	    FROM (
	        SELECT
	            b.custr_nbr,
	            1 as is_code1
	        FROM test_aggregate_collect_set b
	    ) d
	    GROUP BY d.custr_nbr
	) tt
	GROUP BY tt.tag_value
	;
	"""

    sql "DROP TABLE test_aggregate_collect_set"
}

    
