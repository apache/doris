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

suite("test_hive_runtime_filter_partition_pruning", "p0,external") {
    def test_runtime_filter_partition_pruning = {
        qt_runtime_filter_partition_pruning_decimal1 """
            select count(*) from decimal_partition_table where partition_col =
                (select partition_col from decimal_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 1);
        """
        qt_runtime_filter_partition_pruning_decimal2 """
            select count(*) from decimal_partition_table where partition_col in
                (select partition_col from decimal_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 2);
        """
        qt_runtime_filter_partition_pruning_decimal3 """
            select count(*) from decimal_partition_table where abs(partition_col) =
                (select partition_col from decimal_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 1);
        """
        qt_runtime_filter_partition_pruning_int1 """
            select count(*) from int_partition_table where partition_col =
                (select partition_col from int_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 1);
        """
        qt_runtime_filter_partition_pruning_int2 """
            select count(*) from int_partition_table where partition_col in
                (select partition_col from int_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 2);
        """
        qt_runtime_filter_partition_pruning_int3 """
            select count(*) from int_partition_table where abs(partition_col) =
                (select partition_col from int_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 1);
        """
        qt_runtime_filter_partition_pruning_string1 """
            select count(*) from string_partition_table where partition_col =
                (select partition_col from string_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 1);
        """
        qt_runtime_filter_partition_pruning_string2 """
            select count(*) from string_partition_table where partition_col in
                (select partition_col from string_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 2);
        """
        qt_runtime_filter_partition_pruning_date1 """
            select count(*) from date_partition_table where partition_col =
                (select partition_col from date_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 1);
        """
        qt_runtime_filter_partition_pruning_decimal2 """
            select count(*) from date_partition_table where partition_col in
                (select partition_col from date_partition_table
                group by partition_col having count(*) > 0
                order by partition_col desc limit 2);
        """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "${hivePrefix}_test_hive_runtime_filter_partition_pruning"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`partition_tables`"""

            test_runtime_filter_partition_pruning()

            setHivePrefix(hivePrefix)
            hive_docker """drop table if exists default.hive_partition_value_parquet"""
            hive_docker """create table default.hive_partition_value_parquet (v int)
                partitioned by (p int, q string) stored as parquet"""
            hive_docker """insert into default.hive_partition_value_parquet partition(p=1,q='a')
                values (10),(11),(12)"""
            hive_docker """insert into default.hive_partition_value_parquet partition(p=2,q='b')
                values (20),(21)"""
            hive_docker """insert into default.hive_partition_value_parquet partition(p=2,q='b') values (22)"""
            hive_docker """insert into default.hive_partition_value_parquet partition(p=3,q='c') values (30)"""
            // hive_docker submits the whole string as ONE PreparedStatement, so a SET must be its
            // own call. The connection is thread-local and reused, so the setting carries over.
            hive_docker """set hive.exec.dynamic.partition.mode=nonstrict"""
            hive_docker """insert into default.hive_partition_value_parquet partition(p=4,q)
                select 40, cast(null as string)"""
            hive_docker """alter table default.hive_partition_value_parquet
                add partition(p=9,q='empty')"""
            hive_docker """drop table if exists default.hive_partition_value_orc"""
            hive_docker """create table default.hive_partition_value_orc (v int)
                partitioned by (p int, q string) stored as orc"""
            hive_docker """set hive.exec.dynamic.partition.mode=nonstrict"""
            hive_docker """insert into default.hive_partition_value_orc partition(p,q)
                select v,p,q from default.hive_partition_value_parquet"""
            hive_docker """alter table default.hive_partition_value_orc add partition(p=9,q='empty')"""
            // Prove the fixture itself: a silently skipped insert would make every comparison below
            // vacuous, because baseline and optimized queries would read the same reduced data.
            for (String table : ["hive_partition_value_parquet", "hive_partition_value_orc"]) {
                assertEquals("1", hive_docker("select count(*) from default.${table} where p=4")[0][0].toString(),
                        "${table} must carry the null partition value")
                assertEquals("0", hive_docker("select count(*) from default.${table} where p=9")[0][0].toString(),
                        "${table} must carry an empty partition")
            }
            sql """refresh catalog ${catalog_name}"""
            sql """use `${catalog_name}`.`default`"""
            def originalSettings = ["enable_file_scanner_v2", "enable_partition_column_value_only_optimization",
                    "enable_push_down_no_group_agg", "inline_cte_referenced_threshold"].collectEntries { name ->
                [(name): sql("show variables like '${name}'")[0][1]]
            }
            try {
                def queries = [
                    "select min(p),max(p),min(q),max(q) from hive_partition_value_parquet",
                    "select distinct p,q from hive_partition_value_parquet order by p,q",
                    "select p,max(q) from hive_partition_value_parquet group by p order by p",
                    "select max(p) from hive_partition_value_parquet where p=2",
                    "select max(p+1) from hive_partition_value_parquet where p>=2",
                    // A retained predicate on a partition column must still be applied: the
                    // answer is 1, not the unfiltered max of 4. This is the case that used to
                    // make the reader decline the range because a scan conjunct was present.
                    "select max(p) from hive_partition_value_parquet where p<=1",
                    // COUNT(DISTINCT) over a partition column: deduplicating "one row per file"
                    // yields the same value set as deduplicating every row.
                    // One CTE consumed twice through different shapes: a join and a scalar
                    // subquery. Both consumers must inherit the producer's bounded output, so the
                    // runtime filter still reaches the scanned table.
                    """with latest as (select max(p) as p from hive_partition_value_parquet)
                        select t.p,t.q,t.v from hive_partition_value_parquet t
                        join latest l on t.p=l.p
                        where t.p = (select p from latest) order by t.p,t.q,t.v""",
                    "select count(distinct p) from hive_partition_value_parquet",
                    "select count(distinct p) from hive_partition_value_orc",
                    "select p from hive_partition_value_parquet where p>=2 group by p order by p",
                    "select min(p),max(p),min(q),max(q) from hive_partition_value_orc",
                    "select distinct p,q from hive_partition_value_orc order by p,q",
                    "select p,max(q) from hive_partition_value_orc group by p order by p",
                    "select max(p) from hive_partition_value_orc where p=2",
                    """with latest as (select max(p) as p from hive_partition_value_parquet)
                        select t.p,t.q,t.v from hive_partition_value_parquet t
                        join latest l on t.p=l.p order by t.p,t.q,t.v""",
                    // CTE variants of the "latest partition" pattern. inline_cte_referenced_threshold
                    // is 0 below, so the CTE is materialized and its consumers are separate subtrees:
                    // a consumer only keeps the runtime filter that prunes the probe-side scan if it
                    // inherits the producer's effectiveness. Cover the ORC twin, a multi-aggregate
                    // producer, a producer consumed twice, and the scalar-subquery spelling.
                    """with latest as (select max(p) as p from hive_partition_value_orc)
                        select t.p,t.q,t.v from hive_partition_value_orc t
                        join latest l on t.p=l.p order by t.p,t.q,t.v""",
                    """with latest as (select max(p) as p, max(q) as q from hive_partition_value_parquet)
                        select t.p,t.q,t.v from hive_partition_value_parquet t
                        join latest l on t.p=l.p order by t.p,t.q,t.v""",
                    """with latest as (select max(p) as p from hive_partition_value_parquet)
                        select t.p,t.q,t.v from hive_partition_value_parquet t
                        join latest l1 on t.p=l1.p join latest l2 on t.p=l2.p
                        order by t.p,t.q,t.v""",
                    """with latest as (select max(p) as p from hive_partition_value_parquet)
                        select count(*) from hive_partition_value_parquet t
                        where t.p = (select p from latest)""",
                    """with latest as (select max(p) as p from hive_partition_value_orc)
                        select count(*) from hive_partition_value_orc t
                        where t.p = (select p from latest)""",
                    // max(p)+0 forces a PhysicalProject on top of the producer's aggregate. The CTE
                    // consumer must still inherit the bounded output through that wrapper,
                    // otherwise the probe-side runtime filter is dropped.
                    """with latest as (select max(p) + 0 as p from hive_partition_value_parquet)
                        select t.p,t.q,t.v from hive_partition_value_parquet t
                        join latest l on t.p=l.p order by t.p,t.q,t.v""",
                    """select p from (
                        select p,row_number() over(order by p desc) as rn
                        from hive_partition_value_parquet group by p
                        ) t where rn<=2 order by p"""
                ]
                sql "set inline_cte_referenced_threshold=0"
                sql "set enable_partition_column_value_only_optimization=false"
                sql "set enable_push_down_no_group_agg=false"
                sql "set enable_file_scanner_v2=false"
                def baseline = queries.collect { query -> sql(query) }
                sql "set enable_push_down_no_group_agg=true"
                for (boolean scannerV2 : [false, true]) {
                    sql "set enable_file_scanner_v2=${scannerV2}"
                    for (boolean partitionValue : [false, true]) {
                        sql "set enable_partition_column_value_only_optimization=${partitionValue}"
                        queries.eachWithIndex { query, index ->
                            assertEquals(baseline[index], sql(query))
                            explain {
                                sql(query)
                                if (partitionValue) {
                                    contains "pushdown agg=PARTITION_VALUE"
                                } else {
                                    notContains "pushdown agg=PARTITION_VALUE"
                                }
                            }
                        }
                    }
                    sql "set enable_partition_column_value_only_optimization=true"
                    [
                        "select count(*) from hive_partition_value_parquet",
                        "select max(p),count(*) from hive_partition_value_parquet",
                        "select max(v) from hive_partition_value_parquet",
                        "select max(p+random()) from hive_partition_value_parquet",
                        "select max(p) from hive_partition_value_parquet where random()>0.5",
                        "select distinct p+random() from hive_partition_value_parquet",
                        "select max(p) from hive_partition_value_parquet tablesample(50 percent) repeatable 7",
                        "select max(p) from hive_partition_value_parquet " +
                                "where assert_true(p>0,'positive partition required')",
                        // COUNT with no DISTINCT counts rows, and the synthesized stream carries one
                        // row per file, so it must NOT be answered from partition metadata.
                        "select count(p) from hive_partition_value_parquet",
                        // Other distinct aggregates are not duplicate-insensitive in general.
                        "select sum(distinct p) from hive_partition_value_parquet"
                    ].each { query ->
                        explain {
                            sql(query)
                            notContains "pushdown agg=PARTITION_VALUE"
                        }
                    }
                    // A materialized CTE must hand its producer's bounded output on to every
                    // consumer. Otherwise the runtime filter that prunes the probe-side file scan is
                    // dropped, and answering max(p) from partition metadata buys nothing: the scan
                    // still has to read every partition. "-> " is the apply side of a runtime
                    // filter, i.e. the filter actually reaching the scanned table (as opposed to
                    // "<- ", which is only where the filter is built).
                    // [query, expectsPartitionValue]: the second entry is false when the CTE reads a
                    // non-partition column too, so the partition-value pushdown does not apply and
                    // only the runtime filter is asserted.
                    [
                        ["""with latest as (select max(p) as p from hive_partition_value_parquet)
                            select t.p,t.q,t.v from hive_partition_value_parquet t
                            join latest l on t.p=l.p""", true],
                        ["""with latest as (select max(p) as p from hive_partition_value_orc)
                            select t.p,t.q,t.v from hive_partition_value_orc t
                            join latest l on t.p=l.p""", true],
                        ["""with latest as (select max(p) as p, max(q) as q from hive_partition_value_parquet)
                            select t.p,t.q,t.v from hive_partition_value_parquet t
                            join latest l on t.p=l.p""", true],
                        ["""with latest as (select max(p) as p from hive_partition_value_parquet)
                            select t.p,t.q,t.v from hive_partition_value_parquet t
                            join latest l1 on t.p=l1.p join latest l2 on t.p=l2.p""", true],
                        ["""with latest as (select max(p) as p from hive_partition_value_parquet)
                            select count(*) from hive_partition_value_parquet t
                            where t.p = (select p from latest)""", true],
                        ["""with latest as (select max(p) as p from hive_partition_value_orc)
                            select count(*) from hive_partition_value_orc t
                            where t.p = (select p from latest)""", true],
                        // max(p)+0 puts a Project on top of the producer's aggregate.
                        ["""with latest as (select max(p) + 0 as p from hive_partition_value_parquet)
                            select t.p,t.q,t.v from hive_partition_value_parquet t
                            join latest l on t.p=l.p""", true],
                        // A predicate on a visible column bounds the relation as a whole, so a
                        // producer whose root is Project(Filter(...)) must inherit too. This is the
                        // shape an external table read through a partition filter produces.
                        ["""with hot as (select p, v from hive_partition_value_parquet where p = 2)
                            select t.p,t.q,t.v from hive_partition_value_parquet t
                            join hot h on t.p=h.p""", false],
                        // The same predicate underneath the max(p) aggregate.
                        ["""with latest as (select max(p) as p from hive_partition_value_parquet
                                where p < 10)
                            select t.p,t.q,t.v from hive_partition_value_parquet t
                            join latest l on t.p=l.p""", true]
                    ].each { query, expectsPartitionValue ->
                        def plan = sql("explain ${query}").toString()
                        if (expectsPartitionValue) {
                            assertTrue(plan.contains("pushdown agg=PARTITION_VALUE"),
                                    "a CTE must not stop the partition-value pushdown, plan: ${plan}")
                        }
                        assertTrue((plan =~ /runtime filters: RF\d+\[\w+\] ->/).find(),
                                "a CTE must not drop the runtime filter on the probe scan, plan: ${plan}")
                    }
                    // The plan alone is not enough: it can say pushdown agg=PARTITION_VALUE while
                    // the reader declines the range and falls back to an ordinary scan. Prove the
                    // optimization really ran by looking at the profile: the scan must feed the
                    // aggregate one row per nonempty range instead of one row per data row.
                    sql "set enable_profile=true"
                    def totalRows = sql("select count(*) from hive_partition_value_parquet")[0][0] as long
                    profile("partition_value_input_rows") {
                        run {
                            sql """/* partition_value_input_rows */
                                select max(p) from hive_partition_value_parquet"""
                        }
                        check { profileString, exception ->
                            assert exception == null
                            def scanRows = (profileString =~ /InputRows:\s+sum\s+(\d+)/)
                                    .collect { it[1] as long }.max()
                            assertTrue(scanRows < totalRows,
                                    "PARTITION_VALUE must not materialize every row: the scan read " +
                                    "${scanRows} rows for a ${totalRows}-row table")
                        }
                    }
                    // Same check on scanner V1. V1 is only reachable for non-Parquet formats:
                    // FileQueryScanNode stamps parquet_timestamp_semantics_version=1, and
                    // FileScanLocalState::should_use_file_scanner_v2 treats that as a required
                    // timestamp contract, so every Parquet scan runs on V2 whatever
                    // enable_file_scanner_v2 says. ORC carries no such contract and does reach V1.
                    sql "set enable_file_scanner_v2=false"
                    def orcTotalRows = sql("select count(*) from hive_partition_value_orc")[0][0] as long
                    profile("partition_value_input_rows_v1") {
                        run {
                            sql """/* partition_value_input_rows_v1 */
                                select max(p) from hive_partition_value_orc"""
                        }
                        check { profileString, exception ->
                            assert exception == null
                            assertTrue(profileString.contains("UseScannerV2:  false"),
                                    "this case must exercise scanner V1")
                            def scanRows = (profileString =~ /InputRows:\s+sum\s+(\d+)/)
                                    .collect { it[1] as long }.max()
                            assertTrue(scanRows < orcTotalRows,
                                    "PARTITION_VALUE must not materialize every row on scanner V1: " +
                                    "the scan read ${scanRows} rows for a ${orcTotalRows}-row table")
                        }
                    }
                    sql "set enable_file_scanner_v2=true"
                }
            } finally {
                originalSettings.each { name, value -> sql "set ${name}=${value}" }
            }
        } finally {
        }
    }
}

