
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

suite("test_hive_lzo_text_format", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    for (String hivePrefix : ["hive3"]) {
        String hmsPort        = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String externalEnvIp  = context.config.otherConfigs.get("externalEnvIp")
        String catalogName    = "${hivePrefix}_test_hive_lzo_text_format"

        sql """drop catalog if exists ${catalogName}"""
        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                'type'       = 'hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'hadoop.username' = 'hive'
            )
        """

        sql """use `${catalogName}`.`multi_catalog`"""

        // ----------------------------------------------------------------
        // 1. com.hadoop.mapreduce.LzoTextInputFormat (lzo-hadoop mapreduce API)
        // ----------------------------------------------------------------
        order_qt_lzo_count """
            select count(*) from text_lzo_format
        """

        order_qt_lzo_all """
            select id, value, name, score, dt
            from text_lzo_format
            order by id
        """

        order_qt_lzo_filter """
            select id, name
            from text_lzo_format
            where id > 2
            order by id
        """

        order_qt_lzo_agg """
            select sum(value), avg(score), min(dt), max(dt)
            from text_lzo_format
        """

        // ----------------------------------------------------------------
        // 2. com.hadoop.mapred.DeprecatedLzoTextInputFormat (lzo-hadoop legacy mapred API)
        // ----------------------------------------------------------------
        order_qt_deprecated_lzo_count """
            select count(*) from text_deprecated_lzo_format
        """

        order_qt_deprecated_lzo_all """
            select id, value, name, score, dt
            from text_deprecated_lzo_format
            order by id
        """

        order_qt_deprecated_lzo_filter """
            select id, name
            from text_deprecated_lzo_format
            where id > 2
            order by id
        """

        order_qt_deprecated_lzo_agg """
            select sum(value), avg(score), min(dt), max(dt)
            from text_deprecated_lzo_format
        """

        // ----------------------------------------------------------------
        // 3. com.hadoop.mapreduce.LzoTextInputFormat with .lzo.index sidecar
        //    The partition directory contains both:
        //      - part-m-00000.lzo       (data file, must be scanned)
        //      - part-m-00000.lzo.index (Hadoop-LZO sidecar, must be excluded)
        //    Doris must filter out the index sidecar and return the same 5 rows.
        //    Without the fix, Doris would try to read the .lzo.index file as
        //    FORMAT_TEXT + PLAIN compression and return garbage or fail.
        // ----------------------------------------------------------------
        order_qt_indexed_lzo_count """
            select count(*) from text_lzo_indexed_format
        """

        order_qt_indexed_lzo_all """
            select id, value, name, score, dt
            from text_lzo_indexed_format
            order by id
        """

        // Key assertion: result must be identical to the non-indexed table.
        // If .lzo.index were incorrectly scanned, count() would be 6 (or fail).
        order_qt_indexed_vs_plain """
            select
                (select count(*) from text_lzo_format) as plain_cnt,
                (select count(*) from text_lzo_indexed_format) as indexed_cnt
        """

        // ----------------------------------------------------------------
        // 4. Cross-validate: all three tables must return identical row counts
        // ----------------------------------------------------------------
        order_qt_cross_validate """
            select
                (select count(*) from text_lzo_format) as lzo_cnt,
                (select count(*) from text_deprecated_lzo_format) as deprecated_lzo_cnt,
                (select count(*) from text_lzo_indexed_format) as indexed_lzo_cnt
        """

        sql """drop catalog if exists ${catalogName}"""
    }
}
