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

suite("test_hive_date_timezone", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return
    }

    for (String hivePrefix : ["hive3"]) {
        setHivePrefix(hivePrefix)
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfsPort = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String catalogName = "test_hive_date_timezone_${hivePrefix}"

        sql """drop catalog if exists ${catalogName}"""
        sql """
            create catalog if not exists ${catalogName} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfsPort}',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
            );
        """

        try {
            sql """set enable_fallback_to_original_planner=false"""
            sql """switch ${catalogName}"""
            sql """use `schema_change`"""

            sql """set time_zone = 'UTC'"""
            qt_orc_date_utc """select date_col from orc_primitive_types_to_date order by id"""
            qt_parquet_date_utc """select date_col from parquet_primitive_types_to_date order by id"""

            sql """set time_zone = 'America/Mexico_City'"""
            qt_orc_date_west_tz """select date_col from orc_primitive_types_to_date order by id"""
            qt_parquet_date_west_tz """select date_col from parquet_primitive_types_to_date order by id"""

            // The parquet timestamp table exercises the optimized timestamp convert path.
            // Querying the same data under multiple timezone spellings lets this suite cover
            // both fixed-offset normalization and named-timezone lookup behavior.
            sql """set time_zone = 'UTC'"""
            def parquetTimestampUtc = sql """select timestamp_col from parquet_primitive_types_to_timestamp order by id"""
            sql """set time_zone = 'Etc/UTC'"""
            def parquetTimestampEtcUtc = sql """select timestamp_col from parquet_primitive_types_to_timestamp order by id"""
            sql """set time_zone = '+08:00'"""
            def parquetTimestampFixedOffset = sql """select timestamp_col from parquet_primitive_types_to_timestamp order by id"""
            sql """set time_zone = '+8:00'"""
            def parquetTimestampShortOffset = sql """select timestamp_col from parquet_primitive_types_to_timestamp order by id"""
            sql """set time_zone = 'Etc/GMT-8'"""
            def parquetTimestampEtcGmtMinus8 = sql """select timestamp_col from parquet_primitive_types_to_timestamp order by id"""
            sql """set time_zone = '-06:00'"""
            def parquetTimestampFixedMexicoOffset = sql """select timestamp_col from parquet_primitive_types_to_timestamp order by id"""
            sql """set time_zone = 'America/Mexico_City'"""
            def parquetTimestampMexicoCity = sql """select timestamp_col from parquet_primitive_types_to_timestamp order by id"""

            // Equivalent UTC spellings should stay on the same result set.
            assertEquals(parquetTimestampUtc, parquetTimestampEtcUtc)
            // These inputs are normalized to the same fixed offset and should match exactly.
            assertEquals(parquetTimestampFixedOffset, parquetTimestampShortOffset)
            // Etc/GMT-8 is a fixed-offset TZDB name. The sign is POSIX-style, so it means UTC+8.
            assertEquals(parquetTimestampFixedOffset, parquetTimestampEtcGmtMinus8)
            // America/Mexico_City must still read through the named-timezone path, not a constant
            // -06:00 offset. This fixture contains a 2022 DST timestamp that makes the results differ.
            assertEquals(parquetTimestampUtc.size(), parquetTimestampMexicoCity.size())
            assertTrue(parquetTimestampFixedMexicoOffset != parquetTimestampMexicoCity)
        } finally {
            sql """set time_zone = default"""
            sql """switch internal"""
            sql """drop catalog if exists ${catalogName}"""
        }
    }
}
