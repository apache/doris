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

// Fixtures: iceberg/scripts/create_preinstalled_scripts/paimon/run15.sql.
// Golden results are derived from those payloads, including binary values unsuitable for text.
suite("test_paimon_blob_jni", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_blob_jni"
    try {
        sql "drop catalog if exists ${catalogName}"
        sql """
            create catalog ${catalogName} properties (
                'type' = 'paimon',
                'paimon.catalog.type' = 'filesystem',
                'warehouse' = 's3://warehouse/wh',
                's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.region' = 'us-east-1',
                's3.path.style.access' = 'true'
            )
        """
        sql "use `${catalogName}`.`test_paimon_spark_blob`"
        order_qt_show_tables "show tables"

        sql "set enable_file_scanner_v2 = true"
        sql "set enable_paimon_rust_reader = false"
        sql "set enable_count_push_down_for_external_table = false"
        sql "set batch_size = 1024"
        sql "set force_jni_scanner = true"
        sql "set paimon_blob_max_size = 10485760"

        ["blob_basic", "blob_all_null", "blob_size_limit", "blob_large_payload", "blob_multi_columns",
         "blob_partitioned", "blob_multi_snapshot", "blob_rollover", "blob_schema_evolution",
         "blob_compacted", "blob_descriptor_default"].each { table ->
            explain {
                sql "select id, image from ${table}"
                check { plan ->
                    def matcher = plan =~ /paimonNativeReadSplits=(\d+)\/(\d+)/
                    assertTrue(matcher.find(), "Missing Paimon split counters: ${plan}")
                    int nativeSplits = matcher.group(1).toInteger()
                    int totalSplits = matcher.group(2).toInteger()
                    return totalSplits > 0 && nativeSplits == 0
                }
            }
        }

        explain {
            sql "select id, image from blob_basic@options('blob-as-descriptor'='true')"
            check { plan ->
                def matcher = plan =~ /paimonNativeReadSplits=(\d+)\/(\d+)/
                assertTrue(matcher.find(), "Missing Paimon split counters: ${plan}")
                int nativeSplits = matcher.group(1).toInteger()
                int totalSplits = matcher.group(2).toInteger()
                return totalSplits > 0 && nativeSplits == 0
            }
        }

        explain {
            sql "select thumbnail, id, image from blob_multi_columns"
            check { plan ->
                def matcher = plan =~ /paimonNativeReadSplits=(\d+)\/(\d+)/
                assertTrue(matcher.find(), "Missing Paimon split counters: ${plan}")
                int nativeSplits = matcher.group(1).toInteger()
                int totalSplits = matcher.group(2).toInteger()
                return totalSplits > 0 && nativeSplits == 0
            }
        }

        explain {
            sql "select thumbnail, id, image from blob_rollover"
            check { plan ->
                def matcher = plan =~ /paimonNativeReadSplits=(\d+)\/(\d+)/
                assertTrue(matcher.find(), "Missing Paimon split counters: ${plan}")
                int nativeSplits = matcher.group(1).toInteger()
                int totalSplits = matcher.group(2).toInteger()
                return totalSplits > 0 && nativeSplits == 0
            }
        }

        explain {
            sql "select thumbnail, id, image from blob_rollover@options('blob-as-descriptor'='true')"
            check { plan ->
                def matcher = plan =~ /paimonNativeReadSplits=(\d+)\/(\d+)/
                assertTrue(matcher.find(), "Missing Paimon split counters: ${plan}")
                int nativeSplits = matcher.group(1).toInteger()
                int totalSplits = matcher.group(2).toInteger()
                return totalSplits > 0 && nativeSplits == 0
            }
        }

        // ORC anchors also use the JNI reader.
        explain {
            sql "select id, image from blob_orc_fallback"
            check { plan ->
                def matcher = plan =~ /paimonNativeReadSplits=(\d+)\/(\d+)/
                assertTrue(matcher.find(), "Missing Paimon split counters: ${plan}")
                int nativeSplits = matcher.group(1).toInteger()
                int totalSplits = matcher.group(2).toInteger()
                return totalSplits > 0 && nativeSplits == 0
            }
        }

        // Inline output: NULL, empty, binary and UTF-8 payloads, with metadata children left NULL.
        qt_basic_lengths """
            select id, case_name, image is null,
                   length(fl_get_binary(image)),
                   md5(cast(fl_get_binary(image) as string))
            from blob_basic order by id
        """
        qt_basic_bytes """
            select id, hex(cast(fl_get_binary(image) as string))
            from blob_basic where id in (3, 4, 5, 6, 7, 8, 16) order by id
        """
        qt_text_payloads """
            select id, cast(fl_get_binary(image) as string)
            from blob_basic where id in (9, 10, 14, 15, 17) order by id
        """
        // Full-payload checks use independent Spark-written ordinary columns as the oracle.
        // Inline output must leave every metadata child NULL, including size.
        qt_basic_integrity """
            select count(*), sum(image is null),
                   sum(image is not null and length(fl_get_binary(image)) = 0),
                   sum(not (length(fl_get_binary(image)) <=> expected_size)),
                   sum(not (hex(cast(fl_get_binary(image) as string)) <=> expected_hex)),
                   sum(element_at(image, 'uri') is not null
                       or element_at(image, 'offset') is not null
                       or element_at(image, 'size') is not null
                       or element_at(image, 'content_type') is not null
                       or element_at(image, 'checksum') is not null)
            from blob_basic
        """
        qt_blob_predicate """
            select id from blob_basic
            where cast(fl_get_binary(image) as string) = 'hello world' order by id
        """
        qt_and_predicate """
            select id from blob_basic
            where id > 10 and cast(fl_get_binary(image) as string) = 'hello world' order by id
        """
        qt_or_predicate """
            select id, hex(cast(fl_get_binary(image) as string)) from blob_basic
            where id = 1 or cast(fl_get_binary(image) as string) = 'hello world' order by id
        """

        qt_empty_count "select count(*) from blob_empty"
        qt_empty_projection """
            select id, length(fl_get_binary(image)) from blob_empty order by id
        """
        qt_all_null """
            select id, image is null, fl_get_binary(image) is null
            from blob_all_null order by id
        """
        // Two BLOB columns: independent values, projection order and filters on the other column.
        qt_multi_columns """
            select id, note, hex(cast(fl_get_binary(thumbnail) as string)),
                   length(fl_get_binary(image)),
                   md5(cast(fl_get_binary(image) as string))
            from blob_multi_columns order by id
        """
        qt_multi_integrity """
            select count(*),
                   sum(not (length(fl_get_binary(image)) <=> expected_size)
                       or not (hex(cast(fl_get_binary(image) as string)) <=> expected_hex)),
                   sum(not (length(fl_get_binary(thumbnail)) <=> expected_thumbnail_size)
                       or not (hex(cast(fl_get_binary(thumbnail) as string))
                               <=> expected_thumbnail_hex))
            from blob_multi_columns
        """
        // Filter on one BLOB but output only the other, including NULL and empty values.
        qt_only_thumbnail """
            select id, hex(cast(fl_get_binary(thumbnail) as string))
            from blob_multi_columns
            where image is null or length(fl_get_binary(image)) = 0 order by id
        """
        // Partition pruning, later snapshots, compaction and schema changes must preserve row mapping.
        qt_partition_summary """
            select region, count(*), sum(id),
                   sum(not (hex(cast(fl_get_binary(image) as string)) <=> expected_hex))
            from blob_partitioned group by region order by region nulls first
        """
        qt_partition_filter """
            select id, hex(cast(fl_get_binary(image) as string))
            from blob_partitioned where region = 'b' and id in (7, 9, 12) order by id
        """
        qt_null_partition """
            select id, length(fl_get_binary(image))
            from blob_partitioned where region is null order by id
        """
        qt_multi_snapshot """
            select id, case_name, hex(cast(fl_get_binary(image) as string))
            from blob_multi_snapshot order by id, case_name
        """
        qt_compacted """
            select id, hex(cast(fl_get_binary(image) as string))
            from blob_compacted order by id
        """
        qt_schema_evolution """
            select id, label, extra, length(fl_get_binary(image)),
                   hex(cast(fl_get_binary(image) as string))
            from blob_schema_evolution order by id
        """
        qt_orc_fallback """
            select id, hex(cast(fl_get_binary(image) as string))
            from blob_orc_fallback order by id
        """

        // Read 1025 rows to cross the 1024-row batch boundary, checking both BLOB lengths.
        // MD5/HEX samples below check bytes; descriptor cases check rollover across all files.
        qt_rollover_integrity """
            select count(*) as row_count, sum(id) as id_sum,
                   sum(not (length(fl_get_binary(image)) <=> expected_size))
                       as invalid_image_length_count,
                   sum(not (length(fl_get_binary(thumbnail)) <=> expected_thumbnail_size))
                       as invalid_thumbnail_length_count
            from blob_rollover
            where id between 0 and 1024
        """
        qt_rollover_sample """
            select id, length(fl_get_binary(image)),
                   md5(cast(fl_get_binary(image) as string)),
                   length(fl_get_binary(thumbnail)),
                   hex(cast(fl_get_binary(thumbnail) as string))
            from blob_rollover
            where id in (0, 1, 2, 16, 17, 18, 999, 1000, 1001, 1023, 1024, 1025,
                         1999, 2000, 2047, 2048, 2049, 2999, 3000, 3071, 3072,
                         3999, 4000, 4095, 4096)
            order by id
        """
        // Explicit sparse IDs let Paimon apply the filter; keep descending order and LIMIT coverage.
        qt_rollover_sparse """
            select id, md5(cast(fl_get_binary(image) as string)),
                   md5(cast(fl_get_binary(thumbnail) as string))
            from blob_rollover
            where id in (0, 997, 1994, 2991, 3988)
            order by id desc limit 3
        """
        qt_rollover_cross_predicate """
            select id, hex(cast(fl_get_binary(thumbnail) as string))
            from blob_rollover
            where id in (16, 17, 18, 999, 1000, 1001)
              and cast(fl_get_binary(image) as string) like 'row-%'
            order by id
        """

        // Descriptor output returns URI/offset/size while inline remains NULL.
        qt_descriptor_sample """
            select id, image is null,
                   case when image is null then null
                        else right(element_at(image, 'uri'), 5) = '.blob'
                             and element_at(image, 'offset') >= 4 end,
                   element_at(image, 'size'), fl_get_binary(image) is null,
                   element_at(image, 'content_type') is null, element_at(image, 'checksum') is null
            from blob_basic@options('blob-as-descriptor'='true')
            where id in (1, 2, 3, 7, 8, 9, 10, 13, 33, 34, 35, 36, 37, 38, 39)
            order by id
        """
        qt_descriptor_integrity """
            select count(*),
                   sum(not (element_at(image, 'size') <=> expected_size)
                       or not ((element_at(image, 'uri') is null) <=> (expected_size is null))
                       or not ((element_at(image, 'offset') is null) <=> (expected_size is null))
                       or fl_get_binary(image) is not null
                       or element_at(image, 'content_type') is not null
                       or element_at(image, 'checksum') is not null)
            from blob_basic@options('blob-as-descriptor'='true')
        """

        // Each BLOB column has 4097 rows. Metadata errors must be zero, including for NULLs.
        qt_rollover_descriptor_metadata """
            with blob_descriptors as (
                select 'image' as blob_column_name, image is null as is_null_blob,
                       element_at(image, 'uri') as blob_uri,
                       element_at(image, 'offset') as payload_offset,
                       element_at(image, 'size') as payload_size,
                       expected_size as expected_payload_size,
                       fl_get_binary(image) is not null as has_inline,
                       element_at(image, 'content_type') is not null as has_content_type,
                       element_at(image, 'checksum') is not null as has_checksum
                from blob_rollover@options('blob-as-descriptor'='true')
                union all
                select 'thumbnail', thumbnail is null,
                       element_at(thumbnail, 'uri'), element_at(thumbnail, 'offset'),
                       element_at(thumbnail, 'size'), expected_thumbnail_size,
                       fl_get_binary(thumbnail) is not null,
                       element_at(thumbnail, 'content_type') is not null,
                       element_at(thumbnail, 'checksum') is not null
                from blob_rollover@options('blob-as-descriptor'='true')
            )
            select blob_column_name, count(*) as row_count, sum(is_null_blob) as null_count,
                   sum(not (is_null_blob <=> (expected_payload_size is null))
                       or not (payload_size <=> expected_payload_size)
                       or not ((blob_uri is null) <=> is_null_blob)
                       or not ((payload_offset is null) <=> is_null_blob)
                       or (blob_uri is not null and right(blob_uri, 5) != '.blob')
                       or (payload_offset is not null and payload_offset < 4)
                       or has_inline or has_content_type or has_checksum) as invalid_metadata_count
            from blob_descriptors
            group by blob_column_name order by blob_column_name
        """

        // Both BLOB columns must span multiple files. Random file names and counts are not golden values.
        qt_rollover_descriptor_files """
            select 'image' as blob_column_name,
                   count(distinct element_at(image, 'uri')) > 1 as has_multiple_blob_files
            from blob_rollover@options('blob-as-descriptor'='true')
            union all
            select 'thumbnail', count(distinct element_at(thumbnail, 'uri')) > 1
            from blob_rollover@options('blob-as-descriptor'='true')
            order by blob_column_name
        """

        // Within each file: first payload starts at 4; the next starts at offset + size + 16.
        // NULL BLOBs have no record; empty payloads still consume the 16 framing bytes.
        qt_rollover_descriptor_offsets """
            with blob_ranges as (
                select 'image' as blob_column_name, id,
                       element_at(image, 'uri') as blob_uri,
                       element_at(image, 'offset') as payload_offset,
                       element_at(image, 'size') as payload_size
                from blob_rollover@options('blob-as-descriptor'='true')
                where image is not null
                union all
                select 'thumbnail', id, element_at(thumbnail, 'uri'),
                       element_at(thumbnail, 'offset'), element_at(thumbnail, 'size')
                from blob_rollover@options('blob-as-descriptor'='true')
                where thumbnail is not null
            ), expected_offsets as (
                select blob_column_name, payload_offset,
                       lag(payload_offset + payload_size + 16, 1, 4) over (
                           partition by blob_column_name, blob_uri order by payload_offset, id
                       ) as expected_payload_offset
                from blob_ranges
            )
            select blob_column_name, count(*) as payload_count,
                   sum(not (payload_offset <=> expected_payload_offset)) as invalid_offset_count
            from expected_offsets
            group by blob_column_name order by blob_column_name
        """

        // The SDK reader's internal descriptor option must not change the user's output
        // mode or poison the cached table when queries alternate between representations.
        qt_table_default_descriptor """
            select id, element_at(image, 'size'), fl_get_binary(image) is null
            from blob_descriptor_default order by id
        """
        qt_table_override_inline """
            select id, hex(cast(fl_get_binary(image) as string))
            from blob_descriptor_default@options('blob-as-descriptor'='false') order by id
        """
        qt_table_default_again """
            select id, element_at(image, 'size'), fl_get_binary(image) is null
            from blob_descriptor_default order by id
        """

        // Real payloads exercise the default 10 MiB boundary without returning large values.
        qt_large_default_exact """
            select id, length(fl_get_binary(image)),
                   md5(cast(fl_get_binary(image) as string)),
                   length(fl_get_binary(image)) <=> expected_size,
                   md5(cast(fl_get_binary(image) as string)) <=> expected_md5
            from blob_large_payload where id = 1
        """
        test {
            sql "select length(fl_get_binary(image)) from blob_large_payload where id = 2"
            exception "paimon_blob_max_size=10485760"
        }
        test {
            sql "select length(fl_get_binary(image)) from blob_large_payload where id = 3"
            exception "paimon_blob_max_size=10485760"
        }
        test {
            sql """
                select id from blob_large_payload
                where length(fl_get_binary(image)) <= 10485760
            """
            exception "paimon_blob_max_size=10485760"
        }
        qt_large_default_descriptor """
            select id, element_at(image, 'size'),
                   element_at(image, 'size') <=> expected_size,
                   element_at(image, 'uri') is not null,
                   element_at(image, 'offset') >= 4,
                   fl_get_binary(image) is null
            from blob_large_payload@options('blob-as-descriptor'='true') order by id
        """

        sql "set paimon_blob_max_size = 12582912"
        qt_large_raised_content """
            select id, length(fl_get_binary(image)),
                   md5(cast(fl_get_binary(image) as string)),
                   length(fl_get_binary(image)) <=> expected_size,
                   md5(cast(fl_get_binary(image) as string)) <=> expected_md5
            from blob_large_payload order by id
        """

        // Neither ordinary-column projection nor descriptor output needs payload bytes.
        sql "set paimon_blob_max_size = 1"
        qt_large_metadata_only """
            select id, case_name, expected_size, expected_md5
            from blob_large_payload order by id
        """
        qt_large_count "select count(*) from blob_large_payload"
        qt_large_descriptor_with_low_limit """
            select id, element_at(image, 'size'),
                   element_at(image, 'size') <=> expected_size,
                   element_at(image, 'uri') is not null,
                   element_at(image, 'offset') >= 4,
                   fl_get_binary(image) is null
            from blob_large_payload@options('blob-as-descriptor'='true') order by id
        """

        // A successful inline read must not cache the larger limit for later queries.
        sql "set paimon_blob_max_size = 10485760"
        test {
            sql "select length(fl_get_binary(image)) from blob_large_payload where id = 2"
            exception "paimon_blob_max_size=10485760"
        }

        // Keep the smaller boundary cases for NULL, empty and multiple BLOB columns.
        sql "set paimon_blob_max_size = 8"
        qt_limit_allowed """
            select id, length(fl_get_binary(image)),
                   hex(cast(fl_get_binary(image) as string))
            from blob_size_limit where id in (1, 2, 11, 12) order by id
        """
        test {
            sql "select cast(fl_get_binary(image) as string) from blob_size_limit where id = 13"
            exception "paimon_blob_max_size=8"
        }
        test {
            // BLOB-dependent filters run after materialization, so they cannot bypass the limit.
            sql "select id from blob_size_limit where length(fl_get_binary(image)) <= 8"
            exception "paimon_blob_max_size=8"
        }
        qt_limit_without_blob "select id from blob_size_limit where id = 13"
        qt_limit_count "select count(*) from blob_size_limit"
        qt_limit_only_thumbnail """
            select id, hex(cast(fl_get_binary(thumbnail) as string))
            from blob_multi_columns where id = 8
        """
        test {
            sql "select cast(fl_get_binary(image) as string) from blob_multi_columns where id = 8"
            exception "paimon_blob_max_size=8"
        }
        qt_limit_descriptor """
            select id, element_at(image, 'size'), fl_get_binary(image) is null
            from blob_size_limit@options('blob-as-descriptor'='true') where id = 13
        """
        sql "set paimon_blob_max_size = 9"
        qt_limit_raised """
            select id, length(fl_get_binary(image)),
                   hex(cast(fl_get_binary(image) as string))
            from blob_size_limit where id = 13
        """
        sql "set paimon_blob_max_size = 1"
        qt_table_default_with_limit """
            select id, element_at(image, 'size'), fl_get_binary(image) is null
            from blob_descriptor_default order by id
        """
        test {
            sql """
                select cast(fl_get_binary(image) as string)
                from blob_descriptor_default@options('blob-as-descriptor'='false') where id = 10
            """
            exception "paimon_blob_max_size=1"
        }

        // BLOB reading requires FileScannerV2 on both native and JNI paths.
        sql "set enable_file_scanner_v2 = false"
        test {
            sql "select image from blob_basic where id = 10"
            exception "Paimon BLOB columns require enable_file_scanner_v2=true"
        }
        sql "set enable_file_scanner_v2 = true"

        ["0", "-1", "2147483648"].each { invalidSize ->
            test {
                sql "set paimon_blob_max_size = ${invalidSize}"
                exception "paimon_blob_max_size must be between 1 and 2147483647 bytes"
            }
        }
    } finally {
        sql "drop catalog if exists ${catalogName}"
    }
}
