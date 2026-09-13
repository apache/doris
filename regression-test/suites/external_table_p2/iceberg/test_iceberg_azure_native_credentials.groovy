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

import org.apache.doris.regression.action.ProfileAction

// Do not register a successful, empty suite when the Azure fixture is disabled.
String enabled = context.config.otherConfigs.get("enableIcebergAzureNativeTest")
if (!"true".equalsIgnoreCase(enabled)) {
    logger.info("SKIP test_iceberg_azure_native_credentials: enableIcebergAzureNativeTest is not true")
    return
}

// Opt in only with a write-enabled Azure catalog and an authorized test namespace.
// Provision credentials outside the suite; never put a PAT, SAS, or secret into SQL here.
// Run separately with vended SAS, SharedKey, or client-secret OAuth2 catalog fixtures.
// Validate before suite registration: framework dryRun does not execute suite bodies.
String catalog = context.config.otherConfigs.get("icebergAzureNativeCatalog")
String database = context.config.otherConfigs.get("icebergAzureNativeDatabase")
[icebergAzureNativeCatalog: catalog, icebergAzureNativeDatabase: database].each { key, value ->
    if (value == null || !(value ==~ /[A-Za-z_][A-Za-z0-9_]*/)) {
        throw new IllegalArgumentException("${key} must name a preconfigured test catalog/namespace")
    }
}
// Some REST services reject v2 delete-file commits or a particular data format. Select
// the fixture's declared capability explicitly; a remote rejection is never an automatic skip.
String selectedVersions = context.config.otherConfigs.getOrDefault("icebergAzureNativeFormatVersions", "2,3")
String selectedFormats = context.config.otherConfigs.getOrDefault("icebergAzureNativeFileFormats", "parquet,orc")
if (!["2", "3", "2,3"].contains(selectedVersions)) {
    throw new IllegalArgumentException("icebergAzureNativeFormatVersions must be 2, 3, or 2,3")
}
if (!["parquet", "orc", "parquet,orc"].contains(selectedFormats)) {
    throw new IllegalArgumentException("icebergAzureNativeFileFormats must be parquet, orc, or parquet,orc")
}
List<Integer> formatVersions = selectedVersions.split(",").collect { Integer.parseInt(it) }
List<String> fileFormats = selectedFormats.split(",").toList()
if (context.config.dryRun) {
    logger.info("DRY RUN test_iceberg_azure_native_credentials: configuration checked; no Azure suite executed")
    return
}

suite("test_iceberg_azure_native_credentials", "p2,external,iceberg") {
    // An enabled real run must fail on permissions, unsupported table formats, or missing
    // profiles instead of silently treating any of those conditions as a skipped case.
    sql """switch `${catalog}`"""
    sql """use `${database}`"""

    def profileAction = new ProfileAction(context)
    def positiveCounter = { String profile, String name ->
        def matches = profile =~ ("(?m)^\\s*(?:-\\s*)?" + java.util.regex.Pattern.quote(name)
                + ":\\s+([0-9][0-9,.]*)")
        return matches.any { match -> match[1].replace(",", "").toDouble() > 0 }
    }
    def readNativeRange = { String table, String stage ->
        String tag = "azure_native_${table}_${stage}_" + UUID.randomUUID().toString()
        // Disable SQL, block-file and Parquet page caches for this probe. The page cache is
        // independent of enable_file_cache and can otherwise satisfy every column after DML.
        // S3Profile belongs to the common object-store reader; Azure URI checks below identify
        // its Azure use. No JNI/Hadoop reader is an acceptable substitute for these counters.
        "order_qt_${table}_${stage}" """
            select /*+ SET_VAR(enable_profile=true, profile_level=2,
                               enable_file_cache=false, enable_sql_cache=false,
                               enable_parquet_file_page_cache=false) */
                   /* ${tag} */ id, payload, score from ${table}
        """
        String profile = profileAction.getProfileBySql(tag, ["S3Profile", "TotalGetRequest", "TotalBytesRead"])
        assertTrue(positiveCounter(profile, "TotalGetRequest"),
                "${tag}: expected a native object-store GET, not a metadata-only count or cached result")
        assertTrue(positiveCounter(profile, "TotalBytesRead"), "${tag}: expected native range bytes")
        // The tag locates the query/profile without logging credentials or the full profile.
        logger.info("Azure native range verified for profile tag ${tag}")
    }
    def readPositionDeletes = { String table, String stage ->
        // Project stored delete columns, not COUNT(*) alone. The DV reader does not currently
        // collect S3FileReader's profile before destruction, so its row results are checked
        // here without claiming that a missing GET/bytes counter proves missing native I/O.
        "order_qt_${table}_${stage}" """
            select /*+ SET_VAR(enable_file_cache=false, enable_sql_cache=false) */
                   count(*) > 0, min(pos) >= 0, sum(length(file_path)) > 0
            from ${table}\$position_deletes
        """
    }

    // Only fixed suite-owned tables are dropped, at the beginning of each case. Do not drop
    // the supplied catalog/database or remove the resulting tables: retain them for diagnosis.
    for (int version : formatVersions) {
        for (String format : fileFormats) {
            String table = "azure_native_credentials_v${version}_${format}"
            sql """drop table if exists ${table}"""
            sql """
                create table ${table} (id int, payload string, score int) engine=iceberg
                properties (
                    "format-version" = "${version}",
                    "write.format.default" = "${format}",
                    "write.delete.mode" = "merge-on-read",
                    "write.update.mode" = "merge-on-read",
                    "write.merge.mode" = "merge-on-read"
                )
            """
            sql """insert into ${table} values
                   (1, 'one', 10), (2, 'two', 20), (3, 'three', 30), (4, 'four', 40)"""

            // All four rows must share the original data file: deleting id=1 then updating
            // id=2 and merging id=3 must touch that same file and consume its old deletes.
            "order_qt_${table}_single_data_file" """
                select count(*) = 1, sum(record_count) = 4 from ${table}\$data_files
            """

            // Do not snapshot UUID-based file paths. These booleans must both be true and the
            // format must match. The URI predicate excludes S3/HDFS and the OneLake host.
            "order_qt_${table}_azure_files" """
                select count(*) > 0,
                       count(*) = sum(case when
                           file_path regexp '^(abfs|abfss|wasb|wasbs)://[^/]+@[^/]+[.](dfs|blob)[.]core[.]windows[.]net/'
                           or file_path regexp '^https://[^/]+[.](dfs|blob)[.]core[.]windows[.]net/'
                           then 1 else 0 end),
                       min(lower(file_format)), max(lower(file_format))
                from ${table}\$files
            """
            readNativeRange(table, "insert_read")

            // Select manifest-backed fields as well as materialized metadata table fields.
            // This exercises serialized Iceberg FileIO and StaticDataTask, not native data I/O.
            "order_qt_${table}_entries" """
                select count(*) > 0, min(struct_element(data_file, 'record_count')) > 0
                from ${table}\$entries
            """
            "order_qt_${table}_manifests" """
                select count(*) > 0, sum(added_data_files_count) > 0 from ${table}\$manifests
            """
            "order_qt_${table}_all_manifests" """
                select count(*) > 0, sum(length(path)) > 0 from ${table}\$all_manifests
            """

            // Delete only part of the data. V3 must leave Puffin DVs for the subsequent
            // UPDATE and MERGE to consume; V2 exercises ordinary position delete files.
            sql """delete from ${table} where id = 1"""
            String deleteFormat = version == 3 ? "puffin" : format
            "order_qt_${table}_delete_files" """
                select count(*) > 0, sum(record_count) = 1,
                       min(lower(file_format)) = '${deleteFormat}',
                       max(lower(file_format)) = '${deleteFormat}'
                from ${table}\$delete_files
            """
            readPositionDeletes(table, "position_deletes")
            readNativeRange(table, "delete_read")

            sql """update ${table} set score = score + 100 where id = 2"""
            readNativeRange(table, "update_read")
            "order_qt_${table}_old_deletes_before_merge" """
                select count(*) > 0, sum(record_count) = 2,
                       min(lower(file_format)) = '${deleteFormat}',
                       max(lower(file_format)) = '${deleteFormat}'
                from ${table}\$delete_files
            """
            sql """
                merge into ${table} t
                using (select 3 as id, 'three_merged' as payload, 330 as score
                       union all select 5, 'five', 50) s
                on t.id = s.id
                when matched then update set payload = s.payload, score = s.score
                when not matched then insert (id, payload, score) values (s.id, s.payload, s.score)
            """
            readNativeRange(table, "merge_read")
            readPositionDeletes(table, "position_deletes_after_merge")

            sql """insert overwrite table ${table} values (7, 'seven', 70), (8, 'eight', 80)"""
            readNativeRange(table, "overwrite_read")
        }
    }
}
