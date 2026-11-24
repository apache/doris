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

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder

suite("test_iceberg_rest_drop_purge_option", "p0,external,iceberg,external_docker,external_docker_iceberg,nonConcurrent") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("disable iceberg test")
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String endpoint = "http://${host}:${minioPort}"

    sql "drop catalog if exists iceberg_rest_drop_purge_option"
    sql """create catalog iceberg_rest_drop_purge_option properties (
        "type" = "iceberg",
        "iceberg.catalog.type" = "rest",
        "uri" = "http://${host}:${restPort}",
        "s3.endpoint" = "${endpoint}",
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.region" = "us-east-1",
        "use_path_style" = "true"
    )"""
    sql "switch iceberg_rest_drop_purge_option"
    sql "drop database if exists rest_drop_purge_db force"
    sql "drop database if exists rest_drop_purge_cascade_db force"
    sql "drop database if exists rest_drop_purge_enabled_cascade_db force"
    sql "create database rest_drop_purge_db"
    sql "use rest_drop_purge_db"

    def client = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new EndpointConfiguration(endpoint, "us-east-1"))
            .withPathStyleAccessEnabled(true)
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("admin", "password")))
            .build()
    try {
        def captureObjects = { String table ->
            def data = sql "select file_path from `${table}\$files` where content = 0"
            def metadata = sql "select file from `${table}\$metadata_log_entries`"
            assertTrue(!data.isEmpty(), "The table must have data files")
            assertTrue(!metadata.isEmpty(), "The table must have metadata files")
            def manifests = sql "select path from `${table}\$manifests`"
            def snapshots = sql "select manifest_list from `${table}\$snapshots`"
            assertTrue(!manifests.isEmpty(), "The table must have manifest files")
            assertTrue(!snapshots.isEmpty(), "The table must have manifest lists")
            return (data + metadata + manifests + snapshots).collect { it[0].toString() }.unique()
        }
        def checkObjects = { List<String> objects, boolean present ->
            objects.each { String object ->
                URI uri = new URI(object)
                assertTrue(uri.scheme in ["s3", "s3a", "s3n"], "Unexpected object URI: ${object}")
                assertTrue(client.doesObjectExist(uri.host, uri.path.substring(1)) == present,
                        "Unexpected object presence: ${object}, expected ${present}")
            }
        }

        sql "create table purge_default (id int)"
        sql "insert into purge_default values (1), (2)"
        qt_default_rows "select id from purge_default order by id"
        def defaultObjects = captureObjects("purge_default")
        checkObjects(defaultObjects, true)
        sql "drop table purge_default"
        checkObjects(defaultObjects, false)
        qt_default_unregistered "show tables like 'purge_default'"

        sql """alter catalog iceberg_rest_drop_purge_option set properties (
            "iceberg.rest.drop-with-purge-enabled" = "false")"""
        sql "use iceberg_rest_drop_purge_option.rest_drop_purge_db"
        sql "create table keep_table (id int)"
        sql "insert into keep_table values (3), (4)"
        qt_kept_rows "select id from keep_table order by id"
        def keptObjects = captureObjects("keep_table")
        checkObjects(keptObjects, true)
        sql "drop table keep_table"
        checkObjects(keptObjects, true)
        qt_kept_unregistered "show tables like 'keep_table'"

        sql "create database rest_drop_purge_cascade_db"
        sql "use rest_drop_purge_cascade_db"
        sql "create table keep_first (id int)"
        sql "create table keep_second (id int)"
        sql "insert into keep_first values (10)"
        sql "insert into keep_second values (20)"
        def firstObjects = captureObjects("keep_first")
        def secondObjects = captureObjects("keep_second")
        checkObjects(firstObjects, true)
        checkObjects(secondObjects, true)
        sql "use rest_drop_purge_db"
        sql "drop database rest_drop_purge_cascade_db force"
        checkObjects(firstObjects, true)
        checkObjects(secondObjects, true)
        qt_namespace_unregistered "show databases like 'rest_drop_purge_cascade_db'"

        sql """alter catalog iceberg_rest_drop_purge_option set properties (
            "iceberg.rest.drop-with-purge-enabled" = "true")"""
        test {
            sql """alter catalog iceberg_rest_drop_purge_option set properties (
                "iceberg.rest.drop-with-purge-enabled" = "invalid")"""
            exception "iceberg.rest.drop-with-purge-enabled must be true or false"
        }
        sql "use iceberg_rest_drop_purge_option.rest_drop_purge_db"
        sql "create table purge_enabled (id int)"
        sql "insert into purge_enabled values (5)"
        qt_enabled_rows "select id from purge_enabled order by id"
        def enabledObjects = captureObjects("purge_enabled")
        checkObjects(enabledObjects, true)
        sql "drop table purge_enabled"
        checkObjects(enabledObjects, false)
        sql "create database rest_drop_purge_enabled_cascade_db"
        sql "use rest_drop_purge_enabled_cascade_db"
        sql "create table purge_first (id int)"
        sql "create table purge_second (id int)"
        sql "insert into purge_first values (30)"
        sql "insert into purge_second values (40)"
        def purgedFirstObjects = captureObjects("purge_first")
        def purgedSecondObjects = captureObjects("purge_second")
        checkObjects(purgedFirstObjects, true)
        checkObjects(purgedSecondObjects, true)
        sql "use rest_drop_purge_db"
        sql "drop database rest_drop_purge_enabled_cascade_db force"
        checkObjects(purgedFirstObjects, false)
        checkObjects(purgedSecondObjects, false)
        qt_purged_namespace_unregistered "show databases like 'rest_drop_purge_enabled_cascade_db'"
        // Changing the option affects later drops, not objects deliberately retained by earlier ones.
        checkObjects(keptObjects, true)
        checkObjects(firstObjects, true)
        checkObjects(secondObjects, true)
        qt_enabled_unregistered "show tables like 'purge_enabled'"
    } finally {
        client.shutdown()
    }
}
