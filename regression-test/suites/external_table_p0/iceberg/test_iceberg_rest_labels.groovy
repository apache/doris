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

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

// t$labels shows the labels a REST catalog returns with each table load (apache/iceberg#15750). The docker REST
// fixture returns none (a fixture option is apache/iceberg#18047), so this suite puts a small proxy in front of
// it: the proxy forwards every request unchanged and adds this suite's labels to the load-table responses, which
// is all a labelling catalog does differently. Everything after the HTTP response is the real path: the iceberg
// REST client parses the labels, FE plans the labels split and BE reads it through the JNI system-table scanner.
suite("test_iceberg_rest_labels", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String restUri = "http://${externalEnvIp}:${restPort}"

    // Labels the proxy adds, keyed by "<namespace>.<table>". A table without an entry keeps the fixture's
    // response as is, without labels.
    Map<String, Map> labelsByTable = new ConcurrentHashMap<>()
    // Lengths and hop-by-hop headers belong to each connection. Accept-Encoding is dropped so that the fixture
    // answers in plain JSON the proxy can rewrite.
    Set<String> droppedRequestHeaders = ["host", "connection", "keep-alive", "content-length", "transfer-encoding",
            "expect", "accept-encoding"] as Set
    def loadTablePath = ~'^/v1/namespaces/([^/]+)/tables/([^/]+)$'
    def proxyExecutor = Executors.newFixedThreadPool(4)
    HttpServer proxy = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0)
    proxy.setExecutor(proxyExecutor)
    proxy.createContext("/", { HttpExchange exchange ->
        try {
            String rawPath = exchange.requestURI.rawPath
            String rawQuery = exchange.requestURI.rawQuery
            def upstream = (HttpURLConnection) new URL(restUri + rawPath + (rawQuery == null ? "" : "?" + rawQuery))
                    .openConnection()
            upstream.setRequestMethod(exchange.requestMethod)
            upstream.setConnectTimeout(10000)
            upstream.setReadTimeout(60000)
            exchange.requestHeaders.each { String name, List<String> values ->
                if (!droppedRequestHeaders.contains(name.toLowerCase())) {
                    values.each { upstream.addRequestProperty(name, it) }
                }
            }
            byte[] requestBody = exchange.requestBody.bytes
            if (requestBody.length > 0) {
                upstream.setDoOutput(true)
                upstream.outputStream.withCloseable { it.write(requestBody) }
            }
            int status = upstream.responseCode
            InputStream upstreamBody = status >= 400 ? upstream.errorStream : upstream.inputStream
            byte[] body = upstreamBody == null ? new byte[0] : upstreamBody.withCloseable { it.bytes }
            def loadTable = loadTablePath.matcher(rawPath)
            if ("GET" == exchange.requestMethod && status == 200 && loadTable.matches()) {
                Map labels = labelsByTable.get(loadTable.group(1) + "." + loadTable.group(2))
                if (labels != null) {
                    def response = new JsonSlurper().parseText(new String(body, "UTF-8"))
                    response.labels = labels
                    body = JsonOutput.toJson(response).getBytes("UTF-8")
                }
            }
            // Pass back only the content type. The fixture's validators describe the body before the rewrite: a
            // forwarded ETag would let the iceberg client revalidate its cached load and keep stale labels.
            String contentType = upstream.getHeaderField("Content-Type")
            if (contentType != null) {
                exchange.responseHeaders.set("Content-Type", contentType)
            }
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length)
            if (body.length > 0) {
                exchange.responseBody.withCloseable { it.write(body) }
            }
        } finally {
            exchange.close()
        }
    } as HttpHandler)
    proxy.start()
    try {
        // FE loads the tables through the proxy, so it must be reachable from FE; like the HTTP TVF suite, use
        // httpTvfHost when the regression runner is not on the docker host.
        String proxyHost = context.config.otherConfigs.get("httpTvfHost") ?: externalEnvIp
        String proxyUri = "http://${proxyHost}:${proxy.address.port}"

        sql "drop catalog if exists test_iceberg_rest_labels"
        sql """create catalog test_iceberg_rest_labels properties (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'uri' = '${proxyUri}',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}', 's3.region' = 'us-east-1'
        )"""
        // With a zero table cache TTL every statement loads the table, and so its labels, again.
        sql "drop catalog if exists test_iceberg_rest_labels_nocache"
        sql """create catalog test_iceberg_rest_labels_nocache properties (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'uri' = '${proxyUri}',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}', 's3.region' = 'us-east-1',
            'meta.cache.iceberg.table.ttl-second' = '0'
        )"""
        // Vended credentials turn the cross-query table cache off altogether, so this catalog reads the table that
        // each statement loads instead of a cached one.
        sql "drop catalog if exists test_iceberg_rest_labels_vended"
        sql """create catalog test_iceberg_rest_labels_vended properties (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'uri' = '${proxyUri}',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}', 's3.region' = 'us-east-1',
            'iceberg.rest.vended-credentials-enabled' = 'true'
        )"""
        // The same tables read straight from the fixture, which returns no labels. The tables are created through
        // it, so no proxied catalog caches a load before its labels are set.
        sql "drop catalog if exists test_iceberg_rest_labels_direct"
        sql """create catalog test_iceberg_rest_labels_direct properties (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'uri' = '${restUri}',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}', 's3.region' = 'us-east-1'
        )"""

        sql "create database if not exists test_iceberg_rest_labels_direct.test_iceberg_rest_labels_db"
        sql "drop table if exists test_iceberg_rest_labels_direct.test_iceberg_rest_labels_db.labeled"
        sql """create table test_iceberg_rest_labels_direct.test_iceberg_rest_labels_db.labeled (
            id int, email string, phone string, address struct<city: string, zip: string>
        )"""
        sql """insert into test_iceberg_rest_labels_direct.test_iceberg_rest_labels_db.labeled values
            (1, 'a@example.com', '555-0101', named_struct('city', 'Paris', 'zip', '75001')),
            (2, 'b@example.com', '555-0102', named_struct('city', 'Lyon', 'zip', '69001'))"""
        sql "drop table if exists test_iceberg_rest_labels_direct.test_iceberg_rest_labels_db.labeled_empty"
        sql "create table test_iceberg_rest_labels_direct.test_iceberg_rest_labels_db.labeled_empty (id int, note string)"
        sql "drop table if exists test_iceberg_rest_labels_direct.test_iceberg_rest_labels_db.unlabeled"
        sql "create table test_iceberg_rest_labels_direct.test_iceberg_rest_labels_db.unlabeled (id int)"
        sql "insert into test_iceberg_rest_labels_direct.test_iceberg_rest_labels_db.unlabeled values (1), (2)"

        // Field labels are keyed by field id, which the catalog assigns: read the ids from the fixture.
        def loaded = new JsonSlurper().parseText(
                new URL("${restUri}/v1/namespaces/test_iceberg_rest_labels_db/tables/labeled").getText(
                        connectTimeout: 10000, readTimeout: 10000))
        def schema = loaded.metadata.schemas.find { it["schema-id"] == loaded.metadata["current-schema-id"] }
        def emailId = schema.fields.find { it.name == "email" }.id
        def phoneId = schema.fields.find { it.name == "phone" }.id
        def cityId = schema.fields.find { it.name == "address" }.type.fields.find { it.name == "city" }.id
        labelsByTable.put("test_iceberg_rest_labels_db.labeled", [
                "object-labels": ["owner": "team-a", "cost.center": "cc-42", "steward": "数据平台组"],
                "fields": [
                        ["field-id": emailId, "labels": ["classification": "pii"]],
                        ["field-id": phoneId, "labels": ["classification": "pii", "masking": "partial"]],
                        ["field-id": cityId, "labels": ["classification": "location"]]]])
        labelsByTable.put("test_iceberg_rest_labels_db.labeled_empty", ["object-labels": ["owner": "team-b"]])

        sql "switch test_iceberg_rest_labels"
        sql "use test_iceberg_rest_labels_db"
        order_qt_desc "desc labeled\$labels"
        // One row per label. A field label names its field from the current schema, a nested one by its full path.
        order_qt_all "select * from labeled\$labels"
        // BE reads the split's rows by position in the order of the columns it asks for, so another order or a
        // subset of the columns must still put every value in its own column. KEY is a reserved word in Doris,
        // so the key column is quoted.
        order_qt_projection "select value, `key`, field_name, scope, field_id from labeled\$labels"
        order_qt_one_column "select `key` from labeled\$labels"
        order_qt_filter_field "select field_name, `key`, value from labeled\$labels where scope = 'field' and value = 'pii'"
        order_qt_filter_object "select `key`, value from labeled\$labels where field_id is null"
        // COUNT counts labels rather than taking the table's record count: two records, seven labels.
        order_qt_count "select count(*) from labeled\$labels"
        order_qt_count_table "select count(*) from labeled"
        // Labels come with the table load, not with a snapshot: a table without any commit has them too.
        order_qt_no_snapshot "select * from labeled_empty\$labels"
        // A table the catalog returns no labels for, and the labelled table read without the proxy.
        order_qt_unlabeled "select count(*) from unlabeled\$labels"
        order_qt_direct "select count(*) from test_iceberg_rest_labels_direct.test_iceberg_rest_labels_db.labeled\$labels"
        // Without the table cache the labels come straight from the statement's own table load.
        order_qt_vended "select * from test_iceberg_rest_labels_vended.test_iceberg_rest_labels_db.labeled\$labels"

        // field_name follows the field id through schema evolution: a renamed column keeps its labels under its new
        // name, and a dropped one keeps its field_id with a NULL field_name.
        sql "alter table labeled rename column email email_address"
        sql "alter table labeled drop column phone"
        order_qt_schema_evolution "select * from labeled\$labels"
        // Labels are not versioned: a time-travel pin on t$labels is ignored, so the rows are those of the current
        // load and field names still come from the current schema, not from the schema of the pinned snapshot.
        def snapshotId = sql("select snapshot_id from labeled\$snapshots")[0][0]
        order_qt_time_travel "select * from labeled\$labels for version as of ${snapshotId}"

        // Labels belong to the table load, so they follow the table cache like the rest of the table: the cached
        // catalog keeps the labels it loaded until REFRESH TABLE, the uncached one sees new labels at once.
        labelsByTable.put("test_iceberg_rest_labels_db.labeled_empty", ["object-labels": ["owner": "team-c", "tier": "gold"]])
        order_qt_cached "select `key`, value from labeled_empty\$labels"
        order_qt_uncached "select `key`, value from test_iceberg_rest_labels_nocache.test_iceberg_rest_labels_db.labeled_empty\$labels"
        sql "refresh table labeled_empty"
        order_qt_refreshed "select `key`, value from labeled_empty\$labels"

        // Only a REST catalog returns labels with the table load, so no other catalog offers t$labels.
        sql "drop catalog if exists test_iceberg_rest_labels_hadoop"
        sql """create catalog test_iceberg_rest_labels_hadoop properties (
            'type' = 'iceberg', 'iceberg.catalog.type' = 'hadoop',
            'warehouse' = 's3a://warehouse/test_iceberg_rest_labels_hadoop',
            's3.access_key' = 'admin', 's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}', 's3.region' = 'us-east-1'
        )"""
        sql "create database if not exists test_iceberg_rest_labels_hadoop.test_iceberg_rest_labels_db"
        sql "drop table if exists test_iceberg_rest_labels_hadoop.test_iceberg_rest_labels_db.hadoop_table"
        sql "create table test_iceberg_rest_labels_hadoop.test_iceberg_rest_labels_db.hadoop_table (id int)"
        test {
            sql "select * from test_iceberg_rest_labels_hadoop.test_iceberg_rest_labels_db.hadoop_table\$labels"
            exception "Unknown sys table 'hadoop_table\$labels'"
        }

        // The proxy stops with the suite, so its catalogs go too; the tables stay readable through the direct one.
        sql "switch internal"
        sql "drop catalog test_iceberg_rest_labels"
        sql "drop catalog test_iceberg_rest_labels_nocache"
        sql "drop catalog test_iceberg_rest_labels_vended"
    } finally {
        proxy.stop(0)
        proxyExecutor.shutdownNow()
    }
}
