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

import org.apache.http.HttpStatus
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

suite("test_load_with_transfer_encoding", "p0") {
    def table_name = "test_load_with_transfer_encoding"

    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE ${table_name} (
        `place_id` varchar(14) NOT NULL,
        `card_id` varchar(12) NOT NULL,
        `shift_id` varchar(4) NULL,
        `id` bigint NOT NULL AUTO_INCREMENT (1),
        `created` datetime(6) NOT NULL,
        `creater` bigint NULL,
        `deleted` int NOT NULL,
        `updated` datetime(6) NULL,
        `card_type_id` varchar(8) NOT NULL,
        `card_type_name` varchar(20) NULL,
        `cash_balance` int NOT NULL,
        `cashier_id` varchar(8) NULL,
        `client_id` varchar(4) NULL,
        `cost` int NOT NULL,
        `creater_name` varchar(50) NULL,
        `details` varchar(200) NULL,
        `id_name` varchar(50) NOT NULL,
        `id_number` varchar(18) NOT NULL,
        `last_client_id` varchar(4) NULL,
        `login_id` varchar(16) NULL,
        `operation_type` varchar(50) NOT NULL,
        `present` int NOT NULL,
        `present_balance` int NOT NULL,
        `remark` varchar(200) NULL,
        `source_type` varchar(50) NOT NULL,
        `online_account` int NOT NULL
        ) ENGINE = OLAP DUPLICATE KEY (`place_id`, `card_id`, `shift_id`) DISTRIBUTED BY HASH (`operation_type`) BUCKETS 10 PROPERTIES (
        "file_cache_ttl_seconds" = "0",
        "is_being_synced" = "false",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "replication_num" = "1",
        "group_commit_data_bytes" = "134217728"
        );
    """


    String db = context.config.getDbNameByFile(context.file)

    def load_data = { inputFile, int count ->
        String url = """${getS3Url()}/regression/load/data/${inputFile}.json"""
        String fileName

        HttpClients.createDefault().withCloseable { client ->
            def file = new File("${context.config.cacheDataPath}/${inputFile}.json")
            if (file.exists()) {
                log.info("Found ${url} in ${file.getAbsolutePath()}");
                fileName = file.getAbsolutePath()
                return;
            }

            log.info("Start to down data from ${url} to $context.config.cacheDataPath}/");
            CloseableHttpResponse resp = client.execute(RequestBuilder.get(url).build())
            int code = resp.getStatusLine().getStatusCode()

            if (code != HttpStatus.SC_OK) {
                String streamBody = EntityUtils.toString(resp.getEntity())
                log.info("Fail to download data ${url}, code: ${code}, body:\n${streamBody}")
                throw new IllegalStateException("Get http stream failed, status code is ${code}, body:\n${streamBody}")
            }

            InputStream httpFileStream = resp.getEntity().getContent()
            java.nio.file.Files.copy(httpFileStream, file.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING)
            httpFileStream.close()
            fileName = file.getAbsolutePath()
            log.info("File downloaded to: ${fileName}")
        }

        def command = """curl --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword} -H read_json_by_line:false -H Expect:100-continue -H max_filter_ratio:1 -H strict_mode:false -H strip_outer_array:true -H columns:id,created,creater,deleted,updated,card_id,card_type_id,card_type_name,cash_balance,cashier_id,client_id,cost,creater_name,details,id_name,id_number,last_client_id,login_id,operation_type,place_id,present,present_balance,remark,shift_id,source_type,online_account -H format:json -H Transfer-Encoding:chunked -T ${fileName} -XPUT http://${context.config.feHttpAddress}/api/${db}/${table_name}/_stream_load"""
        log.info("stream load: ${command}")
        def process = command.execute()
        def code = process.waitFor()
        def out = process.text
        def json = parseJson(out)
        log.info("stream load result is:: ${out}".toString())
        assertEquals("success", json.Status.toLowerCase())
        assertEquals(count, json.NumberLoadedRows)
        qt_sql """ select count() from ${table_name} """
    }

    load_data.call("test_load_with_transfer_encoding", 15272)
    load_data.call("test_transfer_encoding_small", 10)
    
}

