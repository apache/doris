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

import java.io.File
import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS;

suite("test_export_variant_10k_columns", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    // String bucket = context.config.otherConfigs.get("s3BucketName");
    String bucket = getS3BucketName()

    def table_export_name = "test_export_variant_10k"
    def table_load_name = "test_load_variant_10k"
    def outfile_path_prefix = """${bucket}/export/p0/variant_10k/exp"""

    def waiting_export = { export_label ->
        while (true) {
            def res = sql """ show export where label = "${export_label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                def json = parseJson(res[0][11])
                assert json instanceof List
                // assertEquals("1", json.fileNumber[0][0])
                log.info("outfile_path: ${json.url[0][0]}")
                return json.url[0][0];
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }
    }

    // 1. Create table with variant column
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_export_name} (
        `id` INT NOT NULL,
        `v` VARIANT<PROPERTIES ("variant_max_subcolumns_count" = "2048")> NULL
    )
    DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
    """

    // 2. Generate data with 10000 keys in variant
    // Generate N=100,000 rows.
    // Total 10,000 columns, but each row only has 50 columns (sparse).
    // This simulates a realistic sparse wide table scenario.

    File dataFile = File.createTempFile("variant_10k_data", ".json")
    dataFile.deleteOnExit()
    int num_rows = 1000
    try {
        dataFile.withWriter { writer ->
            StringBuilder sb = new StringBuilder()
            for (int i = 1; i <= num_rows; i++) {
                sb.setLength(0)
                sb.append("{\"id\": ").append(i).append(", \"v\": {")
                // Select 50 keys out of 10000 for each row
                for (int k = 0; k < 50; k++) {
                    if (k > 0) sb.append(", ")
                    // Scatter the keys to ensure coverage of all 10000 columns across rows
                    int keyIdx = (i + k * 200) % 10000
                    sb.append('"k').append(keyIdx).append('":').append(i)
                }
                sb.append("}}\n")
                writer.write(sb.toString())
            }
        }

        // 3. Stream Load
        streamLoad {
            table table_export_name
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            file dataFile.getAbsolutePath()
            time 60000 // 60s
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("Success", json.Status)
                assertEquals(num_rows, json.NumberTotalRows)
                assertEquals(num_rows, json.NumberLoadedRows)
            }
        }
    } finally {
        dataFile.delete()
    }

    // def format = "parquet"
    def format = "native"

    // 4. Export to S3 (Parquet)
    def uuid = UUID.randomUUID().toString()
    // def outFilePath = """/tmp/variant_10k_export"""
    def outFilePath = """${outfile_path_prefix}_${uuid}"""
    def label = "label_${uuid}"

    try {
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "${format}"
            )
            WITH S3(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${getS3Provider()}"
            );
        """
        
        long startExport = System.currentTimeMillis()
        def outfile_url = waiting_export.call(label)
        long endExport = System.currentTimeMillis()
        logger.info("Export ${num_rows} rows with variant took ${endExport - startExport} ms")
        
        // 5. Validate by S3 TVF
        def s3_tvf_sql = """ s3(
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${format}",
                "s3.access_key"= "${ak}",
                "s3.secret_key" = "${sk}",
                "format" = "${format}",
                "provider" = "${getS3Provider()}",
                "region" = "${region}"
            ) """

        // Verify row count
        def res = sql """ SELECT count(*) FROM ${s3_tvf_sql} """
        assertEquals(num_rows, res[0][0])

        def value_type = "VARIANT<PROPERTIES (\"variant_max_subcolumns_count\" = \"2048\")>"
        if (new Random().nextInt(2) == 0) {
            value_type = "text"
        }
        // 6. Load back into Doris (to a new table) to verify import performance/capability
        sql """ DROP TABLE IF EXISTS ${table_load_name} """
        sql """
        CREATE TABLE ${table_load_name} (
            `id` INT,
            `v` ${value_type}
        )
        DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """
        
        // Use LOAD LABEL. The label contains '-' from UUID, so it must be quoted with back-quotes.
        def load_label = "load_${uuid}"
        
        sql """
            LOAD LABEL `${load_label}`
            (
                DATA INFILE("s3://${outFilePath}/*")
                INTO TABLE ${table_load_name}
                FORMAT AS "${format}"
                (id, v)
            )
            WITH S3 (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${getS3Provider()}"
            );
        """
        
        Awaitility.await().atMost(240, SECONDS).pollInterval(5, SECONDS).until({
            def loadResult = sql """
           show load where label = '${load_label}'
           """
            if (loadResult.get(0).get(2) == 'CANCELLED' || loadResult.get(0).get(2) == 'FAILED') {
                println("load failed: " + loadResult.get(0))
                throw new RuntimeException("load failed"+ loadResult.get(0))
            }
            return loadResult.get(0).get(2) == 'FINISHED'
        }) 
        // Check if data loaded
        def load_count = sql """ SELECT count(*) FROM ${table_load_name} """
        assertEquals(num_rows, load_count[0][0])

        // Check variant data integrity (sample)
        // Row 1 has keys: (1 + k*200) % 10000. For k=0, key is k1. Value is 1.
        def check_v = sql """ SELECT cast(v['k1'] as int) FROM ${table_load_name} WHERE id = 1 """
        assertEquals(1, check_v[0][0])

    } finally {
        // try_sql("DROP TABLE IF EXISTS ${table_export_name}")
        // try_sql("DROP TABLE IF EXISTS ${table_load_name}")
    }
}
