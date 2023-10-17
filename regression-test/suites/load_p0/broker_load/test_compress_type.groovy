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

suite("test_compress_type", "load_p0") {
    def tableName = "basic_data"

    // GZ/LZO/BZ2/LZ4FRAME/DEFLATE/LZOP
    def compressTypes = [
            "COMPRESS_TYPE AS \"GZ\"",
            "COMPRESS_TYPE AS \"BZ2\"",
            "COMPRESS_TYPE AS \"LZ4FRAME\"",
            "COMPRESS_TYPE AS \"GZ\"",
            "COMPRESS_TYPE AS \"BZ2\"",
            "COMPRESS_TYPE AS \"LZ4FRAME\"",
            "",
            "",
            "",
            "",
            "",
            "",
    ]

    def fileFormat = [
            "FORMAT AS \"CSV\"",
            "FORMAT AS \"CSV\"",
            "FORMAT AS \"CSV\"",
            "",
            "",
            "",
            "FORMAT AS \"CSV\"",
            "FORMAT AS \"CSV\"",
            "FORMAT AS \"CSV\"",
            "",
            "",
            ""
    ]

    def paths = [
            "s3://doris-build-1308700295/regression/load/data/basic_data.csv.gz",
            "s3://doris-build-1308700295/regression/load/data/basic_data.csv.bz2",
            "s3://doris-build-1308700295/regression/load/data/basic_data.csv.lz4",
            "s3://doris-build-1308700295/regression/load/data/basic_data.csv.gz",
            "s3://doris-build-1308700295/regression/load/data/basic_data.csv.bz2",
            "s3://doris-build-1308700295/regression/load/data/basic_data.csv.lz4",
            "s3://doris-build-1308700295/regression/load/data/basic_data.csv.gz",
            "s3://doris-build-1308700295/regression/load/data/basic_data.csv.bz2",
            "s3://doris-build-1308700295/regression/load/data/basic_data.csv.lz4",
            "s3://doris-build-1308700295/regression/load/data/basic_data.csv.gz",
            "s3://doris-build-1308700295/regression/load/data/basic_data.csv.bz2",
            "s3://doris-build-1308700295/regression/load/data/basic_data.csv.lz4"
    ]
    def labels = []

    def ak = getS3AK()
    def sk = getS3SK()


    def i = 0
    sql new File("""${ context.file.parent }/ddl/basic_data_drop.sql""").text
    sql new File("""${ context.file.parent }/ddl/basic_data.sql""").text
    for (String compressType : compressTypes) {
        def label = "test_s3_load_compress" + UUID.randomUUID().toString().replace("-", "0") + i
        labels.add(label)
        def format_str = fileFormat[i]
        def path = paths[i]
        def sql_str = """
            LOAD LABEL $label (
                DATA INFILE("$path")
                INTO TABLE $tableName
                COLUMNS TERMINATED BY "|"
                $format_str
                $compressType
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "cos.ap-beijing.myqcloud.com",
                "AWS_REGION" = "ap-beijing"
            )
            properties(
                "use_new_load_scan_node" = "true"
            )
            """
        logger.info("submit sql: ${sql_str}");
        sql """${sql_str}"""
        logger.info("Submit load with lable: $label, table: $tableName, path: $path")
        ++i
    }

    i = 0
    for (String label in labels) {
        def max_try_milli_secs = 600000
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
            if (result[0][2].equals("FINISHED")) {
                logger.info("Load FINISHED " + label)
                break
            }
            if (result[0][2].equals("CANCELLED")) {
                def reason = result[0][7]
                logger.info("load failed, index: $i, reason:$reason")
                assertTrue(1 == 2)
                break;
            }
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if(max_try_milli_secs <= 0) {
                assertTrue(1 == 2, "load Timeout: $label")
            }
        }
        i++
    }

    qt_sql """ select count(*) from ${tableName} """
}