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

suite("test_multi_char_line_delimiter", "p2") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()
    def tableName = "test_multi_char_line_delimiter"
    def label = "test_multi_char_line_delimiter"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql new File("""${context.file.parent}/ddl/${tableName}.sql""").text
    sql """
    LOAD LABEL ${label}
        (
            DATA INFILE("s3://${s3BucketName}/regression/load/data/test_multi_char_line_delimiter*.csv")
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY "\tcolumn_separator"
            LINES TERMINATED BY "\nline_delimiter"        
            FORMAT AS CSV  
            (`col1`,`col2`,`col3`,`col4`,`col5`,`col6`,`col7`,`col8`,`col9`,`col10`,`col11`,`col12`,`col13`,`col14`,`col15`,`col16`,`col17`,`col18`,`col19`,`col20`,`col21`,`col22`,`col23`,`col24`,`col25`,`col26`,`col27`,`col28`,`col29`,`col30`,`col31`,`col32`,`col33`,`col34`,`col35`,`col36`,`col37`,`col38`,`col39`,`col40`,`col41`,`col42`,`col43`,`col44`,`col45`,`col46`,`col47`,`col48`,`col49`,`col50`,`col51`,`col52`,`col53`,`col54`,`col55`,`col56`,`col57`)
            SET(`col1`=`col1`,`col2`=`col2`,`col3`=`col3`,`col4`=`col4`,`col5`=`col5`,`col6`=`col6`,`col7`=`col7`,`col8`=`col8`,`col9`=`col9`,`col10`=`col10`,`col11`=`col11`,`col12`=`col12`,`col13`=`col13`,`col14`=`col14`,`col15`=`col15`,`col16`=`col16`,`col17`=`col17`,`col18`=`col18`,`col19`=`col19`,`col20`=`col20`,`col21`=`col21`,`col22`=`col22`,`col23`=`col23`,`col24`=`col24`,`col25`=`col25`,`col26`=`col26`,`col27`=`col27`,`col28`=`col28`,`col29`=`col29`,`col30`=`col30`,`col31`=`col31`,`col32`=`col32`,`col33`=`col33`,`col34`=`col34`,`col35`=`col35`,`col36`=`col36`,`col37`=`col37`,`col38`=`col38`,`col39`=`col39`,`col40`=`col40`,`col41`=`col41`,`col42`=`col42`,`col43`=`col43`,`col44`=`col44`,`col45`=`col45`,`col46`=`col46`,`col47`=`col47`,`col48`=`col48`,`col49`=`col49`,`col50`=`col50`,`col51`=`col51`,`col52`=`col52`,`col53`=`col53`,`col54`=`col54`,`col55`=`col55`,`col56`=`col56`,col57=bitmap_from_string(col57))
        )
        WITH S3
        (
            "s3.region"  =  "${s3Region}",
            "s3.endpoint"  =  "${s3Endpoint}",
            "s3.access_key"   =  "${ak}",
            "s3.secret_key"  =  "${sk}"
        )
        PROPERTIES
        (
            "timeout" = "3600",
            "load_parallelism" = "4"
        );
    """

    def max_try_milli_secs = 600000
    while (max_try_milli_secs > 0) {
        def String[][] result = sql """ show load where label="$label"; """
        logger.info("Load status: " + result[0][2] + ", label: $label")
        if (result[0][2].equals("FINISHED")) {
            logger.info("Load FINISHED " + label)
            break;
        }
        if (result[0][2].equals("CANCELLED")) {
            assertTrue(false, "load failed: $result")
            break;
        }
        Thread.sleep(1000)
        max_try_milli_secs -= 1000
        if(max_try_milli_secs <= 0) {
            assertTrue(1 == 2, "load Timeout: $label")
        }
    }

    def result = sql """ select count(*) from ${tableName}; """
    logger.info("result: ${result[0][0]}")
    assertTrue(result[0][0] == 2060625, "load result is not correct")
    sql """ DROP TABLE IF EXISTS ${tableName} """
}