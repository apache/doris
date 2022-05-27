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

suite("test_csv_with_header", "csv_with_header") {
     //define format
    def format_csv = "csv"
    def format_csv_with_names = "csv_with_names"
    def format_csv_with_names_and_types = "csv_with_names_and_types"
    //define format data file
    def format_csv_file = "csv.txt"
    def format_csv_with_names_file =  "csv_with_names.txt"
    def format_csv_with_names_and_types_file = "csv_with_names_and_types.txt"
    def expect_rows = 10
    def testTable = "test_csv_with_header"

    def test_stream_load = {testTablex, label, format, file_path, exect_rows->
        streamLoad {
            table testTablex
            set 'label',label
            set 'column_separator',','
            set 'format',format
            file file_path
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, exect_rows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }
    def import_from_hdfs = {testTable1, label1, hdfsFilePath, format1, brokerName, hdfsUser, hdfsPasswd->
        sql """
            LOAD LABEL ${label1} (
                DATA INFILE("${hdfsFilePath}") 
                INTO TABLE ${testTable1} COLUMNS TERMINATED BY "," 
                FORMAT as "${format1}" )
                with BROKER "${brokerName}" 
                ("username"="${hdfsUser}", "password"="${hdfsPasswd}")
        """
    }

    def check_import_result = {checklabel, testTable4, expected_rows->
        max_try_secs = 100000
        while(max_try_secs--) {
            result = sql "show load where label = '${checklabel}'"
            if(result[0][2] == "FINISHED") {
                result_count = sql "select count(*) from ${testTable4}"
                assertEquals(result_count[0][0], expected_rows)
                break
            } else {
                sleep(1000)
                max_try_secs = max_try_secs - 1000
                if(max_try_secs < 0) {
                    assertEquals(1, 2)
                }
            }
        }
    }

    sql "DROP TABLE IF EXISTS ${testTable}"
    def result1 = sql """
            CREATE TABLE `${testTable}` (
                `event_day` date NULL COMMENT "",
                `siteid` int(11) NULL DEFAULT "10" COMMENT "",
                `citycode` smallint(6) NULL COMMENT "",
                `username` varchar(32) NULL DEFAULT "" COMMENT "",
                `pv` bigint(20) NULL DEFAULT "0" COMMENT ""
                ) ENGINE=OLAP
                DUPLICATE KEY(`event_day`, `siteid`, `citycode`)
                COMMENT "OLAP"
                DISTRIBUTED BY HASH(`siteid`) BUCKETS 10
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
                )
            """
    //[stream load] format=csv
    def label = UUID.randomUUID().toString()
    test_stream_load.call(testTable, label, format_csv, format_csv_file, expect_rows)

    //[stream load] format=csv_with_names 
    label = UUID.randomUUID().toString()
    test_stream_load.call(testTable, label, format_csv_with_names, format_csv_with_names_file, expect_rows)

    //[stream load] format=csv_with_names_and_types
    label = UUID.randomUUID().toString()
    test_stream_load.call(testTable, label, format_csv_with_names_and_types, format_csv_with_names_and_types_file, expect_rows)

    // check total rows
    def result_count = sql "select count(*) from ${testTable}"
    assertEquals(result_count[0][0], expect_rows*3)

    if (enableHdfs()) {
        //test import data from hdfs
        hdfsUser = getHdfsUser()
        brokerName =getBrokerName()
        hdfsPasswd = getHdfsPasswd()
        hdfsFs = getHdfsFs()
        //[broker load] test normal
        label = UUID.randomUUID().toString().replaceAll("-", "")
        remote_csv_file = uploadToHdfs format_csv_file
        export_result = import_from_hdfs.call(testTable, label, remote_csv_file, format_csv, brokerName, hdfsUser, hdfsPasswd)
        check_import_result.call(label, testTable, expect_rows * 4)

        //[broker load] csv_with_names
        label = UUID.randomUUID().toString().replaceAll("-", "")
        remote_csv_file = uploadToHdfs format_csv_with_names_file
        export_result = import_from_hdfs.call(testTable, label, remote_csv_file, format_csv_with_names, brokerName, hdfsUser, hdfsPasswd)
        check_import_result.call(label, testTable, expect_rows * 5)

         //[broker load] csv_with_names_and_types
        label = UUID.randomUUID().toString().replaceAll("-", "")
        remote_csv_file = uploadToHdfs format_csv_with_names_and_types_file
        export_result = import_from_hdfs.call(testTable, label, remote_csv_file, format_csv_with_names_and_types, brokerName, hdfsUser, hdfsPasswd)
        check_import_result.call(label, testTable, expect_rows * 6)

        def export_to_hdfs = {exportTable, exportLable, hdfsPath, exportFormat, exportBrokerName, exportUserName, exportPasswd->
            sql """ EXPORT TABLE ${exportTable} 
                TO "${hdfsPath}" 
                PROPERTIES ("label" = "${exportLable}", "column_separator"=",","format"="${exportFormat}") 
                WITH BROKER "${exportBrokerName}" ("username"="${exportUserName}", "password"="${exportPasswd}")
            """
        }

        def check_export_result = {checklabel1->
            max_try_secs = 100000
            while(max_try_secs--) {
                result = sql "show export where label='${checklabel1}'"
                if(result[0][2] == "FINISHED") {
                    break
                } else {
                    sleep(1000)
                    max_try_secs = max_try_secs - 1000
                    if(max_try_secs < 0) {
                        assertEquals(1,2)
                    }
                }
            }
        }

        def check_download_result={resultlist, fileFormat, expectedTotalRows->
            int totalLines = 0
            if(fileFormat == format_csv_with_names) {
                expectedTotalRows += resultlist.size()
            }else if(fileFormat == format_csv_with_names_and_types) {
                expectedTotalRows += resultlist.size() * 2
            }
            for(String oneFile :resultlist) {
                totalLines += getTotalLine(oneFile)
                deleteFile(oneFile)
            }
            assertEquals(expectedTotalRows,totalLines)
        }

        resultCount = sql "select count(*) from ${testTable}"
        currentTotalRows = resultCount[0][0]

        // export table to hdfs format=csv
        hdfsDataDir = getHdfsDataDir()
        label = UUID.randomUUID().toString().replaceAll("-", "")
        export_to_hdfs.call(testTable, label, hdfsDataDir + "/" + label, format_csv, brokerName, hdfsUser, hdfsPasswd)
        check_export_result(label)
        result = downloadExportFromHdfs(label + "/export-data")
        check_download_result(result, format_csv, currentTotalRows)

        // export table to hdfs format=csv_with_names
        label = UUID.randomUUID().toString().replaceAll("-", "")
        export_to_hdfs.call(testTable, label, hdfsDataDir + "/" + label, format_csv_with_names, brokerName, hdfsUser, hdfsPasswd)
        check_export_result(label)
        result = downloadExportFromHdfs(label + "/export-data")
        check_download_result(result, format_csv_with_names, currentTotalRows)

        // export table to hdfs format=csv_with_names_and_types
        label = UUID.randomUUID().toString().replaceAll("-", "")
        export_to_hdfs.call(testTable, label, hdfsDataDir + "/" + label, format_csv_with_names_and_types, brokerName, hdfsUser, hdfsPasswd)
        check_export_result(label)
        result = downloadExportFromHdfs(label + "/export-data")
        check_download_result(result, format_csv_with_names_and_types, currentTotalRows)
        
        // select out file to hdfs 
        select_out_file = {outTable, outHdfsPath, outFormat, outHdfsFs, outBroker, outHdfsUser, outPasswd->
            sql """
                SELECT * FROM ${outTable}
                INTO OUTFILE "${outHdfsPath}"
                FORMAT AS "${outFormat}"
                PROPERTIES
                (
                    "broker.name" = "${outBroker}",
                    "column_separator" = ",",
                    "line_delimiter" = "\n",
                    "max_file_size" = "5MB",
                    "broker.username"="${hdfsUser}",
                    "broker.password"="${outPasswd}"
                )
            """
        }
        // select out file to hdfs format=csv
        label = UUID.randomUUID().toString().replaceAll("-", "")
        select_out_file(testTable, hdfsDataDir + "/" + label + "/csv", format_csv, hdfsFs, brokerName, hdfsUser, hdfsPasswd)
        result = downloadExportFromHdfs(label + "/csv")
        check_download_result(result, format_csv, currentTotalRows)

        // select out file to hdfs format=csv_with_names
        label = UUID.randomUUID().toString().replaceAll("-", "")
        select_out_file(testTable, hdfsDataDir + "/" + label + "/csv", format_csv_with_names, hdfsFs, brokerName, hdfsUser, hdfsPasswd)
        result = downloadExportFromHdfs(label + "/csv")
        check_download_result(result, format_csv_with_names, currentTotalRows)

        // select out file to hdfs format=csv_with_names_and_types
        label = UUID.randomUUID().toString().replaceAll("-", "")
        select_out_file(testTable, hdfsDataDir + "/" + label + "/csv", format_csv_with_names_and_types, hdfsFs, brokerName, hdfsUser, hdfsPasswd)
        result = downloadExportFromHdfs(label + "/csv")
        check_download_result(result, format_csv_with_names_and_types, currentTotalRows)
    }    
}
