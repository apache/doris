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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_all_types", "types") {
    StringBuilder strBuilder = new StringBuilder()
    strBuilder.append("curl --location-trusted -u " + context.config.jdbcUser + ":" + context.config.jdbcPassword)
    strBuilder.append(" http://" + context.config.feHttpAddress + "/rest/v1/config/fe")
    String command = strBuilder.toString()
    def process = command.toString().execute()
    def code = process.waitFor()
    def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    def out = process.getText()
    logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def response = parseJson(out.trim())
    assertEquals(response.code, 0)
    assertEquals(response.msg, "success")
    def configJson = response.data.rows
    boolean enableOutfileToLocal = false
    for (Object conf: configJson) {
        assert conf instanceof Map
        if (((Map<String, String>) conf).get("Name").toLowerCase() == "enable_outfile_to_local") {
            enableOutfileToLocal = ((Map<String, String>) conf).get("Value").toLowerCase() == "true"
        }
    }

    if (!enableOutfileToLocal) {
        logger.warn("Please set enable_outfile_to_local to true to run test_outfile")
        return
    }

    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = getS3BucketName()

    // aggModel start 

    def TableName1 = "data_types_agg"

    List<List<String>> dataTypes1 = sql "SHOW data types"

    def createTBSQL1 = "CREATE TABLE IF NOT EXISTS ${TableName1} ("
    def subSQL1 = ""

    def index1 = 0

    def masterKey1 = ""

    dataTypes1.each { row ->
        def dataType = row[0]
        println(dataType)
        index1++
        if (dataType == "ARRAY" || dataType == "MAP") {
            return
        } else if (dataType == "JSONB") {
            subSQL1 += "k${index1} ${dataType} REPLACE, "
        } else if (dataType == "STRING") {
            subSQL1 += "k${index1} ${dataType} REPLACE, "
        } else if (dataType == "FLOAT") {
            subSQL1 += "k${index1} ${dataType} MAX, "
        } else if (dataType == "DOUBLE") {
            subSQL1 += "k${index1} ${dataType} MAX, "
        } else if (dataType == "HLL") {
            subSQL1 += "k${index1} ${dataType} ${dataType}_UNION, "
        } else if (dataType == "BITMAP") {
            subSQL1 += "k${index1} ${dataType} ${dataType}_UNION, "
        } else if (dataType == "QUANTILE_STATE") {
            subSQL1 += "k${index1} ${dataType} QUANTILE_UNION NOT NULL, "
        } else {
            masterKey1 += "k${index1}, "
            createTBSQL1 += "k${index1} ${dataType}, "
        }
    }
    subSQL1 = subSQL1.substring(0, subSQL1.length() - 2)
    masterKey1 = masterKey1.substring(0, masterKey1.length() - 2)
    createTBSQL1 = createTBSQL1.substring(0, createTBSQL1.length() - 2)
    createTBSQL1 += "," + subSQL1
    createTBSQL1 += ")"

    createTBSQL1 += """AGGREGATE KEY(${masterKey1})
    DISTRIBUTED BY HASH(k2) BUCKETS 5 properties("replication_num" = "1");"""
    
    println "${createTBSQL1}"

    sql "${createTBSQL1}"

    // aggModel insert load
    sql """ insert into ${TableName1} (k2, k4, k5, k6, k7, k8, k9, k10, k11, k15, k17, k20, k22, k23, k3, k12, k13, k14, k16, k19, k21) values 
    (1, 1, 'x', '1999-01-08', '1999-01-08 02:05:06', '1999-01-08 02:05:06', '1999-01-08',2, 2, 1, 1, 1, 1, 'x', to_bitmap(17), 1, 1, hll_hash(17), '"a"', to_quantile_state(1,2048), 'text1'),
    (2,0,'b','2000-01-08','2000-01-08 02:05:06','2000-01-08 02:05:06','2000-01-08',2,2,2,2,2,2,'a',to_bitmap(2),2.2,2.2,hll_hash(18),'"b"',to_quantile_state(2,2048),'text2'),
    (3,1,'c','2001-01-08','2001-01-08 02:05:06','2001-01-08 02:05:06','2001-01-08',3,3,3,3,3,3,'c',to_bitmap(3),3.3,3.3,hll_hash(3),'"c"',to_quantile_state(3,2048),'text3'),
    (4,0,'d','2002-01-08','2002-01-08 02:05:06','2002-01-08 02:05:06','2002-01-08',4,4,4,4,4,4,'d',to_bitmap(4),4.4,4.4,hll_hash(4),'"d"',to_quantile_state(4,2048),'text4'),
    (5,1,'e','2003-03-08','2003-03-08 02:05:06','2003-03-08 02:05:06','2003-03-08',5,5,5,5,5,5,'e',to_bitmap(5),5.5,5.5,hll_hash(5),'"e"',to_quantile_state(5,2048),'text5'),
    (6,0,'f','2004-03-08','2004-03-08 02:05:06','2004-03-08 02:05:06','2004-03-08',6,6,6,6,6,6,'f',to_bitmap(6),6.6,6.6,hll_hash(6),'"f"',to_quantile_state(6,2048),'text6'),
    (7,1,'g','2005-03-08','2005-03-08 02:05:06','2005-03-08 02:05:06','2005-03-08',7,7,7,7,7,7,'g',to_bitmap(7),7.7,7.7,hll_hash(7),'"g"',to_quantile_state(7,2048),'text7'),
    (8,0,'h','2007-03-08','2007-03-08 07:05:07','2007-03-08 07:05:07','2007-03-08',8,8,8,8,8,8,'h',to_bitmap(8),8.8,8.8,hll_hash(8),'"h"',to_quantile_state(8,2048),'text8'),
    (9,1,'j','2009-03-08','2009-03-08 07:05:07','2009-03-08 07:05:07','2009-03-08',9,9,9,9,9,9,'j',to_bitmap(9),9.9,9.9,hll_hash(9),'"j"',to_quantile_state(9,2048),'text9'),
    (19,0,'k','2019-03-08','2019-03-08 07:05:07','2019-03-08 07:05:07','2019-03-08',19,19,19,19,19,19,'k',to_bitmap(19),19.9,19.9,hll_hash(19),'"k"',to_quantile_state(19,2048),'text19');
    """

    // aggModel stream load
    streamLoad {
        table "${TableName1}"

        set 'column_separator', '|'  

        set 'columns', 'k2, k4, k5, k6, k7, k8, k9, k10, k11, k15, k17, k20, k22, k23, k1, k3, k12, k13, k14, k16, k18, k19, k21 ,k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k15=k15, k17=k17, k20=k20, k22=k22, k23=k23, k3=to_bitmap(k3), k12=k12, k13=k13, k14=hll_hash(k14), k16=k16, k19=to_quantile_state(k19,2048), k21=k21'

        file 'streamload_input.csv'

        time 10000 
    }

    def uuid = UUID.randomUUID().toString().replace("-", "0")

    def outFilePath = """${context.file.parent}/test_all_types"""

    def columns = "k2, k4, k5, k6, k7, k8, k9, k10, k11, k15, k17, k20, k22, k23, k1, k3, k12, k13, k14, k16, k18, k19, k21"
    String columns_str = ("$columns" != "") ? "($columns)" : "";

    def loadLabel1 = TableName1 + '_' + uuid

    // // aggModel s3 load
    // sql """
    //         LOAD LABEL $loadLabel1 (
    //             DATA INFILE("s3://$bucket/regression/datatypes/ALLTESTCASE.txt")
    //             INTO TABLE $TableName1
    //             COLUMNS TERMINATED BY "|"
    //             $columns_str
    //             SET 
    //             (
    //                 k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k15=k15, k17=k17, k20=k20, k22=k22, k23=k23, k3=to_bitmap(k3), k12=k12, k13=k13, k14=hll_hash(k14), k16=k16, k19=to_quantile_state(k19,2048), k21=k21
    //             )
    //         )
    //         WITH S3 (
    //             "AWS_ACCESS_KEY" = "$ak",
    //             "AWS_SECRET_KEY" = "$sk",
    //             "AWS_ENDPOINT" = "$endpoint",
    //             "AWS_REGION" = "$region"
    //         )
    //         properties(
    //             "use_new_load_scan_node" = "true"
    //         )
            
    //     """
    // waitForS3LoadFinished(loadLabel1)
    
    // def stateResult1 = sql "show load where Label = '${loadLabel1}'"
    // println(stateResult1)

    // aggModel output to .out
    qt_sql "SELECT * FROM ${TableName1} ORDER BY k2"

    // aggModel select into outfile
    try {
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        sql """
            SELECT * FROM ${TableName1} ORDER BY k2 INTO OUTFILE "file://${outFilePath}/";
        """
        File[] files = path.listFiles()
        assert files.length == 1
    } finally {
        try_sql("DROP TABLE IF EXISTS ${TableName1}")
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f : path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }

    sql "DROP TABLE IF EXISTS ${TableName1}"

    // uniModel_read start 

    def TableName2 = "data_types_uni_read"

    def createTBSQL2 = "CREATE TABLE IF NOT EXISTS ${TableName2} ("
    def subSQL2 = ""

    def index2 = 0

    def masterKey2 = ""

    dataTypes1.each { row ->
        def dataType = row[0]
        index2++
        if (dataType == "QUANTILE_STATE") {
            return
        } else if (dataType == "ARRAY") {
            return
        }else if (dataType == "QUANTILE_STATE" || dataType == "HLL" || dataType == "BITMAP" || dataType == "MAP") {
            return
        }else if (dataType == "STRING" || dataType == "JSONB" || dataType == "FLOAT" || dataType == "DOUBLE") {
            subSQL2 += "k${index2} ${dataType}, "
        } else {
            masterKey2 += "k${index2}, "
            createTBSQL2 += "k${index2} ${dataType}, "
            println(dataType)
        }
    }
    subSQL2 = subSQL2.substring(0, subSQL2.length() - 2)
    masterKey2 = masterKey2.substring(0, masterKey2.length() - 2)
    createTBSQL2 = createTBSQL2.substring(0, createTBSQL2.length() - 2)
    createTBSQL2 += "," + subSQL2
    createTBSQL2 += ")"
    

    createTBSQL2 += """UNIQUE KEY(${masterKey2})
    DISTRIBUTED BY HASH(k2) BUCKETS 5 properties("replication_num" = "1");"""

    println "${createTBSQL2}"
    sql "${createTBSQL2}"

    // uniModel_read insert load
    sql """ insert into ${TableName2} (k2, k4, k5, k6, k7, k8, k9, k10, k11, k15, k17, k20, k22, k23, k12, k13, k16, k21) values 
    (1, 1, 'x', '1999-01-08', '1999-01-08 02:05:06', '1999-01-08 02:05:06', '1999-01-08',2, 2, 1, 1, 1, 1, 'x', 1, 1, '"a"', 'text1'),
    (2,0,'b','2000-01-08','2000-01-08 02:05:06','2000-01-08 02:05:06','2000-01-08',2,2,2,2,2,2,'a',2.2,2.2,'"b"','text2'),
    (3,1,'c','2001-01-08','2001-01-08 02:05:06','2001-01-08 02:05:06','2001-01-08',3,3,3,3,3,3,'c',3.3,3.3,'"c"','text3'),
    (4,0,'d','2002-01-08','2002-01-08 02:05:06','2002-01-08 02:05:06','2002-01-08',4,4,4,4,4,4,'d',4.4,4.4,'"d"','text4'),
    (5,1,'e','2003-03-08','2003-03-08 02:05:06','2003-03-08 02:05:06','2003-03-08',5,5,5,5,5,5,'e',5.5,5.5,'"e"','text5'),
    (6,0,'f','2004-03-08','2004-03-08 02:05:06','2004-03-08 02:05:06','2004-03-08',6,6,6,6,6,6,'f',6.6,6.6,'"f"','text6'),
    (7,1,'g','2005-03-08','2005-03-08 02:05:06','2005-03-08 02:05:06','2005-03-08',7,7,7,7,7,7,'g',7.7,7.7,'"g"','text7'),
    (8,0,'h','2007-03-08','2007-03-08 07:05:07','2007-03-08 07:05:07','2007-03-08',8,8,8,8,8,8,'h',8.8,8.8,'"h"','text8'),
    (9,1,'j','2009-03-08','2009-03-08 07:05:07','2009-03-08 07:05:07','2009-03-08',9,9,9,9,9,9,'j',9.9,9.9,'"j"','text9'),
    (19,0,'k','2019-03-08','2019-03-08 07:05:07','2019-03-08 07:05:07','2019-03-08',19,19,19,19,19,19,'k',19.9,19.9,'"k"','text19');
    """

    // uniModel_read stream load
    streamLoad {
        table "${TableName2}"

        set 'column_separator', '|'  

        set 'columns', 'k2, k4, k5, k6, k7, k8, k9, k10, k11, k15, k17, k20, k22, k23, k1, k3, k12, k13, k14, k16, k18, k19, k21, k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k15=k15, k17=k17, k20=k20, k22=k22, k23=k23, k12=k12, k13=k13, k16=k16, k21=k21'

        file 'streamload_input.csv'

        time 10000 
    }

    // // uniModel_read s3 load
    // def loadLabel2 = TableName2 + '_' + uuid

    // sql """
    //         LOAD LABEL $loadLabel2 (
    //             DATA INFILE("s3://$bucket/regression/datatypes/ALLTESTCASE.txt")
    //             INTO TABLE $TableName2
    //             COLUMNS TERMINATED BY "|"
    //             $columns_str
    //             SET 
    //             (
    //                 k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k15=k15, k17=k17, k20=k20, k22=k22, k23=k23, k12=k12, k13=k13, k16=k16, k21=k21
    //             )
    //         )
    //         WITH S3 (
    //             "AWS_ACCESS_KEY" = "$ak",
    //             "AWS_SECRET_KEY" = "$sk",
    //             "AWS_ENDPOINT" = "$endpoint",
    //             "AWS_REGION" = "$region"
    //         )
    //         properties(
    //             "use_new_load_scan_node" = "true"
    //         )
            
    //     """
    // waitForS3LoadFinished(loadLabel2)
    
    // def stateResult2 = sql "show load where Label = '${loadLabel2}'"
    // println(stateResult2)

    // uniModel_read output to .out
    qt_sql "SELECT * FROM ${TableName2} ORDER BY k2"

    // uniModel_read select into outfile
    try {
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        sql """
            SELECT * FROM ${TableName2} ORDER BY k2 INTO OUTFILE "file://${outFilePath}/";
        """
        File[] files = path.listFiles()
        assert files.length == 1
    } finally {
        try_sql("DROP TABLE IF EXISTS ${TableName2}")
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f : path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }

    sql "DROP TABLE IF EXISTS ${TableName2}"

    // uniModel_write start 

    def TableName3 = "data_types_uni_write"

    def createTBSQL3 = "CREATE TABLE IF NOT EXISTS ${TableName3} ("
    def subSQL3 = ""

    def index3 = 0

    def masterKey3 = ""

    dataTypes1.each { row ->
        def dataType = row[0]
        index3++
        if (dataType == "ARRAY") {
            return
        }else if (dataType == "QUANTILE_STATE" || dataType == "HLL" || dataType == "BITMAP" || dataType == "MAP") {
            return
        }else if (dataType == "STRING" || dataType == "JSONB" || dataType == "FLOAT" || dataType == "DOUBLE") {
            subSQL3 += "k${index3} ${dataType}, "
        } else {
            masterKey3 += "k${index3}, "
            createTBSQL3 += "k${index3} ${dataType}, "
            println(dataType)
        }
    }
    subSQL3 = subSQL3.substring(0, subSQL3.length() - 2)
    masterKey3 = masterKey3.substring(0, masterKey3.length() - 2)
    createTBSQL3 = createTBSQL3.substring(0, createTBSQL3.length() - 2)
    createTBSQL3 += "," + subSQL3
    createTBSQL3 += ")"

    createTBSQL3 += """UNIQUE KEY(${masterKey3})
    DISTRIBUTED BY HASH(k2) BUCKETS 5 properties("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");"""

    println "${createTBSQL3}"
    sql "${createTBSQL3}"

    // uniModel_write insert load
    sql """ insert into ${TableName3} (k2, k4, k5, k6, k7, k8, k9, k10, k11, k15, k17, k20, k22, k23, k12, k13, k16, k21) values 
    (1, 1, 'x', '1999-01-08', '1999-01-08 02:05:06', '1999-01-08 02:05:06', '1999-01-08',2, 2, 1, 1, 1, 1, 'x', 1, 1, '"a"', 'text1'),
    (2,0,'b','2000-01-08','2000-01-08 02:05:06','2000-01-08 02:05:06','2000-01-08',2,2,2,2,2,2,'a',2.2,2.2,'"b"','text2'),
    (3,1,'c','2001-01-08','2001-01-08 02:05:06','2001-01-08 02:05:06','2001-01-08',3,3,3,3,3,3,'c',3.3,3.3,'"c"','text3'),
    (4,0,'d','2002-01-08','2002-01-08 02:05:06','2002-01-08 02:05:06','2002-01-08',4,4,4,4,4,4,'d',4.4,4.4,'"d"','text4'),
    (5,1,'e','2003-03-08','2003-03-08 02:05:06','2003-03-08 02:05:06','2003-03-08',5,5,5,5,5,5,'e',5.5,5.5,'"e"','text5'),
    (6,0,'f','2004-03-08','2004-03-08 02:05:06','2004-03-08 02:05:06','2004-03-08',6,6,6,6,6,6,'f',6.6,6.6,'"f"','text6'),
    (7,1,'g','2005-03-08','2005-03-08 02:05:06','2005-03-08 02:05:06','2005-03-08',7,7,7,7,7,7,'g',7.7,7.7,'"g"','text7'),
    (8,0,'h','2007-03-08','2007-03-08 07:05:07','2007-03-08 07:05:07','2007-03-08',8,8,8,8,8,8,'h',8.8,8.8,'"h"','text8'),
    (9,1,'j','2009-03-08','2009-03-08 07:05:07','2009-03-08 07:05:07','2009-03-08',9,9,9,9,9,9,'j',9.9,9.9,'"j"','text9'),
    (19,0,'k','2019-03-08','2019-03-08 07:05:07','2019-03-08 07:05:07','2019-03-08',19,19,19,19,19,19,'k',19.9,19.9,'"k"','text19');
    """

    // uniModel_write stream load
    streamLoad {
        table "${TableName3}"

        set 'column_separator', '|'  

        set 'columns', 'k2, k4, k5, k6, k7, k8, k9, k10, k11, k15, k17, k20, k22, k23, k1, k3, k12, k13, k14, k16, k18, k19, k21, k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k15=k15, k17=k17, k20=k20, k22=k22, k23=k23, k12=k12, k13=k13, k16=k16, k21=k21'

        file 'streamload_input.csv'

        time 10000 
    }
    
    // // uniModel_write s3 load
    // def loadLabel3 = TableName3 + '_' + uuid
    // sql """
    //         LOAD LABEL $loadLabel3 (
    //             DATA INFILE("s3://$bucket/regression/datatypes/ALLTESTCASE.txt")
    //             INTO TABLE $TableName3
    //             COLUMNS TERMINATED BY "|"
    //             $columns_str
    //             SET 
    //             (
    //                 k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k15=k15, k17=k17, k20=k20, k22=k22, k23=k23, k12=k12, k13=k13, k16=k16, k21=k21
    //             )
    //         )
    //         WITH S3 (
    //             "AWS_ACCESS_KEY" = "$ak",
    //             "AWS_SECRET_KEY" = "$sk",
    //             "AWS_ENDPOINT" = "$endpoint",
    //             "AWS_REGION" = "$region"
    //         )
    //         properties(
    //             "use_new_load_scan_node" = "true"
    //         )
            
    //     """
    // waitForS3LoadFinished(loadLabel3)
    
    // def stateResult3 = sql "show load where Label = '${loadLabel3}'"
    // println(stateResult3)

    // uniModel_write output to .out
    qt_sql "SELECT * FROM ${TableName3} ORDER BY k2"

    // uniModel_write select into outfile
    try {
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        sql """
            SELECT * FROM ${TableName3} ORDER BY k2 INTO OUTFILE "file://${outFilePath}/";
        """
        File[] files = path.listFiles()
        assert files.length == 1
    } finally {
        try_sql("DROP TABLE IF EXISTS ${TableName3}")
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f : path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }

    sql "DROP TABLE IF EXISTS ${TableName3}"



    // DupModel start
    def TableName4 = "data_types_dup"
    sql "ADMIN SET FRONTEND CONFIG ('enable_map_type' = 'true')"

    def createTBSQL4 = "CREATE TABLE IF NOT EXISTS ${TableName4} ("
    def subSQL4 = ""

    def index4 = 0

    def masterKey4 = ""

    dataTypes1.each { row ->
        def dataType = row[0]
        index4++
        if (dataType == "QUANTILE_STATE") {
            return
        } else if (dataType == "QUANTILE_STATE" || dataType == "HLL" || dataType == "BITMAP") {
            return
        } else if (dataType == "ARRAY") {
            subSQL4 += "k${index4} ${dataType}<INT>, "
        } else if (dataType == "STRING" || dataType == "JSONB" || dataType == "FLOAT" || dataType == "DOUBLE") {
            subSQL4 += "k${index4} ${dataType}, "
        }  else if (dataType == "MAP") {
            subSQL4 += "k${index4} ${dataType}<STRING, INT>, "
        } else {
            masterKey4 += "k${index4}, "
            createTBSQL4 += "k${index4} ${dataType}, "
            println(dataType)
        }
    }
    subSQL4 = subSQL4.substring(0, subSQL4.length() - 2)
    masterKey4 = masterKey4.substring(0, masterKey4.length() - 2)
    createTBSQL4 = createTBSQL4.substring(0, createTBSQL4.length() - 2)
    createTBSQL4 += "," + subSQL4
    createTBSQL4 += ")"

    createTBSQL4 += """DUPLICATE KEY(${masterKey4})
    DISTRIBUTED BY HASH(k2) BUCKETS 5 properties("replication_num" = "1");"""

    println "${createTBSQL4}"

    sql "${createTBSQL4}"

    // DupModel insert load
    sql """ insert into ${TableName4} (k2, k4, k5, k6, k7, k8, k9, k10, k11, k15, k17, k20, k22, k23, k1, k12, k13, k16, k18, k21) values 
    (1, 1, 'x', '1999-01-08', '1999-01-08 02:05:06', '1999-01-08 02:05:06', '1999-01-08',2, 2, 1, 1, 1, 1, 'x', [1, 1], 1, 1, '"a"', {"k1":1}, 'text1'),
    (2,0,'b','2000-01-08','2000-01-08 02:05:06','2000-01-08 02:05:06','2000-01-08',2,2,2,2,2,2,'a',[2, 2],2.2,2.2,'"b"', {"k2":2},'text2'),
    (3,1,'c','2001-01-08','2001-01-08 02:05:06','2001-01-08 02:05:06','2001-01-08',3,3,3,3,3,3,'c',[3, 3],3.3,3.3,'"c"', {"k3":3},'text3'),
    (4,0,'d','2002-01-08','2002-01-08 02:05:06','2002-01-08 02:05:06','2002-01-08',4,4,4,4,4,4,'d',[4, 4],4.4,4.4,'"d"', {"k4":4},'text4'),
    (5,1,'e','2003-03-08','2003-03-08 02:05:06','2003-03-08 02:05:06','2003-03-08',5,5,5,5,5,5,'e',[5, 5],5.5,5.5,'"e"', {"k5":5},'text5'),
    (6,0,'f','2004-03-08','2004-03-08 02:05:06','2004-03-08 02:05:06','2004-03-08',6,6,6,6,6,6,'f',[6, 6],6.6,6.6,'"f"', {"k6":6},'text6'),
    (7,1,'g','2005-03-08','2005-03-08 02:05:06','2005-03-08 02:05:06','2005-03-08',7,7,7,7,7,7,'g',[7, 7],7.7,7.7,'"g"', {"k7":7},'text7'),
    (8,0,'h','2007-03-08','2007-03-08 07:05:07','2007-03-08 07:05:07','2007-03-08',8,8,8,8,8,8,'h',[8, 8],8.8,8.8,'"h"', {"k8":8},'text8'),
    (9,1,'j','2009-03-08','2009-03-08 07:05:07','2009-03-08 07:05:07','2009-03-08',9,9,9,9,9,9,'j',[9, 9],9.9,9.9,'"j"', {"k9":9},'text9'),
    (19,0,'k','2019-03-08','2019-03-08 07:05:07','2019-03-08 07:05:07','2019-03-08',19,19,19,19,19,19,'k',[19, 19],19.9,19.9,'"k"', {"k19":19},'text19');
    """

    // DupModel stream load
    streamLoad {
        table "${TableName4}"

        set 'column_separator', '|'  

        set 'columns', 'k2, k4, k5, k6, k7, k8, k9, k10, k11, k15, k17, k20, k22, k23, k1, k3, k12, k13, k14, k16, k18, k19, k21, k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k15=k15, k17=k17, k20=k20, k22=k22, k23=k23, k1=k1, k12=k12, k13=k13, k16=k16, k18=k18, k21=k21'

        file 'streamload_input.csv'

        time 10000 
    }

    // // DupModel s3 load

    // def loadLabel4 = TableName4 + '_' + uuid
    // sql """
    //         LOAD LABEL $loadLabel4 (
    //             DATA INFILE("s3://$bucket/regression/datatypes/ALLTESTCASE.txt")
    //             INTO TABLE $TableName4
    //             COLUMNS TERMINATED BY "|"
    //             $columns_str
    //             SET 
    //             (
    //                 k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k15=k15, k17=k17, k20=k20, k22=k22, k23=k23, k1=k1, k12=k12, k13=k13, k16=k16, k18=k18, k21=k21
    //             )
    //         )
    //         WITH S3 (
    //             "AWS_ACCESS_KEY" = "$ak",
    //             "AWS_SECRET_KEY" = "$sk",
    //             "AWS_ENDPOINT" = "$endpoint",
    //             "AWS_REGION" = "$region"
    //         )
    //         properties(
    //             "use_new_load_scan_node" = "true"
    //         )
            
    //     """
    // waitForS3LoadFinished(loadLabel4)
    
    // def stateResult4 = sql "show load where Label = '${loadLabel4}'"
    // println(stateResult4)

    // DupModel output to .out
    qt_sql "SELECT * FROM ${TableName4} ORDER BY k2"

    // DupModel select into outfile
    try {
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        sql """
            SELECT * FROM ${TableName4} ORDER BY k2 INTO OUTFILE "file://${outFilePath}/";
        """
        File[] files = path.listFiles()
        assert files.length == 1
    } finally {
        try_sql("DROP TABLE IF EXISTS ${TableName4}")
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f : path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }

    sql "DROP TABLE IF EXISTS ${TableName4}"


}
