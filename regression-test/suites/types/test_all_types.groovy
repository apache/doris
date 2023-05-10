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

suite("test_all_types", "types") {
    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = getS3BucketName()

    List<List<String>> dataTypes = sql "SHOW data types"
    // aggModel start 

    def TableName = "data_types_agg"
    def createTBSQL = "CREATE TABLE IF NOT EXISTS ${TableName} ("
    def valueCols = ""
    def index = 0
    def keyCols = ""

    dataTypes.each { row ->
        def dataType = row[0]
        index++
        if (dataType == "ARRAY" || dataType == "MAP") {
            return
        } else if (dataType == "DECIMAL128") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMALV3(36,3), "
        } else if (dataType == "DECIMAL32") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMALV3(6,3), "
        } else if (dataType == "DECIMAL64") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMALV3(16,3), "
        } else if (dataType == "DECIMALV2") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMAL, "
        } else if (dataType == "JSONB") {
            valueCols += "k${index} ${dataType} REPLACE, "
        } else if (dataType == "STRING") {
            valueCols += "k${index} ${dataType} REPLACE, "
        } else if (dataType == "FLOAT") {
            valueCols += "k${index} ${dataType} MAX, "
        } else if (dataType == "DOUBLE") {
            valueCols += "k${index} ${dataType} MAX, "
        } else if (dataType == "HLL") {
            valueCols += "k${index} ${dataType} ${dataType}_UNION, "
        } else if (dataType == "BITMAP") {
            valueCols += "k${index} ${dataType} ${dataType}_UNION, "
        } else if (dataType == "QUANTILE_STATE") {
            valueCols += "k${index} ${dataType} QUANTILE_UNION NOT NULL, "
        } else {
            keyCols += "k${index}, "
            createTBSQL += "k${index} ${dataType}, "
        }
    }
    valueCols = valueCols.substring(0, valueCols.length() - 2)
    keyCols = keyCols.substring(0, keyCols.length() - 2)
    createTBSQL = createTBSQL.substring(0, createTBSQL.length() - 2)
    createTBSQL += "," + valueCols
    createTBSQL += ")"

    createTBSQL += """AGGREGATE KEY(${keyCols})
    DISTRIBUTED BY HASH(k2) BUCKETS 5 properties("replication_num" = "1");"""

    sql "${createTBSQL}"

    // aggModel insert load
    sql """ insert into ${TableName} (k2, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k17, k19, k22, k24, k25, k3, k14, k15, k16, k18, k21, k23) values 
    (11,1,'x','1999-01-08','1999-01-08 02:05:06','1999-01-08 02:05:06','1999-01-08',11.11,11.11,11.11,11,1,1,1,1,'x',to_bitmap(1),1,1,hll_hash(17),'"a"',to_quantile_state(1,2048),'text1'),
    (12,0,'b','2000-01-08','2000-01-08 02:05:06','2000-01-08 02:05:06','2000-01-08',22.22,22.22,22.22,22,2,2,2,2,'a',to_bitmap(2),2.2,2.2,hll_hash(18),'"b"',to_quantile_state(2,2048),'text2'),
    (13,1,'c','2001-01-08','2001-01-08 02:05:06','2001-01-08 02:05:06','2001-01-08',33.33,33.33,33.33,33,3,3,3,3,'c',to_bitmap(3),3.3,3.3,hll_hash(3),'"c"',to_quantile_state(3,2048),'text3'),
    (14,0,'d','2002-01-08','2002-01-08 02:05:06','2002-01-08 02:05:06','2002-01-08',44.44,44.44,44.44,44,4,4,4,4,'d',to_bitmap(4),4.4,4.4,hll_hash(4),'"d"',to_quantile_state(4,2048),'text4'),
    (15,1,'e','2003-03-08','2003-03-08 02:05:06','2003-03-08 02:05:06','2003-03-08',55.55,55.55,55.55,55,5,5,5,5,'e',to_bitmap(5),5.5,5.5,hll_hash(5),'"e"',to_quantile_state(5,2048),'text5'),
    (16,0,'f','2004-03-08','2004-03-08 02:05:06','2004-03-08 02:05:06','2004-03-08',66.66,66.66,66.66,66,6,6,6,6,'f',to_bitmap(6),6.6,6.6,hll_hash(6),'"f"',to_quantile_state(6,2048),'text6'),
    (17,1,'g','2005-03-08','2005-03-08 02:05:06','2005-03-08 02:05:06','2005-03-08',77.77,77.77,77.77,77,7,7,7,7,'g',to_bitmap(7),7.7,7.7,hll_hash(7),'"g"',to_quantile_state(7,2048),'text7'),
    (18,0,'h','2007-03-08','2007-03-08 07:05:07','2007-03-08 07:05:07','2007-03-08',88.88,88.88,88.88,88,8,8,8,8,'h',to_bitmap(8),8.8,8.8,hll_hash(8),'"h"',to_quantile_state(8,2048),'text8'),
    (19,1,'j','2009-03-08','2009-03-08 07:05:07','2009-03-08 07:05:07','2009-03-08',99.99,99.99,99.99,99,9,9,9,9,'j',to_bitmap(9),9.9,9.9,hll_hash(9),'"j"',to_quantile_state(9,2048),'text9'),
    (119,0,'k','2019-03-08','2019-03-08 07:05:07','2019-03-08 07:05:07','2019-03-08',19.11,19.11,19.11,11,19,19,19,19,'k',to_bitmap(19),19.9,19.9,hll_hash(19),'"k"',to_quantile_state(19,2048),'text19');
    """

    // aggModel stream load
    streamLoad {
        table "${TableName}"

        set 'column_separator', '|'  

        set 'columns', 'k2, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k17, k19, k22, k24, k25, k1, k3, k14, k15, k16, k18, k20, k21, k23, k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k12=k12, k13=k13, k17=k17, k19=k19, k22=k22, k24=k24, k25=k25, k3=to_bitmap(k3), k14=k14, k15=k15, k16=hll_hash(k16), k18=k18, k21=to_quantile_state(k21,2048), k23=k23'

        file 'streamload_input.csv'

        time 10000 
    }

    def uuid = UUID.randomUUID().toString().replace("-", "0")

    def outFilePath = """${context.file.parent}/test_all_types"""

    def columns = "k2, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k17, k19, k22, k24, k25, k1, k3, k14, k15, k16, k18, k20, k21, k23"
    String columns_str = ("$columns" != "") ? "($columns)" : "";

    def loadLabel = TableName + '_' + uuid

    // aggModel s3 load
    sql """
            LOAD LABEL $loadLabel (
                DATA INFILE("s3://$bucket/regression/datatypes/ALLTESTCASE.txt")
                INTO TABLE $TableName
                COLUMNS TERMINATED BY "|"
                $columns_str
                SET 
                (
                    k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k12=k12, k13=k13, k17=k17, k19=k19, k22=k22, k24=k24, k25=k25, k3=to_bitmap(k3), k14=k14, k15=k15, k16=hll_hash(k16), k18=k18, k21=to_quantile_state(k21,2048), k23=k23
                )
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "$endpoint",
                "AWS_REGION" = "$region"
            )
            properties(
                "use_new_load_scan_node" = "true"
            )
            
        """
    waitForS3LoadFinished(loadLabel)
    
    def stateResult = sql "show load where Label = '${loadLabel}'"
    println(stateResult)

    // aggModel output to .out
    qt_sql "SELECT * FROM ${TableName} ORDER BY k2"

    // aggModel select into outfile
    try {
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        sql """
            SELECT * FROM ${TableName} ORDER BY k2 INTO OUTFILE "file://${outFilePath}/";
        """
        File[] files = path.listFiles()
        assert files.length == 1
    } finally {
        try_sql("DROP TABLE IF EXISTS ${TableName}")
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f : path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }

    sql "DROP TABLE IF EXISTS ${TableName}"

    sql "ADMIN SET FRONTEND CONFIG ('enable_map_type' = 'true')"

    // uniModel_read start 

    TableName = "data_types_uni_read"

    createTBSQL = "CREATE TABLE IF NOT EXISTS ${TableName} ("
    valueCols = ""
    index = 0
    keyCols = ""

    dataTypes.each { row ->
        def dataType = row[0]
        index++
        if (dataType == "QUANTILE_STATE" || dataType == "HLL" || dataType == "BITMAP") {
            return
        } else if (dataType == "DECIMAL128") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMALV3(36,3), "
        } else if (dataType == "DECIMAL32") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMALV3(6,3), "
        } else if (dataType == "DECIMAL64") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMALV3(16,3), "
        } else if (dataType == "DECIMALV2") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMAL, "
        } else if (dataType == "ARRAY") {
            valueCols += "k${index} ${dataType}<INT>, "
        } else if (dataType == "MAP") {
            valueCols += "k${index} ${dataType}<STRING, INT>, "
        } else if (dataType == "STRING" || dataType == "JSONB" || dataType == "FLOAT" || dataType == "DOUBLE") {
            valueCols += "k${index} ${dataType}, "
        } else {
            keyCols += "k${index}, "
            createTBSQL += "k${index} ${dataType}, "
        }
    }
    valueCols = valueCols.substring(0, valueCols.length() - 2)
    keyCols = keyCols.substring(0, keyCols.length() - 2)
    createTBSQL = createTBSQL.substring(0, createTBSQL.length() - 2)
    createTBSQL += "," + valueCols
    createTBSQL += ")"
    
    createTBSQL += """UNIQUE KEY(${keyCols})
    DISTRIBUTED BY HASH(k2) BUCKETS 5 properties("replication_num" = "1");"""

    sql "${createTBSQL}"

    // uniModel_read insert load
    sql """ insert into ${TableName} (k2, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k17, k19, k22, k24, k25, k1, k14, k15, k18, k20, k23) values 
    (11,1,'x','1999-01-08','1999-01-08 02:05:06','1999-01-08 02:05:06','1999-01-08',11.11,11.11,11.11,11,1,1,1,1,'x',[1, 1],1,1,'"a"',{"k1":1},'text1'),
    (12,0,'b','2000-01-08','2000-01-08 02:05:06','2000-01-08 02:05:06','2000-01-08',22.22,22.22,22.22,22,2,2,2,2,'a',[2, 2],2.2,2.2,'"b"',{"k2":2},'text2'),
    (13,1,'c','2001-01-08','2001-01-08 02:05:06','2001-01-08 02:05:06','2001-01-08',33.33,33.33,33.33,33,3,3,3,3,'c',[3, 3],3.3,3.3,'"c"',{"k3":3},'text3'),
    (14,0,'d','2002-01-08','2002-01-08 02:05:06','2002-01-08 02:05:06','2002-01-08',44.44,44.44,44.44,44,4,4,4,4,'d',[4, 4],4.4,4.4,'"d"',{"k4":4},'text4'),
    (15,1,'e','2003-03-08','2003-03-08 02:05:06','2003-03-08 02:05:06','2003-03-08',55.55,55.55,55.55,55,5,5,5,5,'e',[5, 5],5.5,5.5,'"e"',{"k5":5},'text5'),
    (16,0,'f','2004-03-08','2004-03-08 02:05:06','2004-03-08 02:05:06','2004-03-08',66.66,66.66,66.66,66,6,6,6,6,'f',[6, 6],6.6,6.6,'"f"',{"k6":6},'text6'),
    (17,1,'g','2005-03-08','2005-03-08 02:05:06','2005-03-08 02:05:06','2005-03-08',77.77,77.77,77.77,77,7,7,7,7,'g',[7, 7],7.7,7.7,'"g"',{"k7":7},'text7'),
    (18,0,'h','2007-03-08','2007-03-08 07:05:07','2007-03-08 07:05:07','2007-03-08',88.88,88.88,88.88,88,8,8,8,8,'h',[8, 8],8.8,8.8,'"h"',{"k8":8},'text8'),
    (19,1,'j','2009-03-08','2009-03-08 07:05:07','2009-03-08 07:05:07','2009-03-08',99.99,99.99,99.99,99,9,9,9,9,'j',[9, 9],9.9,9.9,'"j"',{"k9":9},'text9'),
    (119,0,'k','2019-03-08','2019-03-08 07:05:07','2019-03-08 07:05:07','2019-03-08',19.11,19.11,19.11,11,19,19,19,19,'k',[19, 19],19.9,19.9,'"k"',{"k19":19},'text19');
    """

    // uniModel_read stream load
    streamLoad {
        table "${TableName}"

        set 'column_separator', '|'  

        set 'columns', 'k2, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k17, k19, k22, k24, k25, k1, k3, k14, k15, k16, k18, k20, k21, k23, k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k12=k12, k13=k13, k17=k17, k19=k19, k22=k22, k24=k24, k25=k25, k1=k1, k14=k14, k15=k15, k18=k18, k23=k23'

        file 'streamload_input.csv'

        time 10000 
    }

    // uniModel_read s3 load
    loadLabel = TableName + '_' + uuid

    sql """
            LOAD LABEL $loadLabel (
                DATA INFILE("s3://$bucket/regression/datatypes/ALLTESTCASE.txt")
                INTO TABLE $TableName
                COLUMNS TERMINATED BY "|"
                $columns_str
                SET 
                (
                    k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k12=k12, k13=k13, k17=k17, k19=k19, k22=k22, k24=k24, k25=k25, k1=k1, k14=k14, k15=k15, k18=k18, k23=k23
                )
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "$endpoint",
                "AWS_REGION" = "$region"
            )
            properties(
                "use_new_load_scan_node" = "true"
            )
            
        """
    waitForS3LoadFinished(loadLabel)
    
    stateResult = sql "show load where Label = '${loadLabel}'"
    println(stateResult)

    // uniModel_read output to .out
    qt_sql "SELECT * FROM ${TableName} ORDER BY k2"

    // uniModel_read select into outfile
    try {
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        sql """
            SELECT * FROM ${TableName} ORDER BY k2 INTO OUTFILE "file://${outFilePath}/";
        """
        File[] files = path.listFiles()
        assert files.length == 1
    } finally {
        try_sql("DROP TABLE IF EXISTS ${TableName}")
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f : path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }

    sql "DROP TABLE IF EXISTS ${TableName}"

    // uniModel_write start 

    TableName = "data_types_uni_write"
    createTBSQL = "CREATE TABLE IF NOT EXISTS ${TableName} ("
    valueCols = ""
    index = 0
    keyCols = ""

    dataTypes.each { row ->
        def dataType = row[0]
        index++
        if (dataType == "QUANTILE_STATE" || dataType == "HLL" || dataType == "BITMAP") {
            return
        } else if (dataType == "ARRAY") {
            valueCols += "k${index} ${dataType}<INT>, "
        } else if (dataType == "MAP") {
            valueCols += "k${index} ${dataType}<STRING, INT>, "
        } else if (dataType == "DECIMAL128") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMALV3(36,3), "
        } else if (dataType == "DECIMAL32") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMALV3(6,3), "
        } else if (dataType == "DECIMAL64") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMALV3(16,3), "
        } else if (dataType == "DECIMALV2") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMAL, "
        } else if (dataType == "STRING" || dataType == "JSONB" || dataType == "FLOAT" || dataType == "DOUBLE") {
            valueCols += "k${index} ${dataType}, "
        } else {
            keyCols += "k${index}, "
            createTBSQL += "k${index} ${dataType}, "
        }
    }
    valueCols = valueCols.substring(0, valueCols.length() - 2)
    keyCols = keyCols.substring(0, keyCols.length() - 2)
    createTBSQL = createTBSQL.substring(0, createTBSQL.length() - 2)
    createTBSQL += "," + valueCols
    createTBSQL += ")"

    createTBSQL += """UNIQUE KEY(${keyCols})
    DISTRIBUTED BY HASH(k2) BUCKETS 5 properties("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");"""

    sql "${createTBSQL}"

    // uniModel_write insert load

    sql """ insert into ${TableName} (k2, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k17, k19, k22, k24, k25, k1, k14, k15, k18, k20, k23) values 
    (11,1,'x','1999-01-08','1999-01-08 02:05:06','1999-01-08 02:05:06','1999-01-08',11.11,11.11,11.11,11,1,1,1,1,'x',[1, 1],1,1,'"a"',{"k1":1},'text1'),
    (12,0,'b','2000-01-08','2000-01-08 02:05:06','2000-01-08 02:05:06','2000-01-08',22.22,22.22,22.22,22,2,2,2,2,'a',[2, 2],2.2,2.2,'"b"',{"k2":2},'text2'),
    (13,1,'c','2001-01-08','2001-01-08 02:05:06','2001-01-08 02:05:06','2001-01-08',33.33,33.33,33.33,33,3,3,3,3,'c',[3, 3],3.3,3.3,'"c"',{"k3":3},'text3'),
    (14,0,'d','2002-01-08','2002-01-08 02:05:06','2002-01-08 02:05:06','2002-01-08',44.44,44.44,44.44,44,4,4,4,4,'d',[4, 4],4.4,4.4,'"d"',{"k4":4},'text4'),
    (15,1,'e','2003-03-08','2003-03-08 02:05:06','2003-03-08 02:05:06','2003-03-08',55.55,55.55,55.55,55,5,5,5,5,'e',[5, 5],5.5,5.5,'"e"',{"k5":5},'text5'),
    (16,0,'f','2004-03-08','2004-03-08 02:05:06','2004-03-08 02:05:06','2004-03-08',66.66,66.66,66.66,66,6,6,6,6,'f',[6, 6],6.6,6.6,'"f"',{"k6":6},'text6'),
    (17,1,'g','2005-03-08','2005-03-08 02:05:06','2005-03-08 02:05:06','2005-03-08',77.77,77.77,77.77,77,7,7,7,7,'g',[7, 7],7.7,7.7,'"g"',{"k7":7},'text7'),
    (18,0,'h','2007-03-08','2007-03-08 07:05:07','2007-03-08 07:05:07','2007-03-08',88.88,88.88,88.88,88,8,8,8,8,'h',[8, 8],8.8,8.8,'"h"',{"k8":8},'text8'),
    (19,1,'j','2009-03-08','2009-03-08 07:05:07','2009-03-08 07:05:07','2009-03-08',99.99,99.99,99.99,99,9,9,9,9,'j',[9, 9],9.9,9.9,'"j"',{"k9":9},'text9'),
    (119,0,'k','2019-03-08','2019-03-08 07:05:07','2019-03-08 07:05:07','2019-03-08',19.11,19.11,19.11,11,19,19,19,19,'k',[19, 19],19.9,19.9,'"k"',{"k19":19},'text19');
    """

    // uniModel_write stream load
    streamLoad {
        table "${TableName}"

        set 'column_separator', '|'  

        set 'columns', 'k2, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k17, k19, k22, k24, k25, k1, k3, k14, k15, k16, k18, k20, k21, k23, k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k12=k12, k13=k13, k17=k17, k19=k19, k22=k22, k24=k24, k25=k25, k1=k1, k14=k14, k15=k15, k18=k18, k23=k23'

        file 'streamload_input.csv'

        time 10000 
    }
    
    // uniModel_write s3 load
    loadLabel = TableName + '_' + uuid
    sql """
            LOAD LABEL $loadLabel (
                DATA INFILE("s3://$bucket/regression/datatypes/ALLTESTCASE.txt")
                INTO TABLE $TableName
                COLUMNS TERMINATED BY "|"
                $columns_str
                SET 
                (
                    k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k12=k12, k13=k13, k17=k17, k19=k19, k22=k22, k24=k24, k25=k25, k1=k1, k14=k14, k15=k15, k18=k18, k23=k23
                )
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "$endpoint",
                "AWS_REGION" = "$region"
            )
            properties(
                "use_new_load_scan_node" = "true"
            )
            
        """
    waitForS3LoadFinished(loadLabel)
    
    stateResult = sql "show load where Label = '${loadLabel}'"
    println(stateResult)

    // uniModel_write output to .out
    qt_sql "SELECT * FROM ${TableName} ORDER BY k2"

    // uniModel_write select into outfile
    try {
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        sql """
            SELECT * FROM ${TableName} ORDER BY k2 INTO OUTFILE "file://${outFilePath}/";
        """
        File[] files = path.listFiles()
        assert files.length == 1
    } finally {
        try_sql("DROP TABLE IF EXISTS ${TableName}")
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f : path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }

    sql "DROP TABLE IF EXISTS ${TableName}"


    // DupModel start
    TableName = "data_types_dup"

    createTBSQL = "CREATE TABLE IF NOT EXISTS ${TableName} ("
    valueCols = ""
    index = 0
    keyCols = ""

    dataTypes.each { row ->
        def dataType = row[0]
        index++
        if (dataType == "QUANTILE_STATE" || dataType == "HLL" || dataType == "BITMAP") {
            return
        } else if (dataType == "DECIMAL128") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMALV3(36,3), "
        } else if (dataType == "DECIMAL32") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMALV3(6,3), "
        } else if (dataType == "DECIMAL64") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMALV3(16,3), "
        } else if (dataType == "DECIMALV2") {
            keyCols += "k${index}, "
            createTBSQL += "k${index} DECIMAL, "
        } else if (dataType == "ARRAY") {
            valueCols += "k${index} ${dataType}<INT>, "
        } else if (dataType == "MAP") {
            valueCols += "k${index} ${dataType}<STRING, INT>, "
        } else if (dataType == "STRING" || dataType == "JSONB" || dataType == "FLOAT" || dataType == "DOUBLE") {
            valueCols += "k${index} ${dataType}, "
        } else {
            keyCols += "k${index}, "
            createTBSQL += "k${index} ${dataType}, "
        }
    }
    valueCols = valueCols.substring(0, valueCols.length() - 2)
    keyCols = keyCols.substring(0, keyCols.length() - 2)
    createTBSQL = createTBSQL.substring(0, createTBSQL.length() - 2)
    createTBSQL += "," + valueCols
    createTBSQL += ")"

    createTBSQL += """DUPLICATE KEY(${keyCols})
    DISTRIBUTED BY HASH(k2) BUCKETS 5 properties("replication_num" = "1");"""

    sql "${createTBSQL}"

    // DupModel insert load
    sql """ insert into ${TableName} (k2, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k17, k19, k22, k24, k25, k1, k14, k15, k18, k20, k23) values 
    (11,1,'x','1999-01-08','1999-01-08 02:05:06','1999-01-08 02:05:06','1999-01-08',11.11,11.11,11.11,11,1,1,1,1,'x',[1, 1],1,1,'"a"',{"k1":1},'text1'),
    (12,0,'b','2000-01-08','2000-01-08 02:05:06','2000-01-08 02:05:06','2000-01-08',22.22,22.22,22.22,22,2,2,2,2,'a',[2, 2],2.2,2.2,'"b"',{"k2":2},'text2'),
    (13,1,'c','2001-01-08','2001-01-08 02:05:06','2001-01-08 02:05:06','2001-01-08',33.33,33.33,33.33,33,3,3,3,3,'c',[3, 3],3.3,3.3,'"c"',{"k3":3},'text3'),
    (14,0,'d','2002-01-08','2002-01-08 02:05:06','2002-01-08 02:05:06','2002-01-08',44.44,44.44,44.44,44,4,4,4,4,'d',[4, 4],4.4,4.4,'"d"',{"k4":4},'text4'),
    (15,1,'e','2003-03-08','2003-03-08 02:05:06','2003-03-08 02:05:06','2003-03-08',55.55,55.55,55.55,55,5,5,5,5,'e',[5, 5],5.5,5.5,'"e"',{"k5":5},'text5'),
    (16,0,'f','2004-03-08','2004-03-08 02:05:06','2004-03-08 02:05:06','2004-03-08',66.66,66.66,66.66,66,6,6,6,6,'f',[6, 6],6.6,6.6,'"f"',{"k6":6},'text6'),
    (17,1,'g','2005-03-08','2005-03-08 02:05:06','2005-03-08 02:05:06','2005-03-08',77.77,77.77,77.77,77,7,7,7,7,'g',[7, 7],7.7,7.7,'"g"',{"k7":7},'text7'),
    (18,0,'h','2007-03-08','2007-03-08 07:05:07','2007-03-08 07:05:07','2007-03-08',88.88,88.88,88.88,88,8,8,8,8,'h',[8, 8],8.8,8.8,'"h"',{"k8":8},'text8'),
    (19,1,'j','2009-03-08','2009-03-08 07:05:07','2009-03-08 07:05:07','2009-03-08',99.99,99.99,99.99,99,9,9,9,9,'j',[9, 9],9.9,9.9,'"j"',{"k9":9},'text9'),
    (119,0,'k','2019-03-08','2019-03-08 07:05:07','2019-03-08 07:05:07','2019-03-08',19.11,19.11,19.11,11,19,19,19,19,'k',[19, 19],19.9,19.9,'"k"',{"k19":19},'text19');
    """


    // DupModel stream load
    streamLoad {
        table "${TableName}"

        set 'column_separator', '|'  

        set 'columns', 'k2, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k17, k19, k22, k24, k25, k1, k3, k14, k15, k16, k18, k20, k21, k23, k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k12=k12, k13=k13, k17=k17, k19=k19, k22=k22, k24=k24, k25=k25, k1=k1, k14=k14, k15=k15, k18=k18, k20=k20, k23=k23'

        file 'streamload_input.csv'

        time 10000 
    }

    // DupModel s3 load

    loadLabel = TableName + '_' + uuid
    sql """
            LOAD LABEL $loadLabel (
                DATA INFILE("s3://$bucket/regression/datatypes/ALLTESTCASE.txt")
                INTO TABLE $TableName
                COLUMNS TERMINATED BY "|"
                $columns_str
                SET 
                (
                    k2=k2, k4=k4, k5=k5, k6=k6, k7=k7, k8=k8, k9=k9, k10=k10, k11=k11, k12=k12, k13=k13, k17=k17, k19=k19, k22=k22, k24=k24, k25=k25, k1=k1, k14=k14, k15=k15, k18=k18, k20=k20, k23=k23
                )
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "$endpoint",
                "AWS_REGION" = "$region"
            )
            properties(
                "use_new_load_scan_node" = "true"
            )
            
        """
    waitForS3LoadFinished(loadLabel)
    
    stateResult = sql "show load where Label = '${loadLabel}'"
    println(stateResult)

    // DupModel output to .out
    qt_sql "SELECT * FROM ${TableName} ORDER BY k2"

    // DupModel select into outfile
    try {
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        sql """
            SELECT * FROM ${TableName} ORDER BY k2 INTO OUTFILE "file://${outFilePath}/";
        """
        File[] files = path.listFiles()
        assert files.length == 1
    } finally {
        try_sql("DROP TABLE IF EXISTS ${TableName}")
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f : path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }

    sql "DROP TABLE IF EXISTS ${TableName}"
}
