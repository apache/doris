<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# BROKER LOAD
## description

    Broker load will load data into Doris via Broker.
    Use `show broker;` to see the Broker deployed in cluster.
    
    Support following data sources:

    1. Baidu HDFS: hdfs for Baidu. Only be used inside Baidu.
    2. Baidu AFS: afs for Baidu. Only be used inside Baidu.
    3. Baidu Object Storage(BOS): BOS on Baidu Cloud.
    4. Apache HDFS.

### Syntax: 

    LOAD LABEL load_label
    (
    data_desc1[, data_desc2, ...]
    )
    WITH BROKER broker_name
    [broker_properties]
    [opt_properties];

    1. load_label

        Unique load label within a database.
        syntax: 
        [database_name.]your_label
     
    2. data_desc

        To describe the data source. 
        syntax: 
            DATA INFILE
            (
            "file_path1"[, file_path2, ...]
            )
            [NEGATIVE]
            INTO TABLE `table_name`
            [PARTITION (p1, p2)]
            [COLUMNS TERMINATED BY "column_separator"]
            [FORMAT AS "file_type"]
            [(column_list)]
            [SET (k1 = func(k2))]
            [WHERE predicate]    

        Explain: 
            file_path: 

            File path. Support wildcard. Must match to file, not directory. 

            PARTITION:

            Data will only be loaded to specified partitions. Data out of partition's range will be filtered. If not specifed, all partitions will be loaded.
                    
            NEGATIVE: 
            
            If this parameter is specified, it is equivalent to importing a batch of "negative" data to offset the same batch of data loaded before.
            
            This parameter applies only to the case where there are value columns and the aggregation type of value columns is only SUM.
            
            column_separator: 
            
            Used to specify the column separator in the import file. Default is `\t`.
            If the character is invisible, it needs to be prefixed with `\\x`, using hexadecimal to represent the separator.

            For example, the separator `\x01` of the hive file is specified as `\\ x01`
            
            file_type: 

            Used to specify the type of imported file, such as parquet, orc, csv. Default values are determined by the file suffix name. 
 
            column_list: 

            Used to specify the correspondence between columns in the import file and columns in the table.

            When you need to skip a column in the import file, specify it as a column name that does not exist in the table.

            syntax: 
            (col_name1, col_name2, ...)
            
            SET:
            
            If this parameter is specified, a column of the source file can be transformed according to a function, and then the transformed result can be loaded into the table. The grammar is `column_name = expression`. Some examples are given to help understand.

            Example 1: There are three columns "c1, c2, c3" in the table. The first two columns in the source file correspond in turn (c1, c2), and the last two columns correspond to c3. Then, column (c1, c2, tmp_c3, tmp_c4) SET (c3 = tmp_c3 + tmp_c4) should be specified.

            Example 2: There are three columns "year, month, day" in the table. There is only one time column in the source file, in the format of "2018-06-01:02:03". Then you can specify columns (tmp_time) set (year = year (tmp_time), month = month (tmp_time), day = day (tmp_time)) to complete the import.

            WHERE:
          
            After filtering the transformed data, data that meets where predicates can be loaded. Only column names in tables can be referenced in WHERE statements.
            
    3. broker_name

        The name of the Broker used can be viewed through the `show broker` command.

    4. broker_properties

        Used to provide Broker access to data sources. Different brokers, and different access methods, need to provide different information.

        1. Baidu HDFS/AFS

            Access to Baidu's internal hdfs/afs currently only supports simple authentication, which needs to be provided:
            
            username: hdfs username
            password: hdfs password

        2. BOS

            bos_endpoint.
            bos_accesskey: cloud user's accesskey
            bos_secret_accesskey: cloud user's secret_accesskey
        
        3. Apache HDFS

            Community version of HDFS supports simple authentication, Kerberos authentication, and HA configuration.

            Simple authentication:
            hadoop.security.authentication = simple (default)
            username: hdfs username
            password: hdfs password

            kerberos authentication: 
            hadoop.security.authentication = kerberos
            kerberos_principal:  kerberos's principal
            kerberos_keytab:  path of kerberos's keytab file. This file should be able to access by Broker
            kerberos_keytab_content: Specify the contents of the KeyTab file in Kerberos after base64 encoding. This option is optional from the kerberos_keytab configuration. 

            namenode HA: 
            By configuring namenode HA, new namenode can be automatically identified when the namenode is switched
            dfs.nameservices: hdfs service name，customize，eg: "dfs.nameservices" = "my_ha"
            dfs.ha.namenodes.xxx: Customize the name of a namenode, separated by commas. XXX is a custom name in dfs. name services, such as "dfs. ha. namenodes. my_ha" = "my_nn"
            dfs.namenode.rpc-address.xxx.nn: Specify RPC address information for namenode, where NN denotes the name of the namenode configured in dfs.ha.namenodes.xxxx, such as: "dfs.namenode.rpc-address.my_ha.my_nn"= "host:port"
            dfs.client.failover.proxy.provider: Specify the provider that client connects to namenode by default: org. apache. hadoop. hdfs. server. namenode. ha. Configured Failover ProxyProvider.

    4. opt_properties

        Used to specify some special parameters. 
        Syntax: 
        [PROPERTIES ("key"="value", ...)]
        
        You can specify the following parameters: 
        
        timout: Specifies the timeout time for the import operation. The default timeout is 4 hours per second.

        max_filter_ratio: Data ratio of maximum tolerance filterable (data irregularity, etc.). Default zero tolerance.

        exc_mem_limit: Memory limit. Default is 2GB. Unit is Bytes.
        
        strict_mode: Whether the data is strictly restricted. The default is false.

        timezone: Specify time zones for functions affected by time zones, such as strftime/alignment_timestamp/from_unixtime, etc. See the documentation for details. If not specified, use the "Asia/Shanghai" time zone.

    5. Load data format sample

        Integer（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）: 1, 1000, 1234
        Float（FLOAT/DOUBLE/DECIMAL）: 1.1, 0.23, .356
        Date（DATE/DATETIME）: 2017-10-03, 2017-06-13 12:34:03. 
        (Note: If it's in other date formats, you can use strftime or time_format functions to convert in the import command)
        
        String（CHAR/VARCHAR）: "I am a student", "a"
        NULL: \N

## example

    1. Load a batch of data from HDFS, specify timeout and filtering ratio. Use the broker with the inscription my_hdfs_broker. Simple authentication.

        LOAD LABEL example_db.label1
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        )
        WITH BROKER my_hdfs_broker
        (
        "username" = "hdfs_user",
        "password" = "hdfs_passwd"
        )
        PROPERTIES
        (
        "timeout" = "3600",
        "max_filter_ratio" = "0.1"
        );
    
        Where hdfs_host is the host of the namenode and hdfs_port is the fs.defaultFS port (default 9000)
        
    2. Load a batch of data from AFS contains multiple files. Import different tables, specify separators, and specify column correspondences.

        LOAD LABEL example_db.label2
        (
        DATA INFILE("afs://afs_host:hdfs_port/user/palo/data/input/file1")
        INTO TABLE `my_table_1`
        COLUMNS TERMINATED BY ","
        (k1, k3, k2, v1, v2),
        DATA INFILE("afs://afs_host:hdfs_port/user/palo/data/input/file2")
        INTO TABLE `my_table_2`
        COLUMNS TERMINATED BY "\t"
        (k1, k2, k3, v2, v1)
        )
        WITH BROKER my_afs_broker
        (
        "username" = "afs_user",
        "password" = "afs_passwd"
        )
        PROPERTIES
        (
        "timeout" = "3600",
        "max_filter_ratio" = "0.1"
        );
        

    3. Load a batch of data from HDFS, specify hive's default delimiter \\x01, and use wildcard * to specify all files in the directory. Use simple authentication and configure namenode HA at the same time

        LOAD LABEL example_db.label3
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/*")
        INTO TABLE `my_table`
        COLUMNS TERMINATED BY "\\x01"
        )
        WITH BROKER my_hdfs_broker
        (
        "username" = "hdfs_user",
        "password" = "hdfs_passwd",
        "dfs.nameservices" = "my_ha",
        "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
        "dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
        "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
        "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        )
    
    4. Load a batch of "negative" data from HDFS. Use Kerberos authentication to provide KeyTab file path.

        LOAD LABEL example_db.label4
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/old_file)
        NEGATIVE
        INTO TABLE `my_table`
        COLUMNS TERMINATED BY "\t"
        )
        WITH BROKER my_hdfs_broker
        (
        "hadoop.security.authentication" = "kerberos",
        "kerberos_principal"="doris@YOUR.COM",
        "kerberos_keytab"="/home/palo/palo.keytab"
        )

    5. Load a batch of data from HDFS, specify partition. At the same time, use Kerberos authentication mode. Provide the KeyTab file content encoded by base64.

        LOAD LABEL example_db.label5
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        (k1, k3, k2, v1, v2)
        )
        WITH BROKER my_hdfs_broker
        (
        "hadoop.security.authentication"="kerberos",
        "kerberos_principal"="doris@YOUR.COM",
        "kerberos_keytab_content"="BQIAAABEAAEACUJBSURVLkNPTQAEcGFsbw"
        )

    6. Load a batch of data from BOS, specify partitions, and make some transformations to the columns of the imported files, as follows:
    
       Table schema: 
        k1 varchar(20)
        k2 int

        Assuming that the data file has only one row of data:

        Adele,1,1

        The columns in the data file correspond to the columns specified in the load statement:
        
        k1,tmp_k2,tmp_k3

        transform as: 

        1) k1: unchanged
        2) k2: sum of tmp_k2 and tmp_k3

        LOAD LABEL example_db.label6
        (
        DATA INFILE("bos://my_bucket/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        (k1, tmp_k2, tmp_k3)
        SET (
          k2 = tmp_k2 + tmp_k3
        )
        )
        WITH BROKER my_bos_broker
        (
        "bos_endpoint" = "http://bj.bcebos.com",
        "bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
        "bos_secret_accesskey"="yyyyyyyyyyyyyyyyyyyy"
        )
    
    7. Load data into tables containing HLL columns, which can be columns in tables or columns in data
    
        If there are three columns in the table (id, v1, v2, v3). The V1 and V2 columns are HLL columns. The imported source file has three columns. Then (column_list) declares that the first column is id, and the second and third columns are temporarily named k1, k2.

        In SET, the HLL column in the table must be specifically declared hll_hash. The V1 column in the table is equal to the hll_hash (k1) column in the original data.The v3 column in the table does not have a corresponding value in the original data, and empty_hll is used to supplement the default value.

        LOAD LABEL example_db.label7
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        (id, k1, k2)
        SET (
          v1 = hll_hash(k1),
          v2 = hll_hash(k2),
          v3 = empty_hll()
        )
        )
        WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

        LOAD LABEL example_db.label8
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        PARTITION (p1, p2)
        COLUMNS TERMINATED BY ","
        (k1, k2, tmp_k3, tmp_k4, v1, v2)
        SET (
          v1 = hll_hash(tmp_k3),
          v2 = hll_hash(tmp_k4)
        )
        )
        WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

    8. Data in load Parquet file specifies FORMAT as parquet. By default, it is judged by file suffix.

        LOAD LABEL example_db.label9
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        FORMAT AS "parquet"
        (k1, k2, k3)
        )
        WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

    9. Extract partition fields in file paths

        If necessary, partitioned fields in the file path are resolved based on the field type defined in the table, similar to the Partition Discovery function in Spark.

        LOAD LABEL example_db.label10
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/dir/city=beijing/*/*")
        INTO TABLE `my_table`
        FORMAT AS "csv"
        (k1, k2, k3)
        COLUMNS FROM PATH AS (city, utc_date)
        SET (uniq_id = md5sum(k1, city))
        )
        WITH BROKER hdfs ("username"="hdfs_user", "password"="hdfs_password");

        Directory `hdfs://hdfs_host:hdfs_port/user/palo/data/input/dir/city=beijing` contains following files:
         
        [hdfs://hdfs_host:hdfs_port/user/palo/data/input/dir/city=beijing/utc_date=2019-06-26/0000.csv, hdfs://hdfs_host:hdfs_port/user/palo/data/input/dir/city=beijing/utc_date=2019-06-26/0001.csv, ...]

        Extract city and utc_date fields in the file path

    10. To filter the load data, columns whose K1 value is greater than K2 value can be imported.
    
        LOAD LABEL example_db.label10
        (
        DATA INFILE("hdfs://hdfs_host:hdfs_port/user/palo/data/input/file")
        INTO TABLE `my_table`
        where k1 > k2
        );
     
## keyword

    BROKER,LOAD
