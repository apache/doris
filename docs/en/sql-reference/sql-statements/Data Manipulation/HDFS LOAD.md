---
{
    "title": "HDFS LOAD",
    "language": "en"
}
---

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

# HDFS LOAD
## description

    Hdfs load will load data into Doris from Apache HDFS by BE directly.(Baidu HDFS is not supported)

### Syntax:

    LOAD LABEL load_label
    (
    data_desc1[, data_desc2, ...]
    )
    WITH HDFS
    [load_properties]
    [opt_properties];

    1. load_label

        Unique load label within a database.
        syntax:
        [database_name.]your_label
     
    2. data_desc

        To describe the data source. 
        syntax:
            [MERGE|APPEND|DELETE]
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
            [PRECEDING FILTER predicate]
            [SET (k1 = func(k2))]
            [WHERE predicate] 
            [DELETE ON label=true]
            [read_properties]

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

            PRECEDING FILTER predicate:

            Used to filter original data. The original data is the data without column mapping and transformation. The user can filter the data before conversion, select the desired data, and then perform the conversion.
            
            SET:
            
            If this parameter is specified, a column of the source file can be transformed according to a function, and then the transformed result can be loaded into the table. The grammar is `column_name = expression`. Some examples are given to help understand.

            Example 1: There are three columns "c1, c2, c3" in the table. The first two columns in the source file correspond in turn (c1, c2), and the last two columns correspond to c3. Then, column (c1, c2, tmp_c3, tmp_c4) SET (c3 = tmp_c3 + tmp_c4) should be specified.

            Example 2: There are three columns "year, month, day" in the table. There is only one time column in the source file, in the format of "2018-06-01:02:03". Then you can specify columns (tmp_time) set (year = year (tmp_time), month = month (tmp_time), day = day (tmp_time)) to complete the import.

            WHERE:
          
            After filtering the transformed data, data that meets where predicates can be loaded. Only column names in tables can be referenced in WHERE statements.

            merge_type:

            The type of data merging supports three types: APPEND, DELETE, and MERGE. APPEND is the default value, which means that all this batch of data needs to be appended to the existing data. DELETE means to delete all rows with the same key as this batch of data. MERGE semantics Need to be used in conjunction with the delete condition, which means that the data that meets the delete on condition is processed according to DELETE semantics and the rest is processed according to APPEND semantics

            delete_on_predicates:

            Only used when merge type is MERGE

            read_properties:

            Used to specify some special parameters.
            Syntax：
            [PROPERTIES ("key"="value", ...)]
        
            You can specify the following parameters:
                
              line_delimiter： Used to specify the line delimiter in the load file. The default is `\n`. You can use a combination of multiple characters as the column separator.

              fuzzy_parse： Boolean type, true to indicate that parse json schema as the first line, this can make import more faster,but need all key keep the order of first line, default value is false. Only use for json format.
            
              jsonpaths: There are two ways to import json: simple mode and matched mode.
                simple mode: it is simple mode without setting the jsonpaths parameter. In this mode, the json data is required to be the object type. For example:
                {"k1": 1, "k2": 2, "k3": "hello"}, where k1, k2, k3 are column names.

                matched mode: the json data is relatively complex, and the corresponding value needs to be matched through the jsonpaths parameter.
            
              strip_outer_array: Boolean type, true to indicate that json data starts with an array object and flattens objects in the array object, default value is false. For example：
                [
                  {"k1" : 1, "v1" : 2},
                  {"k1" : 3, "v1" : 4}
                ]
              if strip_outer_array is true, and two rows of data are generated when imported into Doris.
           
              json_root: json_root is a valid JSONPATH string that specifies the root node of the JSON Document. The default value is "".
           
              num_as_string: Boolean type, true means that when parsing the json data, it will be converted into a number type and converted into a string, and then it will be imported without loss of precision.
            
    3. load_properties
        Syntax:
            if using load with hdfs, you need to specify the following attributes 
            (
                "fs.defaultFS" = "",
                "hdfs_user"="",
                "dfs.nameservices"="my_ha",
                "dfs.ha.namenodes.xxx"="my_nn1,my_nn2",
                "dfs.namenode.rpc-address.xxx.my_nn1"="host1:port",
                "dfs.namenode.rpc-address.xxx.my_nn2"="host2:port",
                "dfs.client.failover.proxy.provider.xxx"="org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
            )
        Explain:
            fs.defaultFS: defaultFS
            hdfs_user: hdfs user

            namenode HA(Optional). By configuring namenode HA, new namenode can be automatically identified when the namenode is switched
            dfs.nameservices: hdfs service name, customize, eg: "dfs.nameservices" = "my_ha"
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

        send_batch_parallelism: Used to set the default parallelism for sending batch, if the value for parallelism exceed `max_send_batch_parallelism_per_job` in BE config, then the coordinator BE will use the value of `max_send_batch_parallelism_per_job`.

    5. Load data format sample

        Integer（TINYINT/SMALLINT/INT/BIGINT/LARGEINT）: 1, 1000, 1234
        Float（FLOAT/DOUBLE/DECIMAL）: 1.1, 0.23, .356
        Date（DATE/DATETIME）: 2017-10-03, 2017-06-13 12:34:03. 
        (Note: If it's in other date formats, you can use strftime or time_format functions to convert in the import command)
        
        String（CHAR/VARCHAR）: "I am a student", "a"
        NULL: \N

## example

    1. LOAD WITH HDFS, normal HDFS cluster
        LOAD LABEL example_db.label_filter
        (
            DATA INFILE("hdfs://host:port/user/data/*/test.txt")
            INTO TABLE `tbl1`
            COLUMNS TERMINATED BY ","
            (k1,k2,v1,v2)
        ) 
        with HDFS (
            "fs.defaultFS"="hdfs://testFs",
            "hdfs_user"="user"
        );
    2. LOAD WITH HDFS, hdfs ha
        LOAD LABEL example_db.label_filter
        (
            DATA INFILE("hdfs://host:port/user/data/*/test.txt")
            INTO TABLE `tbl1`
            COLUMNS TERMINATED BY ","
            (k1,k2,v1,v2)
        ) 
        with HDFS (
            "fs.defaultFS"="hdfs://testFs",
            "hdfs_user"="user"
            "dfs.nameservices"="my_ha",
            "dfs.ha.namenodes.xxx"="my_nn1,my_nn2",
            "dfs.namenode.rpc-address.xxx.my_nn1"="host1:port",
            "dfs.namenode.rpc-address.xxx.my_nn2"="host2:port",
            "dfs.client.failover.proxy.provider.xxx"="org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        );

## keyword

    HDFS,LOAD
