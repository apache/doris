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

suite("test_path_partition_keys", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String nameNodeHost = context.config.otherConfigs.get("externalEnvIp")
        String hdfsPort = context.config.otherConfigs.get("hive2HdfsPort")

        String baseUri = "hdfs://${nameNodeHost}:${hdfsPort}/catalog/tvf/csv/test_path_partition_keys"
        String baseFs = "hdfs://${nameNodeHost}:${hdfsPort}"

        order_qt_hdfs_1 """
        select * from HDFS(
            "uri" = "${baseUri}/dt1=cyw/*",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "column_separator" = ",",
            "path_partition_keys"="dt1" ) order by c1,c2 ;
        """ 

        order_qt_hdfs_2 """
        select * from HDFS(
            "uri" = "${baseUri}/dt1=cyw/*",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "column_separator" = ",",
            "path_partition_keys"="dt1") where dt1!="cyw" order by c1,c2 limit 3;
        """ 

        order_qt_hdfs_3 """
        select dt1,c1,count(*) from HDFS(
            "uri" = "${baseUri}/dt1=hello/*",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "column_separator" = ",",
            "path_partition_keys"="dt1") group by c1,dt1 order by c1;
        """ 
    
        order_qt_hdfs_4 """
        select * from HDFS(
            "uri" = "${baseUri}/dt2=two/dt1=hello/*",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "column_separator" = ",",
            "path_partition_keys"="dt1") order by c1;
        """ 

        order_qt_hdfs_5 """
        select * from HDFS(
            "uri" = "${baseUri}/dt2=two/dt1=cyw/*",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "column_separator" = ",",
            "path_partition_keys"="dt2,dt1");
        """

    }
    
    // LOCAL TVF is disabled; retain the HDFS and S3 partition-key cases.

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    
    sql """ set query_timeout=3600; """ 

    order_qt_s3_1 """ 
    select dt1 from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/test_path_partition_keys/dt1=cyw/b.csv",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",     
        "FORMAT" = "csv",
        "column_separator" = ",",
        "use_path_style" = "false", -- aliyun does not support path_style
        "path_partition_keys"="dt1") 
    """



    order_qt_s3_2 """ 
    select c1,dt1 from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/test_path_partition_keys/dt1=hello/c.csv",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",     
        "FORMAT" = "csv",
        "column_separator" = ",",
        "use_path_style" = "false", -- aliyun does not support path_style
        "path_partition_keys"="dt1") limit 3;
    """


    order_qt_s3_3 """ 
    select * from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/test_path_partition_keys/dt2=two/dt1=hello/c.csv",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",     
        "FORMAT" = "csv",
        "column_separator" = ",",
        "use_path_style" = "false", -- aliyun does not support path_style
        "path_partition_keys"="dt1") limit 3;
    """


    order_qt_s3_4 """ 
    select *from 
    s3(     
        "URI" = "https://${bucket}.${s3_endpoint}/regression/tvf/test_path_partition_keys/dt2=two/dt1=cyw/b.csv",    
        "s3.access_key" = "${ak}",     
        "s3.secret_key" = "${sk}",     
        "FORMAT" = "csv",
        "use_path_style" = "false", -- aliyun does not support path_style
        "column_separator" = ",",
        "path_partition_keys"="dt2,dt1") limit 3;
    """
}
