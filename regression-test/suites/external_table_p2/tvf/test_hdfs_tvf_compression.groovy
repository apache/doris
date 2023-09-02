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

suite("test_hdfs_tvf_compression", "p2,external,tvf,external_remote,external_remote_tvf") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String nameNodeHost = context.config.otherConfigs.get("extHiveHmsHost")
        String hdfsPort = context.config.otherConfigs.get("extHdfsPort")

        String baseUri = "hdfs://${nameNodeHost}:${hdfsPort}/usr/hive/warehouse/multi_catalog.db/test_compress_partitioned/"
        String baseFs = "hdfs://${nameNodeHost}:${hdfsPort}"

        String orderBy_limit = "order by c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21 limit 20 ";
        
        qt_gz_1 """
        select * from HDFS(
            "uri" = "${baseUri}/dt=gzip/000000_0.gz",
            "fs.defaultFS"= "${baseFs}",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "compress_type" = "GZ") ${orderBy_limit};
        """ 

        qt_gz_2 """
        desc function HDFS(
            "uri" = "${baseUri}/dt=gzip/000000_0.gz",
            "fs.defaultFS"= "${baseFs}",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "compress_type" = "GZ");
        """ 


        qt_bz2_1 """        
        select * from 
        HDFS(
            "uri" = "${baseUri}/dt=bzip2/000000_0.bz2",
            "fs.defaultFS"= "${baseFs}",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "compress_type" = "bz2") ${orderBy_limit};
        """


        qt_deflate_1"""
        select * from         
        HDFS(
            "uri" = "${baseUri}/dt=deflate/000000_0_copy_1.deflate",
            "fs.defaultFS"= "${baseFs}",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "compress_type" = "deflate") ${orderBy_limit};
        """

        qt_deflate_2"""
        select c7 from         
        HDFS(
            "uri" = "${baseUri}/dt=deflate/000000_0_copy_1.deflate",
            "fs.defaultFS"= "${baseFs}",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "compress_type" = "deflate") order by c7  limit 22;
        """



        qt_plain_1 """ 
        select * from 
        HDFS(
            "uri" = "${baseUri}/dt=plain/000000_0",
            "fs.defaultFS"= "${baseFs}",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "compress_type" = "plain") ${orderBy_limit};
        """

        qt_plain_2 """ 
        select c3,c4,c10 from 
        HDFS(
            "uri" = "${baseUri}/dt=plain/000000_0",
            "fs.defaultFS"= "${baseFs}",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "compress_type" = "plain") where c2="abc";
        """

        
        qt_mix_1 """
        select * from 
        HDFS(
            "uri" = "${baseUri}/dt=mix/000000_0",
            "fs.defaultFS"= "${baseFs}",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "compress_type" = "mix") ${orderBy_limit};
        """

        qt_mix_2 """
        select c1,count(*) from 
        HDFS(
            "uri" = "${baseUri}/dt=mix/000000_0",
            "fs.defaultFS"= "${baseFs}",
            "hadoop.username" = "hadoop",
            "format" = "csv",
            "compress_type" = "mix") group by c1,c2 limit 50;
        """


    }
}
