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


suite("test_utf8_check","p0,external,tvf,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2","hive3"]) {
    
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_utf8_check"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        def hdfsUserName = "doris"
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """use `${catalog_name}`.`default`"""
        

        sql """ set enable_text_validate_utf8 = true; """     

        test {
            sql """ select * from invalid_utf8_data """ 
            exception """Only support csv data in utf8 codec"""
        }
        
        
        test {
            sql """ select * from invalid_utf8_data2; """ 
            exception """Only support csv data in utf8 codec"""
        }


        def uri = "${defaultFS}" + "/user/doris/preinstalled_data/text/utf8_check/utf8_check_fail.csv"

        
        test {
            sql """ desc function  HDFS(
                "uri" = "${uri}",
                "hadoop.username" = "${hdfsUserName}",            
                "format" = "csv",
                "column_separator"=",")"""    
            exception """Only support csv data in utf8 codec"""
        }

        test {
            sql """select * from HDFS(
                "uri" = "${uri}",
                "hadoop.username" = "${hdfsUserName}",            
                "format" = "csv",
                "column_separator"=",")"""    
            exception """Only support csv data in utf8 codec"""
        }
    

        sql """ set enable_text_validate_utf8 = false; """     

        qt_1 """select * from invalid_utf8_data order by id """ 
    
        qt_2 """ desc function  HDFS(
                "uri" = "${uri}",
                "hadoop.username" = "${hdfsUserName}",            
                "format" = "csv",
                "column_separator"=",")"""    


        qt_3 """select * from   HDFS(
                "uri" = "${uri}",
                "hadoop.username" = "${hdfsUserName}",            
                "format" = "csv",
                "column_separator"=",") order by c1"""    
        qt_4 """select * from invalid_utf8_data2 order by id """ 
    
    
    }

}