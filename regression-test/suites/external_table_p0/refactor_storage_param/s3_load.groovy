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
import org.awaitility.Awaitility

import static groovy.test.GroovyAssert.shouldFail;
import static java.util.concurrent.TimeUnit.SECONDS;

suite("refactor_storage_param_s3_load", "p0,external,external_docker") {
    String enabled = context.config.otherConfigs.get("enableRefactorParamsTest")
    if (enabled == null || enabled.equalsIgnoreCase("false")) {
        return
    }
    String ak = context.config.otherConfigs.get("AWSAK")
    String sk = context.config.otherConfigs.get("AWSSK")
    String endpoint = "s3.ap-northeast-1.amazonaws.com"
    String region = "ap-northeast-1"
    String bucket = "selectdb-qa-datalake-test"

    def s3table = "test_s3load";
    sql """
        drop table  if exists ${s3table}; 
         """
    sql """
        CREATE TABLE ${s3table}(
            user_id            BIGINT       NOT NULL COMMENT "user id",
            name               VARCHAR(20)           COMMENT "name",
            age                INT                   COMMENT "age"
        )
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql """
        insert into ${s3table} values (1, 'a', 10);
    """

    def insertResult = sql """
        SELECT count(1) FROM ${s3table}
    """

    println "insertResult: ${insertResult}"
    assert insertResult.get(0).get(0) == 1

    def outfile_to_S3 = { objBucket, objEndpoint, objRegion, objAk, objSk ->
        def outFilePath = "${objBucket}/outfile_different_s3/exp_"
        // select ... into outfile ...
        def res = sql """
            SELECT * FROM ${s3table} t ORDER BY user_id
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS CSV
            PROPERTIES (
                "s3.endpoint" = "${objEndpoint}",
                "s3.region" = "${objRegion}",
                "s3.secret_key"="${objSk}",
                "s3.access_key" = "${objAk}"
            );
        """
        return res[0][3]
    }
    def outfile_path = outfile_to_S3(bucket, endpoint, region, ak, sk);
    def filePath = outfile_path.replace("s3://${bucket}", "")

    def s3Load = { String objFilePath, String objBucket, String objEndpointName, String objEndpoint, String objRegionName, String objRegion, String objAkName, String objAk, String objSkName, String objSk, String usePathStyle ->

        def dataCountResult = sql """
            SELECT count(*) FROM ${s3table}
        """
        def label = "s3_load_label_" + System.currentTimeMillis()
        def load = sql """
            LOAD LABEL `${label}` (
           data infile ("${objFilePath}")
           into table ${s3table}
            COLUMNS TERMINATED BY "\\\t"
            FORMAT AS "CSV"
             (
                user_id,
                name,
                age
             ))
             with s3
             (
               "${objEndpointName}" = "${objEndpoint}",
               "${objRegionName}" = "${objRegion}",
               "${objSkName}"="${objSk}",
               "use_path_style" = "${usePathStyle}",
               "${objAkName}" = "${objAk}"
             )
             PROPERTIES
            (
                "timeout" = "3600"
            );
        """
        Awaitility.await().atMost(60, SECONDS).pollInterval(5, SECONDS).until({
            def loadResult = sql """
           show load where label = '${label}'
           """
            if (loadResult.get(0).get(2) == 'CANCELLED' || loadResult.get(0).get(2) == 'FAILED') {
                throw new RuntimeException("load failed")
            }
            return loadResult.get(0).get(2) == 'FINISHED'
        })

    }
    s3Load("s3://${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "true")
    s3Load("s3://${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "false")
    s3Load("s3://${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "")
    s3Load("s3://${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "AWS_ACCESS_KEY", ak, "AWS_SECRET_KEY", sk, "")
    s3Load("http://${bucket}.${endpoint}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "false")
    s3Load("http://${bucket}.${endpoint}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "")
    s3Load("https://${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "false")

    shouldFail {
        s3Load("https://${bucket}${filePath}", bucket, "", endpoint, "s3.region", region, "s3.access_key", "", "s3.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("https://${bucket}/${endpoint}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "")
    }
    shouldFail {
        s3Load("https://${bucket}/${endpoint}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "true")
    }
    shouldFail {
        s3Load("s3://${endpoint}/${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("s3://${bucket}/${endpoint}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("s3://${endpoint}/${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "false")
    }
    /*----------obs---------------*/
    ak = context.config.otherConfigs.get("hwYunAk")
    sk = context.config.otherConfigs.get("hwYunSk")
    endpoint = "obs.cn-north-4.myhuaweicloud.com"
    region = "cn-north-4"
    bucket = "doris-build";
    outfile_path = outfile_to_S3(bucket, endpoint, region, ak, sk);
    filePath = outfile_path.replace("s3://${bucket}", "")
    s3Load("s3://${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "true")
    s3Load("s3://${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "false")
    s3Load("s3://${bucket}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "true")
    s3Load("s3://${bucket}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "false")
    s3Load("obs://${bucket}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "true")
    s3Load("obs://${bucket}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "false")
    s3Load("obs://${bucket}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "")
    s3Load("s3://${bucket}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "")
    s3Load("http://${bucket}.${endpoint}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "")
    s3Load("https://${bucket}.${endpoint}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "")
    shouldFail {
        s3Load("https://${bucket}${filePath}", bucket, "", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("https://${bucket}${filePath}", bucket, "", endpoint, "obs.region", region, "obs.access_key", "", "obs.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("https://${bucket}/${endpoint}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "")

    }
    shouldFail {
        s3Load("https://${bucket}/${endpoint}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "true")
    }
    shouldFail {
        s3Load("s3://${endpoint}/${bucket}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("obs://${bucket}/${endpoint}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("obs://${endpoint}/${bucket}${filePath}", bucket, "obs.endpoint", endpoint, "obs.region", region, "obs.access_key", ak, "obs.secret_key", sk, "false")
    }
    
    /*-------------Tencent COS ----------*/
    ak = context.config.otherConfigs.get("txYunAk")
    sk = context.config.otherConfigs.get("txYunSk")
    endpoint = "cos.ap-beijing.myqcloud.com"
    region = "ap-beijing"
    bucket = "doris-build-1308700295";

    outfile_path = outfile_to_S3(bucket, endpoint, region, ak, sk);
    filePath = outfile_path.replace("s3://${bucket}", "")
    s3Load("s3://${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "true")
    s3Load("s3://${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "false")
    s3Load("s3://${bucket}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "true")
    s3Load("s3://${bucket}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "false")
    s3Load("cos://${bucket}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "true")
    s3Load("cos://${bucket}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "false")
    s3Load("cos://${bucket}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "")
    s3Load("s3://${bucket}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "")
    s3Load("http://${bucket}.${endpoint}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "")
    s3Load("https://${bucket}.${endpoint}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "")
      s3Load("http://${bucket}.${endpoint}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "false")
    shouldFail {
        s3Load("https://${bucket}${filePath}", bucket, "", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "false")
    }

    shouldFail {
        s3Load("https://${bucket}${filePath}", bucket, "", endpoint, "cos.region", region, "cos.access_key", "", "cos.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("https://${bucket}/${endpoint}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "obs.secret_key", sk, "")

    }
    shouldFail {
        s3Load("https://${bucket}/${endpoint}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "true")
    }
    shouldFail {
        s3Load("s3://${endpoint}/${bucket}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("cos://${bucket}/${endpoint}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("cos://${endpoint}/${bucket}${filePath}", bucket, "cos.endpoint", endpoint, "cos.region", region, "cos.access_key", ak, "cos.secret_key", sk, "false")
    }
    /************ Aliyun OSS ************/
    /*-----------------Aliyun OSS----------------*/
/*    ak = context.config.otherConfigs.get("aliYunAk")
    sk = context.config.otherConfigs.get("aliYunSk")
    endpoint = "oss-cn-hongkong.aliyuncs.com"
    region = "oss-cn-hongkong"
    bucket = "doris-regression-hk";

    outfile_path = outfile_to_S3(bucket, endpoint, region, ak, sk);
    filePath = outfile_path.replace("s3://${bucket}", "")
    s3Load("s3://${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "true")
    s3Load("s3://${bucket}${filePath}", bucket, "s3.endpoint", endpoint, "s3.region", region, "s3.access_key", ak, "s3.secret_key", sk, "false")
    s3Load("s3://${bucket}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "true")
    s3Load("s3://${bucket}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "false")
    s3Load("cos://${bucket}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "true")
    s3Load("cos://${bucket}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "false")
    s3Load("cos://${bucket}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "")
    s3Load("s3://${bucket}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "")
    s3Load("http://${bucket}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "")
    s3Load("https://${bucket}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "")
    s3Load("http://${bucket}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "true")
    s3Load("http://${bucket}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "false")
    shouldFail {
        s3Load("https://${bucket}${filePath}", bucket, "", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "false")
    }

    shouldFail {
        s3Load("https://${bucket}${filePath}", bucket, "", endpoint, "oss.region", region, "oss.access_key", "", "oss.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("https://${bucket}/${endpoint}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "")

    }
    shouldFail {
        s3Load("https://${bucket}/${endpoint}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "true")
    }
    shouldFail {
        s3Load("s3://${endpoint}/${bucket}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("oss://${bucket}/${endpoint}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "false")
    }
    shouldFail {
        s3Load("oss://${endpoint}/${bucket}${filePath}", bucket, "oss.endpoint", endpoint, "oss.region", region, "oss.access_key", ak, "oss.secret_key", sk, "false")
    }
    */
    

} 


