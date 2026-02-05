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


import com.google.common.base.Strings;
suite("test_tvf_anonymous") {
    if (Strings.isNullOrEmpty(context.config.otherConfigs.get("enableTestTvfAnonymous"))) {
        return
    }

    
    def region = context.config.otherConfigs.get("anymousS3Region")
    def uri = context.config.otherConfigs.get("anymousS3Uri")
    def expectDataCount = context.config.otherConfigs.get("anymousS3ExpectDataCount");
    //aws_credentials_provider_version
   // sql """ ADMIN SET FRONTEND CONFIG ("aws_credentials_provider_version"="v1"); """

    def result = sql """
        SELECT count(1) FROM S3 (                  
        "uri"="${uri}",
         "format" = "csv",     
          "s3.region" = "${region}",  
           "s3.credentials_provider_type"="ANONYMOUS",
           "s3.endpoint" = "https://s3.${region}.amazonaws.com", 
           "column_separator" = ","              );
        """

    def countValue = result[0][0]
    assertTrue(countValue == expectDataCount.toInteger())
    sql """ ADMIN SET FRONTEND CONFIG ("aws_credentials_provider_version"="v2"); """

     result = sql """
        SELECT count(1) FROM S3 (                  
        "uri"="${uri}",
         "format" = "csv",     
          "s3.region" = "${region}",  
           "s3.credentials_provider_type"="ANONYMOUS",
           "s3.endpoint" = "https://s3.${region}.amazonaws.com", 
           "column_separator" = ","              );
        """

     countValue = result[0][0]
    assertTrue(countValue == expectDataCount.toInteger())

    result = sql """
        SELECT count(1) FROM S3 (                  
        "uri"="${uri}",
         "format" = "csv",     
          "s3.region" = "${region}",  
           "s3.endpoint" = "https://s3.${region}.amazonaws.com", 
           "s3.credentials_provider_type"="ANONYMOUS",
           "column_separator" = ","              );
        """

    countValue = result[0][0]
    assertTrue(countValue == expectDataCount.toInteger())
}