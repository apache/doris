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

suite("test_parquet_dict", "p0") {
    try {
        String ak = context.config.otherConfigs.get("ak")
        String sk = context.config.otherConfigs.get("sk")
        qt_s3_tvf """ SELECT * FROM FILE (
            "uri" = "https://doris-regression-hk.oss-cn-hongkong.aliyuncs.com/regression/query_p0/test_page_v2.parquet",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "format" = "parquet"
        ) where user_id='68535cc98406454081424bf8247d783d' ;
        """
    } finally {
    }
}