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

/**
 *
 * Load data use restore. We should use it as long as it works.
 *
 */
suite("load") {
    def s3Region = getS3Region()
    restore {
        location "s3://${getS3BucketName()}/regression/tpcds/sf1000"
        ak "${getS3AK()}"
        sk "${getS3SK()}"
        endpoint "http://${getS3Endpoint()}"
        region "${s3Region}"
        repository "tpcds_backup"
        snapshot "tpcds_customer"
        timestamp "2022-03-31-10-16-46"
        replicationNum 1
        timeout 36000
        tables "customer"
    }

    restore {
        location "s3://${getS3BucketName()}/regression/tpcds/sf1000"
        ak "${getS3AK()}"
        sk "${getS3SK()}"
        endpoint "http://${getS3Endpoint()}"
        region "${s3Region}"
        repository "tpcds_backup"
        snapshot "tpcds"
        timestamp "2022-03-30-12-22-31"
        replicationNum 1
        timeout 72000
        tables "call_center,catalog_page,catalog_returns,catalog_sales,\
customer_address,customer_demographics,date_dim,dbgen_version,\
household_demographics,income_band,inventory,item,promotion,\
reason,ship_mode,store,store_returns,store_sales,time_dim,\
warehouse,web_page,web_returns,web_sales,web_site"
    }
}
