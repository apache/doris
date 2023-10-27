/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("query48", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hms_port = context.config.otherConfigs.get("hms_port")
        String catalog_name = "test_catalog_tpcds100_parquet"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """refresh catalog ${catalog_name}"""
        sql """switch ${catalog_name}"""
        sql """use `tpcds100_parquet`"""
        sql 'set enable_nereids_planner=true'
        sql 'set enable_fallback_to_original_planner=false'
        sql 'set exec_mem_limit=21G'
        sql 'set be_number_for_test=3'
        sql 'set parallel_fragment_exec_instance_num=8; '
        sql 'set parallel_pipeline_task_num=8; '
        sql 'set forbid_unknown_col_stats=true'
        sql 'set broadcast_row_count_limit = 30000000'
        sql 'set enable_nereids_timeout = false'

        qt_ds_shape_48 '''
    explain shape plan
    select sum (ss_quantity)
 from store_sales, store, customer_demographics, customer_address, date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 1999
 and  
 (
  (
   cd_demo_sk = ss_cdemo_sk
   and 
   cd_marital_status = 'U'
   and 
   cd_education_status = 'Primary'
   and 
   ss_sales_price between 100.00 and 150.00  
   )
 or
  (
  cd_demo_sk = ss_cdemo_sk
   and 
   cd_marital_status = 'W'
   and 
   cd_education_status = 'College'
   and 
   ss_sales_price between 50.00 and 100.00   
  )
 or 
 (
  cd_demo_sk = ss_cdemo_sk
  and 
   cd_marital_status = 'D'
   and 
   cd_education_status = '2 yr Degree'
   and 
   ss_sales_price between 150.00 and 200.00  
 )
 )
 and
 (
  (
  ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('MD', 'MN', 'IA')
  and ss_net_profit between 0 and 2000  
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('VA', 'IL', 'TX')
  and ss_net_profit between 150 and 3000 
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('MI', 'WI', 'IN')
  and ss_net_profit between 50 and 25000 
  )
 )
;

    '''
    }
}
