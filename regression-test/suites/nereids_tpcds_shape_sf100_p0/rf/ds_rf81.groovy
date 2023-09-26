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

suite("ds_rf81") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=3'
    sql 'set parallel_fragment_exec_instance_num=8; '
    sql 'set parallel_pipeline_task_num=8; '
    sql 'set forbid_unknown_col_stats=true'
    sql 'set broadcast_row_count_limit = 30000000'
    sql 'set enable_nereids_timeout = false'
    sql 'set enable_pipeline_engine=true'
    sql 'set enable_runtime_filter_prune=false'
    sql 'set expand_runtime_filter_by_inner_join=false'
    String stmt = '''
    explain physical plan
    with customer_total_return as
 (select cr_returning_customer_sk as ctr_customer_sk
        ,ca_state as ctr_state, 
 	sum(cr_return_amt_inc_tax) as ctr_total_return
 from catalog_returns
     ,date_dim
     ,customer_address
 where cr_returned_date_sk = d_date_sk 
   and d_year =2002
   and cr_returning_addr_sk = ca_address_sk 
 group by cr_returning_customer_sk
         ,ca_state )
  select  c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 from customer_total_return ctr1
     ,customer_address
     ,customer
 where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
 			  from customer_total_return ctr2 
                  	  where ctr1.ctr_state = ctr2.ctr_state)
       and ca_address_sk = c_current_addr_sk
       and ca_state = 'CA'
       and ctr1.ctr_customer_sk = c_customer_sk
 order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 limit 100;

    '''
    String plan = sql "${stmt}"
    log.info(plan)
    def getRuntimeFilters = { plantree ->
        {
            def lst = []
            plantree.eachMatch("RF\\d+\\[[^#]+#\\d+->\\[[^\\]]+\\]") {
                ch ->
                    {
                        lst.add(ch.replaceAll("#\\d+", ''))
                    }
            }
            return lst.join(',')
        }
    }
    
    // def outFile = "regression-test/suites/nereids_tpcds_shape_sf100_p0/ddl/rf/rf.81"
    // File file = new File(outFile)
    // file.write(getRuntimeFilters(plan))
    
     assertEquals("RF1[ca_address_sk->[cr_returning_addr_sk],RF0[d_date_sk->[cr_returned_date_sk],RF3[c_customer_sk->[ctr_customer_sk],RF2[ca_address_sk->[c_current_addr_sk]", getRuntimeFilters(plan))
}
