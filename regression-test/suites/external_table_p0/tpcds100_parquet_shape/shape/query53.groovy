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

suite("query53", "p0,external,hive,external_docker,external_docker_hive") {
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

        qt_ds_shape_53 '''
    explain shape plan
    select  * from 
(select i_manufact_id,
sum(ss_sales_price) sum_sales,
avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
from item, store_sales, date_dim, store
where ss_item_sk = i_item_sk and
ss_sold_date_sk = d_date_sk and
ss_store_sk = s_store_sk and
d_month_seq in (1200,1200+1,1200+2,1200+3,1200+4,1200+5,1200+6,1200+7,1200+8,1200+9,1200+10,1200+11) and
((i_category in ('Books','Children','Electronics') and
i_class in ('personal','portable','reference','self-help') and
i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		'exportiunivamalg #9','scholaramalgamalg #9'))
or(i_category in ('Women','Music','Men') and
i_class in ('accessories','classical','fragrances','pants') and
i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		'importoamalg #1')))
group by i_manufact_id, d_qoy ) tmp1
where case when avg_quarterly_sales > 0 
	then abs (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales 
	else null end > 0.1
order by avg_quarterly_sales,
	 sum_sales,
	 i_manufact_id
limit 100;

    '''
    }
}
