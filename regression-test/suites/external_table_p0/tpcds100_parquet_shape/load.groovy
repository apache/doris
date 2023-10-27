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


suite("load", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hms_port = context.config.otherConfigs.get("hms_port")
        String catalog_name = "test_catalog_tpcds100_parquet"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """refresh catalog ${catalog_name}"""
        sql """switch ${catalog_name}"""
        sql """use `tpcds100_parquet`"""
        sql """
alter table call_center modify column cc_class set stats('row_count'='30.0', 'ndv'='3.0', 'num_nulls'='0.0', 'data_size'='166.0', 'min_value'='large', 'max_value'='small')
"""
        sql """
alter table call_center modify column cc_market_manager set stats('row_count'='30.0', 'ndv'='24.0', 'num_nulls'='0.0', 'data_size'='373.0', 'min_value'='Charles Corbett', 'max_value'='Tom Root')
"""
        sql """
alter table call_center modify column cc_division_name set stats('row_count'='30.0', 'ndv'='6.0', 'num_nulls'='0.0', 'data_size'='123.0', 'min_value'='able', 'max_value'='pri')
"""
        sql """
alter table call_center modify column cc_city set stats('row_count'='30.0', 'ndv'='12.0', 'num_nulls'='0.0', 'data_size'='282.0', 'min_value'='Bethel', 'max_value'='Shady Grove')
"""
        sql """
alter table call_center modify column cc_mkt_class set stats('row_count'='30.0', 'ndv'='25.0', 'num_nulls'='0.0', 'data_size'='1031.0', 'min_value'='A bit narrow forms matter animals. Consist', 'max_value'='Yesterday new men can make moreov')
"""
        sql """
alter table call_center modify column cc_company set stats('row_count'='30.0', 'ndv'='6.0', 'num_nulls'='0.0', 'data_size'='120.0', 'min_value'='1', 'max_value'='6')
"""
        sql """
alter table call_center modify column cc_zip set stats('row_count'='30.0', 'ndv'='14.0', 'num_nulls'='0.0', 'data_size'='150.0', 'min_value'='20059', 'max_value'='75281')
"""
        sql """
alter table call_center modify column cc_street_name set stats('row_count'='30.0', 'ndv'='15.0', 'num_nulls'='0.0', 'data_size'='240.0', 'min_value'='1st ', 'max_value'='View ')
"""
        sql """
alter table call_center modify column cc_rec_end_date set stats('row_count'='30.0', 'ndv'='3.0', 'num_nulls'='15.0', 'data_size'='120.0', 'min_value'='2000-01-01', 'max_value'='2001-12-31')
"""
        sql """
alter table call_center modify column cc_closed_date_sk set stats('row_count'='30.0', 'ndv'='0.0', 'num_nulls'='30.0', 'data_size'='240.0')
"""
        sql """
alter table call_center modify column cc_division set stats('row_count'='30.0', 'ndv'='6.0', 'num_nulls'='0.0', 'data_size'='120.0', 'min_value'='1', 'max_value'='6')
"""
        sql """
alter table call_center modify column cc_hours set stats('row_count'='30.0', 'ndv'='3.0', 'num_nulls'='0.0', 'data_size'='214.0', 'min_value'='8AM-12AM', 'max_value'='8AM-8AM')
"""
        sql """
alter table call_center modify column cc_open_date_sk set stats('row_count'='30.0', 'ndv'='15.0', 'num_nulls'='0.0', 'data_size'='240.0', 'min_value'='2450794', 'max_value'='2451146')
"""
        sql """
alter table call_center modify column cc_mkt_desc set stats('row_count'='30.0', 'ndv'='22.0', 'num_nulls'='0.0', 'data_size'='1766.0', 'min_value'='As existing eyebrows miss as the matters. Realistic stories may not face almost by a ', 'max_value'='Young tests could buy comfortable, local users; o')
"""
        sql """
alter table call_center modify column cc_street_number set stats('row_count'='30.0', 'ndv'='15.0', 'num_nulls'='0.0', 'data_size'='88.0', 'min_value'='406', 'max_value'='984')
"""
        sql """
alter table call_center modify column cc_name set stats('row_count'='30.0', 'ndv'='15.0', 'num_nulls'='0.0', 'data_size'='401.0', 'min_value'='California', 'max_value'='Pacific Northwest_1')
"""
        sql """
alter table call_center modify column cc_call_center_sk set stats('row_count'='30.0', 'ndv'='30.0', 'num_nulls'='0.0', 'data_size'='240.0', 'min_value'='1', 'max_value'='30')
"""
        sql """
alter table call_center modify column cc_tax_percentage set stats('row_count'='30.0', 'ndv'='10.0', 'num_nulls'='0.0', 'data_size'='120.0', 'min_value'='0.00', 'max_value'='0.12')
"""
        sql """
alter table call_center modify column cc_call_center_id set stats('row_count'='30.0', 'ndv'='15.0', 'num_nulls'='0.0', 'data_size'='480.0', 'min_value'='AAAAAAAAABAAAAAA', 'max_value'='AAAAAAAAOAAAAAAA')
"""
        sql """
alter table call_center modify column cc_sq_ft set stats('row_count'='30.0', 'ndv'='22.0', 'num_nulls'='0.0', 'data_size'='120.0', 'min_value'='1670015', 'max_value'='31896816')
"""
        sql """
alter table call_center modify column cc_country set stats('row_count'='30.0', 'ndv'='1.0', 'num_nulls'='0.0', 'data_size'='390.0', 'min_value'='United States', 'max_value'='United States')
"""
        sql """
alter table call_center modify column cc_company_name set stats('row_count'='30.0', 'ndv'='6.0', 'num_nulls'='0.0', 'data_size'='110.0', 'min_value'='able', 'max_value'='pri')
"""
        sql """
alter table call_center modify column cc_mkt_id set stats('row_count'='30.0', 'ndv'='6.0', 'num_nulls'='0.0', 'data_size'='120.0', 'min_value'='1', 'max_value'='6')
"""
        sql """
alter table call_center modify column cc_state set stats('row_count'='30.0', 'ndv'='8.0', 'num_nulls'='0.0', 'data_size'='60.0', 'min_value'='AL', 'max_value'='TN')
"""
        sql """
alter table call_center modify column cc_county set stats('row_count'='30.0', 'ndv'='8.0', 'num_nulls'='0.0', 'data_size'='423.0', 'min_value'='Barrow County', 'max_value'='Ziebach County')
"""
        sql """
alter table call_center modify column cc_rec_start_date set stats('row_count'='30.0', 'ndv'='4.0', 'num_nulls'='0.0', 'data_size'='120.0', 'min_value'='1998-01-01', 'max_value'='2002-01-01')
"""
        sql """
alter table call_center modify column cc_employees set stats('row_count'='30.0', 'ndv'='22.0', 'num_nulls'='0.0', 'data_size'='120.0', 'min_value'='2935', 'max_value'='69020')
"""
        sql """
alter table call_center modify column cc_manager set stats('row_count'='30.0', 'ndv'='22.0', 'num_nulls'='0.0', 'data_size'='368.0', 'min_value'='Alden Snyder', 'max_value'='Wayne Ray')
"""
        sql """
alter table call_center modify column cc_suite_number set stats('row_count'='30.0', 'ndv'='14.0', 'num_nulls'='0.0', 'data_size'='234.0', 'min_value'='Suite 0', 'max_value'='Suite W')
"""
        sql """
alter table call_center modify column cc_street_type set stats('row_count'='30.0', 'ndv'='9.0', 'num_nulls'='0.0', 'data_size'='140.0', 'min_value'='Avenue', 'max_value'='Way')
"""
        sql """
alter table call_center modify column cc_gmt_offset set stats('row_count'='30.0', 'ndv'='2.0', 'num_nulls'='0.0', 'data_size'='120.0', 'min_value'='-6.00', 'max_value'='-5.00')
"""
        sql """
alter table catalog_page modify column cp_catalog_page_number set stats('row_count'='20400.0', 'ndv'='189.0', 'num_nulls'='208.0', 'data_size'='81600.0', 'min_value'='1', 'max_value'='188')
"""
        sql """
alter table catalog_page modify column cp_description set stats('row_count'='20400.0', 'ndv'='20498.0', 'num_nulls'='190.0', 'data_size'='1507423.0', 'min_value'='A bit asleep rooms cannot feel short dry secondary leads. Ab', 'max_value'='Youngsters should get very. Bad, necessary years must pick telecommunications. Co')
"""
        sql """
alter table catalog_page modify column cp_department set stats('row_count'='20400.0', 'ndv'='1.0', 'num_nulls'='205.0', 'data_size'='201950.0', 'min_value'='DEPARTMENT', 'max_value'='DEPARTMENT')
"""
        sql """
alter table catalog_page modify column cp_type set stats('row_count'='20400.0', 'ndv'='3.0', 'num_nulls'='197.0', 'data_size'='155039.0', 'min_value'='bi-annual', 'max_value'='quarterly')
"""
        sql """
alter table catalog_page modify column cp_catalog_page_sk set stats('row_count'='20400.0', 'ndv'='20554.0', 'num_nulls'='0.0', 'data_size'='163200.0', 'min_value'='1', 'max_value'='20400')
"""
        sql """
alter table catalog_page modify column cp_catalog_page_id set stats('row_count'='20400.0', 'ndv'='20341.0', 'num_nulls'='0.0', 'data_size'='326400.0', 'min_value'='AAAAAAAAAAABAAAA', 'max_value'='AAAAAAAAPPPDAAAA')
"""
        sql """
alter table catalog_page modify column cp_catalog_number set stats('row_count'='20400.0', 'ndv'='109.0', 'num_nulls'='197.0', 'data_size'='81600.0', 'min_value'='1', 'max_value'='109')
"""
        sql """
alter table catalog_page modify column cp_end_date_sk set stats('row_count'='20400.0', 'ndv'='97.0', 'num_nulls'='206.0', 'data_size'='163200.0', 'min_value'='2450844', 'max_value'='2453186')
"""
        sql """
alter table catalog_page modify column cp_start_date_sk set stats('row_count'='20400.0', 'ndv'='91.0', 'num_nulls'='196.0', 'data_size'='163200.0', 'min_value'='2450815', 'max_value'='2453005')
"""
        sql """
alter table catalog_returns modify column cr_refunded_cash set stats('row_count'='1.4404374E7', 'ndv'='633778.0', 'num_nulls'='287638.0', 'data_size'='5.7617496E7', 'min_value'='0.00', 'max_value'='24544.84')
"""
        sql """
alter table catalog_returns modify column cr_warehouse_sk set stats('row_count'='1.4404374E7', 'ndv'='15.0', 'num_nulls'='288581.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='15')
"""
        sql """
alter table catalog_returns modify column cr_return_quantity set stats('row_count'='1.4404374E7', 'ndv'='100.0', 'num_nulls'='287844.0', 'data_size'='5.7617496E7', 'min_value'='1', 'max_value'='100')
"""
        sql """
alter table catalog_returns modify column cr_returning_addr_sk set stats('row_count'='1.4404374E7', 'ndv'='1000237.0', 'num_nulls'='288203.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='1000000')
"""
        sql """
alter table catalog_returns modify column cr_reason_sk set stats('row_count'='1.4404374E7', 'ndv'='55.0', 'num_nulls'='287890.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='55')
"""
        sql """
alter table catalog_returns modify column cr_return_amount set stats('row_count'='1.4404374E7', 'ndv'='610930.0', 'num_nulls'='288408.0', 'data_size'='5.7617496E7', 'min_value'='0.00', 'max_value'='28778.31')
"""
        sql """
alter table catalog_returns modify column cr_store_credit set stats('row_count'='1.4404374E7', 'ndv'='439743.0', 'num_nulls'='288118.0', 'data_size'='5.7617496E7', 'min_value'='0.00', 'max_value'='22167.49')
"""
        sql """
alter table catalog_returns modify column cr_fee set stats('row_count'='1.4404374E7', 'ndv'='9958.0', 'num_nulls'='288038.0', 'data_size'='5.7617496E7', 'min_value'='0.50', 'max_value'='100.00')
"""
        sql """
alter table catalog_returns modify column cr_net_loss set stats('row_count'='1.4404374E7', 'ndv'='600646.0', 'num_nulls'='287954.0', 'data_size'='5.7617496E7', 'min_value'='0.50', 'max_value'='15781.83')
"""
        sql """
alter table catalog_returns modify column cr_return_amt_inc_tax set stats('row_count'='1.4404374E7', 'ndv'='934014.0', 'num_nulls'='288246.0', 'data_size'='5.7617496E7', 'min_value'='0.00', 'max_value'='29353.87')
"""
        sql """
alter table catalog_returns modify column cr_catalog_page_sk set stats('row_count'='1.4404374E7', 'ndv'='11515.0', 'num_nulls'='288041.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='17108')
"""
        sql """
alter table catalog_returns modify column cr_returned_date_sk set stats('row_count'='1.4404374E7', 'ndv'='2100.0', 'num_nulls'='0.0', 'data_size'='1.15234992E8', 'min_value'='2450821', 'max_value'='2452921')
"""
        sql """
alter table catalog_returns modify column cr_returning_hdemo_sk set stats('row_count'='1.4404374E7', 'ndv'='7251.0', 'num_nulls'='288369.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='7200')
"""
        sql """
alter table catalog_returns modify column cr_item_sk set stats('row_count'='1.4404374E7', 'ndv'='205012.0', 'num_nulls'='0.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='204000')
"""
        sql """
alter table catalog_returns modify column cr_refunded_customer_sk set stats('row_count'='1.4404374E7', 'ndv'='1977657.0', 'num_nulls'='287207.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='2000000')
"""
        sql """
alter table catalog_returns modify column cr_return_ship_cost set stats('row_count'='1.4404374E7', 'ndv'='354439.0', 'num_nulls'='287952.0', 'data_size'='5.7617496E7', 'min_value'='0.00', 'max_value'='14130.96')
"""
        sql """
alter table catalog_returns modify column cr_refunded_cdemo_sk set stats('row_count'='1.4404374E7', 'ndv'='1900770.0', 'num_nulls'='287556.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='1920800')
"""
        sql """
alter table catalog_returns modify column cr_returning_cdemo_sk set stats('row_count'='1.4404374E7', 'ndv'='1913762.0', 'num_nulls'='288128.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='1920800')
"""
        sql """
alter table catalog_returns modify column cr_call_center_sk set stats('row_count'='1.4404374E7', 'ndv'='30.0', 'num_nulls'='288179.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='30')
"""
        sql """
alter table catalog_returns modify column cr_refunded_hdemo_sk set stats('row_count'='1.4404374E7', 'ndv'='7251.0', 'num_nulls'='288053.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='7200')
"""
        sql """
alter table catalog_returns modify column cr_order_number set stats('row_count'='1.4404374E7', 'ndv'='9425725.0', 'num_nulls'='0.0', 'data_size'='1.15234992E8', 'min_value'='2', 'max_value'='16000000')
"""
        sql """
alter table catalog_returns modify column cr_return_tax set stats('row_count'='1.4404374E7', 'ndv'='106530.0', 'num_nulls'='288599.0', 'data_size'='5.7617496E7', 'min_value'='0.00', 'max_value'='2390.75')
"""
        sql """
alter table catalog_returns modify column cr_reversed_charge set stats('row_count'='1.4404374E7', 'ndv'='443607.0', 'num_nulls'='288476.0', 'data_size'='5.7617496E7', 'min_value'='0.00', 'max_value'='23801.24')
"""
        sql """
alter table catalog_returns modify column cr_returned_time_sk set stats('row_count'='1.4404374E7', 'ndv'='87677.0', 'num_nulls'='0.0', 'data_size'='1.15234992E8', 'min_value'='0', 'max_value'='86399')
"""
        sql """
alter table catalog_returns modify column cr_ship_mode_sk set stats('row_count'='1.4404374E7', 'ndv'='20.0', 'num_nulls'='287768.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='20')
"""
        sql """
alter table catalog_returns modify column cr_returning_customer_sk set stats('row_count'='1.4404374E7', 'ndv'='1991754.0', 'num_nulls'='287581.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='2000000')
"""
        sql """
alter table catalog_returns modify column cr_refunded_addr_sk set stats('row_count'='1.4404374E7', 'ndv'='1000237.0', 'num_nulls'='287752.0', 'data_size'='1.15234992E8', 'min_value'='1', 'max_value'='1000000')
"""
        sql """
alter table catalog_sales modify column cs_warehouse_sk set stats('row_count'='1.43997065E8', 'ndv'='15.0', 'num_nulls'='719624.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='15')
"""
        sql """
alter table catalog_sales modify column cs_ship_date_sk set stats('row_count'='1.43997065E8', 'ndv'='1933.0', 'num_nulls'='719625.0', 'data_size'='1.15197652E9', 'min_value'='2450817', 'max_value'='2452744')
"""
        sql """
alter table catalog_sales modify column cs_ext_list_price set stats('row_count'='1.43997065E8', 'ndv'='1147040.0', 'num_nulls'='719642.0', 'data_size'='5.7598826E8', 'min_value'='1.00', 'max_value'='29997.00')
"""
        sql """
alter table catalog_sales modify column cs_quantity set stats('row_count'='1.43997065E8', 'ndv'='100.0', 'num_nulls'='720147.0', 'data_size'='5.7598826E8', 'min_value'='1', 'max_value'='100')
"""
        sql """
alter table catalog_sales modify column cs_net_paid_inc_tax set stats('row_count'='1.43997065E8', 'ndv'='1996833.0', 'num_nulls'='719354.0', 'data_size'='5.7598826E8', 'min_value'='0.00', 'max_value'='31745.52')
"""
        sql """
alter table catalog_sales modify column cs_sold_time_sk set stats('row_count'='1.43997065E8', 'ndv'='87677.0', 'num_nulls'='720917.0', 'data_size'='1.15197652E9', 'min_value'='0', 'max_value'='86399')
"""
        sql """
alter table catalog_sales modify column cs_promo_sk set stats('row_count'='1.43997065E8', 'ndv'='986.0', 'num_nulls'='720194.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='1000')
"""
        sql """
alter table catalog_sales modify column cs_list_price set stats('row_count'='1.43997065E8', 'ndv'='29396.0', 'num_nulls'='720328.0', 'data_size'='5.7598826E8', 'min_value'='1.00', 'max_value'='300.00')
"""
        sql """
alter table catalog_sales modify column cs_ext_ship_cost set stats('row_count'='1.43997065E8', 'ndv'='538649.0', 'num_nulls'='719848.0', 'data_size'='5.7598826E8', 'min_value'='0.00', 'max_value'='14896.00')
"""
        sql """
alter table catalog_sales modify column cs_net_paid set stats('row_count'='1.43997065E8', 'ndv'='1445396.0', 'num_nulls'='719706.0', 'data_size'='5.7598826E8', 'min_value'='0.00', 'max_value'='29760.00')
"""
        sql """
alter table catalog_sales modify column cs_sold_date_sk set stats('row_count'='1.43997065E8', 'ndv'='1836.0', 'num_nulls'='78387.0', 'data_size'='1.151349423E9', 'min_value'='2450815', 'max_value'='2452654')
"""
        sql """
alter table catalog_sales modify column cs_ext_discount_amt set stats('row_count'='1.43997065E8', 'ndv'='1015004.0', 'num_nulls'='719820.0', 'data_size'='5.7598826E8', 'min_value'='0.00', 'max_value'='29765.00')
"""
        sql """
alter table catalog_sales modify column cs_ship_addr_sk set stats('row_count'='1.43997065E8', 'ndv'='1000237.0', 'num_nulls'='718680.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='1000000')
"""
        sql """
alter table catalog_sales modify column cs_catalog_page_sk set stats('row_count'='1.43997065E8', 'ndv'='11515.0', 'num_nulls'='719180.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='17108')
"""
        sql """
alter table catalog_sales modify column cs_ext_tax set stats('row_count'='1.43997065E8', 'ndv'='187782.0', 'num_nulls'='719627.0', 'data_size'='5.7598826E8', 'min_value'='0.00', 'max_value'='2619.36')
"""
        sql """
alter table catalog_sales modify column cs_net_profit set stats('row_count'='1.43997065E8', 'ndv'='1711952.0', 'num_nulls'='0.0', 'data_size'='5.7598826E8', 'min_value'='-10000.00', 'max_value'='19840.00')
"""
        sql """
alter table catalog_sales modify column cs_bill_cdemo_sk set stats('row_count'='1.43997065E8', 'ndv'='1915709.0', 'num_nulls'='720208.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='1920800')
"""
        sql """
alter table catalog_sales modify column cs_item_sk set stats('row_count'='1.43997065E8', 'ndv'='205012.0', 'num_nulls'='0.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='204000')
"""
        sql """
alter table catalog_sales modify column cs_bill_hdemo_sk set stats('row_count'='1.43997065E8', 'ndv'='7251.0', 'num_nulls'='719849.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='7200')
"""
        sql """
alter table catalog_sales modify column cs_net_paid_inc_ship set stats('row_count'='1.43997065E8', 'ndv'='1962646.0', 'num_nulls'='0.0', 'data_size'='5.7598826E8', 'min_value'='0.00', 'max_value'='43725.00')
"""
        sql """
alter table catalog_sales modify column cs_coupon_amt set stats('row_count'='1.43997065E8', 'ndv'='1103106.0', 'num_nulls'='719631.0', 'data_size'='5.7598826E8', 'min_value'='0.00', 'max_value'='28422.94')
"""
        sql """
alter table catalog_sales modify column cs_net_paid_inc_ship_tax set stats('row_count'='1.43997065E8', 'ndv'='2637498.0', 'num_nulls'='0.0', 'data_size'='5.7598826E8', 'min_value'='0.00', 'max_value'='45460.80')
"""
        sql """
alter table catalog_sales modify column cs_order_number set stats('row_count'='1.43997065E8', 'ndv'='1.605073E7', 'num_nulls'='0.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='16000000')
"""
        sql """
alter table catalog_sales modify column cs_ext_wholesale_cost set stats('row_count'='1.43997065E8', 'ndv'='393180.0', 'num_nulls'='719924.0', 'data_size'='5.7598826E8', 'min_value'='1.00', 'max_value'='10000.00')
"""
        sql """
alter table catalog_sales modify column cs_ship_customer_sk set stats('row_count'='1.43997065E8', 'ndv'='1993190.0', 'num_nulls'='720582.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='2000000')
"""
        sql """
alter table catalog_sales modify column cs_ship_mode_sk set stats('row_count'='1.43997065E8', 'ndv'='20.0', 'num_nulls'='720146.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='20')
"""
        sql """
alter table catalog_sales modify column cs_ext_sales_price set stats('row_count'='1.43997065E8', 'ndv'='1014482.0', 'num_nulls'='719228.0', 'data_size'='5.7598826E8', 'min_value'='0.00', 'max_value'='29808.00')
"""
        sql """
alter table catalog_sales modify column cs_bill_addr_sk set stats('row_count'='1.43997065E8', 'ndv'='1000237.0', 'num_nulls'='718886.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='1000000')
"""
        sql """
alter table catalog_sales modify column cs_ship_cdemo_sk set stats('row_count'='1.43997065E8', 'ndv'='1916125.0', 'num_nulls'='720292.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='1920800')
"""
        sql """
alter table catalog_sales modify column cs_ship_hdemo_sk set stats('row_count'='1.43997065E8', 'ndv'='7251.0', 'num_nulls'='720450.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='7200')
"""
        sql """
alter table catalog_sales modify column cs_wholesale_cost set stats('row_count'='1.43997065E8', 'ndv'='9905.0', 'num_nulls'='721114.0', 'data_size'='5.7598826E8', 'min_value'='1.00', 'max_value'='100.00')
"""
        sql """
alter table catalog_sales modify column cs_sales_price set stats('row_count'='1.43997065E8', 'ndv'='29157.0', 'num_nulls'='719781.0', 'data_size'='5.7598826E8', 'min_value'='0.00', 'max_value'='300.00')
"""
        sql """
alter table catalog_sales modify column cs_call_center_sk set stats('row_count'='1.43997065E8', 'ndv'='30.0', 'num_nulls'='719767.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='30')
"""
        sql """
alter table catalog_sales modify column cs_bill_customer_sk set stats('row_count'='1.43997065E8', 'ndv'='1993691.0', 'num_nulls'='719473.0', 'data_size'='1.15197652E9', 'min_value'='1', 'max_value'='2000000')
"""
        sql """
alter table customer modify column c_salutation set stats('row_count'='2000000.0', 'ndv'='6.0', 'num_nulls'='69840.0', 'data_size'='6257882.0', 'min_value'='Dr.', 'max_value'='Sir')
"""
        sql """
alter table customer modify column c_preferred_cust_flag set stats('row_count'='2000000.0', 'ndv'='2.0', 'num_nulls'='69778.0', 'data_size'='1930222.0', 'min_value'='N', 'max_value'='Y')
"""
        sql """
alter table customer modify column c_first_sales_date_sk set stats('row_count'='2000000.0', 'ndv'='3644.0', 'num_nulls'='69950.0', 'data_size'='1.6E7', 'min_value'='2448998', 'max_value'='2452648')
"""
        sql """
alter table customer modify column c_customer_sk set stats('row_count'='2000000.0', 'ndv'='1994393.0', 'num_nulls'='0.0', 'data_size'='1.6E7', 'min_value'='1', 'max_value'='2000000')
"""
        sql """
alter table customer modify column c_login set stats('row_count'='2000000.0', 'ndv'='0.0', 'num_nulls'='2000000.0', 'data_size'='0.0')
"""
        sql """
alter table customer modify column c_current_cdemo_sk set stats('row_count'='2000000.0', 'ndv'='1221921.0', 'num_nulls'='69943.0', 'data_size'='1.6E7', 'min_value'='1', 'max_value'='1920798')
"""
        sql """
alter table customer modify column c_first_name set stats('row_count'='2000000.0', 'ndv'='5140.0', 'num_nulls'='69769.0', 'data_size'='1.1267996E7', 'min_value'='Aaron', 'max_value'='Zulma')
"""
        sql """
alter table customer modify column c_current_hdemo_sk set stats('row_count'='2000000.0', 'ndv'='7251.0', 'num_nulls'='69657.0', 'data_size'='1.6E7', 'min_value'='1', 'max_value'='7200')
"""
        sql """
alter table customer modify column c_current_addr_sk set stats('row_count'='2000000.0', 'ndv'='866672.0', 'num_nulls'='0.0', 'data_size'='1.6E7', 'min_value'='1', 'max_value'='1000000')
"""
        sql """
alter table customer modify column c_last_name set stats('row_count'='2000000.0', 'ndv'='4989.0', 'num_nulls'='70098.0', 'data_size'='1.1833714E7', 'min_value'='Aaron', 'max_value'='Zuniga')
"""
        sql """
alter table customer modify column c_customer_id set stats('row_count'='2000000.0', 'ndv'='1994557.0', 'num_nulls'='0.0', 'data_size'='3.2E7', 'min_value'='AAAAAAAAAAAAABAA', 'max_value'='AAAAAAAAPPPPPAAA')
"""
        sql """
alter table customer modify column c_last_review_date_sk set stats('row_count'='2000000.0', 'ndv'='366.0', 'num_nulls'='70102.0', 'data_size'='1.6E7', 'min_value'='2452283', 'max_value'='2452648')
"""
        sql """
alter table customer modify column c_birth_month set stats('row_count'='2000000.0', 'ndv'='12.0', 'num_nulls'='69896.0', 'data_size'='8000000.0', 'min_value'='1', 'max_value'='12')
"""
        sql """
alter table customer modify column c_birth_country set stats('row_count'='2000000.0', 'ndv'='210.0', 'num_nulls'='69626.0', 'data_size'='1.6806197E7', 'min_value'='AFGHANISTAN', 'max_value'='ZIMBABWE')
"""
        sql """
alter table customer modify column c_birth_year set stats('row_count'='2000000.0', 'ndv'='69.0', 'num_nulls'='69986.0', 'data_size'='8000000.0', 'min_value'='1924', 'max_value'='1992')
"""
        sql """
alter table customer modify column c_birth_day set stats('row_count'='2000000.0', 'ndv'='31.0', 'num_nulls'='70166.0', 'data_size'='8000000.0', 'min_value'='1', 'max_value'='31')
"""
        sql """
alter table customer modify column c_first_shipto_date_sk set stats('row_count'='2000000.0', 'ndv'='3644.0', 'num_nulls'='70080.0', 'data_size'='1.6E7', 'min_value'='2449028', 'max_value'='2452678')
"""
        sql """
alter table customer modify column c_email_address set stats('row_count'='2000000.0', 'ndv'='1936613.0', 'num_nulls'='70200.0', 'data_size'='5.3014147E7', 'min_value'='Aaron.Aaron@imee5iHArPCAQ.org', 'max_value'='Zulma.Wright@AqokXsju9f2yj.org')
"""
        sql """
alter table customer_address modify column ca_street_name set stats('row_count'='1000000.0', 'ndv'='8155.0', 'num_nulls'='30178.0', 'data_size'='8445649.0', 'min_value'='10th ', 'max_value'='Woodland Woodland')
"""
        sql """
alter table customer_address modify column ca_suite_number set stats('row_count'='1000000.0', 'ndv'='75.0', 'num_nulls'='30047.0', 'data_size'='7652799.0', 'min_value'='Suite 0', 'max_value'='Suite Y')
"""
        sql """
alter table customer_address modify column ca_state set stats('row_count'='1000000.0', 'ndv'='51.0', 'num_nulls'='30124.0', 'data_size'='1939752.0', 'min_value'='AK', 'max_value'='WY')
"""
        sql """
alter table customer_address modify column ca_location_type set stats('row_count'='1000000.0', 'ndv'='3.0', 'num_nulls'='30172.0', 'data_size'='8728128.0', 'min_value'='apartment', 'max_value'='single family')
"""
        sql """
alter table customer_address modify column ca_address_sk set stats('row_count'='1000000.0', 'ndv'='1000237.0', 'num_nulls'='0.0', 'data_size'='8000000.0', 'min_value'='1', 'max_value'='1000000')
"""
        sql """
alter table customer_address modify column ca_country set stats('row_count'='1000000.0', 'ndv'='1.0', 'num_nulls'='30097.0', 'data_size'='1.2608739E7', 'min_value'='United States', 'max_value'='United States')
"""
        sql """
alter table customer_address modify column ca_address_id set stats('row_count'='1000000.0', 'ndv'='999950.0', 'num_nulls'='0.0', 'data_size'='1.6E7', 'min_value'='AAAAAAAAAAAABAAA', 'max_value'='AAAAAAAAPPPPOAAA')
"""
        sql """
alter table customer_address modify column ca_county set stats('row_count'='1000000.0', 'ndv'='1824.0', 'num_nulls'='30028.0', 'data_size'='1.3540273E7', 'min_value'='Abbeville County', 'max_value'='Ziebach County')
"""
        sql """
alter table customer_address modify column ca_street_number set stats('row_count'='1000000.0', 'ndv'='1001.0', 'num_nulls'='30226.0', 'data_size'='2805540.0', 'min_value'='1', 'max_value'='999')
"""
        sql """
alter table customer_address modify column ca_zip set stats('row_count'='1000000.0', 'ndv'='7733.0', 'num_nulls'='30370.0', 'data_size'='4848150.0', 'min_value'='00601', 'max_value'='99981')
"""
        sql """
alter table customer_address modify column ca_city set stats('row_count'='1000000.0', 'ndv'='975.0', 'num_nulls'='30183.0', 'data_size'='8681993.0', 'min_value'='Aberdeen', 'max_value'='Zion')
"""
        sql """
alter table customer_address modify column ca_street_type set stats('row_count'='1000000.0', 'ndv'='20.0', 'num_nulls'='30124.0', 'data_size'='4073296.0', 'min_value'='Ave', 'max_value'='Wy')
"""
        sql """
alter table customer_address modify column ca_gmt_offset set stats('row_count'='1000000.0', 'ndv'='6.0', 'num_nulls'='30131.0', 'data_size'='4000000.0', 'min_value'='-10.00', 'max_value'='-5.00')
"""
        sql """
alter table customer_demographics modify column cd_dep_employed_count set stats('row_count'='1920800.0', 'ndv'='7.0', 'num_nulls'='0.0', 'data_size'='7683200.0', 'min_value'='0', 'max_value'='6')
"""
        sql """
alter table customer_demographics modify column cd_demo_sk set stats('row_count'='1920800.0', 'ndv'='1916366.0', 'num_nulls'='0.0', 'data_size'='1.53664E7', 'min_value'='1', 'max_value'='1920800')
"""
        sql """
alter table customer_demographics modify column cd_education_status set stats('row_count'='1920800.0', 'ndv'='7.0', 'num_nulls'='0.0', 'data_size'='1.83848E7', 'min_value'='2 yr Degree', 'max_value'='Unknown')
"""
        sql """
alter table customer_demographics modify column cd_credit_rating set stats('row_count'='1920800.0', 'ndv'='4.0', 'num_nulls'='0.0', 'data_size'='1.34456E7', 'min_value'='Good', 'max_value'='Unknown')
"""
        sql """
alter table customer_demographics modify column cd_dep_count set stats('row_count'='1920800.0', 'ndv'='7.0', 'num_nulls'='0.0', 'data_size'='7683200.0', 'min_value'='0', 'max_value'='6')
"""
        sql """
alter table customer_demographics modify column cd_purchase_estimate set stats('row_count'='1920800.0', 'ndv'='20.0', 'num_nulls'='0.0', 'data_size'='7683200.0', 'min_value'='500', 'max_value'='10000')
"""
        sql """
alter table customer_demographics modify column cd_marital_status set stats('row_count'='1920800.0', 'ndv'='5.0', 'num_nulls'='0.0', 'data_size'='1920800.0', 'min_value'='D', 'max_value'='W')
"""
        sql """
alter table customer_demographics modify column cd_dep_college_count set stats('row_count'='1920800.0', 'ndv'='7.0', 'num_nulls'='0.0', 'data_size'='7683200.0', 'min_value'='0', 'max_value'='6')
"""
        sql """
alter table customer_demographics modify column cd_gender set stats('row_count'='1920800.0', 'ndv'='2.0', 'num_nulls'='0.0', 'data_size'='1920800.0', 'min_value'='F', 'max_value'='M')
"""
        sql """
alter table date_dim modify column d_quarter_seq set stats('row_count'='73049.0', 'ndv'='801.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='1', 'max_value'='801')
"""
        sql """
alter table date_dim modify column d_last_dom set stats('row_count'='73049.0', 'ndv'='2419.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='2415020', 'max_value'='2488372')
"""
        sql """
alter table date_dim modify column d_dom set stats('row_count'='73049.0', 'ndv'='31.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='1', 'max_value'='31')
"""
        sql """
alter table date_dim modify column d_current_day set stats('row_count'='73049.0', 'ndv'='1.0', 'num_nulls'='0.0', 'data_size'='73049.0', 'min_value'='N', 'max_value'='N')
"""
        sql """
alter table date_dim modify column d_week_seq set stats('row_count'='73049.0', 'ndv'='10448.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='1', 'max_value'='10436')
"""
        sql """
alter table date_dim modify column d_current_month set stats('row_count'='73049.0', 'ndv'='2.0', 'num_nulls'='0.0', 'data_size'='73049.0', 'min_value'='N', 'max_value'='Y')
"""
        sql """
alter table date_dim modify column d_first_dom set stats('row_count'='73049.0', 'ndv'='2410.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='2415021', 'max_value'='2488070')
"""
        sql """
alter table date_dim modify column d_moy set stats('row_count'='73049.0', 'ndv'='12.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='1', 'max_value'='12')
"""
        sql """
alter table date_dim modify column d_dow set stats('row_count'='73049.0', 'ndv'='7.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='0', 'max_value'='6')
"""
        sql """
alter table date_dim modify column d_holiday set stats('row_count'='73049.0', 'ndv'='2.0', 'num_nulls'='0.0', 'data_size'='73049.0', 'min_value'='N', 'max_value'='Y')
"""
        sql """
alter table date_dim modify column d_current_quarter set stats('row_count'='73049.0', 'ndv'='2.0', 'num_nulls'='0.0', 'data_size'='73049.0', 'min_value'='N', 'max_value'='Y')
"""
        sql """
alter table date_dim modify column d_following_holiday set stats('row_count'='73049.0', 'ndv'='2.0', 'num_nulls'='0.0', 'data_size'='73049.0', 'min_value'='N', 'max_value'='Y')
"""
        sql """
alter table date_dim modify column d_date_id set stats('row_count'='73049.0', 'ndv'='72907.0', 'num_nulls'='0.0', 'data_size'='1168784.0', 'min_value'='AAAAAAAAAAAAFCAA', 'max_value'='AAAAAAAAPPPPECAA')
"""
        sql """
alter table date_dim modify column d_fy_week_seq set stats('row_count'='73049.0', 'ndv'='10448.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='1', 'max_value'='10436')
"""
        sql """
alter table date_dim modify column d_current_week set stats('row_count'='73049.0', 'ndv'='1.0', 'num_nulls'='0.0', 'data_size'='73049.0', 'min_value'='N', 'max_value'='N')
"""
        sql """
alter table date_dim modify column d_day_name set stats('row_count'='73049.0', 'ndv'='7.0', 'num_nulls'='0.0', 'data_size'='521779.0', 'min_value'='Friday', 'max_value'='Wednesday')
"""
        sql """
alter table date_dim modify column d_same_day_ly set stats('row_count'='73049.0', 'ndv'='72450.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='2414657', 'max_value'='2487705')
"""
        sql """
alter table date_dim modify column d_date set stats('row_count'='73049.0', 'ndv'='73250.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='1900-01-02', 'max_value'='2100-01-01')
"""
        sql """
alter table date_dim modify column d_month_seq set stats('row_count'='73049.0', 'ndv'='2398.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='0', 'max_value'='2400')
"""
        sql """
alter table date_dim modify column d_weekend set stats('row_count'='73049.0', 'ndv'='2.0', 'num_nulls'='0.0', 'data_size'='73049.0', 'min_value'='N', 'max_value'='Y')
"""
        sql """
alter table date_dim modify column d_quarter_name set stats('row_count'='73049.0', 'ndv'='799.0', 'num_nulls'='0.0', 'data_size'='438294.0', 'min_value'='1900Q1', 'max_value'='2100Q1')
"""
        sql """
alter table date_dim modify column d_fy_quarter_seq set stats('row_count'='73049.0', 'ndv'='801.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='1', 'max_value'='801')
"""
        sql """
alter table date_dim modify column d_current_year set stats('row_count'='73049.0', 'ndv'='2.0', 'num_nulls'='0.0', 'data_size'='73049.0', 'min_value'='N', 'max_value'='Y')
"""
        sql """
alter table date_dim modify column d_qoy set stats('row_count'='73049.0', 'ndv'='4.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='1', 'max_value'='4')
"""
        sql """
alter table date_dim modify column d_same_day_lq set stats('row_count'='73049.0', 'ndv'='72231.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='2414930', 'max_value'='2487978')
"""
        sql """
alter table date_dim modify column d_year set stats('row_count'='73049.0', 'ndv'='202.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='1900', 'max_value'='2100')
"""
        sql """
alter table date_dim modify column d_fy_year set stats('row_count'='73049.0', 'ndv'='202.0', 'num_nulls'='0.0', 'data_size'='292196.0', 'min_value'='1900', 'max_value'='2100')
"""
        sql """
alter table date_dim modify column d_date_sk set stats('row_count'='73049.0', 'ndv'='73042.0', 'num_nulls'='0.0', 'data_size'='584392.0', 'min_value'='2415022', 'max_value'='2488070')
"""
        sql """
alter table household_demographics modify column hd_buy_potential set stats('row_count'='7200.0', 'ndv'='6.0', 'num_nulls'='0.0', 'data_size'='54000.0', 'min_value'='0-500', 'max_value'='Unknown')
"""
        sql """
alter table household_demographics modify column hd_income_band_sk set stats('row_count'='7200.0', 'ndv'='20.0', 'num_nulls'='0.0', 'data_size'='57600.0', 'min_value'='1', 'max_value'='20')
"""
        sql """
alter table household_demographics modify column hd_demo_sk set stats('row_count'='7200.0', 'ndv'='7251.0', 'num_nulls'='0.0', 'data_size'='57600.0', 'min_value'='1', 'max_value'='7200')
"""
        sql """
alter table household_demographics modify column hd_dep_count set stats('row_count'='7200.0', 'ndv'='10.0', 'num_nulls'='0.0', 'data_size'='28800.0', 'min_value'='0', 'max_value'='9')
"""
        sql """
alter table household_demographics modify column hd_vehicle_count set stats('row_count'='7200.0', 'ndv'='6.0', 'num_nulls'='0.0', 'data_size'='28800.0', 'min_value'='-1', 'max_value'='4')
"""
        sql """
alter table income_band modify column ib_income_band_sk set stats('row_count'='20.0', 'ndv'='20.0', 'num_nulls'='0.0', 'data_size'='160.0', 'min_value'='1', 'max_value'='20')
"""
        sql """
alter table income_band modify column ib_lower_bound set stats('row_count'='20.0', 'ndv'='20.0', 'num_nulls'='0.0', 'data_size'='80.0', 'min_value'='0', 'max_value'='190001')
"""
        sql """
alter table income_band modify column ib_upper_bound set stats('row_count'='20.0', 'ndv'='20.0', 'num_nulls'='0.0', 'data_size'='80.0', 'min_value'='10000', 'max_value'='200000')
"""
        sql """
alter table inventory modify column inv_date_sk set stats('row_count'='3.9933E8', 'ndv'='261.0', 'num_nulls'='0.0', 'data_size'='3.19464E9', 'min_value'='2450815', 'max_value'='2452635')
"""
        sql """
alter table inventory modify column inv_quantity_on_hand set stats('row_count'='3.9933E8', 'ndv'='1006.0', 'num_nulls'='1.9969395E7', 'data_size'='1.59732E9', 'min_value'='0', 'max_value'='1000')
"""
        sql """
alter table inventory modify column inv_warehouse_sk set stats('row_count'='3.9933E8', 'ndv'='15.0', 'num_nulls'='0.0', 'data_size'='3.19464E9', 'min_value'='1', 'max_value'='15')
"""
        sql """
alter table inventory modify column inv_item_sk set stats('row_count'='3.9933E8', 'ndv'='205012.0', 'num_nulls'='0.0', 'data_size'='3.19464E9', 'min_value'='1', 'max_value'='204000')
"""
        sql """
alter table item modify column i_item_desc set stats('row_count'='204000.0', 'ndv'='148398.0', 'num_nulls'='508.0', 'data_size'='2.0471814E7', 'min_value'='A', 'max_value'='Youngsters used to save quite colour')
"""
        sql """
alter table item modify column i_container set stats('row_count'='204000.0', 'ndv'='1.0', 'num_nulls'='510.0', 'data_size'='1424430.0', 'min_value'='Unknown', 'max_value'='Unknown')
"""
        sql """
alter table item modify column i_wholesale_cost set stats('row_count'='204000.0', 'ndv'='6622.0', 'num_nulls'='489.0', 'data_size'='816000.0', 'min_value'='0.02', 'max_value'='88.91')
"""
        sql """
alter table item modify column i_manufact_id set stats('row_count'='204000.0', 'ndv'='1005.0', 'num_nulls'='498.0', 'data_size'='816000.0', 'min_value'='1', 'max_value'='1000')
"""
        sql """
alter table item modify column i_brand_id set stats('row_count'='204000.0', 'ndv'='951.0', 'num_nulls'='516.0', 'data_size'='816000.0', 'min_value'='1001001', 'max_value'='10016017')
"""
        sql """
alter table item modify column i_formulation set stats('row_count'='204000.0', 'ndv'='152702.0', 'num_nulls'='530.0', 'data_size'='4069400.0', 'min_value'='00000822816917sky894', 'max_value'='yellow98911509228741')
"""
        sql """
alter table item modify column i_current_price set stats('row_count'='204000.0', 'ndv'='9094.0', 'num_nulls'='518.0', 'data_size'='816000.0', 'min_value'='0.09', 'max_value'='99.99')
"""
        sql """
alter table item modify column i_size set stats('row_count'='204000.0', 'ndv'='7.0', 'num_nulls'='515.0', 'data_size'='880961.0', 'min_value'='N/A', 'max_value'='small')
"""
        sql """
alter table item modify column i_rec_start_date set stats('row_count'='204000.0', 'ndv'='4.0', 'num_nulls'='522.0', 'data_size'='816000.0', 'min_value'='1997-10-27', 'max_value'='2001-10-27')
"""
        sql """
alter table item modify column i_manufact set stats('row_count'='204000.0', 'ndv'='1003.0', 'num_nulls'='544.0', 'data_size'='2298787.0', 'min_value'='able', 'max_value'='pripripri')
"""
        sql """
alter table item modify column i_item_sk set stats('row_count'='204000.0', 'ndv'='205012.0', 'num_nulls'='0.0', 'data_size'='1632000.0', 'min_value'='1', 'max_value'='204000')
"""
        sql """
alter table item modify column i_manager_id set stats('row_count'='204000.0', 'ndv'='100.0', 'num_nulls'='506.0', 'data_size'='816000.0', 'min_value'='1', 'max_value'='100')
"""
        sql """
alter table item modify column i_item_id set stats('row_count'='204000.0', 'ndv'='103230.0', 'num_nulls'='0.0', 'data_size'='3264000.0', 'min_value'='AAAAAAAAAAAABAAA', 'max_value'='AAAAAAAAPPPPBAAA')
"""
        sql """
alter table item modify column i_class_id set stats('row_count'='204000.0', 'ndv'='16.0', 'num_nulls'='491.0', 'data_size'='816000.0', 'min_value'='1', 'max_value'='16')
"""
        sql """
alter table item modify column i_class set stats('row_count'='204000.0', 'ndv'='99.0', 'num_nulls'='499.0', 'data_size'='1585937.0', 'min_value'='accent', 'max_value'='womens watch')
"""
        sql """
alter table item modify column i_category set stats('row_count'='204000.0', 'ndv'='10.0', 'num_nulls'='482.0', 'data_size'='1201703.0', 'min_value'='Books', 'max_value'='Women')
"""
        sql """
alter table item modify column i_category_id set stats('row_count'='204000.0', 'ndv'='10.0', 'num_nulls'='515.0', 'data_size'='816000.0', 'min_value'='1', 'max_value'='10')
"""
        sql """
alter table item modify column i_brand set stats('row_count'='204000.0', 'ndv'='713.0', 'num_nulls'='510.0', 'data_size'='3287671.0', 'min_value'='amalgamalg #1', 'max_value'='univunivamalg #9')
"""
        sql """
alter table item modify column i_units set stats('row_count'='204000.0', 'ndv'='21.0', 'num_nulls'='503.0', 'data_size'='852562.0', 'min_value'='Box', 'max_value'='Unknown')
"""
        sql """
alter table item modify column i_rec_end_date set stats('row_count'='204000.0', 'ndv'='3.0', 'num_nulls'='102000.0', 'data_size'='816000.0', 'min_value'='1999-10-27', 'max_value'='2001-10-26')
"""
        sql """
alter table item modify column i_color set stats('row_count'='204000.0', 'ndv'='92.0', 'num_nulls'='524.0', 'data_size'='1094247.0', 'min_value'='almond', 'max_value'='yellow')
"""
        sql """
alter table item modify column i_product_name set stats('row_count'='204000.0', 'ndv'='200390.0', 'num_nulls'='514.0', 'data_size'='4546148.0', 'min_value'='able', 'max_value'='pripripripripriought')
"""
        sql """
alter table promotion modify column p_start_date_sk set stats('row_count'='1000.0', 'ndv'='574.0', 'num_nulls'='17.0', 'data_size'='8000.0', 'min_value'='2450100', 'max_value'='2450915')
"""
        sql """
alter table promotion modify column p_channel_catalog set stats('row_count'='1000.0', 'ndv'='1.0', 'num_nulls'='14.0', 'data_size'='986.0', 'min_value'='N', 'max_value'='N')
"""
        sql """
alter table promotion modify column p_channel_demo set stats('row_count'='1000.0', 'ndv'='1.0', 'num_nulls'='16.0', 'data_size'='984.0', 'min_value'='N', 'max_value'='N')
"""
        sql """
alter table promotion modify column p_channel_email set stats('row_count'='1000.0', 'ndv'='1.0', 'num_nulls'='13.0', 'data_size'='987.0', 'min_value'='N', 'max_value'='N')
"""
        sql """
alter table promotion modify column p_end_date_sk set stats('row_count'='1000.0', 'ndv'='571.0', 'num_nulls'='12.0', 'data_size'='8000.0', 'min_value'='2450116', 'max_value'='2450967')
"""
        sql """
alter table promotion modify column p_channel_press set stats('row_count'='1000.0', 'ndv'='1.0', 'num_nulls'='15.0', 'data_size'='985.0', 'min_value'='N', 'max_value'='N')
"""
        sql """
alter table promotion modify column p_channel_tv set stats('row_count'='1000.0', 'ndv'='1.0', 'num_nulls'='14.0', 'data_size'='986.0', 'min_value'='N', 'max_value'='N')
"""
        sql """
alter table promotion modify column p_discount_active set stats('row_count'='1000.0', 'ndv'='1.0', 'num_nulls'='19.0', 'data_size'='981.0', 'min_value'='N', 'max_value'='N')
"""
        sql """
alter table promotion modify column p_channel_details set stats('row_count'='1000.0', 'ndv'='991.0', 'num_nulls'='10.0', 'data_size'='39304.0', 'min_value'='Able ambitions set anyway principles. Extra forests ju', 'max_value'='Young, valuable companies watch walls. Payments can flour')
"""
        sql """
alter table promotion modify column p_purpose set stats('row_count'='1000.0', 'ndv'='1.0', 'num_nulls'='13.0', 'data_size'='6909.0', 'min_value'='Unknown', 'max_value'='Unknown')
"""
        sql """
alter table promotion modify column p_promo_id set stats('row_count'='1000.0', 'ndv'='1004.0', 'num_nulls'='0.0', 'data_size'='16000.0', 'min_value'='AAAAAAAAAABAAAAA', 'max_value'='AAAAAAAAPPCAAAAA')
"""
        sql """
alter table promotion modify column p_cost set stats('row_count'='1000.0', 'ndv'='1.0', 'num_nulls'='14.0', 'data_size'='8000.0', 'min_value'='1000.00', 'max_value'='1000.00')
"""
        sql """
alter table promotion modify column p_channel_event set stats('row_count'='1000.0', 'ndv'='1.0', 'num_nulls'='14.0', 'data_size'='986.0', 'min_value'='N', 'max_value'='N')
"""
        sql """
alter table promotion modify column p_item_sk set stats('row_count'='1000.0', 'ndv'='970.0', 'num_nulls'='14.0', 'data_size'='8000.0', 'min_value'='280', 'max_value'='203966')
"""
        sql """
alter table promotion modify column p_response_target set stats('row_count'='1000.0', 'ndv'='1.0', 'num_nulls'='19.0', 'data_size'='4000.0', 'min_value'='1', 'max_value'='1')
"""
        sql """
alter table promotion modify column p_promo_sk set stats('row_count'='1000.0', 'ndv'='986.0', 'num_nulls'='0.0', 'data_size'='8000.0', 'min_value'='1', 'max_value'='1000')
"""
        sql """
alter table promotion modify column p_promo_name set stats('row_count'='1000.0', 'ndv'='10.0', 'num_nulls'='18.0', 'data_size'='3924.0', 'min_value'='able', 'max_value'='pri')
"""
        sql """
alter table promotion modify column p_channel_dmail set stats('row_count'='1000.0', 'ndv'='2.0', 'num_nulls'='13.0', 'data_size'='987.0', 'min_value'='N', 'max_value'='Y')
"""
        sql """
alter table promotion modify column p_channel_radio set stats('row_count'='1000.0', 'ndv'='1.0', 'num_nulls'='13.0', 'data_size'='987.0', 'min_value'='N', 'max_value'='N')
"""
        sql """
alter table reason modify column r_reason_desc set stats('row_count'='55.0', 'ndv'='54.0', 'num_nulls'='0.0', 'data_size'='758.0', 'min_value'='Did not fit', 'max_value'='unauthoized purchase')
"""
        sql """
alter table reason modify column r_reason_sk set stats('row_count'='55.0', 'ndv'='55.0', 'num_nulls'='0.0', 'data_size'='440.0', 'min_value'='1', 'max_value'='55')
"""
        sql """
alter table reason modify column r_reason_id set stats('row_count'='55.0', 'ndv'='55.0', 'num_nulls'='0.0', 'data_size'='880.0', 'min_value'='AAAAAAAAABAAAAAA', 'max_value'='AAAAAAAAPCAAAAAA')
"""
        sql """
alter table ship_mode modify column sm_ship_mode_sk set stats('row_count'='20.0', 'ndv'='20.0', 'num_nulls'='0.0', 'data_size'='160.0', 'min_value'='1', 'max_value'='20')
"""
        sql """
alter table ship_mode modify column sm_type set stats('row_count'='20.0', 'ndv'='6.0', 'num_nulls'='0.0', 'data_size'='150.0', 'min_value'='EXPRESS', 'max_value'='TWO DAY')
"""
        sql """
alter table ship_mode modify column sm_code set stats('row_count'='20.0', 'ndv'='4.0', 'num_nulls'='0.0', 'data_size'='87.0', 'min_value'='AIR', 'max_value'='SURFACE')
"""
        sql """
alter table ship_mode modify column sm_carrier set stats('row_count'='20.0', 'ndv'='20.0', 'num_nulls'='0.0', 'data_size'='133.0', 'min_value'='AIRBORNE', 'max_value'='ZOUROS')
"""
        sql """
alter table ship_mode modify column sm_contract set stats('row_count'='20.0', 'ndv'='20.0', 'num_nulls'='0.0', 'data_size'='252.0', 'min_value'='2mM8l', 'max_value'='yVfotg7Tio3MVhBg6Bkn')
"""
        sql """
alter table ship_mode modify column sm_ship_mode_id set stats('row_count'='20.0', 'ndv'='20.0', 'num_nulls'='0.0', 'data_size'='320.0', 'min_value'='AAAAAAAAABAAAAAA', 'max_value'='AAAAAAAAPAAAAAAA')
"""
        sql """
alter table store modify column s_geography_class set stats('row_count'='402.0', 'ndv'='1.0', 'num_nulls'='3.0', 'data_size'='2793.0', 'min_value'='Unknown', 'max_value'='Unknown')
"""
        sql """
alter table store modify column s_company_id set stats('row_count'='402.0', 'ndv'='1.0', 'num_nulls'='4.0', 'data_size'='1608.0', 'min_value'='1', 'max_value'='1')
"""
        sql """
alter table store modify column s_closed_date_sk set stats('row_count'='402.0', 'ndv'='69.0', 'num_nulls'='296.0', 'data_size'='3216.0', 'min_value'='2450823', 'max_value'='2451313')
"""
        sql """
alter table store modify column s_county set stats('row_count'='402.0', 'ndv'='9.0', 'num_nulls'='4.0', 'data_size'='5693.0', 'min_value'='Barrow County', 'max_value'='Ziebach County')
"""
        sql """
alter table store modify column s_city set stats('row_count'='402.0', 'ndv'='18.0', 'num_nulls'='5.0', 'data_size'='3669.0', 'min_value'='Bethel', 'max_value'='Union')
"""
        sql """
alter table store modify column s_division_name set stats('row_count'='402.0', 'ndv'='1.0', 'num_nulls'='5.0', 'data_size'='2779.0', 'min_value'='Unknown', 'max_value'='Unknown')
"""
        sql """
alter table store modify column s_zip set stats('row_count'='402.0', 'ndv'='101.0', 'num_nulls'='6.0', 'data_size'='1980.0', 'min_value'='20059', 'max_value'='79431')
"""
        sql """
alter table store modify column s_state set stats('row_count'='402.0', 'ndv'='9.0', 'num_nulls'='2.0', 'data_size'='800.0', 'min_value'='AL', 'max_value'='TN')
"""
        sql """
alter table store modify column s_rec_end_date set stats('row_count'='402.0', 'ndv'='3.0', 'num_nulls'='201.0', 'data_size'='1608.0', 'min_value'='1999-03-13', 'max_value'='2001-03-12')
"""
        sql """
alter table store modify column s_floor_space set stats('row_count'='402.0', 'ndv'='300.0', 'num_nulls'='3.0', 'data_size'='1608.0', 'min_value'='5004767', 'max_value'='9997773')
"""
        sql """
alter table store modify column s_number_employees set stats('row_count'='402.0', 'ndv'='97.0', 'num_nulls'='5.0', 'data_size'='1608.0', 'min_value'='200', 'max_value'='300')
"""
        sql """
alter table store modify column s_company_name set stats('row_count'='402.0', 'ndv'='1.0', 'num_nulls'='3.0', 'data_size'='2793.0', 'min_value'='Unknown', 'max_value'='Unknown')
"""
        sql """
alter table store modify column s_street_number set stats('row_count'='402.0', 'ndv'='266.0', 'num_nulls'='5.0', 'data_size'='1150.0', 'min_value'='100', 'max_value'='986')
"""
        sql """
alter table store modify column s_market_id set stats('row_count'='402.0', 'ndv'='10.0', 'num_nulls'='6.0', 'data_size'='1608.0', 'min_value'='1', 'max_value'='10')
"""
        sql """
alter table store modify column s_rec_start_date set stats('row_count'='402.0', 'ndv'='4.0', 'num_nulls'='4.0', 'data_size'='1608.0', 'min_value'='1997-03-13', 'max_value'='2001-03-13')
"""
        sql """
alter table store modify column s_store_name set stats('row_count'='402.0', 'ndv'='10.0', 'num_nulls'='2.0', 'data_size'='1575.0', 'min_value'='able', 'max_value'='pri')
"""
        sql """
alter table store modify column s_suite_number set stats('row_count'='402.0', 'ndv'='74.0', 'num_nulls'='2.0', 'data_size'='3140.0', 'min_value'='Suite 0', 'max_value'='Suite Y')
"""
        sql """
alter table store modify column s_street_name set stats('row_count'='402.0', 'ndv'='255.0', 'num_nulls'='6.0', 'data_size'='3384.0', 'min_value'='10th ', 'max_value'='Woodland ')
"""
        sql """
alter table store modify column s_market_desc set stats('row_count'='402.0', 'ndv'='310.0', 'num_nulls'='5.0', 'data_size'='23261.0', 'min_value'='Able, early horses help much strong minutes. Equally empty homes answer highly procedures. Coll', 'max_value'='Years get acute years. Right likely players mus')
"""
        sql """
alter table store modify column s_division_id set stats('row_count'='402.0', 'ndv'='1.0', 'num_nulls'='2.0', 'data_size'='1608.0', 'min_value'='1', 'max_value'='1')
"""
        sql """
alter table store modify column s_country set stats('row_count'='402.0', 'ndv'='1.0', 'num_nulls'='4.0', 'data_size'='5174.0', 'min_value'='United States', 'max_value'='United States')
"""
        sql """
alter table store modify column s_tax_percentage set stats('row_count'='402.0', 'ndv'='12.0', 'num_nulls'='5.0', 'data_size'='1608.0', 'min_value'='0.00', 'max_value'='0.11')
"""
        sql """
alter table store modify column s_hours set stats('row_count'='402.0', 'ndv'='3.0', 'num_nulls'='4.0', 'data_size'='2848.0', 'min_value'='8AM-12AM', 'max_value'='8AM-8AM')
"""
        sql """
alter table store modify column s_manager set stats('row_count'='402.0', 'ndv'='300.0', 'num_nulls'='5.0', 'data_size'='5075.0', 'min_value'='Alan Elam', 'max_value'='Zachary Price')
"""
        sql """
alter table store modify column s_market_manager set stats('row_count'='402.0', 'ndv'='285.0', 'num_nulls'='7.0', 'data_size'='5129.0', 'min_value'='Aaron Trent', 'max_value'='Zane Perez')
"""
        sql """
alter table store modify column s_store_id set stats('row_count'='402.0', 'ndv'='201.0', 'num_nulls'='0.0', 'data_size'='6432.0', 'min_value'='AAAAAAAAAABAAAAA', 'max_value'='AAAAAAAAPNAAAAAA')
"""
        sql """
alter table store modify column s_gmt_offset set stats('row_count'='402.0', 'ndv'='2.0', 'num_nulls'='4.0', 'data_size'='1608.0', 'min_value'='-6.00', 'max_value'='-5.00')
"""
        sql """
alter table store modify column s_store_sk set stats('row_count'='402.0', 'ndv'='398.0', 'num_nulls'='0.0', 'data_size'='3216.0', 'min_value'='1', 'max_value'='402')
"""
        sql """
alter table store modify column s_street_type set stats('row_count'='402.0', 'ndv'='20.0', 'num_nulls'='6.0', 'data_size'='1657.0', 'min_value'='Ave', 'max_value'='Wy')
"""
        sql """
alter table store_returns modify column sr_hdemo_sk set stats('row_count'='2.879508E7', 'ndv'='7251.0', 'num_nulls'='1008547.0', 'data_size'='2.3036064E8', 'min_value'='1', 'max_value'='7200')
"""
        sql """
alter table store_returns modify column sr_return_ship_cost set stats('row_count'='2.879508E7', 'ndv'='295559.0', 'num_nulls'='1007846.0', 'data_size'='1.1518032E8', 'min_value'='0.00', 'max_value'='9578.25')
"""
        sql """
alter table store_returns modify column sr_reversed_charge set stats('row_count'='2.879508E7', 'ndv'='431405.0', 'num_nulls'='1009035.0', 'data_size'='1.1518032E8', 'min_value'='0.00', 'max_value'='16099.52')
"""
        sql """
alter table store_returns modify column sr_returned_date_sk set stats('row_count'='2.879508E7', 'ndv'='2003.0', 'num_nulls'='14368.0', 'data_size'='2.30245689E8', 'min_value'='2450820', 'max_value'='2452822')
"""
        sql """
alter table store_returns modify column sr_ticket_number set stats('row_count'='2.879508E7', 'ndv'='1.6790866E7', 'num_nulls'='0.0', 'data_size'='2.3036064E8', 'min_value'='1', 'max_value'='23999996')
"""
        sql """
alter table store_returns modify column sr_net_loss set stats('row_count'='2.879508E7', 'ndv'='526606.0', 'num_nulls'='1007153.0', 'data_size'='1.1518032E8', 'min_value'='0.50', 'max_value'='10447.72')
"""
        sql """
alter table store_returns modify column sr_reason_sk set stats('row_count'='2.879508E7', 'ndv'='55.0', 'num_nulls'='1008299.0', 'data_size'='2.3036064E8', 'min_value'='1', 'max_value'='55')
"""
        sql """
alter table store_returns modify column sr_item_sk set stats('row_count'='2.879508E7', 'ndv'='205012.0', 'num_nulls'='0.0', 'data_size'='2.3036064E8', 'min_value'='1', 'max_value'='204000')
"""
        sql """
alter table store_returns modify column sr_return_tax set stats('row_count'='2.879508E7', 'ndv'='89281.0', 'num_nulls'='1008618.0', 'data_size'='1.1518032E8', 'min_value'='0.00', 'max_value'='1611.71')
"""
        sql """
alter table store_returns modify column sr_refunded_cash set stats('row_count'='2.879508E7', 'ndv'='601705.0', 'num_nulls'='1008003.0', 'data_size'='1.1518032E8', 'min_value'='0.00', 'max_value'='17556.95')
"""
        sql """
alter table store_returns modify column sr_fee set stats('row_count'='2.879508E7', 'ndv'='9958.0', 'num_nulls'='1008291.0', 'data_size'='1.1518032E8', 'min_value'='0.50', 'max_value'='100.00')
"""
        sql """
alter table store_returns modify column sr_cdemo_sk set stats('row_count'='2.879508E7', 'ndv'='1916366.0', 'num_nulls'='1006835.0', 'data_size'='2.3036064E8', 'min_value'='1', 'max_value'='1920800')
"""
        sql """
alter table store_returns modify column sr_return_amt set stats('row_count'='2.879508E7', 'ndv'='526057.0', 'num_nulls'='1007419.0', 'data_size'='1.1518032E8', 'min_value'='0.00', 'max_value'='18973.20')
"""
        sql """
alter table store_returns modify column sr_store_credit set stats('row_count'='2.879508E7', 'ndv'='426217.0', 'num_nulls'='1007102.0', 'data_size'='1.1518032E8', 'min_value'='0.00', 'max_value'='15642.11')
"""
        sql """
alter table store_returns modify column sr_addr_sk set stats('row_count'='2.879508E7', 'ndv'='1000237.0', 'num_nulls'='1008253.0', 'data_size'='2.3036064E8', 'min_value'='1', 'max_value'='1000000')
"""
        sql """
alter table store_returns modify column sr_return_amt_inc_tax set stats('row_count'='2.879508E7', 'ndv'='865392.0', 'num_nulls'='1006919.0', 'data_size'='1.1518032E8', 'min_value'='0.00', 'max_value'='20002.89')
"""
        sql """
alter table store_returns modify column sr_store_sk set stats('row_count'='2.879508E7', 'ndv'='200.0', 'num_nulls'='1007164.0', 'data_size'='2.3036064E8', 'min_value'='1', 'max_value'='400')
"""
        sql """
alter table store_returns modify column sr_return_quantity set stats('row_count'='2.879508E7', 'ndv'='100.0', 'num_nulls'='1007948.0', 'data_size'='1.1518032E8', 'min_value'='1', 'max_value'='100')
"""
        sql """
alter table store_returns modify column sr_customer_sk set stats('row_count'='2.879508E7', 'ndv'='1994323.0', 'num_nulls'='1008429.0', 'data_size'='2.3036064E8', 'min_value'='1', 'max_value'='2000000')
"""
        sql """
alter table store_returns modify column sr_return_time_sk set stats('row_count'='2.879508E7', 'ndv'='32660.0', 'num_nulls'='1009330.0', 'data_size'='2.3036064E8', 'min_value'='28799', 'max_value'='61199')
"""
        sql """
alter table store_sales modify column ss_sold_time_sk set stats('row_count'='2.87997024E8', 'ndv'='47252.0', 'num_nulls'='1.29533E7', 'data_size'='2.303976192E9', 'min_value'='28800', 'max_value'='75599')
"""
        sql """
alter table store_sales modify column ss_cdemo_sk set stats('row_count'='2.87997024E8', 'ndv'='1916366.0', 'num_nulls'='1.2955252E7', 'data_size'='2.303976192E9', 'min_value'='1', 'max_value'='1920800')
"""
        sql """
alter table store_sales modify column ss_promo_sk set stats('row_count'='2.87997024E8', 'ndv'='986.0', 'num_nulls'='1.2954088E7', 'data_size'='2.303976192E9', 'min_value'='1', 'max_value'='1000')
"""
        sql """
alter table store_sales modify column ss_ext_discount_amt set stats('row_count'='2.87997024E8', 'ndv'='961784.0', 'num_nulls'='1.2958053E7', 'data_size'='1.151988096E9', 'min_value'='0.00', 'max_value'='19225.00')
"""
        sql """
alter table store_sales modify column ss_ext_sales_price set stats('row_count'='2.87997024E8', 'ndv'='727803.0', 'num_nulls'='1.2955462E7', 'data_size'='1.151988096E9', 'min_value'='0.00', 'max_value'='19878.00')
"""
        sql """
alter table store_sales modify column ss_net_profit set stats('row_count'='2.87997024E8', 'ndv'='1377531.0', 'num_nulls'='1.2955156E7', 'data_size'='1.151988096E9', 'min_value'='-10000.00', 'max_value'='9889.00')
"""
        sql """
alter table store_sales modify column ss_addr_sk set stats('row_count'='2.87997024E8', 'ndv'='1000237.0', 'num_nulls'='1.2956686E7', 'data_size'='2.303976192E9', 'min_value'='1', 'max_value'='1000000')
"""
        sql """
alter table store_sales modify column ss_ticket_number set stats('row_count'='2.87997024E8', 'ndv'='2.3905324E7', 'num_nulls'='0.0', 'data_size'='2.303976192E9', 'min_value'='1', 'max_value'='24000000')
"""
        sql """
alter table store_sales modify column ss_wholesale_cost set stats('row_count'='2.87997024E8', 'ndv'='9905.0', 'num_nulls'='1.2958327E7', 'data_size'='1.151988096E9', 'min_value'='1.00', 'max_value'='100.00')
"""
        sql """
alter table store_sales modify column ss_item_sk set stats('row_count'='2.87997024E8', 'ndv'='205012.0', 'num_nulls'='0.0', 'data_size'='2.303976192E9', 'min_value'='1', 'max_value'='204000')
"""
        sql """
alter table store_sales modify column ss_ext_list_price set stats('row_count'='2.87997024E8', 'ndv'='770803.0', 'num_nulls'='1.295504E7', 'data_size'='1.151988096E9', 'min_value'='1.00', 'max_value'='20000.00')
"""
        sql """
alter table store_sales modify column ss_sold_date_sk set stats('row_count'='2.87997024E8', 'ndv'='1823.0', 'num_nulls'='157893.0', 'data_size'='2.302713047E9', 'min_value'='2450816', 'max_value'='2452642')
"""
        sql """
alter table store_sales modify column ss_store_sk set stats('row_count'='2.87997024E8', 'ndv'='200.0', 'num_nulls'='1.2950651E7', 'data_size'='2.303976192E9', 'min_value'='1', 'max_value'='400')
"""
        sql """
alter table store_sales modify column ss_coupon_amt set stats('row_count'='2.87997024E8', 'ndv'='961784.0', 'num_nulls'='1.2958053E7', 'data_size'='1.151988096E9', 'min_value'='0.00', 'max_value'='19225.00')
"""
        sql """
alter table store_sales modify column ss_quantity set stats('row_count'='2.87997024E8', 'ndv'='100.0', 'num_nulls'='1.2953654E7', 'data_size'='1.151988096E9', 'min_value'='1', 'max_value'='100')
"""
        sql """
alter table store_sales modify column ss_list_price set stats('row_count'='2.87997024E8', 'ndv'='19640.0', 'num_nulls'='1.2952108E7', 'data_size'='1.151988096E9', 'min_value'='1.00', 'max_value'='200.00')
"""
        sql """
alter table store_sales modify column ss_sales_price set stats('row_count'='2.87997024E8', 'ndv'='19767.0', 'num_nulls'='1.2958587E7', 'data_size'='1.151988096E9', 'min_value'='0.00', 'max_value'='200.00')
"""
        sql """
alter table store_sales modify column ss_customer_sk set stats('row_count'='2.87997024E8', 'ndv'='1994393.0', 'num_nulls'='1.2952082E7', 'data_size'='2.303976192E9', 'min_value'='1', 'max_value'='2000000')
"""
        sql """
alter table store_sales modify column ss_ext_wholesale_cost set stats('row_count'='2.87997024E8', 'ndv'='393180.0', 'num_nulls'='1.296006E7', 'data_size'='1.151988096E9', 'min_value'='1.00', 'max_value'='10000.00')
"""
        sql """
alter table store_sales modify column ss_net_paid set stats('row_count'='2.87997024E8', 'ndv'='1146886.0', 'num_nulls'='1.2954554E7', 'data_size'='1.151988096E9', 'min_value'='0.00', 'max_value'='19878.00')
"""
        sql """
alter table store_sales modify column ss_ext_tax set stats('row_count'='2.87997024E8', 'ndv'='139557.0', 'num_nulls'='1.2957705E7', 'data_size'='1.151988096E9', 'min_value'='0.00', 'max_value'='1762.38')
"""
        sql """
alter table store_sales modify column ss_hdemo_sk set stats('row_count'='2.87997024E8', 'ndv'='7251.0', 'num_nulls'='1.2957139E7', 'data_size'='2.303976192E9', 'min_value'='1', 'max_value'='7200')
"""
        sql """
alter table store_sales modify column ss_net_paid_inc_tax set stats('row_count'='2.87997024E8', 'ndv'='1545140.0', 'num_nulls'='1.2958941E7', 'data_size'='1.151988096E9', 'min_value'='0.00', 'max_value'='21344.38')
"""
        sql """
alter table time_dim modify column t_hour set stats('row_count'='86400.0', 'ndv'='24.0', 'num_nulls'='0.0', 'data_size'='345600.0', 'min_value'='0', 'max_value'='23')
"""
        sql """
alter table time_dim modify column t_time_id set stats('row_count'='86400.0', 'ndv'='85663.0', 'num_nulls'='0.0', 'data_size'='1382400.0', 'min_value'='AAAAAAAAAAAABAAA', 'max_value'='AAAAAAAAPPPPAAAA')
"""
        sql """
alter table time_dim modify column t_shift set stats('row_count'='86400.0', 'ndv'='3.0', 'num_nulls'='0.0', 'data_size'='460800.0', 'min_value'='first', 'max_value'='third')
"""
        sql """
alter table time_dim modify column t_sub_shift set stats('row_count'='86400.0', 'ndv'='4.0', 'num_nulls'='0.0', 'data_size'='597600.0', 'min_value'='afternoon', 'max_value'='night')
"""
        sql """
alter table time_dim modify column t_meal_time set stats('row_count'='86400.0', 'ndv'='3.0', 'num_nulls'='50400.0', 'data_size'='248400.0', 'min_value'='breakfast', 'max_value'='lunch')
"""
        sql """
alter table time_dim modify column t_time set stats('row_count'='86400.0', 'ndv'='86684.0', 'num_nulls'='0.0', 'data_size'='345600.0', 'min_value'='0', 'max_value'='86399')
"""
        sql """
alter table time_dim modify column t_minute set stats('row_count'='86400.0', 'ndv'='60.0', 'num_nulls'='0.0', 'data_size'='345600.0', 'min_value'='0', 'max_value'='59')
"""
        sql """
alter table time_dim modify column t_second set stats('row_count'='86400.0', 'ndv'='60.0', 'num_nulls'='0.0', 'data_size'='345600.0', 'min_value'='0', 'max_value'='59')
"""
        sql """
alter table time_dim modify column t_am_pm set stats('row_count'='86400.0', 'ndv'='2.0', 'num_nulls'='0.0', 'data_size'='172800.0', 'min_value'='AM', 'max_value'='PM')
"""
        sql """
alter table time_dim modify column t_time_sk set stats('row_count'='86400.0', 'ndv'='87677.0', 'num_nulls'='0.0', 'data_size'='691200.0', 'min_value'='0', 'max_value'='86399')
"""
        sql """
alter table warehouse modify column w_zip set stats('row_count'='15.0', 'ndv'='15.0', 'num_nulls'='0.0', 'data_size'='75.0', 'min_value'='28721', 'max_value'='78721')
"""
        sql """
alter table warehouse modify column w_warehouse_name set stats('row_count'='15.0', 'ndv'='14.0', 'num_nulls'='1.0', 'data_size'='230.0', 'min_value'='Bad cards must make.', 'max_value'='Rooms cook ')
"""
        sql """
alter table warehouse modify column w_street_name set stats('row_count'='15.0', 'ndv'='14.0', 'num_nulls'='1.0', 'data_size'='128.0', 'min_value'='3rd ', 'max_value'='Wilson Elm')
"""
        sql """
alter table warehouse modify column w_state set stats('row_count'='15.0', 'ndv'='8.0', 'num_nulls'='0.0', 'data_size'='30.0', 'min_value'='AL', 'max_value'='SD')
"""
        sql """
alter table warehouse modify column w_city set stats('row_count'='15.0', 'ndv'='11.0', 'num_nulls'='0.0', 'data_size'='111.0', 'min_value'='Bethel', 'max_value'='Union')
"""
        sql """
alter table warehouse modify column w_county set stats('row_count'='15.0', 'ndv'='8.0', 'num_nulls'='0.0', 'data_size'='207.0', 'min_value'='Barrow County', 'max_value'='Ziebach County')
"""
        sql """
alter table warehouse modify column w_street_type set stats('row_count'='15.0', 'ndv'='10.0', 'num_nulls'='1.0', 'data_size'='58.0', 'min_value'='Ave', 'max_value'='Wy')
"""
        sql """
alter table warehouse modify column w_warehouse_sq_ft set stats('row_count'='15.0', 'ndv'='14.0', 'num_nulls'='1.0', 'data_size'='60.0', 'min_value'='73065', 'max_value'='977787')
"""
        sql """
alter table warehouse modify column w_street_number set stats('row_count'='15.0', 'ndv'='14.0', 'num_nulls'='1.0', 'data_size'='40.0', 'min_value'='15', 'max_value'='957')
"""
        sql """
alter table warehouse modify column w_suite_number set stats('row_count'='15.0', 'ndv'='13.0', 'num_nulls'='1.0', 'data_size'='111.0', 'min_value'='Suite 0', 'max_value'='Suite X')
"""
        sql """
alter table warehouse modify column w_country set stats('row_count'='15.0', 'ndv'='1.0', 'num_nulls'='0.0', 'data_size'='195.0', 'min_value'='United States', 'max_value'='United States')
"""
        sql """
alter table warehouse modify column w_warehouse_sk set stats('row_count'='15.0', 'ndv'='15.0', 'num_nulls'='0.0', 'data_size'='120.0', 'min_value'='1', 'max_value'='15')
"""
        sql """
alter table warehouse modify column w_gmt_offset set stats('row_count'='15.0', 'ndv'='2.0', 'num_nulls'='1.0', 'data_size'='60.0', 'min_value'='-6.00', 'max_value'='-5.00')
"""
        sql """
alter table warehouse modify column w_warehouse_id set stats('row_count'='15.0', 'ndv'='15.0', 'num_nulls'='0.0', 'data_size'='240.0', 'min_value'='AAAAAAAABAAAAAAA', 'max_value'='AAAAAAAAPAAAAAAA')
"""
        sql """
alter table web_page modify column wp_char_count set stats('row_count'='2040.0', 'ndv'='1363.0', 'num_nulls'='25.0', 'data_size'='8160.0', 'min_value'='303', 'max_value'='8523')
"""
        sql """
alter table web_page modify column wp_web_page_id set stats('row_count'='2040.0', 'ndv'='1019.0', 'num_nulls'='0.0', 'data_size'='32640.0', 'min_value'='AAAAAAAAAABAAAAA', 'max_value'='AAAAAAAAPPEAAAAA')
"""
        sql """
alter table web_page modify column wp_web_page_sk set stats('row_count'='2040.0', 'ndv'='2032.0', 'num_nulls'='0.0', 'data_size'='16320.0', 'min_value'='1', 'max_value'='2040')
"""
        sql """
alter table web_page modify column wp_customer_sk set stats('row_count'='2040.0', 'ndv'='475.0', 'num_nulls'='1471.0', 'data_size'='16320.0', 'min_value'='711', 'max_value'='1996257')
"""
        sql """
alter table web_page modify column wp_autogen_flag set stats('row_count'='2040.0', 'ndv'='2.0', 'num_nulls'='25.0', 'data_size'='2015.0', 'min_value'='N', 'max_value'='Y')
"""
        sql """
alter table web_page modify column wp_rec_start_date set stats('row_count'='2040.0', 'ndv'='4.0', 'num_nulls'='21.0', 'data_size'='8160.0', 'min_value'='1997-09-03', 'max_value'='2001-09-03')
"""
        sql """
alter table web_page modify column wp_url set stats('row_count'='2040.0', 'ndv'='1.0', 'num_nulls'='25.0', 'data_size'='36270.0', 'min_value'='http://www.foo.com', 'max_value'='http://www.foo.com')
"""
        sql """
alter table web_page modify column wp_image_count set stats('row_count'='2040.0', 'ndv'='7.0', 'num_nulls'='20.0', 'data_size'='8160.0', 'min_value'='1', 'max_value'='7')
"""
        sql """
alter table web_page modify column wp_type set stats('row_count'='2040.0', 'ndv'='7.0', 'num_nulls'='19.0', 'data_size'='12856.0', 'min_value'='ad', 'max_value'='welcome')
"""
        sql """
alter table web_page modify column wp_creation_date_sk set stats('row_count'='2040.0', 'ndv'='134.0', 'num_nulls'='20.0', 'data_size'='16320.0', 'min_value'='2450672', 'max_value'='2450815')
"""
        sql """
alter table web_page modify column wp_link_count set stats('row_count'='2040.0', 'ndv'='24.0', 'num_nulls'='16.0', 'data_size'='8160.0', 'min_value'='2', 'max_value'='25')
"""
        sql """
alter table web_page modify column wp_access_date_sk set stats('row_count'='2040.0', 'ndv'='101.0', 'num_nulls'='19.0', 'data_size'='16320.0', 'min_value'='2452548', 'max_value'='2452648')
"""
        sql """
alter table web_page modify column wp_max_ad_count set stats('row_count'='2040.0', 'ndv'='5.0', 'num_nulls'='21.0', 'data_size'='8160.0', 'min_value'='0', 'max_value'='4')
"""
        sql """
alter table web_page modify column wp_rec_end_date set stats('row_count'='2040.0', 'ndv'='3.0', 'num_nulls'='1020.0', 'data_size'='8160.0', 'min_value'='1999-09-03', 'max_value'='2001-09-02')
"""
        sql """
alter table web_returns modify column wr_refunded_hdemo_sk set stats('row_count'='7197670.0', 'ndv'='7251.0', 'num_nulls'='324230.0', 'data_size'='5.758136E7', 'min_value'='1', 'max_value'='7200')
"""
        sql """
alter table web_returns modify column wr_order_number set stats('row_count'='7197670.0', 'ndv'='4249346.0', 'num_nulls'='0.0', 'data_size'='5.758136E7', 'min_value'='1', 'max_value'='5999999')
"""
        sql """
alter table web_returns modify column wr_returning_customer_sk set stats('row_count'='7197670.0', 'ndv'='1926139.0', 'num_nulls'='324024.0', 'data_size'='5.758136E7', 'min_value'='1', 'max_value'='2000000')
"""
        sql """
alter table web_returns modify column wr_fee set stats('row_count'='7197670.0', 'ndv'='9958.0', 'num_nulls'='324065.0', 'data_size'='2.879068E7', 'min_value'='0.50', 'max_value'='100.00')
"""
        sql """
alter table web_returns modify column wr_refunded_addr_sk set stats('row_count'='7197670.0', 'ndv'='999503.0', 'num_nulls'='324482.0', 'data_size'='5.758136E7', 'min_value'='1', 'max_value'='1000000')
"""
        sql """
alter table web_returns modify column wr_returning_addr_sk set stats('row_count'='7197670.0', 'ndv'='999584.0', 'num_nulls'='323850.0', 'data_size'='5.758136E7', 'min_value'='1', 'max_value'='1000000')
"""
        sql """
alter table web_returns modify column wr_returning_hdemo_sk set stats('row_count'='7197670.0', 'ndv'='7251.0', 'num_nulls'='323999.0', 'data_size'='5.758136E7', 'min_value'='1', 'max_value'='7200')
"""
        sql """
alter table web_returns modify column wr_return_amt set stats('row_count'='7197670.0', 'ndv'='511026.0', 'num_nulls'='323473.0', 'data_size'='2.879068E7', 'min_value'='0.00', 'max_value'='28346.31')
"""
        sql """
alter table web_returns modify column wr_refunded_cash set stats('row_count'='7197670.0', 'ndv'='500099.0', 'num_nulls'='324693.0', 'data_size'='2.879068E7', 'min_value'='0.00', 'max_value'='26466.56')
"""
        sql """
alter table web_returns modify column wr_return_amt_inc_tax set stats('row_count'='7197670.0', 'ndv'='749255.0', 'num_nulls'='323171.0', 'data_size'='2.879068E7', 'min_value'='0.00', 'max_value'='29493.38')
"""
        sql """
alter table web_returns modify column wr_reason_sk set stats('row_count'='7197670.0', 'ndv'='55.0', 'num_nulls'='323666.0', 'data_size'='5.758136E7', 'min_value'='1', 'max_value'='55')
"""
        sql """
alter table web_returns modify column wr_item_sk set stats('row_count'='7197670.0', 'ndv'='205012.0', 'num_nulls'='0.0', 'data_size'='5.758136E7', 'min_value'='1', 'max_value'='204000')
"""
        sql """
alter table web_returns modify column wr_return_ship_cost set stats('row_count'='7197670.0', 'ndv'='306759.0', 'num_nulls'='323341.0', 'data_size'='2.879068E7', 'min_value'='0.00', 'max_value'='13602.60')
"""
        sql """
alter table web_returns modify column wr_returned_time_sk set stats('row_count'='7197670.0', 'ndv'='87677.0', 'num_nulls'='323677.0', 'data_size'='5.758136E7', 'min_value'='0', 'max_value'='86399')
"""
        sql """
alter table web_returns modify column wr_account_credit set stats('row_count'='7197670.0', 'ndv'='344817.0', 'num_nulls'='324422.0', 'data_size'='2.879068E7', 'min_value'='0.00', 'max_value'='23028.27')
"""
        sql """
alter table web_returns modify column wr_returning_cdemo_sk set stats('row_count'='7197670.0', 'ndv'='1865149.0', 'num_nulls'='323899.0', 'data_size'='5.758136E7', 'min_value'='1', 'max_value'='1920800')
"""
        sql """
alter table web_returns modify column wr_reversed_charge set stats('row_count'='7197670.0', 'ndv'='346916.0', 'num_nulls'='323810.0', 'data_size'='2.879068E7', 'min_value'='0.00', 'max_value'='22972.36')
"""
        sql """
alter table web_returns modify column wr_net_loss set stats('row_count'='7197670.0', 'ndv'='493020.0', 'num_nulls'='324438.0', 'data_size'='2.879068E7', 'min_value'='0.50', 'max_value'='15068.96')
"""
        sql """
alter table web_returns modify column wr_return_quantity set stats('row_count'='7197670.0', 'ndv'='100.0', 'num_nulls'='323764.0', 'data_size'='2.879068E7', 'min_value'='1', 'max_value'='100')
"""
        sql """
alter table web_returns modify column wr_web_page_sk set stats('row_count'='7197670.0', 'ndv'='2032.0', 'num_nulls'='324900.0', 'data_size'='5.758136E7', 'min_value'='1', 'max_value'='2040')
"""
        sql """
alter table web_returns modify column wr_return_tax set stats('row_count'='7197670.0', 'ndv'='91974.0', 'num_nulls'='323621.0', 'data_size'='2.879068E7', 'min_value'='0.00', 'max_value'='2551.16')
"""
        sql """
alter table web_returns modify column wr_returned_date_sk set stats('row_count'='7197670.0', 'ndv'='2180.0', 'num_nulls'='3300.0', 'data_size'='5.7554958E7', 'min_value'='2450820', 'max_value'='2453002')
"""
        sql """
alter table web_returns modify column wr_refunded_customer_sk set stats('row_count'='7197670.0', 'ndv'='1923644.0', 'num_nulls'='324191.0', 'data_size'='5.758136E7', 'min_value'='1', 'max_value'='2000000')
"""
        sql """
alter table web_returns modify column wr_refunded_cdemo_sk set stats('row_count'='7197670.0', 'ndv'='1868495.0', 'num_nulls'='323863.0', 'data_size'='5.758136E7', 'min_value'='1', 'max_value'='1920800')
"""
        sql """
alter table web_sales modify column ws_order_number set stats('row_count'='7.2001237E7', 'ndv'='6015811.0', 'num_nulls'='0.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='6000000')
"""
        sql """
alter table web_sales modify column ws_ship_mode_sk set stats('row_count'='7.2001237E7', 'ndv'='20.0', 'num_nulls'='17823.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='20')
"""
        sql """
alter table web_sales modify column ws_sold_time_sk set stats('row_count'='7.2001237E7', 'ndv'='87677.0', 'num_nulls'='17931.0', 'data_size'='5.76009896E8', 'min_value'='0', 'max_value'='86399')
"""
        sql """
alter table web_sales modify column ws_ship_addr_sk set stats('row_count'='7.2001237E7', 'ndv'='997336.0', 'num_nulls'='17931.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='1000000')
"""
        sql """
alter table web_sales modify column ws_net_paid_inc_tax set stats('row_count'='7.2001237E7', 'ndv'='1806188.0', 'num_nulls'='18102.0', 'data_size'='2.88004948E8', 'min_value'='0.00', 'max_value'='32492.90')
"""
        sql """
alter table web_sales modify column ws_promo_sk set stats('row_count'='7.2001237E7', 'ndv'='986.0', 'num_nulls'='18116.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='1000')
"""
        sql """
alter table web_sales modify column ws_net_paid_inc_ship set stats('row_count'='7.2001237E7', 'ndv'='1758868.0', 'num_nulls'='0.0', 'data_size'='2.88004948E8', 'min_value'='0.00', 'max_value'='43468.92')
"""
        sql """
alter table web_sales modify column ws_quantity set stats('row_count'='7.2001237E7', 'ndv'='100.0', 'num_nulls'='18014.0', 'data_size'='2.88004948E8', 'min_value'='1', 'max_value'='100')
"""
        sql """
alter table web_sales modify column ws_ext_sales_price set stats('row_count'='7.2001237E7', 'ndv'='956806.0', 'num_nulls'='17843.0', 'data_size'='2.88004948E8', 'min_value'='0.00', 'max_value'='29810.00')
"""
        sql """
alter table web_sales modify column ws_ship_date_sk set stats('row_count'='7.2001237E7', 'ndv'='1952.0', 'num_nulls'='17883.0', 'data_size'='5.76009896E8', 'min_value'='2450817', 'max_value'='2452762')
"""
        sql """
alter table web_sales modify column ws_web_site_sk set stats('row_count'='7.2001237E7', 'ndv'='24.0', 'num_nulls'='18030.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='24')
"""
        sql """
alter table web_sales modify column ws_bill_cdemo_sk set stats('row_count'='7.2001237E7', 'ndv'='1835731.0', 'num_nulls'='17833.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='1920800')
"""
        sql """
alter table web_sales modify column ws_net_profit set stats('row_count'='7.2001237E7', 'ndv'='1553517.0', 'num_nulls'='0.0', 'data_size'='2.88004948E8', 'min_value'='-9997.00', 'max_value'='19840.00')
"""
        sql """
alter table web_sales modify column ws_bill_customer_sk set stats('row_count'='7.2001237E7', 'ndv'='1899439.0', 'num_nulls'='17882.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='2000000')
"""
        sql """
alter table web_sales modify column ws_ext_ship_cost set stats('row_count'='7.2001237E7', 'ndv'='516897.0', 'num_nulls'='17923.0', 'data_size'='2.88004948E8', 'min_value'='0.00', 'max_value'='14927.00')
"""
        sql """
alter table web_sales modify column ws_net_paid set stats('row_count'='7.2001237E7', 'ndv'='1299831.0', 'num_nulls'='17968.0', 'data_size'='2.88004948E8', 'min_value'='0.00', 'max_value'='29810.00')
"""
        sql """
alter table web_sales modify column ws_coupon_amt set stats('row_count'='7.2001237E7', 'ndv'='921269.0', 'num_nulls'='18027.0', 'data_size'='2.88004948E8', 'min_value'='0.00', 'max_value'='27591.16')
"""
        sql """
alter table web_sales modify column ws_wholesale_cost set stats('row_count'='7.2001237E7', 'ndv'='9905.0', 'num_nulls'='17850.0', 'data_size'='2.88004948E8', 'min_value'='1.00', 'max_value'='100.00')
"""
        sql """
alter table web_sales modify column ws_ship_customer_sk set stats('row_count'='7.2001237E7', 'ndv'='1898561.0', 'num_nulls'='17886.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='2000000')
"""
        sql """
alter table web_sales modify column ws_bill_hdemo_sk set stats('row_count'='7.2001237E7', 'ndv'='7251.0', 'num_nulls'='18011.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='7200')
"""
        sql """
alter table web_sales modify column ws_sold_date_sk set stats('row_count'='7.2001237E7', 'ndv'='1823.0', 'num_nulls'='39474.0', 'data_size'='5.75694101E8', 'min_value'='2450816', 'max_value'='2452642')
"""
        sql """
alter table web_sales modify column ws_ship_cdemo_sk set stats('row_count'='7.2001237E7', 'ndv'='1822804.0', 'num_nulls'='17903.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='1920800')
"""
        sql """
alter table web_sales modify column ws_warehouse_sk set stats('row_count'='7.2001237E7', 'ndv'='15.0', 'num_nulls'='17812.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='15')
"""
        sql """
alter table web_sales modify column ws_ext_tax set stats('row_count'='7.2001237E7', 'ndv'='174574.0', 'num_nulls'='17800.0', 'data_size'='2.88004948E8', 'min_value'='0.00', 'max_value'='2682.90')
"""
        sql """
alter table web_sales modify column ws_item_sk set stats('row_count'='7.2001237E7', 'ndv'='205012.0', 'num_nulls'='0.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='204000')
"""
        sql """
alter table web_sales modify column ws_ship_hdemo_sk set stats('row_count'='7.2001237E7', 'ndv'='7251.0', 'num_nulls'='17833.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='7200')
"""
        sql """
alter table web_sales modify column ws_ext_wholesale_cost set stats('row_count'='7.2001237E7', 'ndv'='393180.0', 'num_nulls'='17814.0', 'data_size'='2.88004948E8', 'min_value'='1.00', 'max_value'='10000.00')
"""
        sql """
alter table web_sales modify column ws_net_paid_inc_ship_tax set stats('row_count'='7.2001237E7', 'ndv'='2384404.0', 'num_nulls'='0.0', 'data_size'='2.88004948E8', 'min_value'='0.00', 'max_value'='44479.52')
"""
        sql """
alter table web_sales modify column ws_ext_discount_amt set stats('row_count'='7.2001237E7', 'ndv'='960285.0', 'num_nulls'='17890.0', 'data_size'='2.88004948E8', 'min_value'='0.00', 'max_value'='29982.00')
"""
        sql """
alter table web_sales modify column ws_web_page_sk set stats('row_count'='7.2001237E7', 'ndv'='2032.0', 'num_nulls'='17920.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='2040')
"""
        sql """
alter table web_sales modify column ws_sales_price set stats('row_count'='7.2001237E7', 'ndv'='29057.0', 'num_nulls'='18005.0', 'data_size'='2.88004948E8', 'min_value'='0.00', 'max_value'='300.00')
"""
        sql """
alter table web_sales modify column ws_ext_list_price set stats('row_count'='7.2001237E7', 'ndv'='1129644.0', 'num_nulls'='18001.0', 'data_size'='2.88004948E8', 'min_value'='1.02', 'max_value'='29997.00')
"""
        sql """
alter table web_sales modify column ws_list_price set stats('row_count'='7.2001237E7', 'ndv'='29396.0', 'num_nulls'='17824.0', 'data_size'='2.88004948E8', 'min_value'='1.00', 'max_value'='300.00')
"""
        sql """
alter table web_sales modify column ws_bill_addr_sk set stats('row_count'='7.2001237E7', 'ndv'='998891.0', 'num_nulls'='17801.0', 'data_size'='5.76009896E8', 'min_value'='1', 'max_value'='1000000')
"""
        sql """
alter table web_site modify column web_street_name set stats('row_count'='24.0', 'ndv'='24.0', 'num_nulls'='0.0', 'data_size'='219.0', 'min_value'='11th ', 'max_value'='Wilson Ridge')
"""
        sql """
alter table web_site modify column web_suite_number set stats('row_count'='24.0', 'ndv'='20.0', 'num_nulls'='0.0', 'data_size'='196.0', 'min_value'='Suite 130', 'max_value'='Suite U')
"""
        sql """
alter table web_site modify column web_mkt_class set stats('row_count'='24.0', 'ndv'='18.0', 'num_nulls'='0.0', 'data_size'='758.0', 'min_value'='About rural reasons shall no', 'max_value'='Wide, final representat')
"""
        sql """
alter table web_site modify column web_class set stats('row_count'='24.0', 'ndv'='1.0', 'num_nulls'='0.0', 'data_size'='168.0', 'min_value'='Unknown', 'max_value'='Unknown')
"""
        sql """
alter table web_site modify column web_market_manager set stats('row_count'='24.0', 'ndv'='21.0', 'num_nulls'='0.0', 'data_size'='294.0', 'min_value'='Albert Leung', 'max_value'='Zachery Oneil')
"""
        sql """
alter table web_site modify column web_company_name set stats('row_count'='24.0', 'ndv'='6.0', 'num_nulls'='0.0', 'data_size'='97.0', 'min_value'='able', 'max_value'='pri')
"""
        sql """
alter table web_site modify column web_zip set stats('row_count'='24.0', 'ndv'='14.0', 'num_nulls'='0.0', 'data_size'='120.0', 'min_value'='28828', 'max_value'='78828')
"""
        sql """
alter table web_site modify column web_country set stats('row_count'='24.0', 'ndv'='1.0', 'num_nulls'='0.0', 'data_size'='312.0', 'min_value'='United States', 'max_value'='United States')
"""
        sql """
alter table web_site modify column web_company_id set stats('row_count'='24.0', 'ndv'='6.0', 'num_nulls'='0.0', 'data_size'='96.0', 'min_value'='1', 'max_value'='6')
"""
        sql """
alter table web_site modify column web_mkt_id set stats('row_count'='24.0', 'ndv'='6.0', 'num_nulls'='0.0', 'data_size'='96.0', 'min_value'='1', 'max_value'='6')
"""
        sql """
alter table web_site modify column web_manager set stats('row_count'='24.0', 'ndv'='19.0', 'num_nulls'='0.0', 'data_size'='297.0', 'min_value'='Adam Stonge', 'max_value'='Tommy Jones')
"""
        sql """
alter table web_site modify column web_site_sk set stats('row_count'='24.0', 'ndv'='24.0', 'num_nulls'='0.0', 'data_size'='192.0', 'min_value'='1', 'max_value'='24')
"""
        sql """
alter table web_site modify column web_city set stats('row_count'='24.0', 'ndv'='11.0', 'num_nulls'='0.0', 'data_size'='232.0', 'min_value'='Centerville', 'max_value'='Salem')
"""
        sql """
alter table web_site modify column web_rec_start_date set stats('row_count'='24.0', 'ndv'='4.0', 'num_nulls'='0.0', 'data_size'='96.0', 'min_value'='1997-08-16', 'max_value'='2001-08-16')
"""
        sql """
alter table web_site modify column web_street_number set stats('row_count'='24.0', 'ndv'='14.0', 'num_nulls'='0.0', 'data_size'='70.0', 'min_value'='184', 'max_value'='973')
"""
        sql """
alter table web_site modify column web_state set stats('row_count'='24.0', 'ndv'='9.0', 'num_nulls'='0.0', 'data_size'='48.0', 'min_value'='AL', 'max_value'='TN')
"""
        sql """
alter table web_site modify column web_tax_percentage set stats('row_count'='24.0', 'ndv'='8.0', 'num_nulls'='0.0', 'data_size'='96.0', 'min_value'='0.00', 'max_value'='0.12')
"""
        sql """
alter table web_site modify column web_county set stats('row_count'='24.0', 'ndv'='9.0', 'num_nulls'='0.0', 'data_size'='331.0', 'min_value'='Barrow County', 'max_value'='Ziebach County')
"""
        sql """
alter table web_site modify column web_site_id set stats('row_count'='24.0', 'ndv'='12.0', 'num_nulls'='0.0', 'data_size'='384.0', 'min_value'='AAAAAAAAABAAAAAA', 'max_value'='AAAAAAAAOAAAAAAA')
"""
        sql """
alter table web_site modify column web_mkt_desc set stats('row_count'='24.0', 'ndv'='15.0', 'num_nulls'='0.0', 'data_size'='1561.0', 'min_value'='Acres see else children. Mutual too', 'max_value'='Well similar decisions used to keep hardly democratic, personal priorities.')
"""
        sql """
alter table web_site modify column web_gmt_offset set stats('row_count'='24.0', 'ndv'='2.0', 'num_nulls'='0.0', 'data_size'='96.0', 'min_value'='-6.00', 'max_value'='-5.00')
"""
        sql """
alter table web_site modify column web_street_type set stats('row_count'='24.0', 'ndv'='15.0', 'num_nulls'='0.0', 'data_size'='96.0', 'min_value'='Avenue', 'max_value'='Wy')
"""
        sql """
alter table web_site modify column web_open_date_sk set stats('row_count'='24.0', 'ndv'='12.0', 'num_nulls'='0.0', 'data_size'='192.0', 'min_value'='2450628', 'max_value'='2450807')
"""
        sql """
alter table web_site modify column web_rec_end_date set stats('row_count'='24.0', 'ndv'='3.0', 'num_nulls'='12.0', 'data_size'='96.0', 'min_value'='1999-08-16', 'max_value'='2001-08-15')
"""
        sql """
alter table web_site modify column web_name set stats('row_count'='24.0', 'ndv'='4.0', 'num_nulls'='0.0', 'data_size'='144.0', 'min_value'='site_0', 'max_value'='site_3')
"""
        sql """
alter table web_site modify column web_close_date_sk set stats('row_count'='24.0', 'ndv'='8.0', 'num_nulls'='4.0', 'data_size'='192.0', 'min_value'='2443328', 'max_value'='2447131')
"""
    }

}
