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

suite("test_external_credit_data", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_external_credit_data"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""

        sql """switch ${catalog_name};"""
        sql """use regression;"""

        // Table crdmm_data shows the credit card data after desensitization.
        // The query includes the conversion of string types to other types,
        // and the processing of materialized columns for nested subqueries.
        order_qt_credit_data """
        select
        substring(begin_time, 1, 7) as dt,
        case
        when t06_rh_per_hhoossuu_testxx_count>0 then 1
        else 0
        end as hhoossuu,
        case
        when t06_rh_per_ccrr_testxx_count>0 then 1
        else 0
        end as ccrr,
        eudd,
        crda,
        case
        when month_ffzzhh>0
        and month_ffzzhh <= 3000 then 1
        when month_ffzzhh>3000
        and month_ffzzhh <= 5000 then 2
        when month_ffzzhh>5000
        and month_ffzzhh <= 10000 then 3
        when month_ffzzhh>10000
        and month_ffzzhh <= 30000 then 4
        when month_ffzzhh>30000 then 5
        else 0
        end as yue_ffzzhh,
        total_ffzzhh,
        ges_tare_esu,
        crdyy_usrate,
        count(distinct apply_id) as cnt
        from(
        select
        *,
        case
        when t06_rh_edu_level in ('30') then 3
        when t06_rh_edu_level in ('20') then 2
        when t06_rh_edu_level in ('10') then 1
        else 4
        end as eudd,
        case
        when cast(t06_rh_not_destroy_testxx_crdyy_max_cdyy_limit_per_org as int)>0
        and cast(t06_rh_not_destroy_testxx_crdyy_max_cdyy_limit_per_org as int)<= 10000 then 1
        when cast(t06_rh_not_destroy_testxx_crdyy_max_cdyy_limit_per_org as int)>10000
        and cast(t06_rh_not_destroy_testxx_crdyy_max_cdyy_limit_per_org as int)<= 30000 then 2
        when cast(t06_rh_not_destroy_testxx_crdyy_max_cdyy_limit_per_org as int)>30000
        and cast(t06_rh_not_destroy_testxx_crdyy_max_cdyy_limit_per_org as int)<= 50000 then 3
        when cast(t06_rh_not_destroy_testxx_crdyy_max_cdyy_limit_per_org as int)>50000 then 4
        else 0
        end as crda,
        case
        when cast(t06_rh_uncleared_cdyy_testxx_balance_sum as double)>0
        and cast(t06_rh_uncleared_cdyy_testxx_balance_sum as double)<= 10000 then 1
        when cast(t06_rh_uncleared_cdyy_testxx_balance_sum as double)>10000
        and cast(t06_rh_uncleared_cdyy_testxx_balance_sum as double)<= 50000 then 2
        when cast(t06_rh_uncleared_cdyy_testxx_balance_sum as double)>50000
        and cast(t06_rh_uncleared_cdyy_testxx_balance_sum as double)<= 100000 then 3
        when cast(t06_rh_uncleared_cdyy_testxx_balance_sum as double)>100000
        and cast(t06_rh_uncleared_cdyy_testxx_balance_sum as double)<= 200000 then 4
        when cast(t06_rh_uncleared_cdyy_testxx_balance_sum as double)>200000 then 5
        else 0
        end as total_ffzzhh,
        case
        when t06_rh_uncleared_cdyy_testxx_balance_sum <= 0
        or t06_rh_uncleared_cdyy_testxx_balance_sum = ''
        or t06_rh_uncleared_cdyy_testxx_balance_sum is null then 'No Loan bal'
        when t06_rh_uncleared_cdyy_testxx_limit_amount_sum <= 0
        or t06_rh_uncleared_cdyy_testxx_limit_amount_sum = ''
        or t06_rh_uncleared_cdyy_testxx_limit_amount_sum is null then 'No Loan'
        when greatest(t06_rh_uncleared_cdyy_testxx_balance_sum,
        0)/ t06_rh_uncleared_cdyy_testxx_limit_amount_sum between 0.00 and 0.10 then '0%-10%'
        when greatest(t06_rh_uncleared_cdyy_testxx_balance_sum,
        0)/ t06_rh_uncleared_cdyy_testxx_limit_amount_sum between 0.10 and 0.25 then '10%-25%'
        when greatest(t06_rh_uncleared_cdyy_testxx_balance_sum,
        0)/ t06_rh_uncleared_cdyy_testxx_limit_amount_sum between 0.25 and 0.5 then '25%-50%'
        when greatest(t06_rh_uncleared_cdyy_testxx_balance_sum,
        0)/ t06_rh_uncleared_cdyy_testxx_limit_amount_sum between 0.5 and 0.8 then '50%-80%'
        when greatest(t06_rh_uncleared_cdyy_testxx_balance_sum,
        0)/ t06_rh_uncleared_cdyy_testxx_limit_amount_sum between 0.8 and 0.9 then '80%-90%'
        when greatest(t06_rh_uncleared_cdyy_testxx_balance_sum,
        0)/ t06_rh_uncleared_cdyy_testxx_limit_amount_sum >= 0.9 then '90%+'
        else 'Error'
        end as ges_tare_esu,
        case
        when cast(T06_RH_NOT_DESTROY_testxx_crdyy_cdyy_LIMIT_RATIO as double)>1 then 6
        when cast(T06_RH_NOT_DESTROY_testxx_crdyy_cdyy_LIMIT_RATIO as double)>0.9 then 5
        when cast(T06_RH_NOT_DESTROY_testxx_crdyy_cdyy_LIMIT_RATIO as double)>0.8 then 4
        when cast(T06_RH_NOT_DESTROY_testxx_crdyy_cdyy_LIMIT_RATIO as double)>0.5 then 3
        when cast(T06_RH_NOT_DESTROY_testxx_crdyy_cdyy_LIMIT_RATIO as double)>0.25 then 2
        when cast(T06_RH_NOT_DESTROY_testxx_crdyy_cdyy_LIMIT_RATIO as double)>0 then 1
        else 0
        end as crdyy_usrate
        from
        crdmm_data) a
        group by
        substring(begin_time, 1, 7),
        case
        when t06_rh_per_hhoossuu_testxx_count>0 then 1
        else 0
        end,
        case
        when t06_rh_per_ccrr_testxx_count>0 then 1
        else 0
        end,
        eudd,
        crda,
        case
        when month_ffzzhh>0
        and month_ffzzhh <= 3000 then 1
        when month_ffzzhh>3000
        and month_ffzzhh <= 5000 then 2
        when month_ffzzhh>5000
        and month_ffzzhh <= 10000 then 3
        when month_ffzzhh>10000
        and month_ffzzhh <= 30000 then 4
        when month_ffzzhh>30000 then 5
        else 0
        end,
        total_ffzzhh,
        ges_tare_esu,
        crdyy_usrate
        LIMIT 0,
        200;
        """

        sql """drop catalog if exists ${catalog_name};"""
    }
}
