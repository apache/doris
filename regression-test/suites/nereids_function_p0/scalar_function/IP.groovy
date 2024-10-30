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

suite("nereids_scalar_fn_IP") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'

    def cidr_v6 = 64
	def cidr_v4 = 32
    // for table fn_test_ip_nullable
    qt_sql """ select count() from fn_test_ip_nullable; """
    // test_ip_cidr_to_range_function
    qt_sql_cidr_ipv6 "select id, struct_element(ipv6_cidr_to_range(ip6,$cidr_v6), 'min') as min_range, struct_element(ipv6_cidr_to_range(ip6, $cidr_v6), 'max') as max_range from fn_test_ip_nullable order by id"
    qt_sql_cidr_ipv4 "select id, struct_element(ipv4_cidr_to_range(ip4, $cidr_v4), 'min') as min_range, struct_element(ipv4_cidr_to_range(ip4, $cidr_v4), 'max') as max_range from fn_test_ip_nullable order by id"
    qt_sql_cidr_ipv6_all """ select id, ipv6_cidr_to_range(ip6, 16) from fn_test_ip_nullable order by id; """
    qt_sql_cidr_ipv4_all """ select id, ipv4_cidr_to_range(ip4, 16) from fn_test_ip_nullable order by id; """


    // test IPV4_STRING_TO_NUM/IPV6_STRING_TO_NUM (we have null value in ip4 and ip6 column in fn_test_ip_nullable table)
    test {
        sql 'select id, ipv6_string_to_num(ip6) from fn_test_ip_nullable order by id'
        exception "Null Input"
    }

    test {
        sql "select id, ipv6_string_to_num(ip6_str) from fn_test_ip_nullable order by id"
        exception "Invalid IPv6 value"
    }

    test  {
        sql 'select id, ipv4_string_to_num(ip4) from fn_test_ip_nullable order by id'
        exception "Null Input"
    }

    test {
        sql "select id, ipv4_string_to_num(ip4_str) from fn_test_ip_nullable order by id"
        exception "Invalid IPv4 value"
    }

    // test ipv_num_to_string
    qt_sql_num2string_ipv6 "select id, ipv6_num_to_string(ipv6_string_to_num_or_default(ip6)) from fn_test_ip_nullable order by id"
    qt_sql_num2string_ipv6_str "select id, ipv6_num_to_string(ip6_str) from fn_test_ip_nullable order by id"
    qt_sql_num2string_ipv4 "select id, ipv4_num_to_string(ipv4_string_to_num_or_default(ip4)) from fn_test_ip_nullable order by id"
    qt_sql_num2string_ipv4_str "select id, ipv4_num_to_string(ip4_str) from fn_test_ip_nullable order by id"

    // test INET_NTOA/INET6_NTOA
    qt_sql_inet6_ntoa "select id, inet6_ntoa(ipv6_string_to_num_or_default(ip6)) from fn_test_ip_nullable order by id"
    qt_sql_inet6_ntoa_str "select id, inet6_ntoa(ip6_str) from fn_test_ip_nullable order by id"
    qt_sql_inet_ntoa "select id, inet_ntoa(ipv4_string_to_num_or_default(ip4)) from fn_test_ip_nullable order by id"
    qt_sql_inet_ntoa_str "select id, inet_ntoa(ip4_str) from fn_test_ip_nullable order by id"

    // test IPV4_STRING_TO_NUM_OR_DEFAULT/IPV6_STRING_TO_NUM_OR_DEFAULT
    qt_sql_string2num_or_default_ipv6 "select id, hex(ipv6_string_to_num_or_default(ip6)) from fn_test_ip_nullable order by id"
    qt_sql_string2num_or_default_ipv6_str "select id, hex(ipv6_string_to_num_or_default(ip6_str)) from fn_test_ip_nullable order by id"
    qt_sql_string2num_or_default_ipv4 "select id, ipv4_string_to_num_or_default(ip4) from fn_test_ip_nullable order by id"
    qt_sql_string2num_or_default_ipv4_str "select id, ipv4_string_to_num_or_default(ip4_str) from fn_test_ip_nullable order by id"

    // test IPV4_STRING_TO_NUM_OR_NULL/IPV6_STRING_TO_NUM_OR_NULL
    qt_sql_string2num_or_null_ipv6 "select id, hex(ipv6_string_to_num_or_null(ip6)) from fn_test_ip_nullable order by id"
    qt_sql_string2num_or_null_ipv6_str "select id, hex(ipv6_string_to_num_or_null(ip6_str)) from fn_test_ip_nullable order by id"
    qt_sql_string2num_or_null_ipv4 "select id, ipv4_string_to_num_or_null(ip4) from fn_test_ip_nullable order by id"
    qt_sql_string2num_or_null_ipv4_str "select id, ipv4_string_to_num_or_null(ip4_str) from fn_test_ip_nullable order by id"

    // test IS_IPV4_COMPAT/IS_IPV4_MAPPED
    qt_sql_is_ipv4_compat "select id, is_ipv4_compat(ip6) from fn_test_ip_nullable order by id"
    qt_sql_is_ipv4_compat_str6 "select id, is_ipv4_mapped(INET6_ATON(ip6_str)) from fn_test_ip_nullable order by id"
    qt_sql_is_ipv4_compat_str4 "select id, is_ipv4_mapped(INET6_ATON(ip4_str)) from fn_test_ip_nullable order by id"
    
    qt_sql_is_ipv4_mapped "select id, is_ipv4_mapped(ip6) from fn_test_ip_nullable order by id"
    qt_sql_is_ipv4_mapped_str6 "select id, is_ipv4_mapped(INET6_ATON(ip6_str)) from fn_test_ip_nullable order by id"
    qt_sql_is_ipv4_mapped_str4 "select id, is_ipv4_mapped(INET6_ATON(ip4_str)) from fn_test_ip_nullable order by id"

    // test IS_IP_ADDRESS_IN_RANGE
    def cidr_prefix_v6 = '2001:db8::/32'
    def cidr_prefix_v4 = '::ffff:192.168.0.4/128'    
    qt_sql_is_ip_address_in_range_ipv6 "select id, is_ip_address_in_range(ip6, '$cidr_prefix_v6') from fn_test_ip_nullable order by id"
    test {
        sql "select id, is_ip_address_in_range(ip6_str, '$cidr_prefix_v6') from fn_test_ip_nullable order by id"
        exception "Neither IPv4 nor IPv6"
    }
    qt_sql_is_ip_address_in_range_ipv4 "select id, is_ip_address_in_range(ip4, '$cidr_prefix_v4') from fn_test_ip_nullable order by id"
    test {
        sql "select id, is_ip_address_in_range(ip4_str, '$cidr_prefix_v4') from fn_test_ip_nullable order by id"
        exception "Neither IPv4 nor IPv6"
    }
    qt_sql_is_ip_address_in_range_null "select id, is_ip_address_in_range(ip6, null) from fn_test_ip_nullable order by id"
    qt_sql_is_ip_address_in_range_null_str "select id, is_ip_address_in_range(ip6_str, null) from fn_test_ip_nullable order by id"
    qt_sql_is_ip_address_in_range_null "select id, is_ip_address_in_range(ip4, null) from fn_test_ip_nullable order by id"
    qt_sql_is_ip_address_in_range_null_str "select id, is_ip_address_in_range(ip4_str, null) from fn_test_ip_nullable order by id"

    // test IS_IPV4_STRING/IS_IPV6_STRING
    qt_sql_is_ipv4_string "select id, is_ipv4_string(ip4) from fn_test_ip_nullable order by id"
    qt_sql_is_ipv4_string1 "select id, is_ipv4_string(ip4_str) from fn_test_ip_nullable order by id"
    qt_sql_is_ipv6_string "select id, is_ipv6_string(ip6) from fn_test_ip_nullable order by id"
    qt_sql_is_ipv6_string1 "select id, is_ipv6_string(ip6_str) from fn_test_ip_nullable order by id"
    qt_sql_is_ipv6_string "select id, is_ipv6_string(ip4) from fn_test_ip_nullable order by id"
    qt_sql_is_ipv6_string1 "select id, is_ipv6_string(ip4_str) from fn_test_ip_nullable order by id"
    qt_sql_is_ipv4_string "select id, is_ipv4_string(ip6) from fn_test_ip_nullable order by id"
    qt_sql_is_ipv4_string1 "select id, is_ipv4_string(ip6_str) from fn_test_ip_nullable order by id"

    // test TO_IPV4/TO_IPV6 (we have null value in ip4 and ip6 column in fn_test_ip_nullable table)
    test {
        sql "select id, to_ipv4(ip4) from fn_test_ip_nullable order by id"
        exception "not NULL"
    }

    test {
        sql "select id, to_ipv6(ip6) from fn_test_ip_nullable order by id"
        exception "not NULL"
    }

    // test TO_IPV4_OR_DEFAULT/TO_IPV6_OR_DEFAULT
    qt_sql_to_ipv6_or_default "select id, to_ipv6_or_default(ip6) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv6_or_default_str "select id, to_ipv6_or_default(ip6_str) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv4_or_default "select id, to_ipv4_or_default(ip4) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv4_or_default_str "select id, to_ipv4_or_default(ip4_str) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv6_or_default "select id, to_ipv6_or_default(ip4) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv6_or_default_st "select id, to_ipv6_or_default(ip4_str) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv4_or_default "select id, to_ipv4_or_default(ip6) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv4_or_default_st "select id, to_ipv4_or_default(ip6_str) from fn_test_ip_nullable order by id"

    // test TO_IPV4_OR_NULL/TO_IPV6_OR_NULL
    qt_sql_to_ipv6_or_null "select id, to_ipv6_or_null(ip6) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv6_or_null_str "select id, to_ipv6_or_null(ip6_str) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv4_or_null "select id, to_ipv4_or_null(ip4) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv4_or_null_str "select id, to_ipv4_or_null(ip4_str) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv6_or_null "select id, to_ipv6_or_null(ip4) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv6_or_null_str "select id, to_ipv6_or_null(ip4_str) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv4_or_null "select id, to_ipv4_or_null(ip6) from fn_test_ip_nullable order by id"
    qt_sql_to_ipv4_or_null_str "select id, to_ipv4_or_null(ip6_str) from fn_test_ip_nullable order by id"


    // for table fn_test_ip_not_nullable
    qt_sql_not_null """ select count() from fn_test_ip_not_nullable; """
    // test_ip_cidr_to_range_function
    qt_sql_not_null_cidr_ipv6 "select id, struct_element(ipv6_cidr_to_range(ip6,$cidr_v6), 'min') as min_range, struct_element(ipv6_cidr_to_range(ip6, $cidr_v6), 'max') as max_range from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_cidr_ipv4 "select id, struct_element(ipv4_cidr_to_range(ip4, $cidr_v4), 'min') as min_range, struct_element(ipv4_cidr_to_range(ip4, $cidr_v4), 'max') as max_range from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_cidr_ipv6_all """ select id, ipv6_cidr_to_range(ip6, 16) from fn_test_ip_not_nullable order by id; """
    qt_sql_not_null_cidr_ipv4_all """ select id, ipv4_cidr_to_range(ip4, 16) from fn_test_ip_not_nullable order by id; """


    // test IPV4_STRING_TO_NUM/IPV6_STRING_TO_NUM
    qt_sql_not_null_ipv6_string_to_num 'select id, hex(ipv6_string_to_num(ip6)) from fn_test_ip_not_nullable order by id'

    // string has 'null' this invalid data
    test {
        sql "select id, ipv6_string_to_num(ip6_str) from fn_test_ip_not_nullable order by id"
        exception "Invalid IPv6 value"
    }

    qt_sql_not_null_ipv4_string_to_num 'select id, ipv4_string_to_num(ip4) from fn_test_ip_not_nullable order by id'

    // string has 'null' this invalid data
    test {
        sql "select id, ipv4_string_to_num(ip4_str) from fn_test_ip_not_nullable order by id"
        exception "Invalid IPv4 value"
    }


    // test ipv_num_to_string
    qt_sql_not_null_num2string_ipv6 "select id, ipv6_num_to_string(ipv6_string_to_num_or_default(ip6)) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_num2string_ipv6_str "select id, ipv6_num_to_string(ip6_str) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_num2string_ipv4 "select id, ipv4_num_to_string(ipv4_string_to_num_or_default(ip4)) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_num2string_ipv4_str "select id, ipv4_num_to_string(ip4_str) from fn_test_ip_not_nullable order by id"

    // test INET_NTOA/INET6_NTOA
    qt_sql_not_null_inet6_ntoa "select id, inet6_ntoa(ipv6_string_to_num_or_default(ip6)) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_inet6_ntoa_str "select id, inet6_ntoa(ip6_str) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_inet_ntoa "select id, inet_ntoa(ipv4_string_to_num_or_default(ip4)) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_inet_ntoa_str "select id, inet_ntoa(ip4_str) from fn_test_ip_not_nullable order by id"

    // test IPV4_STRING_TO_NUM_OR_DEFAULT/IPV6_STRING_TO_NUM_OR_DEFAULT
    qt_sql_not_null_string2num_or_default_ipv6 "select id, hex(ipv6_string_to_num_or_default(ip6)) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_string2num_or_default_ipv6_str "select id, hex(ipv6_string_to_num_or_default(ip6_str)) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_string2num_or_default_ipv4 "select id, ipv4_string_to_num_or_default(ip4) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_string2num_or_default_ipv4_str "select id, ipv4_string_to_num_or_default(ip4_str) from fn_test_ip_not_nullable order by id"

    // test IPV4_STRING_TO_NUM_OR_NULL/IPV6_STRING_TO_NUM_OR_NULL
    qt_sql_not_null_string2num_or_null_ipv6 "select id, hex(ipv6_string_to_num_or_null(ip6)) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_string2num_or_null_ipv6_str "select id, hex(ipv6_string_to_num_or_null(ip6_str)) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_string2num_or_null_ipv4 "select id, ipv4_string_to_num_or_null(ip4) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_string2num_or_null_ipv4_str "select id, ipv4_string_to_num_or_null(ip4_str) from fn_test_ip_not_nullable order by id"

    // test IS_IPV4_COMPAT/IS_IPV4_MAPPED
    qt_sql_not_null_is_ipv4_compat "select id, is_ipv4_compat(ip6) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ipv4_compat_str6 "select id, is_ipv4_mapped(INET6_ATON(ip6_str)) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ipv4_compat_str4 "select id, is_ipv4_mapped(INET6_ATON(ip4_str)) from fn_test_ip_not_nullable order by id"
    
    qt_sql_not_null_is_ipv4_mapped "select id, is_ipv4_mapped(ip6) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ipv4_mapped_str6 "select id, is_ipv4_mapped(INET6_ATON(ip6_str)) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ipv4_mapped_str4 "select id, is_ipv4_mapped(INET6_ATON(ip4_str)) from fn_test_ip_not_nullable order by id"

    // test IS_IP_ADDRESS_IN_RANGE
    qt_sql_not_null_is_ip_address_in_range_ipv6 "select id, is_ip_address_in_range(ip6, '$cidr_prefix_v6') from fn_test_ip_not_nullable order by id"

    test {
        sql "select id, is_ip_address_in_range(ip6_str, '$cidr_prefix_v6') from fn_test_ip_not_nullable order by id"
        exception "Neither IPv4 nor IPv6"
    }

    qt_sql_not_null_is_ip_address_in_range_ipv4 "select id, is_ip_address_in_range(ip4, '$cidr_prefix_v4') from fn_test_ip_not_nullable order by id"

    test {
        sql "select id, is_ip_address_in_range(ip4_str, '$cidr_prefix_v4') from fn_test_ip_not_nullable order by id"
        exception "Neither IPv4 nor IPv6"
    }

    qt_sql_not_null_is_ip_address_in_range_null "select id, is_ip_address_in_range(ip6, null) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ip_address_in_range_null_str "select id, is_ip_address_in_range(ip6_str, null) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ip_address_in_range_null "select id, is_ip_address_in_range(ip4, null) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ip_address_in_range_null_str "select id, is_ip_address_in_range(ip4_str, null) from fn_test_ip_not_nullable order by id"

    // test IS_IPV4_STRING/IS_IPV6_STRING
    qt_sql_not_null_is_ipv4_string "select id, is_ipv4_string(ip4) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ipv4_string1 "select id, is_ipv4_string(ip4_str) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ipv6_string "select id, is_ipv6_string(ip6) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ipv6_string1 "select id, is_ipv6_string(ip6_str) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ipv6_string "select id, is_ipv6_string(ip4) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ipv6_string1 "select id, is_ipv6_string(ip4_str) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ipv4_string "select id, is_ipv4_string(ip6) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_is_ipv4_string1 "select id, is_ipv4_string(ip6_str) from fn_test_ip_not_nullable order by id"

    // test TO_IPV4/TO_IPV6
    qt_sql_not_null_to_ipv4 "select id, to_ipv4(ip4) from fn_test_ip_not_nullable order by id"

    test {
        sql "select id, to_ipv4(ip4_str) from fn_test_ip_not_nullable order by id"
        exception "Invalid IPv4 value"
    }
    qt_sql_not_null_to_ipv6 "select id, to_ipv6(ip6) from fn_test_ip_not_nullable order by id"

    test {
        sql "select id, to_ipv6(ip6_str) from fn_test_ip_not_nullable order by id"
        exception "Invalid IPv6 value"
    }

    // test TO_IPV4_OR_DEFAULT/TO_IPV6_OR_DEFAULT
    qt_sql_not_null_to_ipv6_or_default "select id, to_ipv6_or_default(ip6) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv6_or_default_str "select id, to_ipv6_or_default(ip6_str) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv4_or_default "select id, to_ipv4_or_default(ip4) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv4_or_default_str "select id, to_ipv4_or_default(ip4_str) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv6_or_default "select id, to_ipv6_or_default(ip4) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv6_or_default_st "select id, to_ipv6_or_default(ip4_str) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv4_or_default "select id, to_ipv4_or_default(ip6) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv4_or_default_st "select id, to_ipv4_or_default(ip6_str) from fn_test_ip_not_nullable order by id"

    // test TO_IPV4_OR_NULL/TO_IPV6_OR_NULL
    qt_sql_not_null_to_ipv6_or_null "select id, to_ipv6_or_null(ip6) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv6_or_null_str "select id, to_ipv6_or_null(ip6_str) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv4_or_null "select id, to_ipv4_or_null(ip4) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv4_or_null_str "select id, to_ipv4_or_null(ip4_str) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv6_or_null "select id, to_ipv6_or_null(ip4) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv6_or_null_str "select id, to_ipv6_or_null(ip4_str) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv4_or_null "select id, to_ipv4_or_null(ip6) from fn_test_ip_not_nullable order by id"
    qt_sql_not_null_to_ipv4_or_null_str "select id, to_ipv4_or_null(ip6_str) from fn_test_ip_not_nullable order by id"

}