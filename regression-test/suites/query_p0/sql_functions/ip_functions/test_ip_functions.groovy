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
suite("test_ip_functions", "arrow_flight_sql") {
    sql "set batch_size = 4096;"

    qt_sql "SELECT ipv4_num_to_string(-1);"
    qt_sql "SELECT ipv4_num_to_string(2130706433);"
    qt_sql "SELECT ipv4_num_to_string(4294967298);"
    qt_sql "SELECT inet_ntoa(3232235521);"

    qt_sql "SELECT ipv4_string_to_num('127.0.0.1');"
    qt_sql "SELECT ipv4_string_to_num_or_null('');"
    qt_sql "SELECT ipv4_string_to_num_or_default('');"
    qt_sql "SELECT ipv4_string_to_num_or_default('127.0.0.1');"
    qt_sql "SELECT ipv4_string_to_num_or_default('abc');"
    qt_sql "SELECT ipv4_string_to_num_or_default(NULL);"
    qt_sql "SELECT inet_aton('192.168.0.1');"
    qt_sql "SELECT inet_aton('192.168');"
    qt_sql "SELECT inet_aton('');"
    qt_sql "SELECT inet_aton(NULL);"

    qt_sql "SELECT ipv6_num_to_string(unhex('0A0005091'));"
    qt_sql "SELECT ipv6_num_to_string(unhex('2A0206B8000000000000000000000011'));"
    qt_sql "SELECT ipv6_num_to_string(unhex('FDFE0000000000005A55CAFFFEFA9089'));"
    qt_sql "SELECT ipv6_num_to_string(unhex(''));"
    qt_sql "SELECT ipv6_num_to_string(unhex('KK'));"
    qt_sql "SELECT ipv6_num_to_string(unhex('0A000509'));"
    qt_sql "SELECT ipv6_num_to_string(unhex('abcd123456'));"
    qt_sql "SELECT ipv6_num_to_string(unhex('ffffffffffffffffffffffffffffffffffffffffffffffffffffff'));"
    qt_sql "SELECT inet6_ntoa(unhex('0A0005091'));"
    qt_sql "SELECT inet6_ntoa(unhex('2A0206B8000000000000000000000011'));"
    qt_sql "SELECT inet6_ntoa(unhex(NULL));"
    qt_sql "SELECT inet6_ntoa(unhex('00000000000000000000000000000000'));"
    qt_sql "SELECT inet6_ntoa(unhex('0000000000000000000000000000'));"
    qt_sql "SELECT inet6_ntoa(unhex('000'));"
    qt_sql "SELECT inet6_ntoa(unhex('aaaaaaaaFFFFFFFFFFFFFFFFaaaaaaaa'));"
    qt_sql "SELECT inet6_ntoa(unhex('aaaa@#'));"
    qt_sql "SELECT inet6_ntoa(unhex('\0'));"
    qt_sql "SELECT inet6_ntoa(unhex('00000000000000000000FFFF7F000001'));"

    qt_sql "SELECT hex(ipv6_string_to_num('192.168.0.1'));"
    qt_sql "SELECT hex(ipv6_string_to_num('2a02:6b8::11'));"
    qt_sql "SELECT hex(ipv6_string_to_num('::'));"
    qt_sql "SELECT hex(ipv6_string_to_num('a0:50:9100::'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_default('192.168.0.1'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_default('2a02:6b8::11'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_default('::'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_default('KK'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_default('ffffffffffffffffffffffffffffffffffffffffffffffffffffff'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_null('192.168.0.1'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_null('2a02:6b8::11'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_null('::'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_null('KK'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_null('ffffffffffffffffffffffffffffffffffffffffffffffffffffff'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_null(''));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_null(NULL));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_null('\0'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_null('00'));"
    qt_sql "SELECT hex(ipv6_string_to_num_or_null('aaaa:aaaa:ffff:ffff:ffff:ffff:aaaa:aaaa'));"
    qt_sql "SELECT hex(inet6_aton('192.168.0.1'));"
    qt_sql "SELECT hex(inet6_aton('2a02:6b8::11'));"
    qt_sql "SELECT hex(inet6_aton(''));"
    qt_sql "SELECT hex(inet6_aton(NULL));"
    qt_sql "SELECT hex(inet6_aton('KK'));"

    qt_sql "SELECT is_ipv4_compat(inet6_aton('::10.0.5.9'));"
    qt_sql "SELECT is_ipv4_compat(inet6_aton('::ffff:10.0.5.9'));"
    qt_sql "SELECT is_ipv4_compat(inet6_aton('::'));"
    qt_sql "SELECT is_ipv4_compat(inet6_aton('::c0a8:0001'));"
    qt_sql "SELECT is_ipv4_compat(inet6_aton('::0.0.0.0'));"
    qt_sql "SELECT is_ipv4_compat(inet6_aton('::255.255.255.255'));"
    qt_sql "SELECT is_ipv4_compat(inet6_aton(''));"
    qt_sql "SELECT is_ipv4_compat(inet6_aton(NULL));"
    qt_sql "SELECT is_ipv4_compat(inet6_aton('KK'));"

    qt_sql "SELECT is_ipv4_mapped(inet6_aton('::10.0.5.9'));"
    qt_sql "SELECT is_ipv4_mapped(inet6_aton('::ffff:10.0.5.9'));"
    qt_sql "SELECT is_ipv4_mapped(inet6_aton('::'));"
    qt_sql "SELECT is_ipv4_mapped(inet6_aton('::ffff:c0a8:0001'));"
    qt_sql "SELECT is_ipv4_mapped(inet6_aton(''));"
    qt_sql "SELECT is_ipv4_mapped(inet6_aton(NULL));"
    qt_sql "SELECT is_ipv4_mapped(inet6_aton('KK'));"

    qt_sql "SELECT ipv6_num_to_string(ipv6_string_to_num('192.168.0.1'));"
    qt_sql "SELECT ipv6_num_to_string(ipv6_string_to_num('::ffff:10.0.5.9'));"
    qt_sql "SELECT ipv6_num_to_string(ipv6_string_to_num('::ffff:c0a8:0001'));"
    qt_sql "SELECT ipv6_num_to_string(ipv6_string_to_num('::'));"
    qt_sql "SELECT ipv6_num_to_string(ipv6_string_to_num('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'));"
}
