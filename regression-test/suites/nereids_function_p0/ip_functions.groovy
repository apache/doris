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
suite("ip_functions") {
    sql "set batch_size = 4096;"

    qt_ip1 "SELECT ipv4_num_to_string(-1);"
    qt_ip2 "SELECT ipv4_num_to_string(2130706433);"
    qt_ip3 "SELECT ipv4_num_to_string(4294967298);"
    qt_ip4 "SELECT ipv4_num_to_string(3232235521);"

    qt_ip5 "SELECT inet_ntoa(-1);"
    qt_ip6 "SELECT inet_ntoa(2130706433);"
    qt_ip7 "SELECT inet_ntoa(4294967298);"
    qt_ip8 "SELECT inet_ntoa(3232235521);"

    qt_ip9  "SELECT ipv4_string_to_num('127.0.0.1');"
    qt_ip10 "SELECT ipv4_string_to_num_or_null('');"
    qt_ip11 "SELECT ipv4_string_to_num_or_default('');"
    qt_ip11_1 "SELECT ipv4_string_to_num_or_default('127.0.0.1');"
    qt_ip11_2 "SELECT ipv4_string_to_num_or_default('abc');"
    qt_ip11_3 "SELECT ipv4_string_to_num_or_default(NULL);"
    qt_ip12 "SELECT inet_aton('192.168.0.1');"
    qt_ip12_1 "SELECT inet_aton('192.168');"
    qt_ip12_2 "SELECT inet_aton('');"
    qt_ip12_3 "SELECT inet_aton(NULL);"

    qt_ip13 "SELECT ipv6_num_to_string(unhex('0A0005091'));"
    qt_ip14 "SELECT ipv6_num_to_string(unhex('2A0206B8000000000000000000000011'));"
    qt_ip15 "SELECT ipv6_num_to_string(unhex('FDFE0000000000005A55CAFFFEFA9089'));"
    qt_ip16 "SELECT ipv6_num_to_string(unhex(''));"
    qt_ip17 "SELECT ipv6_num_to_string(unhex('KK'));"
    qt_ip18 "SELECT ipv6_num_to_string(unhex('0A000509'));"
    qt_ip19 "SELECT ipv6_num_to_string(unhex('abcd123456'));"
    qt_ip20 "SELECT ipv6_num_to_string(unhex('ffffffffffffffffffffffffffffffffffffffffffffffffffffff'));"
    qt_ip21 "SELECT inet6_ntoa(unhex('0A0005091'));"
    qt_ip22 "SELECT inet6_ntoa(unhex('2A0206B8000000000000000000000011'));"
    qt_ip23 "SELECT inet6_ntoa(unhex(NULL));"
    qt_ip24 "SELECT inet6_ntoa(unhex('00000000000000000000000000000000'));"
    qt_ip25 "SELECT inet6_ntoa(unhex('0000000000000000000000000000'));"
    qt_ip26 "SELECT inet6_ntoa(unhex('000'));"
    qt_ip27 "SELECT inet6_ntoa(unhex('aaaaaaaaFFFFFFFFFFFFFFFFaaaaaaaa'));"
    qt_ip28 "SELECT inet6_ntoa(unhex('aaaa@#'));"
    qt_ip29 "SELECT inet6_ntoa(unhex('\0'));"
    qt_ip30 "SELECT inet6_ntoa(unhex('00000000000000000000FFFF7F000001'));"

    qt_ip31 "SELECT hex(ipv6_string_to_num('192.168.0.1'));"
    qt_ip32 "SELECT hex(ipv6_string_to_num('2a02:6b8::11'));"
    qt_ip33 "SELECT hex(ipv6_string_to_num('::'));"
    qt_ip34 "SELECT hex(ipv6_string_to_num('a0:50:9100::'));"
    qt_ip35 "SELECT hex(ipv6_string_to_num_or_default('192.168.0.1'));"
    qt_ip36 "SELECT hex(ipv6_string_to_num_or_default('2a02:6b8::11'));"
    qt_ip37 "SELECT hex(ipv6_string_to_num_or_default('::'));"
    qt_ip38 "SELECT hex(ipv6_string_to_num_or_default('KK'));"
    qt_ip39 "SELECT hex(ipv6_string_to_num_or_default('ffffffffffffffffffffffffffffffffffffffffffffffffffffff'));"
    qt_ip40 "SELECT hex(ipv6_string_to_num_or_null('192.168.0.1'));"
    qt_ip41 "SELECT hex(ipv6_string_to_num_or_null('2a02:6b8::11'));"
    qt_ip42 "SELECT hex(ipv6_string_to_num_or_null('::'));"
    qt_ip43 "SELECT hex(ipv6_string_to_num_or_null('KK'));"
    qt_ip44 "SELECT hex(ipv6_string_to_num_or_null('ffffffffffffffffffffffffffffffffffffffffffffffffffffff'));"
    qt_ip45 "SELECT hex(ipv6_string_to_num_or_null(''));"
    qt_ip46 "SELECT hex(ipv6_string_to_num_or_null(NULL));"
    qt_ip47 "SELECT hex(ipv6_string_to_num_or_null('\0'));"
    qt_ip48 "SELECT hex(ipv6_string_to_num_or_null('00'));"
    qt_ip49 "SELECT hex(ipv6_string_to_num_or_null('aaaa:aaaa:ffff:ffff:ffff:ffff:aaaa:aaaa'));"
    qt_ip50 "SELECT hex(inet6_aton('192.168.0.1'));"
    qt_ip51 "SELECT hex(inet6_aton('2a02:6b8::11'));"
    qt_ip51_1 "SELECT hex(inet6_aton(''));"
    qt_ip51_2 "SELECT hex(inet6_aton(NULL));"
    qt_ip51_3 "SELECT hex(inet6_aton('KK'));"

    qt_ip52 "SELECT is_ipv4_string('255.255.255.255');"
    qt_ip53 "SELECT is_ipv4_string('255.255.255.256');"
    qt_ip54 "SELECT is_ipv6_string('2001:5b0:23ff:fffa::113');"
    qt_ip55 "SELECT is_ipv6_string('2001:da8:e000:1691:2eaa:7eff:ffe7:7924e');"

    qt_ip56 "SELECT is_ipv4_compat(inet6_aton('::10.0.5.9'));"
    qt_ip57 "SELECT is_ipv4_compat(inet6_aton('::ffff:10.0.5.9'));"
    qt_ip58 "SELECT is_ipv4_compat(inet6_aton('::'));"
    qt_ip59 "SELECT is_ipv4_compat(inet6_aton('::c0a8:0001'));"
    qt_ip60 "SELECT is_ipv4_compat(inet6_aton('::0.0.0.0'));"
    qt_ip61 "SELECT is_ipv4_compat(inet6_aton('::255.255.255.255'));"
    qt_ip61_1 "SELECT is_ipv4_compat(inet6_aton(''));"
    qt_ip61_2 "SELECT is_ipv4_compat(inet6_aton(NULL));"
    qt_ip61_3 "SELECT is_ipv4_compat(inet6_aton('KK'));"

    qt_ip62 "SELECT is_ipv4_mapped(inet6_aton('::10.0.5.9'));"
    qt_ip63 "SELECT is_ipv4_mapped(inet6_aton('::ffff:10.0.5.9'));"
    qt_ip64 "SELECT is_ipv4_mapped(inet6_aton('::'));"
    qt_ip65 "SELECT is_ipv4_mapped(inet6_aton('::ffff:c0a8:0001'));"
    qt_ip65_1 "SELECT is_ipv4_mapped(inet6_aton(''));"
    qt_ip65_2 "SELECT is_ipv4_mapped(inet6_aton(NULL));"
    qt_ip65_3 "SELECT is_ipv4_mapped(inet6_aton('KK'));"
}