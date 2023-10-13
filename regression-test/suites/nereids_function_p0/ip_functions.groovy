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

    qt_ip1 "SELECT ipv4numtostring(-1);"
    qt_ip2 "SELECT ipv4numtostring(2130706433);"
    qt_ip3 "SELECT ipv4numtostring(4294967298);"
    qt_ip4 "SELECT ipv4numtostring(3232235521);"

    qt_ip5 "SELECT inet_ntoa(-1);"
    qt_ip6 "SELECT inet_ntoa(2130706433);"
    qt_ip7 "SELECT inet_ntoa(4294967298);"
    qt_ip8 "SELECT inet_ntoa(3232235521);"

}