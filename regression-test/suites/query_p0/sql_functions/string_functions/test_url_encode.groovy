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

suite("test_url_encode") {
    sql " drop table if exists test_url_encode"
    sql """
        create table test_url_encode (
            k0 int,
            a string not null,
            b string null
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    order_qt_empty_nullable "select url_encode(b) from test_url_encode"
    order_qt_empty_not_nullable "select url_encode(a) from test_url_encode"

    sql """ insert into test_url_encode values (1, 'ABCDEFGHIJKLMNOPQRSTUWXYZ', 'ABCDEFGHIJKLMNOPQRSTUWXYZ'),
        (2, '1234567890', '1234567890'), (3, '~!@#%^&*()<>?,./:{}|[]\\_+-=', '~!@#%^&*()<>?,./:{}|[]\\_+-='),
        (4, '', ''), (5, '/home/doris/directory/', '/home/doris/directory/'), (6, '', null);
    """

    order_qt_nullable "select url_encode(b) from test_url_encode"
    order_qt_not_nullable "select url_encode(a) from test_url_encode"
    order_qt_nullable_no_null "select url_encode(nullable(a)) from test_url_encode"
    order_qt_const_nullable "select url_encode('') from test_url_encode" // choose one case to test const multi-rows
    order_qt_const_not_nullable "select url_encode('/home/doris/directory/')"
    order_qt_const_nullable_no_null "select url_encode('/home/doris/directory/')"
}
