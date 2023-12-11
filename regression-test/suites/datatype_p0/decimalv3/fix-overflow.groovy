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

suite("fix-overflow") {
    sql "set check_overflow_for_decimal=true;"
    sql "drop table if exists fix_overflow_l;"
    sql """
        create table fix_overflow_l(k1 decimalv3(38,1)) distributed by hash(k1) properties("replication_num"="1");
    """
    sql "drop table if exists fix_overflow_r;"
    sql """
        create table fix_overflow_r(k1 decimalv3(38,1)) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        insert into fix_overflow_l values(1.1);
    """
    qt_sql """
        select l.k1, r.k1, l.k1 * r.k1 from fix_overflow_l l left join fix_overflow_r r on l.k1=r.k1;
    """
}
