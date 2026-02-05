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

suite("covar_samp") {
    sql """
        drop table if exists baseall;
    """
    sql """
    create table baseall(
        id int,
        x double,
        y double
    ) distributed by hash(id) buckets 1
    properties ("replication_num"="1");
    """
    sql """
    insert into baseall values
        (1, 1.0, 2.0),
        (2, 2.0, 3.0),
        (3, 3.0, 4.0),
        (4, 4.0, NULL),
        (5, NULL, 5.0);
    """
    qt_covar_samp """select covar_samp(x,y) from baseall;"""
    qt_covar_samp_group_by """select id, covar_samp(x, y) from baseall group by id order by id;"""
}
