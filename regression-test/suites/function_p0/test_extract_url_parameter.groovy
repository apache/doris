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

suite("extract_url_parameter") {

    sql """
        drop table if exists extract_table;
    """
    sql """
    create table extract_table (
            name varchar(30)
    ) ENGINE=OLAP
    UNIQUE KEY(`name`)
    DISTRIBUTED BY HASH(`name`) BUCKETS 1
    PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
        insert into extract_table values ('?ada=123&bbb=112&ccc=1234')
    """

    qt_sql """ 
        select extract_url_parameter(name, 'ccc'), length(extract_url_parameter(name, 'ccc')),
        extract_url_parameter(name, 'ada'), length(extract_url_parameter(name, 'ada')),
        extract_url_parameter(name, 'bbb'), length(extract_url_parameter(name, 'bbb'))
        from extract_table
    """

    qt_sql """
        select 
        extract_url_parameter('?ada=123&bbb=112&ccc=1234', "ccc"), length(extract_url_parameter('?ada=123&bbb=112&ccc=1234', "ccc")),
        extract_url_parameter('?ada=123&bbb=112&ccc=1234', "ada"), length(extract_url_parameter('?ada=123&bbb=112&ccc=1234', "ada")),
        extract_url_parameter('?ada=123&bbb=112&ccc=1234', "bbb"), length(extract_url_parameter('?ada=123&bbb=112&ccc=1234', "bbb"))
    """

    sql """
        drop table if exists extract_table;
    """

}
