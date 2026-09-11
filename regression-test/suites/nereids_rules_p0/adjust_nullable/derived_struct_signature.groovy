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

suite("derived_struct_signature") {
    order_qt_left_named_struct """
        select count(*) as c
        from numbers('number'='2') l
        left join numbers('number'='1') r
          on l.number = r.number
         and length(to_json(named_struct('l', l.number, 'r', r.number))) > 0
    """

    order_qt_inner_named_struct """
        select count(*) as c
        from numbers('number'='2') l
        inner join numbers('number'='1') r
          on l.number = r.number
         and length(to_json(named_struct('l', l.number, 'r', r.number))) > 0
    """

    order_qt_left_without_struct """
        select count(*) as c
        from numbers('number'='2') l
        left join numbers('number'='1') r on l.number = r.number
    """

    order_qt_left_preserved_side_only """
        select count(*) as c
        from numbers('number'='2') l
        left join numbers('number'='1') r
          on l.number = r.number
         and length(to_json(named_struct('l', l.number))) > 0
    """

    order_qt_left_struct """
        select count(*) as c
        from numbers('number'='2') l
        left join numbers('number'='1') r
          on l.number = r.number
         and length(to_json(struct(l.number, r.number))) > 0
    """

    order_qt_right_named_struct """
        select count(*) as c
        from numbers('number'='1') l
        right join numbers('number'='2') r
          on l.number = r.number
         and length(to_json(named_struct('l', l.number, 'r', r.number))) > 0
    """

    order_qt_full_named_struct """
        select count(*) as c
        from numbers('number'='2') l
        full join numbers('number'='1') r
          on l.number = r.number
         and length(to_json(named_struct('l', l.number, 'r', r.number))) > 0
    """

    order_qt_nested_struct """
        select count(*) as c
        from numbers('number'='2') l
        left join numbers('number'='1') r
          on l.number = r.number
         and length(to_json(named_struct('nested', struct(l.number, r.number)))) > 0
    """

    qt_map_entries_nested_struct """
        select if(cast(map_from_entries(map_entries(map(
            cast('2026-01-01 00:00:00' as datetimev2(0)),
            struct(cast('2026-01-01 00:00:00.123456' as datetimev2(6)))))) as string)
            like '%.123456%', 1, 0)
    """
}
