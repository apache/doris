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

suite("test_mask_function") {
    sql "set enable_fallback_to_original_planner=false;"
    sql """
        drop table if exists table_mask_test;
    """
    
    sql """
        create table table_mask_test (
        id int not null,
        name varchar(20) not null,
        phone char(11) null
        )
        ENGINE=OLAP
        distributed by hash(id)
        properties(
        'replication_num' = '1'
        );
    """

    sql """
        insert into table_mask_test values
            (1, 'Jack Hawk', '18112349876'),
            (2, 'Sam Smith', null),
            (3, 'Tom Cruise', '18212349876'),
            (4, 'Bruce Willis', null),
            (5, 'Ming Li', '19943216789'),
            (6, 'Meimei Han', '13556780000')
        ;
    """

    qt_select_all """
        select * from table_mask_test order by id;
    """

    qt_select_mask """
        select name, mask(name), mask(name, '*'), mask(name, '*', '#') from table_mask_test order by id;
    """

    qt_select_mask_nullable """
        select phone, mask(phone), mask(phone, '*'), mask(phone, '*', '#', '@') from table_mask_test order by id;
    """

    qt_select_mask_first_n """
        select name, mask_first_n(name), mask_first_n(name, 3), mask_first_n(name, 100) from table_mask_test order by id;
    """

    qt_select_mask_first_n_nullable """
        select phone, mask_first_n(phone), mask_first_n(phone, 3), mask_first_n(phone, 100) from table_mask_test order by id;
    """

    qt_select_mask_last_n """
        select name, mask_last_n(name), mask_last_n(name, 3), mask_last_n(name, 100) from table_mask_test order by id;
    """

    qt_select_mask_last_n_nullable """
        select phone, mask_last_n(phone), mask_last_n(phone, 3), mask_last_n(phone, 100) from table_mask_test order by id;
    """

    qt_select_digital_masking """
        select digital_masking(13812345678);
    """

    test {
        sql """
            select mask('abcd', name) from table_mask_test order by id;
        """
        exception "Argument at index 1 for function mask must be constant"
    }

    test {
        sql """
            select mask('abcd', '>', name) from table_mask_test order by id;
        """
        exception "Argument at index 2 for function mask must be constant"
    }

    test {
        sql """
            select mask('abcd', '>', '<', `name`) from table_mask_test order by id;
        """
        exception "Argument at index 3 for function mask must be constant"
    }

    test {
        sql """ select mask_last_n("12345", -100); """
        exception "function mask_last_n only accept non-negative input for 2nd argument but got -100"
    }
    test {
        sql """ select mask_first_n("12345", -100); """
        exception "function mask_first_n only accept non-negative input for 2nd argument but got -100"
    }
    test {
        sql """ select mask_last_n("12345", id) from table_mask_test; """
        exception "mask_last_n must accept literal for 2nd argument"
    }
    test {
        sql """ select mask_first_n("12345", id) from table_mask_test; """
        exception "mask_first_n must accept literal for 2nd argument"
    }
}
