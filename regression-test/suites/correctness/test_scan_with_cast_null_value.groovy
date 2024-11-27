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

suite("test_scan_with_cast_null_value") {

  sql """
        DROP TABLE IF EXISTS datetest
    """
    sql """
        create table datetest (
            id int,
            dt date
        )
        DUPLICATE key (id)
        distributed by hash(id) buckets 1
        properties(
        "replication_num" = "1"
        );
    """
    sql """
        insert into datetest values (1, '2024-01-01');
    """
    def expressions = ["=", "<>", "<", ">", "<=", ">="]
  expressions.each { expr ->
    qt_select_empty """
        select dt from datetest WHERE dt ${expr} 1;
    """
  }
    qt_select_empty """
        select dt from datetest WHERE dt in (1);
    """
    qt_select_empty """
        select dt from datetest WHERE dt not in (1);
    """

    sql """
      set enable_fold_constant_by_be = true;
    """
  expressions.each { expr ->
    qt_select_empty_fold_constant"""
        select dt from datetest WHERE dt ${expr} 1;
    """
  }
    qt_select_empty_fold_constant """
        select dt from datetest WHERE dt in (1);
    """
    qt_select_empty_fold_constant """
        select dt from datetest WHERE dt not in (1);
    """
}