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

suite("test_split_part") {
  test {
    sql """
      select
          name
      from
          tpch_tiny_nation
      where
          split_part("bCKHDX07at", "5.7.37", cast(name as int)) is not null;
    """
  }

  qt_1 "select split_part(k8, '1', 1), k8, split_part(concat(k8, '12'), '1', 1) from test_query_db.test order by k8 limit 2;"

  sql """
    DROP TABLE IF EXISTS `test_split_part_non_const`;
  """
  sql """
      CREATE TABLE `test_split_part_non_const`(
      `id` LARGEINT,
      `name` VARCHAR(20),
      `age` SMALLINT,
      `part` int,
      `address` VARCHAR(100),
      `test_t` string,
      `date` DATE
      )
      DUPLICATE KEY (`id`,`name`)
      DISTRIBUTED BY HASH (`id`) BUCKETS 3
      PROPERTIES("replication_num" = "1");
  """
  sql """
      INSERT INTO test_split_part_non_const VALUES (1,"kkk",18, -1, "beijing","a,b,c,d,e,f","2022-06-28"),
          (2, "kkk",18, -2, "shanghai","a,b,c,d,e,f","2022-07-28"),
          (3, "kkk",20, -3, "beijing","a,b,c,d,e,f","2022-06-28"),
          (4, "hhh",45, -4, "beijing","a,b,c,d,e,f","2022-05-28");
  """
  qt_non_const1 """
      select *, split_part(test_t, ',', id) from test_split_part_non_const order by id, name, age;
  """

  qt_non_const2 """
      select *, split_part(test_t, ',c', id) from test_split_part_non_const order by id, name, age;
  """

  qt_non_const3 """
      select *, split_part(test_t, ',', part) from test_split_part_non_const order by id, name, age;
  """

  qt_non_const4 """
      select *, split_part(test_t, ',c', part) from test_split_part_non_const order by id, name, age;
  """
}