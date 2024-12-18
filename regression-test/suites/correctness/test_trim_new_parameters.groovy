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

suite("test_trim_new_parameters") {
     sql """ DROP TABLE IF EXISTS tbl_trim_new_parameters """
      sql """
        CREATE TABLE tbl_trim_new_parameters (
            id INT DEFAULT '10',
            username VARCHAR(32) DEFAULT ''
        ) ENGINE=OLAP
        AGGREGATE KEY(id,username)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES (
         "replication_allocation" = "tag.location.default: 1",
         "in_memory" = "false",
         "storage_format" = "V2"
        );
    """
     sql """
        insert into tbl_trim_new_parameters values(1,'abcabccccabc')
    """
    sql """
        insert into tbl_trim_new_parameters values(2,'abcabcabc')
    """
    sql """
        insert into tbl_trim_new_parameters values(3,'')
    """

    List<List<Object>> results = sql "select id,trim(username,'abc') from tbl_trim_new_parameters order by id"

    assertEquals(results.size(), 3)
    assertEquals(results[0][0], 1)
    assertEquals(results[1][0], 2)
    assertEquals(results[2][0], 3)
    assertEquals(results[0][1], 'ccc')
    assertEquals(results[1][1], '')
    assertEquals(results[2][1], '')

    List<List<Object>> trim = sql "select trim('   abc   ')"
    assertEquals(trim[0][0], 'abc')

    List<List<Object>> ltrim = sql "select ltrim('   abc   ')"
    assertEquals(ltrim[0][0], 'abc   ')    

    List<List<Object>> rtrim = sql "select rtrim('   abc   ')"
    assertEquals(rtrim[0][0], '   abc')   

    trim = sql "select trim('abcabcTTTabcabc','abc')"
    assertEquals(trim[0][0], 'TTT')

    ltrim = sql "select ltrim('abcabcTTTbc','abc')"
    assertEquals(ltrim[0][0], 'TTTbc')    

    rtrim = sql "select rtrim('bcTTTabcabc','abc')"
    assertEquals(rtrim[0][0], 'bcTTT')   

    def trim_one = sql "select trim('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaabcTTTabcabcaaaaaaaaaaaaaaaaaaaaaaaaaabaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','a')"
    assertEquals(trim_one[0][0], 'baaaaaaaaaaabcTTTabcabcaaaaaaaaaaaaaaaaaaaaaaaaaab')  
}
