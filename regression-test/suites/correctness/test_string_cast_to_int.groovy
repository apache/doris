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

suite("test_string_cast_to_int") {


   def numberStrings = ["12345","000123","00000","3.13", "000.000", ".0000000", "123.000" , "123e5","12345e-2" , "e3", "abc","3.abc" , "e3","0e3","1e3", "000abc" , ".0000000", ".123456" , "  12345" , "12334   " , "12334556671231231321"]

    numberStrings.each { expr ->
    sql """ set debug_skip_fold_constant = false ;"""

    qt_select_by_fe """
      select cast("${expr}" as int);
    """
    List<List<Object>> feresults = sql """  select cast("${expr}" as int); """;

    sql """ set debug_skip_fold_constant = true ;"""

    qt_select_by_be """
        select cast("${expr}" as int);
    """
    
    List<List<Object>> beresults =  sql """  select cast("${expr}" as int); """;
    assertEquals(feresults[0][0],beresults[0][0]);
    }

}


