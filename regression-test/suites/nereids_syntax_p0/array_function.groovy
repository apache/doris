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

suite("array_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    qt_0 """SELECT ARRAY_MAP(x->x+1, ARRAY('crqdt', 'oxpaa', 'xwadf', 'znwln'))"""
    qt_1 "SELECT ARRAY_MAP((x,y)->x+y, ARRAY('kdjah', 'ptytj', 'quxhq'), ARRAY('vzhwj', 'bmkrc', 'snaek'))"
    qt_2 "SELECT ARRAY_MAP(x->x+1, ARRAY(87, 33, -49))"
    qt_3 "SELECT ARRAY_MAP((x,y)->x+y, ARRAY(-41, NULL, -18), ARRAY(98, 47, NULL))"
    qt_4 "SELECT ARRAY_MAP(x->x+1, ARRAY(-82.31, -72.18, 35.59, -67.13))"
    qt_5 "SELECT ARRAY_MAP((x,y)->x+y, ARRAY(-37.03, 81.89, 56.38, -36.76), ARRAY(1.56, -14.58, 42.22, -56.13))"    
     test {
         sql "select array(), array(null), array(1), array('abc'), array(null, 1), array(1, null)"
         result([["[]", "[NULL]", "[1]", "[\"abc\"]", "[NULL, 1]", "[1, NULL]"]])
     }

     test {
         sql "select array(), array('a'), array(number, 'a') from numbers('number'='3')"
         result([
             ["[]", "[\"a\"]", "[\"0\", \"a\"]"],
             ["[]", "[\"a\"]", "[\"1\", \"a\"]"],
             ["[]", "[\"a\"]", "[\"2\", \"a\"]"]
         ])
     }

    test {
        sql """
            SELECT [[[2]], [['aa'],[2,1.0]]]
        """
        result([
                ["""[[["2"]], [["aa"], ["2.0", "1.0"]]]"""]
        ])
    }
}
