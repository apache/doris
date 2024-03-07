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

suite("inpredicate_with_struct") {
    sql """ set enable_nereids_planner = true;"""
    sql """ set enable_fallback_to_original_planner=false;"""
    // support struct type
    order_qt_in_predicate_11 """
         select struct(1,"2")  in (struct(1,3), null);
    """
    order_qt_in_predicate_12 """
         select struct(1,"2")  not in (struct(1,3), null);
    """
    order_qt_in_predicate_13 """
         select struct(1,"2")  in (struct(1,3), struct(1,2));
    """
    order_qt_in_predicate_14 """
         select struct(1,"2")  not in (struct(1,3), struct(1,2));
    """
    order_qt_in_predicate_15 """
         select struct(1,"2")  in (struct(1,3), struct(1,"2"), struct(1,1));
    """
    order_qt_in_predicate_16 """
         select struct(1,"2")  not in (struct(1,3), struct(1,"2"), struct(1,1));
    """
}

