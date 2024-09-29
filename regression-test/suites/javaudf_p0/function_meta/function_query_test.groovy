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

 suite('function_query_test', 'p0,restart_fe') {
     def tableName = 'function_query_test'
     qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

     qt_select1 ''' SELECT java_udf_int_test(1) result; '''
     qt_select2 """ SELECT udaf_my_sum_int(user_id) result FROM ${tableName} ORDER BY result; """
     qt_select3 """ SELECT user_id, e1 FROM ${tableName} lateral view  udtf_int(user_id) temp as e1 order by user_id; """
     qt_select4 """ SELECT java_udf_int_test_global_2(user_id) result FROM ${tableName} ORDER BY result; """
 }