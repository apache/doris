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

 suite('test_dictionary_restart', 'p0,restart_fe') {
    sql "use test_dictionary_upgrade"

    // validate dict count
    def dict_res = sql "show dictionaries"
    log.info("Dictionaries after restart: " + dict_res.toString())
    assertTrue(dict_res.size() == 2)

    // validate dict names
    def remaining_dicts = dict_res.collect { it[1] }
    assertTrue(remaining_dicts.contains("user_dict"))
    assertTrue(remaining_dicts.contains("area_dict"))

    // validate dict status NORMAL
    waitAllDictionariesReady()

    // validate dict version and get it
    sql "refresh dictionary area_dict"
    qt_sql "select dict_get('test_dictionary_upgrade.area_dict', 'city', cast('2001:0db8:85a3:0000:0000:8a2e:0370:7334' as ipv6))"
}