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

suite("test_topn_embedded_nul") {
    // A STRING value is binary, so a NUL byte inside a key must not truncate
    // the JSON key that topn() emits, and two keys differing after the NUL
    // must stay distinct in the result.
    qt_topn_single_key """
        SELECT topn(s, 1) FROM (SELECT concat('a', unhex('00'), 'b') AS s) t
    """

    qt_topn_distinct_keys """
        SELECT topn(s, 2) FROM (
            SELECT concat('a', unhex('00'), 'b') AS s
            UNION ALL SELECT concat('a', unhex('00'), 'b')
            UNION ALL SELECT concat('a', unhex('00'), 'c')
        ) t
    """

    qt_topn_array_element_hex """
        SELECT hex(element_at(topn_array(s, 1), 1)) FROM (
            SELECT concat('a', unhex('00'), 'b') AS s
        ) t
    """
}
