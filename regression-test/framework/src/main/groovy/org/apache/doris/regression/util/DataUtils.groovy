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

package org.apache.doris.regression.util

import groovy.transform.CompileStatic

@CompileStatic
class DataUtils {
    // null first, order by column.toString asc
    static List<List<Object>> sortByToString(List<List<Object>> originData) {
        def comparator = Comparator.<String>naturalOrder()
        originData.sort(false, { List<Object> row1, List<Object> row2 ->
            for (int i = 0; i < row1.size(); ++i) {
                Object column1 = row1[i]
                Object column2 = row2[i]
                if (column1 == column2) {
                    continue
                }
                if (column1 == null) {
                    return -1
                } else if (column2 == null) {
                    return 1
                }
                int result = comparator.compare(column1.toString(), column2.toString())
                if (result != 0) {
                    return result
                }
            }
            return 0
        })
    }
}
