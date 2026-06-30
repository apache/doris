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
import org.junit.Assert

@CompileStatic
class ResultUtils {
    static List<List<Object>> normalizeRows(List<List<Object>> rows) {
        List<List<Object>> normalizedRows = new ArrayList<>()
        rows.each { List<Object> row ->
            List<Object> normalizedRow = new ArrayList<>()
            row.each { Object value ->
                normalizedRow.add(value == null ? null : value.toString())
            }
            normalizedRows.add(normalizedRow)
        }
        return normalizedRows
    }

    static void assertSparkDorisResultEquals(List<List<Object>> sparkRows, List<List<Object>> dorisRows) {
        Assert.assertEquals(normalizeRows(sparkRows), normalizeRows(dorisRows))
    }
}
