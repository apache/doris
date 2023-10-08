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

package org.apache.doris.statistics;

import java.util.ArrayList;
import java.util.List;

public class StatsMockUtil {

    public static ResultRow mockResultRow(boolean col) {
        List<String> vals = new ArrayList<String>() {{
                add("0");
                add("1");
                add("2");
                add("3");
                add("-1");
                add("5");
                if (col) {
                    add(null);
                } else {
                    add("6");
                }
                add("7");
                add("8");
                add("0");
                add("10");
                add("11");
                add("12");
                add(String.valueOf(System.currentTimeMillis()));
            }};
        return new ResultRow(vals);
    }
}
