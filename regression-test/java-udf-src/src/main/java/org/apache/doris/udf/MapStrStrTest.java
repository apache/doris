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

package org.apache.doris.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.*;

public class MapStrStrTest extends UDF {
    public String evaluate(HashMap<String, String> hashMap) {
        StringBuffer sb = new StringBuffer();
        Set<String> sortSet = new TreeSet<String>();
        for (Map.Entry<String, String> entry : hashMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            sortSet.add(key + value);
        }
        for (String item : sortSet) {
            sb.append(item);
        }
        String ans = sb.toString();
        return ans;
    }
}

