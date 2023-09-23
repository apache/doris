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
import org.apache.orc.impl.IntegerReader;

import java.util.*;

public class MapssTest extends UDF {
    public HashMap<String, String> evaluate(HashMap<String, String> mp) {
        HashMap<String, String> ans = new HashMap<>();
        for (Map.Entry<String, String> it : mp.entrySet()) {
            ans.put(it.getKey() + "114", it.getValue() + "514");
        }
        return ans;
    }
}
