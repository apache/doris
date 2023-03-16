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

import java.util.ArrayList;

public class ArrayReturnArrayStringTest extends UDF {
    public ArrayList<String> evaluate(ArrayList<String> res) {
        String value = "";
        if (res == null) {
            return null;
        }
        for (int i = 0; i < res.size(); ++i) {
            String data = res.get(i);
            if (data != null) {
                value = value + data;
            }
        }
        ArrayList<String> result = new ArrayList<String>();
        result.add(value);
        return result;
    }
}
