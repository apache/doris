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
// This file is copied from
// https://github.com/klout/brickhouse/blob/master/src/main/java/brickhouse/udf/json/ConvertToCamelCaseUDF.java
// and modified by Doris

package org.apache.doris.udf.json;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "to_camel_case",
    value = "_FUNC_(a) - Converts a string containing underscores to CamelCase"
)
public class ConvertToCamelCaseUDF extends UDF {

    public String evaluate(String underscore) {
        return ToCamelCase(underscore);
    }

    static public String ToCamelCase(String underscore) {
        StringBuilder sb = new StringBuilder();
        String[] splArr = underscore.toLowerCase().split("_");

        sb.append(splArr[0]);
        for (int i = 1; i < splArr.length; ++i) {
            String word = splArr[i];
            char firstChar = word.charAt(0);
            if (firstChar >= 'a' && firstChar <= 'z') {
                sb.append((char) (word.charAt(0) + 'A' - 'a'));
                sb.append(word.substring(1));
            } else {
                sb.append(word);
            }

        }
        return sb.toString();
    }
}
