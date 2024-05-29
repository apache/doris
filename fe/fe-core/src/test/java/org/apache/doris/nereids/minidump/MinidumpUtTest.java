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

package org.apache.doris.nereids.minidump;

import org.json.JSONObject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Used for add unit test of minidump
 * Input: minidump filter which describe table information and sessionVariables which different with default
 *        example: MinidumpUtTestData.json. We use this json file as input because it can be modified easily
 * Output: ResultPlan in json format, which we can get information from it directly
 */
class MinidumpUtTest {

    @Disabled
    @Test
    public void testMinidumpUt() {
        Minidump minidump = null;
        String filePath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        String directory = filePath.substring(0, filePath.indexOf("/target/test-classes"));
        String currentMinidumpPath = "/src/test/java/org/apache/doris/nereids/minidump/MinidumpUtTestData.json";
        try {
            minidump = MinidumpUtils.jsonMinidumpLoad(directory + currentMinidumpPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        MinidumpUtils.setConnectContext(minidump);
        JSONObject resultPlan = MinidumpUtils.executeSql("select * from t1 where l1 = 1");
        assert (minidump != null);
        assert (resultPlan != null);
    }
}
