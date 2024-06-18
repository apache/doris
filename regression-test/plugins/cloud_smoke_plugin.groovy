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
import groovy.json.JsonOutput
import org.apache.doris.regression.suite.Suite

Suite.metaClass.compareCloudVersion = { v1, v2 ->
    String[] v1Arr = v1.split("\\.");
    String[] v2Arr = v2.split("\\.");

    for (int i = 0; i < v1Arr.size() || i < v2Arr.size(); ++i) {
        if (i >= v1Arr.size()) {
            return -1;
        }

        if (i >= v2Arr.size()) {
            return 1;
        }

        int n1 = v1Arr[i].toInteger()
        int n2 = v2Arr[i].toInteger()
        if (n1 > n2) {
            return 1
        } else if (n1 < n2) {
            return -1
        } else if (n1 == n2) {
            continue
        }
    }

    return 0;
}

logger.info("Added 'cloud smoke' function to Suite")

