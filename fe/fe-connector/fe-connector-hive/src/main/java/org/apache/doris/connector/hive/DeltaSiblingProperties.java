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

package org.apache.doris.connector.hive;

import java.util.HashMap;
import java.util.Map;

/** The gateway and its Delta sibling share HMS/storage settings and catalog-owned caches. */
final class DeltaSiblingProperties {
    private DeltaSiblingProperties() {
    }

    static Map<String, String> synthesize(Map<String, String> gatewayProperties) {
        Map<String, String> properties = new HashMap<>(gatewayProperties);
        // The sibling must resolve this gateway's HMS tables, even if a filesystem flavor was supplied.
        properties.put("deltalake.catalog.type", "hms");
        return properties;
    }
}
