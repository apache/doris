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

package org.apache.doris.catalog.authorizer;

import java.util.Map;

public class RangerHiveAuthorizerProvider {

    private static volatile Map<String, RangerHivePlugin> hivePluginMap = null;

    /**
     * if some catalogs use a same ranger hive service, make them share the same authorizer plugin
     *
     * @param serviceUrl url of ranger admin
     * @param serviceName name of hive service in ranger admin
     * @return
     */
    public static RangerHivePlugin getHivePlugin(String serviceUrl, String serviceName) {
        String id = serviceUrl + serviceName;

        if (!hivePluginMap.containsKey(id)) {
            synchronized (RangerHiveAuthorizerProvider.class) {
                if (!hivePluginMap.containsKey(id)) {
                    RangerHivePlugin plugin = new RangerHivePlugin(serviceUrl + serviceName);
                    plugin.init();

                    hivePluginMap.put(id, plugin);
                }
            }
        }

        return hivePluginMap.get(id);
    }
}
