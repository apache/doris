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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.apache.doris.catalog.authorizer.ranger.BackgroundLoadedRangerPlugin;

import org.apache.ranger.plugin.service.RangerAuthContextListener;

/**
 * The plugin over a Ranger service of type {@code hive}. Loading when it is built, on a thread of its own:
 * binding a catalog to it returns at once, and it is the first check against that catalog that waits for
 * the Ranger admin, see {@link BackgroundLoadedRangerPlugin}. The groups its requests carry are Ranger's
 * own, where Hive's plugin would have asked Hadoop's group mapping, which Doris has no equivalent of.
 */
public class RangerHivePlugin extends BackgroundLoadedRangerPlugin {
    public RangerHivePlugin(String serviceName) {
        this(serviceName, null);
    }

    public RangerHivePlugin(String serviceName, RangerAuthContextListener rangerAuthContextListener) {
        super(serviceName, null, null);
        // Registered before the load starts, so that the listener hears of the engine the load installs.
        registerAuthContextEventListener(rangerAuthContextListener);
        init();
    }
}
