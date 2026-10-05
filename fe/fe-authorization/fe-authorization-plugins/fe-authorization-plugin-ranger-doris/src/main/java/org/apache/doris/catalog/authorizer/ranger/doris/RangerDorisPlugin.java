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

package org.apache.doris.catalog.authorizer.ranger.doris;

import org.apache.doris.catalog.authorizer.ranger.LoadedRangerPlugin;

import org.apache.ranger.plugin.service.RangerAuthContextListener;

/**
 * The plugin over a Ranger service of type {@code doris}: built with the service's policies or not at all, so
 * that an FE whose instance scope it governs starts with them or does not start; see
 * {@link LoadedRangerPlugin}.
 */
public class RangerDorisPlugin extends LoadedRangerPlugin {
    public RangerDorisPlugin(String serviceName) {
        this(serviceName, null);
    }

    public RangerDorisPlugin(String serviceName, RangerAuthContextListener rangerAuthContextListener) {
        super(serviceName, null, null);
        // Registered before the load, so that the listener hears of the engine the load installs.
        registerAuthContextEventListener(rangerAuthContextListener);
        init();
    }
}
