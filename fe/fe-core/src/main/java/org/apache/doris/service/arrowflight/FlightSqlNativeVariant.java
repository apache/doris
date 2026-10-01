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

package org.apache.doris.service.arrowflight;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;

import java.util.Collection;

public final class FlightSqlNativeVariant {
    private FlightSqlNativeVariant() {
    }

    public static boolean isEnabled(ConnectContext context) {
        if (context == null) {
            return false;
        }
        // Schema analysis temporarily installs SET_VAR state under this monitor. Metadata
        // requests must wait for that scope to end instead of observing another query's hints.
        synchronized (context) {
            if (!context.getSessionVariable().isEnableArrowFlightSqlNativeVariant()) {
                return false;
            }
        }
        try {
            Collection<Backend> backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values();
            // A Flight ticket may be proxied through a BE outside the query's result sinks.
            // Require every registered BE, including unknown heartbeat capabilities, to support the format.
            return !backends.isEmpty() && backends.stream().allMatch(Backend::isArrowFlightNativeVariantSupported);
        } catch (AnalysisException e) {
            return false;
        }
    }
}
