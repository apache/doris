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
import org.apache.doris.common.Config;
import org.apache.doris.system.Backend;

import org.apache.arrow.flight.CallStatus;

import java.util.Collection;

public final class FlightSqlNativeVariant {
    private FlightSqlNativeVariant() {
    }

    public static boolean isSupported() {
        try {
            Collection<Backend> backends = Env.getCurrentSystemInfo().getAllBackendsByAllCluster().values();
            // Flight tickets can be proxied through a BE outside the result sinks.
            return !backends.isEmpty() && backends.stream().allMatch(Backend::isArrowFlightNativeVariantSupported);
        } catch (AnalysisException e) {
            return false;
        }
    }

    static void requireSupported() {
        if (!Config.enable_variant_v2) {
            throw CallStatus.UNIMPLEMENTED.withDescription(
                    "Native Arrow Flight output only supports Variant V2, not legacy Variant; "
                            + "cast the result to STRING for text output").toRuntimeException();
        }
        if (!isSupported()) {
            // Never silently change the wire type while a cluster is being upgraded.
            throw CallStatus.UNIMPLEMENTED.withDescription(
                    "Native Arrow Variant requires support from every registered BE; "
                            + "complete the BE upgrade or cast the result to STRING").toRuntimeException();
        }
    }
}
