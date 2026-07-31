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

package org.apache.doris.connector.adbc;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Remembers, per catalog, that this driver has no partitioned execution.
 *
 * <p>Whether {@code executePartitioned} exists is a property of the driver, not of the query, so probing it
 * once and keeping the answer is what stops every scan in a catalog from paying for a failed remote call.
 * It lives on the connector because a scan plan provider is built per query; a memo held there would
 * re-probe forever. Same reasoning, and the same lifetime, as {@link AdbcSchemaStrategy}.
 *
 * <p><b>Two states, not three.</b> "Not probed yet" and "known to support it" both mean "ask the driver",
 * so they are one state; only "known not to support it" changes what happens next. The flag never moves
 * back: a driver does not gain a method while a catalog is alive, and replacing the driver file means
 * restarting FE, which builds a new connector.
 */
public final class AdbcPartitionedReadSupport {

    private final AtomicBoolean unsupported = new AtomicBoolean(false);

    /** True once the driver has answered {@code NOT_IMPLEMENTED}; scans then plan a single range. */
    public boolean isKnownUnsupported() {
        return unsupported.get();
    }

    /**
     * Records that the driver has no partitioned execution.
     *
     * @return true if this call is the one that recorded it, so the caller logs the downgrade once per
     *         catalog rather than once per query
     */
    public boolean markUnsupported() {
        return unsupported.compareAndSet(false, true);
    }
}
