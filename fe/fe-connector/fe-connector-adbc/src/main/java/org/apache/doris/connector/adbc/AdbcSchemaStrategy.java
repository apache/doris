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

import java.util.concurrent.atomic.AtomicReference;

/**
 * Remembers, per catalog, which call this driver answers table schemas with.
 *
 * <p>A driver's answer is a property of the driver, not of the table, so probing it once and keeping the
 * result is what stops every table in a catalog from paying for one failed remote call. It lives on the
 * connector rather than the per-statement metadata object for the same reason -- a per-statement memo would
 * re-probe on every statement.
 *
 * <p>Only ever moves away from {@link Kind#UNKNOWN}; there is no invalidation, because a driver does not
 * gain or lose a method while a catalog is alive. Replacing the driver file means restarting FE, which
 * builds a new connector.
 */
public final class AdbcSchemaStrategy {

    public enum Kind {
        /** Not probed yet: try getTableSchema first, fall back to executeSchema. */
        UNKNOWN,
        /** getTableSchema works; a later failure is about the table, not the driver. */
        GET_TABLE_SCHEMA,
        /** getTableSchema is not implemented here; go straight to executeSchema. */
        EXECUTE_SCHEMA
    }

    private final AtomicReference<Kind> kind = new AtomicReference<>(Kind.UNKNOWN);

    public Kind get() {
        return kind.get();
    }

    public void set(Kind newKind) {
        kind.set(newKind);
    }
}
