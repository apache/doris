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

package org.apache.doris.datasource.lance;

/**
 * Three-valued outcome of resolving the dataset a (db, table) name pair points at,
 * produced by {@code LanceExternalCatalog#checkIndexJobDataset}. Durable-action
 * callers (the dispatcher's refresh driver, RESOLVE FORCE_RELEASE) must not fold
 * "verified gone" and "could not tell" together, so the verdict is its own type
 * instead of a nullable locator.
 */
public final class LanceIndexDatasetCheck {
    /** The verdict on the names. */
    public enum Outcome {
        /** The names resolve and carry a normalized durable locator. */
        PRESENT,
        /** The namespace answered that the database or table does not exist. */
        VERIFIED_ABSENT,
        /** The resolution failed (provider unreachable, credentials expired); the cause is logged. */
        UNRESOLVED
    }

    public final Outcome outcome;
    /** The normalized durable locator; non-null only when the outcome is PRESENT. */
    public final String locator;

    private LanceIndexDatasetCheck(Outcome outcome, String locator) {
        this.outcome = outcome;
        this.locator = locator;
    }

    public static LanceIndexDatasetCheck present(String locator) {
        return new LanceIndexDatasetCheck(Outcome.PRESENT, locator);
    }

    public static LanceIndexDatasetCheck verifiedAbsent() {
        return new LanceIndexDatasetCheck(Outcome.VERIFIED_ABSENT, null);
    }

    public static LanceIndexDatasetCheck unresolved() {
        return new LanceIndexDatasetCheck(Outcome.UNRESOLVED, null);
    }
}
