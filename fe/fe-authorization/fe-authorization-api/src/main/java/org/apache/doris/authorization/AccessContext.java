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

package org.apache.doris.authorization;

import java.util.Optional;

/**
 * What surrounds one access decision: the circumstances of the statement asking for it, as opposed to who is
 * asking ({@link AuthorizedSubject}) or about what ({@link AuthorizedResource}).
 *
 * <p>This is where a decision that depends on more than the subject and the resource - the address the client
 * connected from, the time of day, which statement is running - gets its input. A plugin that decides purely
 * from grants ignores it; the built-in model does.</p>
 *
 * <p>Every value is optional because the checks Doris makes do not all originate from a client statement:
 * background jobs and internal maintenance ask the same questions with no connection behind them. Absent
 * means "not applicable here", never "denied" - a plugin that requires a signal it did not get must say so
 * rather than treat the gap as a decision.</p>
 *
 * <p>An implementation is expected to be lazy: the engine hands one to every check, so producing a query id
 * that nobody reads would be paid for on each of them.</p>
 */
public interface AccessContext {

    /** No circumstances known - the check does not come from a client statement. */
    AccessContext NONE = new AccessContext() {
        @Override
        public String toString() {
            return "no access context";
        }
    };

    /** The address the client connected from. */
    default Optional<String> getClientIp() {
        return Optional.empty();
    }

    /** The id of the query being planned, in the form the engine prints it in logs and profiles. */
    default Optional<String> getQueryId() {
        return Optional.empty();
    }
}
