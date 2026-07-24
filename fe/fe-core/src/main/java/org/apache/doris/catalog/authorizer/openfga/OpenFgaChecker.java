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

package org.apache.doris.catalog.authorizer.openfga;

import java.util.ArrayList;
import java.util.List;

/**
 * Seam over the OpenFGA client so the access controller can be unit tested with a mock and does not
 * depend on the OpenFGA SDK directly.
 *
 * <p>Implementations must fail closed: any transport or protocol error has to return {@code false}
 * (deny) for {@link #check}, never {@code true}. A failing authorization service must not silently
 * grant access.
 */
public interface OpenFgaChecker {
    /**
     * Returns true only if OpenFGA explicitly allows the relationship. Returns false when denied and
     * also when the check cannot be completed (fail closed).
     */
    boolean check(OpenFgaCheckRequest request);

    /**
     * Checks several relationships. Results are returned in the same order as the input. The default
     * implementation issues the checks one by one; an implementation backed by the OpenFGA SDK may
     * override this to use a single server side BatchCheck.
     */
    default List<Boolean> batchCheck(List<OpenFgaCheckRequest> requests) {
        List<Boolean> results = new ArrayList<>(requests.size());
        for (OpenFgaCheckRequest request : requests) {
            results.add(check(request));
        }
        return results;
    }
}
