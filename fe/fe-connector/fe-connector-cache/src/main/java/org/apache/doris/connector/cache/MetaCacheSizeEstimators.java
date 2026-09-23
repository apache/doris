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

package org.apache.doris.connector.cache;

/** Shared estimators for small or structurally unknown metadata-cache entries. */
public final class MetaCacheSizeEstimators {
    private MetaCacheSizeEstimators() {
    }

    /**
     * Returns a visit-bounded complete graph estimator. Values that exceed the visit budget are rejected from a
     * weighted cache instead of admitting an arbitrarily low sample. Large connector values should use a
     * type-specific construction-time estimator that counts their payload in a cheap linear pass.
     */
    public static <K, V> MetaCacheSizeEstimator<K, V> reflective() {
        return (key, value) -> MetaCacheSizeEstimate.complete(JvmSizeUtils.saturatedAdd(
                ReflectiveObjectSizeEstimator.estimateComplete(key),
                ReflectiveObjectSizeEstimator.estimateComplete(value)));
    }
}
