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

package org.apache.doris.cluster;

/**
 * Cluster guard interface for enforcing cluster-level policies such as
 * node limits and time-based validity.
 * <p>
 * Business code in fe-core should only depend on this interface.
 * Implementations are discovered via {@link java.util.ServiceLoader}.
 * When no implementation is found on the classpath, a no-op default is used.
 * </p>
 */
public interface ClusterGuard {

    /**
     * Perform initialization during FE startup.
     *
     * @param dorisHomeDir the DORIS_HOME directory
     * @throws ClusterGuardException if initialization fails
     */
    void onStartup(String dorisHomeDir) throws ClusterGuardException;

    /**
     * Check time validity of the cluster guard policy.
     *
     * @throws ClusterGuardException if the time-based policy is violated
     */
    void checkTimeValidity() throws ClusterGuardException;

    /**
     * Check whether the current node count is within allowed limits.
     *
     * @param currentNodeCount the current number of nodes in the cluster
     * @throws ClusterGuardException if the node count exceeds the limit
     */
    void checkNodeLimit(int currentNodeCount) throws ClusterGuardException;

    /**
     * Get the guard information as a JSON string.
     * <p>
     * The content and structure of the returned JSON is entirely
     * determined by the implementation. The core code treats it
     * as an opaque payload.
     * </p>
     *
     * @return a JSON string with guard details, or "{}" if not available
     */
    String getGuardInfo();
}
