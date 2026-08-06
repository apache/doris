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
 * No-op implementation of {@link ClusterGuard} used when no SPI provider is found.
 * All policy checks pass unconditionally — this is the default behavior
 * for the open-source edition.
 */
public class NoOpClusterGuard implements ClusterGuard {

    public static final NoOpClusterGuard INSTANCE = new NoOpClusterGuard();

    private NoOpClusterGuard() {
        // singleton
    }

    @Override
    public void onStartup(String dorisHomeDir) throws ClusterGuardException {
        // no-op
    }

    @Override
    public void checkTimeValidity() throws ClusterGuardException {
        // no-op: always valid
    }

    @Override
    public void checkNodeLimit(int currentNodeCount) throws ClusterGuardException {
        // no-op: unlimited
    }

    @Override
    public String getGuardInfo() {
        return "{}";
    }
}
