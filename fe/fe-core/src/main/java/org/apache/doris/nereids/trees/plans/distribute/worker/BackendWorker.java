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

package org.apache.doris.nereids.trees.plans.distribute.worker;

import org.apache.doris.system.Backend;

import java.util.Objects;

/** BackendWorker */
public class BackendWorker implements DistributedPlanWorker {
    private final Backend backend;

    public BackendWorker(Backend backend) {
        this.backend = backend;
    }

    @Override
    public long id() {
        return backend.getId();
    }

    @Override
    public String address() {
        return backend.getAddress();
    }

    @Override
    public String host() {
        return backend.getHost();
    }

    @Override
    public int port() {
        return backend.getBePort();
    }

    @Override
    public boolean available() {
        return backend.isQueryAvailable();
    }

    @Override
    public int hashCode() {
        return Objects.hash(backend.getId());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BackendWorker)) {
            return false;
        }
        return backend.getId() == ((BackendWorker) obj).backend.getId();
    }

    @Override
    public String toString() {
        return "BackendWorker(id: " + backend.getId() + ", address: " + backend.getAddress() + ")";
    }
}
