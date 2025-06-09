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

package org.apache.doris.dictionary;

import org.apache.doris.system.Backend;

public class DictionaryDistribution {
    private final Backend backend;
    private final long version;
    private final long memoryBytes;

    public DictionaryDistribution(Backend backend, long version, long memoryBytes) {
        this.backend = backend;
        this.version = version;
        this.memoryBytes = memoryBytes;
    }

    public Long getBackendId() {
        return backend.getId();
    }

    public long getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return backend.getHost() + ":" + backend.getBePort() + " ver=" + version + " memory=" + memoryBytes;
    }
}
