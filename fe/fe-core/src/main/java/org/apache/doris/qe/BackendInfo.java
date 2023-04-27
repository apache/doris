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

package org.apache.doris.qe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BackendInfo {
    private long numCores = 1;
    private static volatile BackendInfo instance = null;
    private static final Map<Long, BackendInfo> Info = new ConcurrentHashMap<>();
    private static volatile long minNumCores = Long.MAX_VALUE;

    private BackendInfo(long numCores) {
        this.numCores = numCores;
    }

    public static BackendInfo get() {
        if (instance == null) {
            synchronized (BackendInfo.class) {
                if (instance == null) {
                    instance = new BackendInfo(minNumCores);
                }
            }
        }
        return instance;
    }

    public long getNumCores() {
        return numCores;
    }

    public void clear() {
        Info.clear();
        minNumCores = Long.MAX_VALUE;
    }

    public void addBeInfo(long beId, long numCores) {
        Info.put(beId, new BackendInfo(numCores));
        minNumCores = Math.min(minNumCores, numCores);
    }

    public long getMinNumCores() {
        return minNumCores;
    }

    public long getParallelExecInstanceNum() {
        if (getMinNumCores() == Long.MAX_VALUE) {
            return 1;
        }
        return (getMinNumCores() + 1) / 2;
    }

    public BackendInfo getBeInfoById(long beId) {
        return Info.get(beId);
    }
}
