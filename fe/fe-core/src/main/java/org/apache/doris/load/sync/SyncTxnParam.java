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

package org.apache.doris.load.sync;

import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TUniqueId;

public class SyncTxnParam {
    private TUniqueId loadId;
    private long txnId;
    private TTxnParams txnConf;
    private Backend backend;

    public SyncTxnParam setTxnConf(TTxnParams txnConf) {
        this.txnConf = txnConf;
        return this;
    }

    public SyncTxnParam setLoadId(TUniqueId loadId) {
        this.loadId = loadId;
        return this;
    }

    public SyncTxnParam setTxnId(long txnId) {
        this.txnId = txnId;
        return this;
    }

    public SyncTxnParam setBackend(Backend backend) {
        this.backend = backend;
        return this;
    }

    public long getTxnId() {
        return txnId;
    }

    public TUniqueId getLoadId() {
        return loadId;
    }

    public TTxnParams getTxnConf() {
        return txnConf;
    }

    public Backend getBackend() {
        return backend;
    }
}
