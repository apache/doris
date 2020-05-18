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

package org.apache.doris.transaction;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

// saves all TxnStateChangeListeners
public class TxnStateCallbackFactory {
    private static final Logger LOG = LogManager.getLogger(TxnStateCallbackFactory.class);

    private Map<Long, TxnStateChangeCallback> callbacks = Maps.newHashMap();

    public synchronized boolean addCallback(TxnStateChangeCallback callback) {
        if (callbacks.containsKey(callback.getId())) {
            return false;
        }
        callbacks.put(callback.getId(), callback);
        LOG.info("add callback of txn state : {}. current callback size: {}",
                callback.getId(), callbacks.size());
        return true;
    }

    public synchronized void removeCallback(long id) {
        if (callbacks.remove(id) != null) {
            LOG.info("remove callback of txn state : {}. current callback size: {}",
                    id, callbacks.size());
        }
    }

    public synchronized TxnStateChangeCallback getCallback(long id) {
        return callbacks.get(id);
    }
}
