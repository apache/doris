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

/*
 * Author: Chenmingyu
 * Date: Mar 14, 2019
 */

// saves all TxnStateChangeListeners
public class TxnStateListenerRegistry {
    private static final Logger LOG = LogManager.getLogger(TxnStateListenerRegistry.class);

    private Map<Long, TxnStateChangeListener> listeners = Maps.newHashMap();

    public synchronized boolean register(TxnStateChangeListener listener) {
        if (listeners.containsKey(listener.getId())) {
            return false;
        }
        listeners.put(listener.getId(), listener);
        LOG.info("register txn state listener: {}", listener.getId());
        return true;
    }

    public synchronized void unregister(long id) {
        listeners.remove(id);
        LOG.info("unregister txn state listener: {}", id);
    }

    public synchronized TxnStateChangeListener getListener(long id) {
        return listeners.get(id);
    }
}
