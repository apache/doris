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

package org.apache.doris.common.publish;

import org.apache.doris.system.Backend;

import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

// Handler which will be call back by processor.
public class ResponseHandler {
    private Set<Backend> nodes;
    private CountDownLatch latch;

    public ResponseHandler(Collection<Backend> nodes) {
        this.nodes = Sets.newConcurrentHashSet(nodes);
        latch = new CountDownLatch(nodes.size());
    }

    public void onResponse(Backend node) {
        if (nodes.remove(node)) {
            latch.countDown();
        }
    }

    public void onFailure(Backend node, Throwable t) {
        if (nodes.remove(node)) {
            latch.countDown();
        }
    }

    public boolean awaitAllInMs(long millions) throws InterruptedException {
        return latch.await(millions, TimeUnit.MILLISECONDS);
    }

    public Backend[] pendingNodes() {
        return nodes.toArray(new Backend[0]);
    }
}
