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

import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// Fixed time scheduled publisher.
// You can register your routine publish here.
public class FixedTimePublisher {
    private static FixedTimePublisher INSTANCE;

    private ScheduledThreadPoolExecutor scheduler = ThreadPoolManager.newScheduledThreadPool(1, "Fixed-Time-Publisher");
    private ClusterStatePublisher publisher;

    public FixedTimePublisher(ClusterStatePublisher publisher) {
        this.publisher = publisher;
    }

    public static FixedTimePublisher getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new FixedTimePublisher(ClusterStatePublisher.getInstance());
        }
        return INSTANCE;
    }

    public void register(Callback callback, long intervalMs) {
        scheduler.scheduleAtFixedRate(new Worker(callback), 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    private class Worker implements Runnable {
        private Callback callback;

        public Worker(Callback callback) {
            this.callback = callback;
        }

        @Override
        public void run() {
            ClusterStateUpdate.Builder builder = ClusterStateUpdate.builder();
            builder.addUpdate(callback.getTopicUpdate());
            ClusterStateUpdate state = builder.build();
            Listener listener = Listeners.nullToNoOpListener(callback.getListener());

            publisher.publish(state, listener, Config.meta_publish_timeout_ms);
        }
    }

    public static interface Callback {
        public TopicUpdate getTopicUpdate();

        public Listener getListener();
    }
}
