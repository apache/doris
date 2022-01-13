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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class SyncLifeCycle {
    private Logger logger = LogManager.getLogger(SyncLifeCycle.class);

    protected volatile boolean running = false;
    public Thread thread;

    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("process thread has an error", e);
        }
    };

    public abstract void process();

    public boolean isStart() {
        return this.running;
    }

    public void start() {
        if (isStart()) {
            throw new RuntimeException(this.getClass().getName() + " has startup , don't repeat start");
        }

        thread = new Thread(new Runnable() {
            public void run() {
                process();
            }
        });
        thread.setUncaughtExceptionHandler(handler);
        thread.start();

        this.running = true;
    }

    public void stop() {
        if (!isStart()) {
            // Repeated stops are considered successful
            return;
        }
        this.running = false;

        if (thread != null) {
            // Deadlock prevention
            if (thread == Thread.currentThread()) {
                return;
            }

            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
}