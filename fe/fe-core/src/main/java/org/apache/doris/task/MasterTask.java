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

package org.apache.doris.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class MasterTask implements Runnable {
    private static final Logger LOG = LogManager.getLogger(MasterTask.class);

    protected long signature;

    @Override
    public void run() {
        try {
            exec();
        } catch (Exception e) {
            LOG.error("task exec error ", e);
        }
    }

    public long getSignature() {
        return signature;
    }

    /**
     * implement in child
     */
    protected abstract void exec();

}
