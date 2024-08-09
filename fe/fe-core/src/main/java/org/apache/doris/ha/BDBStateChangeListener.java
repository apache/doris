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

package org.apache.doris.ha;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.LogUtils;

import com.google.common.base.Preconditions;
import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BDBStateChangeListener implements StateChangeListener {
    public static final Logger LOG = LogManager.getLogger(BDBStateChangeListener.class);
    private final boolean isElectable;

    public BDBStateChangeListener(boolean isElectable) {
        this.isElectable = isElectable;
    }

    @Override
    public synchronized void stateChange(StateChangeEvent sce) throws RuntimeException {
        FrontendNodeType newType = null;
        switch (sce.getState()) {
            case MASTER: {
                newType = FrontendNodeType.MASTER;
                break;
            }
            case REPLICA: {
                if (isElectable) {
                    newType = FrontendNodeType.FOLLOWER;
                } else {
                    newType = FrontendNodeType.OBSERVER;
                }
                break;
            }
            case UNKNOWN: {
                newType = FrontendNodeType.UNKNOWN;
                break;
            }
            default: {
                String msg = "this node is " + sce.getState().name();
                LOG.warn(msg);
                LogUtils.stdout(msg);
                return;
            }
        }
        Preconditions.checkNotNull(newType);
        Env.getCurrentEnv().notifyNewFETypeTransfer(newType);
    }

}
