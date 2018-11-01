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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.persist.EditLog;

import com.sleepycat.je.rep.StateChangeEvent;
import com.sleepycat.je.rep.StateChangeListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BDBStateChangeListener implements StateChangeListener {
    public static final Logger LOG = LogManager.getLogger(EditLog.class);
    
    public BDBStateChangeListener() {
    }

    @Override
    public synchronized void stateChange(StateChangeEvent sce) throws RuntimeException {
        FrontendNodeType originalType = Catalog.getInstance().getFeType();
        switch (sce.getState()) {
            case MASTER: {
                String msg = "transfer from " + originalType.name() + " to MASTER";
                System.out.println(msg);
                LOG.warn(msg);
                if (originalType == FrontendNodeType.MASTER) {
                    return;
                }
                Catalog.getInstance().setFeType(FrontendNodeType.MASTER);
                break;
            }
            case REPLICA: {
                if (originalType == FrontendNodeType.MASTER) {
                    String errMsg = "master transfer to REPLICA, will exit";
                    LOG.error(errMsg);
                    System.out.println(errMsg);
                    System.exit(-1);
                }

                if (Catalog.getInstance().isElectable()) {
                    String msg = "transfer from " + originalType.name() + " to FOLLOWER";
                    System.out.println(msg);
                    LOG.warn(msg);
                    Catalog.getInstance().setFeType(FrontendNodeType.FOLLOWER);
                } else {
                    String msg = "transfer from " + originalType.name() + " to OBSERVER";
                    System.out.println(msg);
                    LOG.warn(msg);
                    Catalog.getInstance().setFeType(FrontendNodeType.OBSERVER);
                }
                break;
            }
            case UNKNOWN: {
                if (originalType == FrontendNodeType.MASTER) {
                    String errMsg = "master transfer to UNKNOWN, will exit";
                    LOG.error(errMsg);
                    System.out.println(errMsg);
                    System.exit(-1);
                }

                if (originalType == FrontendNodeType.FOLLOWER) {
                    if (Catalog.getInstance().isElectable()) {
                        String msg = "transfer from FOLLOWER to UNKNOWN";
                        System.out.println(msg);
                        LOG.warn(msg);
                    } else {
                        String msg = "transfer from OBSERVER to UNKNOWN";
                        System.out.println(msg);
                        LOG.warn(msg);
                    }
                } else {
                    String msg = "transfer from " + originalType.name() + " to UNKNOWN";
                    System.out.println(msg);
                    LOG.warn(msg);
                }

                Catalog.getInstance().setFeType(FrontendNodeType.UNKNOWN);
                break;
            }
            default: {
                if (originalType == FrontendNodeType.MASTER) {
                    String errMsg = "master transfer to DETACHED, will exit";
                    LOG.error(errMsg);
                    System.out.println(errMsg);
                    System.exit(-1);
                }
                
                String errMsg = "this node is DETACHED.";
                LOG.error(errMsg);
                System.out.println(errMsg);
                break;
            }
        }
    }

}
