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

package org.apache.doris.system;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class FQDNManager extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(FQDNManager.class);

    private SystemInfoService nodeMgr;

    public FQDNManager(SystemInfoService nodeMgr) {
        super("FQDN mgr", FeConstants.ip_check_interval_second * 1000L);
        this.nodeMgr = nodeMgr;
    }

    /**
     * At each round: check if ip of be or fe has already been changed
     */
    @Override
    protected void runAfterCatalogReady() {
        updateBeIp();
        updateFeIp();
    }

    private void updateFeIp() {
        for (Frontend fe : Env.getCurrentEnv().getFrontends(null /* all */)) {
            if (!Strings.isNullOrEmpty(fe.getHostName())) {
                try {
                    InetAddress inetAddress = InetAddress.getByName(fe.getHostName());
                    if (!fe.getIp().equalsIgnoreCase(inetAddress.getHostAddress())) {
                        String oldIp = fe.getIp();
                        String newIp = inetAddress.getHostAddress();
                        Env.getCurrentEnv().modifyFrontendIp(fe.getNodeName(), newIp);
                        LOG.warn("ip for {} of fe has been changed from {} to {}",
                                fe.getHostName(), oldIp, fe.getIp());
                    }
                } catch (UnknownHostException e) {
                    LOG.warn("unknown host name for fe, {}", fe.getHostName(), e);
                } catch (DdlException e) {
                    LOG.warn("fail to update ip for fe, {}", fe.getHostName(), e);
                }
            }
        }
    }

    private void updateBeIp() {
        for (Backend be : nodeMgr.getIdToBackend().values()) {
            if (be.getHostName() != null) {
                try {
                    InetAddress inetAddress = InetAddress.getByName(be.getHostName());
                    if (!be.getIp().equalsIgnoreCase(inetAddress.getHostAddress())) {
                        String ip = be.getIp();
                        ClientPool.backendPool.clearPool(new TNetworkAddress(ip, be.getBePort()));
                        be.setIp(inetAddress.getHostAddress());
                        Env.getCurrentEnv().getEditLog().logBackendStateChange(be);
                        LOG.warn("ip for {} of be has been changed from {} to {}", be.getHostName(), ip, be.getIp());
                    }
                } catch (UnknownHostException e) {
                    LOG.warn("unknown host name for be, {}", be.getHostName(), e);
                }
            }
        }
    }
}
