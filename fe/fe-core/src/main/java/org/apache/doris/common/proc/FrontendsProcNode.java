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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.system.Frontend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/*
 * Show current added frontends
 * SHOW PROC /frontends/
 */
public class FrontendsProcNode implements ProcNodeInterface {
    private static final Logger LOG = LogManager.getLogger(FrontendsProcNode.class);

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("IP").add("HostName").add("EditLogPort").add("HttpPort").add("QueryPort").add("RpcPort")
            .add("Role").add("IsMaster").add("ClusterId").add("Join").add("Alive")
            .add("ReplayedJournalId").add("LastHeartbeat").add("IsHelper").add("ErrMsg").add("Version")
            .add("CurrentConnected")
            .build();
    
    public static final int HOSTNAME_INDEX = 2;

    private Catalog catalog;
    
    public FrontendsProcNode(Catalog catalog) {
        this.catalog = catalog;
    }
    
    @Override
    public ProcResult fetchResult() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        
        List<List<String>> infos = Lists.newArrayList();

        getFrontendsInfo(catalog, infos);

        for (List<String> info : infos) {
            result.addRow(info);
        }

        return result;
    }

    public static void getFrontendsInfo(Catalog catalog, List<List<String>> infos) {
        String masterIp = "";
        int masterPort = -1;
        try {
            InetSocketAddress master = catalog.getHaProtocol().getLeader();
            masterIp = master.getAddress().getHostAddress();
            masterPort = master.getPort();
        } catch (Exception e) {
            // this may happen when majority of FOLLOWERS are down and no MASTER right now.
            LOG.warn("failed to get leader: {}", e.getMessage());
        }

        // get all node which are joined in bdb group
        List<InetSocketAddress> allFe = catalog.getHaProtocol().getElectableNodes(true /* include leader */);
        allFe.addAll(catalog.getHaProtocol().getObserverNodes());
        List<Pair<String, Integer>> allFeHosts = convertToHostPortPair(allFe);
        List<Pair<String, Integer>> helperNodes = catalog.getHelperNodes();

        Pair<String, Integer> selfNode = Catalog.getCurrentCatalog().getSelfNode();

        for (Frontend fe : catalog.getFrontends(null /* all */)) {

            List<String> info = new ArrayList<String>();
            info.add(fe.getNodeName());
            info.add(fe.getHost());

            info.add(NetUtils.getHostnameByIp(fe.getHost()));
            info.add(Integer.toString(fe.getEditLogPort()));
            info.add(Integer.toString(Config.http_port));

            if (fe.getHost().equals(catalog.getSelfNode().first)) {
                info.add(Integer.toString(Config.query_port));
                info.add(Integer.toString(Config.rpc_port));
            } else {
                info.add(Integer.toString(fe.getQueryPort()));
                info.add(Integer.toString(fe.getRpcPort()));
            }

            info.add(fe.getRole().name());
            info.add(String.valueOf(fe.getHost().equals(masterIp) && fe.getEditLogPort() == masterPort));

            info.add(Integer.toString(catalog.getClusterId()));
            info.add(String.valueOf(isJoin(allFeHosts, fe)));
            
            if (fe.getHost().equals(catalog.getSelfNode().first)) {
                info.add("true");
                info.add(Long.toString(catalog.getEditLog().getMaxJournalId()));
            } else {
                info.add(String.valueOf(fe.isAlive()));
                info.add(Long.toString(fe.getReplayedJournalId()));
            }
            info.add(TimeUtils.longToTimeString(fe.getLastUpdateTime()));
            info.add(String.valueOf(isHelperNode(helperNodes, fe)));
            info.add(fe.getHeartbeatErrMsg());
            info.add(fe.getVersion());
            // To indicate which FE we currently connected
            info.add(fe.getHost().equals(selfNode.first) ? "Yes" : "No");

            infos.add(info);
        }
    }
    
    private static boolean isHelperNode(List<Pair<String, Integer>> helperNodes, Frontend fe) {
        return helperNodes.stream().anyMatch(p -> p.first.equals(fe.getHost()) && p.second == fe.getEditLogPort());
    }

    private static boolean isJoin(List<Pair<String, Integer>> allFeHosts, Frontend fe) {
        for (Pair<String, Integer> pair : allFeHosts) {
            if (fe.getHost().equals(pair.first) && fe.getEditLogPort() == pair.second) {
                return true;
            }
        }
        return false;
    }
    
    private static List<Pair<String, Integer>> convertToHostPortPair(List<InetSocketAddress> addrs) {
        List<Pair<String, Integer>> hostPortPair = Lists.newArrayList();
        for (InetSocketAddress addr : addrs) {
            hostPortPair.add(Pair.create(addr.getAddress().getHostAddress(), addr.getPort()));
        }
        return hostPortPair;
    }
}

