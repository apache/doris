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

package org.apache.doris.catalog;

import org.apache.doris.analysis.ModifyBrokerClause;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.util.TimeUtils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Broker manager
 */
public class BrokerMgr {
    public static final ImmutableList<String> BROKER_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("IP").add("Port").add("Alive")
            .add("LastStartTime").add("LastUpdateTime").add("ErrMsg")
            .build();

    // we need IP to find the co-location broker.
    // { BrokerName -> { IP -> [FsBroker] } }
    private final Map<String, ArrayListMultimap<String, FsBroker>> brokersMap = Maps.newHashMap();
    // { BrokerName -> { list of FsBroker }
    private final Map<String, List<FsBroker>> brokerListMap = Maps.newHashMap();
    private final ReentrantLock lock = new ReentrantLock();
    private BrokerProcNode procNode = null;

    public BrokerMgr() {
    }

    public Map<String, List<FsBroker>> getBrokerListMap() {
        return brokerListMap;
    }

    public void execute(ModifyBrokerClause clause) throws DdlException {
        switch (clause.getOp()) {
            case OP_ADD:
                addBrokers(clause.getBrokerName(), clause.getHostPortPairs());
                break;
            case OP_DROP:
                dropBrokers(clause.getBrokerName(), clause.getHostPortPairs());
                break;
            case OP_DROP_ALL:
                dropAllBroker(clause.getBrokerName());
                break;
            default:
                break;
        }
    }

    public boolean contaisnBroker(String brokerName) {
        lock.lock();
        try {
            return brokersMap.containsKey(brokerName);
        } finally {
            lock.unlock();
        }
    }

    public FsBroker getAnyBroker(String brokerName) {
        lock.lock();
        try {
            List<FsBroker> brokerList = brokerListMap.get(brokerName);
            if (brokerList == null || brokerList.isEmpty()) {
                return null;
            }

            Collections.shuffle(brokerList);
            for (FsBroker fsBroker : brokerList) {
                if (fsBroker.isAlive) {
                    return fsBroker;
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    public FsBroker getBroker(String brokerName, String host) throws AnalysisException {
        lock.lock();
        try {
            ArrayListMultimap<String, FsBroker> brokerAddsMap = brokersMap.get(brokerName);
            if (brokerAddsMap == null || brokerAddsMap.size() == 0) {
                throw new AnalysisException("Unknown broker name(" + brokerName + ")");
            }
            List<FsBroker> brokers = brokerAddsMap.get(host);
            if (brokers.isEmpty()) {
                brokers = brokerListMap.get(brokerName);
            }

            Collections.shuffle(brokers);
            for (FsBroker fsBroker : brokers) {
                if (fsBroker.isAlive) {
                    return fsBroker;
                }
            }

            throw new AnalysisException("failed to find alive broker");
        } finally {
            lock.unlock();
        }
    }

    // find broker which is exactly matching name, host and port. return null if not found
    public FsBroker getBroker(String name, String host, int port) {
        lock.lock();
        try {
            ArrayListMultimap<String, FsBroker> brokerAddsMap = brokersMap.get(name);
            if (brokerAddsMap == null || brokerAddsMap.size() == 0) {
                return null;
            }

            List<FsBroker> addressList = brokerAddsMap.get(host);
            if (addressList.isEmpty()) {
                return null;
            }

            for (FsBroker fsBroker : addressList) {
                if (fsBroker.port == port) {
                    return fsBroker;
                }
            }
            return null;

        } finally {
            lock.unlock();
        }
    }

    public void addBrokers(String name, Collection<Pair<String, Integer>> addresses) throws DdlException {
        lock.lock();
        try {
            ArrayListMultimap<String, FsBroker> brokerAddrsMap = brokersMap.get(name);
            if (brokerAddrsMap == null) {
                brokerAddrsMap = ArrayListMultimap.create();
            }

            List<FsBroker> addedBrokerAddress = Lists.newArrayList();
            for (Pair<String, Integer> pair : addresses) {
                List<FsBroker> addressList = brokerAddrsMap.get(pair.first);
                for (FsBroker addr : addressList) {
                    if (addr.port == pair.second) {
                        throw new DdlException("Broker(" + pair.first + ":" + pair.second
                                + ") has already in brokers.");
                    }
                }
                addedBrokerAddress.add(new FsBroker(pair.first, pair.second));
            }
            Catalog.getInstance().getEditLog().logAddBroker(new ModifyBrokerInfo(name, addedBrokerAddress));
            for (FsBroker address : addedBrokerAddress) {
                brokerAddrsMap.put(address.ip, address);
            }
            brokersMap.put(name, brokerAddrsMap);
            brokerListMap.put(name, Lists.newArrayList(brokerAddrsMap.values()));
        } finally {
            lock.unlock();
        }
    }

    public void replayAddBrokers(String name, List<FsBroker> addresses) {
        lock.lock();
        try {
            ArrayListMultimap<String, FsBroker> brokerAddrsMap = brokersMap.get(name);
            if (brokerAddrsMap == null) {
                brokerAddrsMap = ArrayListMultimap.create();
                brokersMap.put(name, brokerAddrsMap);
            }
            for (FsBroker address : addresses) {
                brokerAddrsMap.put(address.ip, address);
            }

            brokerListMap.put(name, Lists.newArrayList(brokerAddrsMap.values()));
        } finally {
            lock.unlock();
        }
    }

    public void dropBrokers(String name, Collection<Pair<String, Integer>> addresses) throws DdlException {
        lock.lock();
        try {
            ArrayListMultimap<String, FsBroker> brokerAddrsMap = brokersMap.get(name);
            if (brokerAddrsMap == null) {
                throw new DdlException("Unknown broker name(" + name + ")");
            }

            List<FsBroker> dropedAddressList = Lists.newArrayList();
            for (Pair<String, Integer> pair : addresses) {
                List<FsBroker> addressList = brokerAddrsMap.get(pair.first);
                boolean found = false;
                for (FsBroker addr : addressList) {
                    if (addr.port == pair.second) {
                        dropedAddressList.add(addr);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new DdlException("Broker(" + pair.first + ":" + pair.second + ") has not in brokers.");
                }
            }
            Catalog.getInstance().getEditLog().logDropBroker(new ModifyBrokerInfo(name, dropedAddressList));
            for (FsBroker address : dropedAddressList) {
                brokerAddrsMap.remove(address.ip, address);
            }

            brokerListMap.put(name, Lists.newArrayList(brokerAddrsMap.values()));
        } finally {
            lock.unlock();
        }
    }

    public void replayDropBrokers(String name, List<FsBroker> addresses) {
        lock.lock();
        try {
            ArrayListMultimap<String, FsBroker> brokerAddrsMap = brokersMap.get(name);
            for (FsBroker addr : addresses) {
                brokerAddrsMap.remove(addr.ip, addr);
            }

            brokerListMap.put(name, Lists.newArrayList(brokerAddrsMap.values()));
        } finally {
            lock.unlock();
        }
    }

    public void dropAllBroker(String name) throws DdlException {
        lock.lock();
        try {
            if (!brokersMap.containsKey(name)) {
                throw new DdlException("Unknown broker name(" + name + ")");
            }
            Catalog.getInstance().getEditLog().logDropAllBroker(name);
            brokersMap.remove(name);
            brokerListMap.remove(name);
        } finally {
            lock.unlock();
        }
    }

    public void replayDropAllBroker(String name) {
        lock.lock();
        try {
            brokersMap.remove(name);
            brokerListMap.remove(name);
        } finally {
            lock.unlock();
        }
    }

    public List<List<String>> getBrokersInfo() {
        lock.lock();
        try {
            if (procNode == null) {
                procNode = new BrokerProcNode();
            }
            return procNode.fetchResult().getRows();
        } finally {
            lock.unlock();
        }
    }

    public BrokerProcNode getProcNode() {
        lock.lock();
        try {
            if (procNode == null) {
                procNode = new BrokerProcNode();
            }
            return procNode;
        } finally {
            lock.unlock();
        }
    }

    public class BrokerProcNode implements ProcNodeInterface {
        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(BROKER_PROC_NODE_TITLE_NAMES);

            lock.lock();
            try {
                for (Map.Entry<String, ArrayListMultimap<String, FsBroker>> entry : brokersMap.entrySet()) {
                    String brokerName = entry.getKey();
                    for (FsBroker broker : entry.getValue().values()) {
                        List<String> row = Lists.newArrayList();
                        row.add(brokerName);
                        row.add(broker.ip);
                        row.add(String.valueOf(broker.port));
                        row.add(String.valueOf(broker.isAlive));
                        row.add(TimeUtils.longToTimeString(broker.lastStartTime));
                        row.add(TimeUtils.longToTimeString(broker.lastUpdateTime));
                        row.add(broker.heartbeatErrMsg);
                        result.addRow(row);
                    }
                }
            } finally {
                lock.unlock();
            }
            return result;
        }
    }

    public static class ModifyBrokerInfo implements Writable {
        public String brokerName;
        public List<FsBroker> brokerAddresses;

        public ModifyBrokerInfo() {
        }

        public ModifyBrokerInfo(String brokerName, List<FsBroker> brokerAddresses) {
            this.brokerName = brokerName;
            this.brokerAddresses = brokerAddresses;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, brokerName);
            out.writeInt(brokerAddresses.size());
            for (FsBroker address : brokerAddresses) {
                address.write(out);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            brokerName = Text.readString(in);
            int size = in.readInt();
            brokerAddresses = Lists.newArrayList();
            for (int i = 0; i < size; ++i) {
                brokerAddresses.add(FsBroker.readIn(in));
            }
        }

        public static ModifyBrokerInfo readIn(DataInput in) throws IOException {
            ModifyBrokerInfo info = new ModifyBrokerInfo();
            info.readFields(in);
            return info;
        }
    }
}

