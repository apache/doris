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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Broker manager
 */
public class BrokerMgr {
    public static final ImmutableList<String> BROKER_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("BrokerName").add("BrokerAddress").build();

    private final Random random = new Random(System.currentTimeMillis());

    public static class BrokerAddress implements Writable, Comparable<BrokerAddress> {
        public String ip;
        public int port;

        public BrokerAddress() {
        }

        public BrokerAddress(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BrokerAddress)) {
                return false;
            }

            BrokerAddress that = (BrokerAddress) o;

            if (port != that.port) {
                return false;
            }
            return ip.equals(that.ip);

        }

        @Override
        public int hashCode() {
            int result = ip.hashCode();
            result = 31 * result + port;
            return result;
        }

        @Override
        public int compareTo(BrokerAddress o) {
            int ret = ip.compareTo(o.ip);
            if (ret != 0) {
                return ret;
            }
            return port - o.port;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, ip);
            out.writeInt(port);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            ip = Text.readString(in);
            port = in.readInt();
        }

        @Override
        public String toString() {
            return ip + ":" + port;
        }

        public static BrokerAddress readIn(DataInput in) throws IOException {
            BrokerAddress address = new BrokerAddress();
            address.readFields(in);
            return address;
        }
    }

    // we need IP to find the co-location broker.
    // { BrokerName -> { IP -> [BrokerAddress] } }
    private final Map<String, ArrayListMultimap<String, BrokerAddress>> brokersMap = Maps.newHashMap();
    private final Map<String, List<BrokerAddress>> addressListMap = Maps.newHashMap();
    private final ReentrantLock lock = new ReentrantLock();
    private BrokerProcNode procNode = null;

    public BrokerMgr() {
    }

    public Map<String, List<BrokerAddress>> getAddressListMap() {
        return addressListMap;
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

    // Get all brokers' name
    public Collection<String> getBrokerNames() {
        lock.lock();
        try {
            return brokersMap.keySet();
        } finally {
            lock.unlock();
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

    public BrokerAddress getAnyBroker(String name) {
        lock.lock();
        try {
            List<BrokerAddress> addressList = addressListMap.get(name);
            if (addressList == null || addressList.isEmpty()) {
                return null;
            }
            return addressList.get(random.nextInt(addressList.size()));
        } finally {
            lock.unlock();
        }
    }

    public BrokerAddress getBroker(String name, String host) throws AnalysisException {
        lock.lock();
        try {
            ArrayListMultimap<String, BrokerAddress> brokerAddsMap = brokersMap.get(name);
            if (brokerAddsMap == null || brokerAddsMap.size() == 0) {
                throw new AnalysisException("Unknown broker name(" + name + ")");
            }
            List<BrokerAddress> addressList = brokerAddsMap.get(host);
            if (addressList.isEmpty()) {
                addressList = addressListMap.get(name);
            }
            return addressList.get(random.nextInt(addressList.size()));
        } finally {
            lock.unlock();
        }
    }

    public List<BrokerAddress> getBrokers(String name, List<String> hosts) throws AnalysisException {
        lock.lock();
        try {
            ArrayListMultimap<String, BrokerAddress> brokerAddsMap = brokersMap.get(name);
            if (brokerAddsMap == null || brokerAddsMap.size() == 0) {
                throw new AnalysisException("Unknown broker name(" + name + ")");
            }
            List<BrokerAddress> brokerList = Lists.newArrayList();
            for (String host : hosts) {
                List<BrokerAddress> addressList = brokerAddsMap.get(host);
                if (addressList.isEmpty()) {
                    addressList = addressListMap.get(name);
                }
                brokerList.add(addressList.get(random.nextInt(addressList.size())));
            }
            return brokerList;
        } finally {
            lock.unlock();
        }
    }

    public void addBrokers(String name, Collection<Pair<String, Integer>> addresses) throws DdlException {
        lock.lock();
        try {
            ArrayListMultimap<String, BrokerAddress> brokerAddrsMap = brokersMap.get(name);
            if (brokerAddrsMap == null) {
                brokerAddrsMap = ArrayListMultimap.create();
            }

            List<BrokerAddress> addedBrokerAddress = Lists.newArrayList();
            for (Pair<String, Integer> pair : addresses) {
                List<BrokerAddress> addressList = brokerAddrsMap.get(pair.first);
                for (BrokerAddress addr : addressList) {
                    if (addr.port == pair.second) {
                        throw new DdlException("Broker(" + pair.first + ":" + pair.second
                                + ") has already in brokers.");
                    }
                }
                addedBrokerAddress.add(new BrokerAddress(pair.first, pair.second));
            }
            Catalog.getInstance().getEditLog().logAddBroker(new ModifyBrokerInfo(name, addedBrokerAddress));
            for (BrokerAddress address : addedBrokerAddress) {
                brokerAddrsMap.put(address.ip, address);
            }
            brokersMap.put(name, brokerAddrsMap);
            addressListMap.put(name, Lists.newArrayList(brokerAddrsMap.values()));
        } finally {
            lock.unlock();
        }
    }

    public void replayAddBrokers(String name, List<BrokerAddress> addresses) {
        lock.lock();
        try {
            ArrayListMultimap<String, BrokerAddress> brokerAddrsMap = brokersMap.get(name);
            if (brokerAddrsMap == null) {
                brokerAddrsMap = ArrayListMultimap.create();
                brokersMap.put(name, brokerAddrsMap);
            }
            for (BrokerAddress address : addresses) {
                brokerAddrsMap.put(address.ip, address);
            }

            addressListMap.put(name, Lists.newArrayList(brokerAddrsMap.values()));
        } finally {
            lock.unlock();
        }
    }

    public void dropBrokers(String name, Collection<Pair<String, Integer>> addresses) throws DdlException {
        lock.lock();
        try {
            ArrayListMultimap<String, BrokerAddress> brokerAddrsMap = brokersMap.get(name);
            if (brokerAddrsMap == null) {
                throw new DdlException("Unknown broker name(" + name + ")");
            }

            List<BrokerAddress> dropedAddressList = Lists.newArrayList();
            for (Pair<String, Integer> pair : addresses) {
                List<BrokerAddress> addressList = brokerAddrsMap.get(pair.first);
                boolean found = false;
                for (BrokerAddress addr : addressList) {
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
            for (BrokerAddress address : dropedAddressList) {
                brokerAddrsMap.remove(address.ip, address);
            }

            addressListMap.put(name, Lists.newArrayList(brokerAddrsMap.values()));
        } finally {
            lock.unlock();
        }
    }

    public void replayDropBrokers(String name, List<BrokerAddress> addresses) {
        lock.lock();
        try {
            ArrayListMultimap<String, BrokerAddress> brokerAddrsMap = brokersMap.get(name);
            for (BrokerAddress addr : addresses) {
                brokerAddrsMap.remove(addr.ip, addr);
            }

            addressListMap.put(name, Lists.newArrayList(brokerAddrsMap.values()));
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
            addressListMap.remove(name);
        } finally {
            lock.unlock();
        }
    }

    public void replayDropAllBroker(String name) {
        lock.lock();
        try {
            brokersMap.remove(name);
            addressListMap.remove(name);
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
                for (Map.Entry<String, ArrayListMultimap<String, BrokerAddress>> entry : brokersMap.entrySet()) {
                    String brokerName = entry.getKey();
                    for (BrokerAddress address : entry.getValue().values()) {
                        List<String> row = Lists.newArrayList();
                        row.add(brokerName);
                        row.add(address.toString());
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
        public List<BrokerAddress> brokerAddresses;

        public ModifyBrokerInfo() {
        }

        public ModifyBrokerInfo(String brokerName, List<BrokerAddress> brokerAddresses) {
            this.brokerName = brokerName;
            this.brokerAddresses = brokerAddresses;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, brokerName);
            out.writeInt(brokerAddresses.size());
            for (BrokerAddress address : brokerAddresses) {
                address.write(out);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            brokerName = Text.readString(in);
            int size = in.readInt();
            brokerAddresses = Lists.newArrayList();
            for (int i = 0; i < size; ++i) {
                brokerAddresses.add(BrokerAddress.readIn(in));
            }
        }

        public static ModifyBrokerInfo readIn(DataInput in) throws IOException {
            ModifyBrokerInfo info = new ModifyBrokerInfo();
            info.readFields(in);
            return info;
        }
    }
}

