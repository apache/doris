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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyBrokerOp;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.ArrayUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Broker manager
 */
public class BrokerMgr {

    /**
     * Design Explanation for Column Schema:
     *
     * The schema is split into multiple arrays to handle dynamic column visibility cleanly
     * without introducing complex functional interfaces or runtime overhead.
     *
     * Current arrays:
     * 1. HISTORICAL_COLUMNS: The stable core columns that always exist. Do not modify the order
     *    to maintain backward compatibility for existing `SHOW BROKER` parsing tools.
     * 2. EXTENSION_COLUMNS: Unconditionally displayed columns added in the future.
     * 3. FQDN_CONDITIONAL_COLUMNS: Columns that only appear when Config.enable_fqdn_mode is true.
     *
     * Future column additions can easily fit into this structure:
     * - Non-conditional columns: Append to EXTENSION_COLUMNS.
     * - New FQDN-related conditional columns: Append to FQDN_CONDITIONAL_COLUMNS.
     * - Columns based on OTHER conditions (e.g., Config.isCloudMode):
     *   Create a new specific array (e.g., CLOUD_CONDITIONAL_COLUMNS) and append it to the
     *   merge logic in ALL_COLUMNS (and handle its visibility in the static block accordingly).
     *
     * NOTE ON COLUMN ORDER:
     * This structure dynamically merges arrays, implying that conditional columns will always
     * be placed at the end of the schema. This is perfectly acceptable for "brokers" metadata,
     * as appending newly introduced environment-specific columns to the end is the standard
     * convention in Doris to maintain backward compatibility.
     */

    // 1. Historical core columns (Do not modify order)
    private static final String[] HISTORICAL_COLUMN_NAMES = new String[]{
            "Name", "Host", "Port", "Alive",
            "LastStartTime", "LastUpdateTime", "ErrMsg"
    };

    // 2. Future unconditionally added columns
    private static final String[] EXTENSION_COLUMN_NAMES = new String[]{
        // Intentionally empty. Future unconditional columns go here.
    };

    // 3. Conditionally displayed columns depending on FQDN mode
    private static final String[] FQDN_CONDITIONAL_COLUMN_NAMES = new String[]{
            "Ip"
    };

    // Combine historical and extension columns as UNCONDITIONAL_COLUMN_NAMES (always displayed)
    private static final String[] UNCONDITIONAL_COLUMN_NAMES =
            ArrayUtils.addAll(HISTORICAL_COLUMN_NAMES, EXTENSION_COLUMN_NAMES);

    // Append conditional columns based on UNCONDITIONAL_COLUMN_NAMES
    // Future other conditional arrays can be appended here, e.g.,
    // ArrayUtils.addAll(UNCONDITIONAL_COLUMN_NAMES, FQDN_CONDITIONAL_COLUMN_NAMES, CLOUD_CONDITIONAL_COLUMN_NAMES)
    private static final String[] ALL_COLUMN_NAMES =
            ArrayUtils.addAll(UNCONDITIONAL_COLUMN_NAMES, FQDN_CONDITIONAL_COLUMN_NAMES);

    public static final ImmutableList<String> BROKER_PROC_NODE_TITLE_NAMES;

    static {
        // Config is initialized at the very beginning of FE startup,
        // so it is safe to read Config.enable_fqdn_mode here.
        String[] columns = Config.enable_fqdn_mode ? ALL_COLUMN_NAMES : UNCONDITIONAL_COLUMN_NAMES;
        BROKER_PROC_NODE_TITLE_NAMES = ImmutableList.copyOf(columns);
    }

    public static final int HOSTNAME_INDEX = 2;

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

    public List<FsBroker> getAllBrokers() {
        List<FsBroker> brokers = Lists.newArrayList();
        lock.lock();
        try {
            for (List<FsBroker> list : brokerListMap.values()) {
                brokers.addAll(list);
            }
        } finally {
            lock.unlock();
        }
        return brokers;
    }

    public void execute(ModifyBrokerOp modifyBrokerOp) throws DdlException {
        switch (modifyBrokerOp.getOp()) {
            case OP_ADD:
                addBrokers(modifyBrokerOp.getBrokerName(), modifyBrokerOp.getHostPortPairs());
                break;
            case OP_DROP:
                dropBrokers(modifyBrokerOp.getBrokerName(), modifyBrokerOp.getHostPortPairs());
                break;
            case OP_DROP_ALL:
                dropAllBroker(modifyBrokerOp.getBrokerName());
                break;
            default:
                break;
        }
    }

    public boolean containsBroker(String brokerName) {
        lock.lock();
        try {
            return brokersMap.containsKey(brokerName);
        } finally {
            lock.unlock();
        }
    }

    public List<FsBroker> getBrokers(String brokerName) {
        List<FsBroker> result = null;
        lock.lock();
        try {
            List<FsBroker> brokerList = brokerListMap.get(brokerName);
            if (brokerList == null || brokerList.isEmpty()) {
                return null;
            }
            result = new ArrayList<>(brokerList);
        } finally {
            lock.unlock();
        }
        return result;
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

    public FsBroker getAnyAliveBroker() {
        lock.lock();
        try {
            List<FsBroker> allBrokers = new ArrayList<>();
            for (List<FsBroker> list : brokerListMap.values()) {
                allBrokers.addAll(list);
            }

            Collections.shuffle(allBrokers);
            for (FsBroker fsBroker : allBrokers) {
                if (fsBroker.isAlive) {
                    return fsBroker;
                }
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

    public FsBroker getBroker(String brokerName, String host) throws AnalysisException {
        if (brokerName == null) {
            throw new AnalysisException("Unknown broker name(" + brokerName + ")");
        }
        if (brokerName.equalsIgnoreCase(BrokerDesc.MULTI_LOAD_BROKER)) {
            return new FsBroker("127.0.0.1", 0);
        }
        lock.lock();
        try {
            ArrayListMultimap<String, FsBroker> brokerAddsMap = brokersMap.get(brokerName);
            if (brokerAddsMap == null || brokerAddsMap.isEmpty()) {
                throw new AnalysisException("Unknown broker name(" + brokerName + ")");
            }
            List<FsBroker> brokers = brokerAddsMap.get(host);
            for (FsBroker fsBroker : brokers) {
                if (fsBroker.isAlive) {
                    return fsBroker;
                }
            }

            // not find, get an arbitrary one
            brokers = brokerListMap.get(brokerName);
            Collections.shuffle(brokers);
            for (FsBroker fsBroker : brokers) {
                if (fsBroker.isAlive) {
                    return fsBroker;
                }
            }

            throw new AnalysisException("failed to find alive broker: " + brokerName);
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
                        throw new DdlException("Broker(" + NetUtils
                                .getHostPortInAccessibleFormat(pair.first, pair.second)
                                + ") has already in brokers.");
                    }
                }
                addedBrokerAddress.add(new FsBroker(pair.first, pair.second));
            }
            Env.getCurrentEnv().getEditLog().logAddBroker(new ModifyBrokerInfo(name, addedBrokerAddress));
            for (FsBroker address : addedBrokerAddress) {
                brokerAddrsMap.put(address.host, address);
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
                brokerAddrsMap.put(address.host, address);
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

            List<FsBroker> droppedAddressList = Lists.newArrayList();
            for (Pair<String, Integer> pair : addresses) {
                List<FsBroker> addressList = brokerAddrsMap.get(pair.first);
                boolean found = false;
                for (FsBroker addr : addressList) {
                    if (addr.port == pair.second) {
                        droppedAddressList.add(addr);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new DdlException("Broker(" + NetUtils
                            .getHostPortInAccessibleFormat(pair.first, pair.second) + ") has not in brokers.");
                }
            }
            Env.getCurrentEnv().getEditLog().logDropBroker(new ModifyBrokerInfo(name, droppedAddressList));
            for (FsBroker address : droppedAddressList) {
                brokerAddrsMap.remove(address.host, address);
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
                brokerAddrsMap.remove(addr.host, addr);
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
            Env.getCurrentEnv().getEditLog().logDropAllBroker(name);
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
            List<BrokerSnapshot> brokerSnapshots = Lists.newArrayList();
            lock.lock();
            try {
                for (Map.Entry<String, ArrayListMultimap<String, FsBroker>> entry : brokersMap.entrySet()) {
                    String brokerName = entry.getKey();
                    for (FsBroker broker : entry.getValue().values()) {
                        brokerSnapshots.add(new BrokerSnapshot(brokerName, broker));
                    }
                }
            } finally {
                lock.unlock();
            }
            for (BrokerSnapshot brokerSnapshot : brokerSnapshots) {
                List<String> row = Lists.newArrayList();
                row.add(brokerSnapshot.name);
                row.add(brokerSnapshot.host);
                row.add(String.valueOf(brokerSnapshot.port));
                row.add(String.valueOf(brokerSnapshot.isAlive));
                row.add(TimeUtils.longToTimeString(brokerSnapshot.lastStartTime));
                row.add(TimeUtils.longToTimeString(brokerSnapshot.lastUpdateTime));
                row.add(brokerSnapshot.heartbeatErrMsg);
                if (Config.enable_fqdn_mode) {
                    row.add(Env.getCurrentEnv().getDnsCache().get(brokerSnapshot.host));
                }
                result.addRow(row);
            }
            return result;
        }
    }

    /**
     * An immutable snapshot used to capture and display fields within a lock
     */
    private static class BrokerSnapshot {
        final String name;
        final String host;
        final int port;
        final boolean isAlive;
        final long lastStartTime;
        final long lastUpdateTime;
        final String heartbeatErrMsg;

        BrokerSnapshot(String name, FsBroker broker) {
            this.name = name;
            this.host = broker.host;
            this.port = broker.port;
            this.isAlive = broker.isAlive;
            this.lastStartTime = broker.lastStartTime;
            this.lastUpdateTime = broker.lastUpdateTime;
            this.heartbeatErrMsg = broker.heartbeatErrMsg;
        }
    }

    public static class ModifyBrokerInfo implements Writable {
        @SerializedName(value = "n")
        public String brokerName;
        @SerializedName(value = "a")
        public List<FsBroker> brokerAddresses;

        public ModifyBrokerInfo() {
        }

        public ModifyBrokerInfo(String brokerName, List<FsBroker> brokerAddresses) {
            this.brokerName = brokerName;
            this.brokerAddresses = brokerAddresses;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, GsonUtils.GSON.toJson(this));
        }

        public static ModifyBrokerInfo read(DataInput in) throws IOException {
            return GsonUtils.GSON.fromJson(Text.readString(in), ModifyBrokerInfo.class);
        }
    }
}
