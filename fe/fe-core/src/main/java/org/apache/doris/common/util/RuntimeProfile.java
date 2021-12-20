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

package org.apache.doris.common.util;

import org.apache.doris.common.Pair;
import org.apache.doris.common.Reference;
import org.apache.doris.thrift.TCounter;
import org.apache.doris.thrift.TRuntimeProfileNode;
import org.apache.doris.thrift.TRuntimeProfileTree;
import org.apache.doris.thrift.TUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * It is accessed by two kinds of thread, one is to create this RuntimeProfile
 * , named 'query thread', the other is to call
 * {@link org.apache.doris.common.proc.CurrentQueryInfoProvider}.
 */
public class RuntimeProfile {
    private static final Logger LOG = LogManager.getLogger(RuntimeProfile.class);
    public static String ROOT_COUNTER = "";
    private Counter counterTotalTime;
    private double localTimePercent;

    private Map<String, String> infoStrings = Maps.newHashMap();
    private List<String> infoStringsDisplayOrder = Lists.newArrayList();
    private ReentrantReadWriteLock infoStringsLock = new ReentrantReadWriteLock();

    private Map<String, Counter> counterMap = Maps.newConcurrentMap();
    private Map<String, TreeSet<String>> childCounterMap = Maps.newConcurrentMap();
    // protect TreeSet in ChildCounterMap
    private ReentrantReadWriteLock counterLock = new ReentrantReadWriteLock();

    private Map<String, RuntimeProfile> childMap = Maps.newConcurrentMap();
    private LinkedList<Pair<RuntimeProfile, Boolean>> childList = Lists.newLinkedList();
    private ReentrantReadWriteLock childLock = new ReentrantReadWriteLock();

    private String name;

    public RuntimeProfile(String name) {
        this();
        this.name = name;
    }

    public RuntimeProfile() {
        this.counterTotalTime = new Counter(TUnit.TIME_NS, 0);
        this.localTimePercent = 0;
        this.counterMap.put("TotalTime", counterTotalTime);
    }

    public String getName() {
        return name;
    }

    public Counter getCounterTotalTime() {
        return counterTotalTime;
    }

    public Map<String, Counter> getCounterMap() {
        return counterMap;
    }

    public List<Pair<RuntimeProfile, Boolean>> getChildList() {
        return childList;
    }

    public Map<String, RuntimeProfile> getChildMap() {
        return childMap;
    }

    public Map<String, TreeSet<String>> getChildCounterMap() {
        return childCounterMap;
    }

    public double getLocalTimePercent() {
        return localTimePercent;
    }

    public Counter addCounter(String name, TUnit type, String parentCounterName) {
        counterLock.writeLock().lock();
        try {
            Counter counter = this.counterMap.get(name);
            if (counter != null) {
                return counter;
            } else {
                Preconditions.checkState(parentCounterName.equals(ROOT_COUNTER)
                        || this.counterMap.containsKey(parentCounterName));
                Counter newCounter = new Counter(type, 0);
                this.counterMap.put(name, newCounter);

                Set<String> childCounters = childCounterMap.get(parentCounterName);
                if (childCounters == null) {
                    childCounterMap.put(parentCounterName, new TreeSet<String>());
                    childCounters = childCounterMap.get(parentCounterName);
                }
                childCounters.add(name);
                return newCounter;
            }
        } finally {
            counterLock.writeLock().unlock();
        }
    }

    public void update(final TRuntimeProfileTree thriftProfile) {
        Reference<Integer> idx = new Reference<Integer>(0);
        update(thriftProfile.nodes, idx);
        Preconditions.checkState(idx.getRef().equals(thriftProfile.nodes.size()));
    }

    // preorder traversal, idx should be modified in the traversal process
    private void update(List<TRuntimeProfileNode> nodes, Reference<Integer> idx) {
        TRuntimeProfileNode node = nodes.get(idx.getRef());

        // update this level's counters
        if (node.counters != null) {
            for (TCounter tcounter : node.counters) {
                Counter counter = counterMap.get(tcounter.name);
                if (counter == null) {
                    counterMap.put(tcounter.name, new Counter(tcounter.type, tcounter.value));
                } else {
                    if (counter.getType() != tcounter.type) {
                        LOG.error("Cannot update counters with the same name but different types"
                                + " type=" + tcounter.type);
                    } else {
                        counter.setValue(tcounter.value);
                    }
                }
            }

            if (node.child_counters_map != null) {
                // update childCounters
                for (Map.Entry<String, Set<String>> entry :
                        node.child_counters_map.entrySet()) {
                    String parentCounterName = entry.getKey();

                    counterLock.writeLock().lock();
                    try {
                        Set<String> childCounters = childCounterMap.get(parentCounterName);
                        if (childCounters == null) {
                            childCounterMap.put(parentCounterName, new TreeSet<String>());
                            childCounters = childCounterMap.get(parentCounterName);
                        }
                        childCounters.addAll(entry.getValue());
                    } finally {
                        counterLock.writeLock().unlock();
                    }
                }
            }
        }

        if (node.info_strings_display_order != null) {
            Map<String, String> nodeInfoStrings = node.info_strings;
            for (String key : node.info_strings_display_order) {
                String value = nodeInfoStrings.get(key);
                Preconditions.checkState(value != null);
                infoStringsLock.writeLock().lock();
                try {
                    if (this.infoStrings.containsKey(key)) {
                        // exists then replace
                        this.infoStrings.put(key, value);
                    } else {
                        this.infoStrings.put(key, value);
                        this.infoStringsDisplayOrder.add(key);
                    }
                } finally {
                    infoStringsLock.writeLock().unlock();
                }
            }
        }

        idx.setRef(idx.getRef() + 1);

        for (int i = 0; i < node.num_children; i++) {
            TRuntimeProfileNode tchild = nodes.get(idx.getRef());
            String childName = tchild.name;
            RuntimeProfile childProfile;

            childLock.writeLock().lock();
            try {
                childProfile = this.childMap.get(childName);
                if (childProfile == null) {
                    childMap.put(childName, new RuntimeProfile(childName));
                    childProfile = this.childMap.get(childName);
                    Pair<RuntimeProfile, Boolean> pair = Pair.create(childProfile, tchild.indent);
                    this.childList.add(pair);
                }
            } finally {
                childLock.writeLock().unlock();
            }
            childProfile.update(nodes, idx);
        }
    }

    // Print the profile:
    //  1. Profile Name
    //  2. Info Strings
    //  3. Counters
    //  4. Children
    public void prettyPrint(StringBuilder builder, String prefix) {
        Counter counter = this.counterMap.get("TotalTime");
        Preconditions.checkState(counter != null);
        // 1. profile name
        builder.append(prefix).append(name).append(":");
        // total time
        if (counter.getValue() != 0) {
            try (Formatter fmt = new Formatter()) {
                builder.append("(Active: ")
                        .append(this.printCounter(counter.getValue(), counter.getType()))
                        .append(", % non-child: ").append(fmt.format("%.2f", localTimePercent))
                        .append("%)");
            }
        }
        builder.append("\n");

        // 2. info String
        infoStringsLock.readLock().lock();
        try {
            for (String key : this.infoStringsDisplayOrder) {
                builder.append(prefix).append("   - ").append(key).append(": ")
                        .append(this.infoStrings.get(key)).append("\n");
            }
        } finally {
            infoStringsLock.readLock().unlock();
        }

        // 3. counters
        printChildCounters(prefix, ROOT_COUNTER, builder);

        // 4. children
        childLock.readLock().lock();
        try {
            for (int i = 0; i < childList.size(); i++) {
                Pair<RuntimeProfile, Boolean> pair = childList.get(i);
                boolean indent = pair.second;
                RuntimeProfile profile = pair.first;
                profile.prettyPrint(builder, prefix + (indent ? "  " : ""));
            }
        } finally {
            childLock.readLock().unlock();
        }
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        prettyPrint(builder, "");
        return builder.toString();
    }

    private void printChildCounters(String prefix, String counterName, StringBuilder builder) {
        Set<String> childCounterSet = childCounterMap.get(counterName);
        if (childCounterSet == null) {
            return;
        }

        counterLock.readLock().lock();
        try {
            for (String childCounterName : childCounterSet) {
                Counter counter = this.counterMap.get(childCounterName);
                Preconditions.checkState(counter != null);
                builder.append(prefix).append("   - ").append(childCounterName).append(": ")
                        .append(printCounter(counter.getValue(), counter.getType())).append("\n");
                this.printChildCounters(prefix + "  ", childCounterName, builder);
            }
        } finally {
            counterLock.readLock().unlock();
        }
    }

    public static String printCounter(long value, TUnit type) {
        StringBuilder builder = new StringBuilder();
        long tmpValue = value;
        switch (type) {
            case UNIT: {
                Pair<Double, String> pair = DebugUtil.getUint(tmpValue);
                if (pair.second.isEmpty()) {
                    builder.append(tmpValue);
                } else {
                    builder.append(pair.first).append(pair.second)
                            .append(" (").append(tmpValue).append(")");
                }
                break;
            }
            case TIME_NS: {
                if (tmpValue >= DebugUtil.BILLION) {
                    // If the time is over a second, print it up to ms.
                    tmpValue /= DebugUtil.MILLION;
                    DebugUtil.printTimeMs(tmpValue, builder);
                } else if (tmpValue >= DebugUtil.MILLION) {
                    // if the time is over a ms, print it up to microsecond in the unit of ms.
                    tmpValue /= 1000;
                    builder.append(tmpValue / 1000).append(".").append(tmpValue % 1000).append("ms");
                } else if (tmpValue > 1000) {
                    // if the time is over a microsecond, print it using unit microsecond
                    builder.append(tmpValue / 1000).append(".").append(tmpValue % 1000).append("us");
                } else {
                    builder.append(tmpValue).append("ns");
                }
                break;
            }
            case BYTES: {
                Pair<Double, String> pair = DebugUtil.getByteUint(tmpValue);
                Formatter fmt = new Formatter();
                builder.append(fmt.format("%.2f", pair.first)).append(" ").append(pair.second);
                fmt.close();
                break;
            }
            case BYTES_PER_SECOND: {
                Pair<Double, String> pair = DebugUtil.getByteUint(tmpValue);
                builder.append(pair.first).append(" ").append(pair.second).append("/sec");
                break;
            }
            case DOUBLE_VALUE: {
                Formatter fmt = new Formatter();
                builder.append(fmt.format("%.2f", (double) tmpValue));
                fmt.close();
                break;
            }
            case UNIT_PER_SECOND: {
                Pair<Double, String> pair = DebugUtil.getUint(tmpValue);
                if (pair.second.isEmpty()) {
                    builder.append(tmpValue);
                } else {
                    builder.append(pair.first).append(pair.second)
                            .append(" ").append("/sec");
                }
                break;
            }
            default: {
                Preconditions.checkState(false, "type=" + type);
                break;
            }
        }
        return builder.toString();
    }

    public void addChild(RuntimeProfile child) {
        if (child == null) {
            return;
        }

        childLock.writeLock().lock();
        try {
            if (childMap.containsKey(child.name)) {
                childList.removeIf(e -> e.first.name.equals(child.name));
            }
            this.childMap.put(child.name, child);
            Pair<RuntimeProfile, Boolean> pair = Pair.create(child, true);
            this.childList.add(pair);
        } finally {
            childLock.writeLock().unlock();
        }
    }

    public void addFirstChild(RuntimeProfile child) {
        if (child == null) {
            return;
        }
        childLock.writeLock().lock();
        try {
            if (childMap.containsKey(child.name)) {
                childList.removeIf(e -> e.first.name.equals(child.name));
            }
            this.childMap.put(child.name, child);
            Pair<RuntimeProfile, Boolean> pair = Pair.create(child, true);
            this.childList.addFirst(pair);
        } finally {
            childLock.writeLock().unlock();
        }
    }

    // Because the profile of summary and child fragment is not a real parent-child relationship
    // Each child profile needs to calculate the time proportion consumed by itself
    public void computeTimeInChildProfile() {
        childMap.values().
                forEach(RuntimeProfile::computeTimeInProfile);
    }

    public void computeTimeInProfile() {
        computeTimeInProfile(this.counterTotalTime.getValue());
    }

    private void computeTimeInProfile(long total) {
        if (total == 0) {
            return;
        }

        childLock.readLock().lock();
        try {
            // Add all the total times in all the children
            long totalChildTime = 0;
            for (int i = 0; i < this.childList.size(); ++i) {
                totalChildTime += childList.get(i).first.getCounterTotalTime().getValue();
            }

            long localTime = this.getCounterTotalTime().getValue() - totalChildTime;
            // Counters have some margin, set to 0 if it was negative.
            localTime = Math.max(0, localTime);
            this.localTimePercent = Double.valueOf(localTime) / Double.valueOf(total);
            this.localTimePercent = Math.min(1.0, this.localTimePercent) * 100;

            // Recurse on children
            for (int i = 0; i < this.childList.size(); i++) {
                childList.get(i).first.computeTimeInProfile(total);
            }
        } finally {
            childLock.readLock().unlock();
        }
    }

    // from bigger to smaller
    public void sortChildren() {
        childLock.writeLock().lock();
        try {
            this.childList.sort((profile1, profile2) -> Long.compare(profile2.first.getCounterTotalTime().getValue(),
                    profile1.first.getCounterTotalTime().getValue()));
        } finally {
            childLock.writeLock().unlock();
        }
    }

    public void addInfoString(String key, String value) {
        infoStringsLock.writeLock().lock();
        try {
            String target = this.infoStrings.get(key);
            if (target == null) {
                this.infoStrings.put(key, value);
                this.infoStringsDisplayOrder.add(key);
            } else {
                this.infoStrings.put(key, value);
            }
        } finally {
            infoStringsLock.writeLock().unlock();
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    // Returns the value to which the specified key is mapped;
    // or null if this map contains no mapping for the key.
    public String getInfoString(String key) {
        return infoStrings.get(key);
    }
}

