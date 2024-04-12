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
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.thrift.TCounter;
import org.apache.doris.thrift.TRuntimeProfileNode;
import org.apache.doris.thrift.TRuntimeProfileTree;
import org.apache.doris.thrift.TUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
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
    public static String MAX_TIME_PRE = "max ";
    public static String MIN_TIME_PRE = "min ";
    public static String AVG_TIME_PRE = "avg ";
    public static String SUM_TIME_PRE = "sum ";
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

    private List<String> planNodeInfos = Lists.newArrayList();
    // name should not changed.
    private final String name;

    private Long timestamp = -1L;

    private Boolean isDone = false;
    private Boolean isCancel = false;
    // In pipelineX, we have explicitly split the Operator into sink and operator,
    // and we can distinguish them using tags.
    // In the old pipeline, we can only differentiate them based on their position
    // in the profile, which is quite tricky and only transitional.
    private Boolean isPipelineX = false;
    private Boolean isSinkOperator = false;

    private int nodeid = -1;

    public RuntimeProfile(String name) {
        this.localTimePercent = 0;
        if (Strings.isNullOrEmpty(name)) {
            throw new RuntimeException("Profile name must not be null");
        }
        this.name = name;
        this.counterTotalTime = new Counter(TUnit.TIME_NS, 0, 1);
        this.counterMap.put("TotalTime", counterTotalTime);
    }

    public RuntimeProfile(String name, int nodeId) {
        this.localTimePercent = 0;
        if (Strings.isNullOrEmpty(name)) {
            throw new RuntimeException("Profile name must not be null");
        }
        this.name = name;
        this.counterTotalTime = new Counter(TUnit.TIME_NS, 0, 3);
        this.counterMap.put("TotalTime", counterTotalTime);
        this.nodeid = nodeId;
    }

    public void setIsCancel(Boolean isCancel) {
        this.isCancel = isCancel;
    }

    public Boolean getIsCancel() {
        return isCancel;
    }

    public void setIsDone(Boolean isDone) {
        this.isDone = isDone;
    }

    public Boolean getIsDone() {
        return isDone;
    }

    public String getName() {
        return name;
    }

    public Counter getCounterTotalTime() {
        Counter totalTimeCounter = counterMap.get("TotalTime");
        if (totalTimeCounter == null) {
            return counterTotalTime;
        }
        return totalTimeCounter;
    }

    public int nodeId() {
        return this.nodeid;
    }

    public Boolean sinkOperator() {
        return this.isSinkOperator;
    }

    public void setIsPipelineX(boolean isPipelineX) {
        this.isPipelineX = isPipelineX;
    }

    public Map<String, Counter> getCounterMap() {
        return counterMap;
    }

    public List<Pair<RuntimeProfile, Boolean>> getChildList() {
        return childList;
    }

    public boolean isEmpty() {
        return childList.isEmpty();
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

    public void addCounter(String name, Counter newCounter, String parentCounterName) {
        counterLock.writeLock().lock();
        try {
            Counter counter = this.counterMap.get(name);
            if (counter != null) {
                return;
            } else {
                Preconditions.checkState(parentCounterName.equals(ROOT_COUNTER)
                        || this.counterMap.containsKey(parentCounterName));
                this.counterMap.put(name, newCounter);

                Set<String> childCounters = childCounterMap.get(parentCounterName);
                if (childCounters == null) {
                    childCounterMap.put(parentCounterName, new TreeSet<String>());
                    childCounters = childCounterMap.get(parentCounterName);
                }
                childCounters.add(name);
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
        // Make sure to update the latest LoadChannel profile according to the
        // timestamp.
        if (node.timestamp != -1 && node.timestamp < timestamp) {
            return;
        }
        if (node.isSetMetadata()) {
            this.nodeid = (int) node.getMetadata();
        }
        if (node.isSetIsSink()) {
            this.isSinkOperator = node.is_sink;
        }
        Preconditions.checkState(timestamp == -1 || node.timestamp != -1);
        // update this level's counters
        if (node.counters != null) {
            for (TCounter tcounter : node.counters) {
                Counter counter = counterMap.get(tcounter.name);
                if (counter == null) {
                    counterMap.put(tcounter.name, new Counter(tcounter.type, tcounter.value, tcounter.level));
                } else {
                    counter.setLevel(tcounter.level);
                    if (counter.getType() != tcounter.type) {
                        LOG.error("Cannot update counters with the same name but different types"
                                + " type=" + tcounter.type);
                    } else {
                        counter.setValue(tcounter.type, tcounter.value);
                    }
                }
            }

            if (node.child_counters_map != null) {
                // update childCounters
                for (Map.Entry<String, Set<String>> entry : node.child_counters_map.entrySet()) {
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
                    Pair<RuntimeProfile, Boolean> pair = Pair.of(childProfile, tchild.indent);
                    this.childList.add(pair);
                }
            } finally {
                childLock.writeLock().unlock();
            }
            childProfile.update(nodes, idx);
        }
    }

    public class Brief {
        String name;
        long rowsReturned = 0;
        String totalTime = "";
        List<Brief> children = new ArrayList<>();
    }

    public Brief toBrief() {
        Brief brief = new Brief();
        brief.name = this.name;
        brief.rowsReturned = 0L;

        counterLock.readLock().lock();
        try {
            Counter rowsReturnedCounter = counterMap.get("RowsReturned");
            if (rowsReturnedCounter != null) {
                brief.rowsReturned = rowsReturnedCounter.getValue();
            }
            Counter totalTimeCounter = counterMap.get("TotalTime");
            if (totalTimeCounter != null) {
                brief.totalTime = printCounter(totalTimeCounter.getValue(), totalTimeCounter.getType());
            }
        } finally {
            counterLock.readLock().unlock();
        }

        childLock.readLock().lock();
        try {
            for (Pair<RuntimeProfile, Boolean> pair : childList) {
                brief.children.add(pair.first.toBrief());
            }
        } finally {
            childLock.readLock().unlock();
        }

        return brief;
    }

    private void printActimeCounter(StringBuilder builder, boolean isPipelineX) {
        if (!isPipelineX) {
            Counter counter = this.counterMap.get("TotalTime");
            Preconditions.checkState(counter != null);
            if (counter.getValue() != 0) {
                try (Formatter fmt = new Formatter()) {
                    builder.append("(Active: ")
                            .append(RuntimeProfile.printCounter(counter.getValue(), counter.getType()))
                            .append(", % non-child: ").append(fmt.format("%.2f", localTimePercent))
                            .append("%)");
                }
            }
        } else {
            Counter counter = this.counterMap.get("ExecTime");
            if (counter == null) {
                counter = this.counterMap.get("TotalTime");
            }
            if (counter.getValue() != 0) {
                try (Formatter fmt = new Formatter()) {
                    builder.append("(ExecTime: ")
                            .append(RuntimeProfile.printCounter(counter.getValue(), counter.getType()))
                            .append(")");
                }
            }
        }
    }

    // Print the profile:
    // 1. Profile Name
    // 2. Info Strings
    // 3. Counters
    // 4. Children
    public void prettyPrint(StringBuilder builder, String prefix) {
        prettyPrint(builder, prefix, false);
    }

    public void prettyPrint(StringBuilder builder, String prefix, boolean isPipelineX) {
        // 1. profile name
        builder.append(prefix).append(name).append(":");
        // total time
        printActimeCounter(builder, isPipelineX);

        builder.append("\n");

        // plan node info
        printPlanNodeInfo(prefix + "   ", builder);

        // 2. info String
        infoStringsLock.readLock().lock();
        try {
            for (String key : this.infoStringsDisplayOrder) {
                builder.append(prefix);
                if (SummaryProfile.EXECUTION_SUMMARY_KEYS_IDENTATION.containsKey(key)) {
                    for (int i = 0; i < SummaryProfile.EXECUTION_SUMMARY_KEYS_IDENTATION.get(key); i++) {
                        builder.append("  ");
                    }
                }
                builder.append("   - ").append(key).append(": ")
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
                profile.prettyPrint(builder, prefix + (indent ? "  " : ""), isPipelineX);
            }
        } finally {
            childLock.readLock().unlock();
        }
    }

    private void printPlanNodeInfo(String prefix, StringBuilder builder) {
        if (planNodeInfos.isEmpty()) {
            return;
        }
        builder.append(prefix + "- " + "PlanInfo\n");

        for (String info : planNodeInfos) {
            builder.append(prefix + "   - " + info + "\n");
        }
    }

    private static List<RuntimeProfile> getChildListFromLists(String profileName, List<RuntimeProfile> profiles) {
        List<RuntimeProfile> ret = new ArrayList<RuntimeProfile>();
        for (RuntimeProfile profile : profiles) {
            RuntimeProfile tmp = profile.getChildMap().get(profileName);
            if (tmp != null) {
                ret.add(profile.getChildMap().get(profileName));
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("could not find {} from {}", profileName, profile.toString());
                }
            }
        }
        return ret;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        prettyPrint(builder, "", isPipelineX);
        return builder.toString();
    }

    public static void mergeProfiles(List<RuntimeProfile> profiles,
            RuntimeProfile simpleProfile, Map<Integer, String> planNodeMap) {
        mergeCounters(ROOT_COUNTER, profiles, simpleProfile);
        if (profiles.size() < 1) {
            return;
        }
        RuntimeProfile templateProfile = profiles.get(0);
        for (int i = 0; i < templateProfile.childList.size(); i++) {
            RuntimeProfile templateChildProfile = templateProfile.childList.get(i).first;
            List<RuntimeProfile> allChilds = getChildListFromLists(templateChildProfile.name, profiles);
            RuntimeProfile newCreatedMergedChildProfile = new RuntimeProfile(templateChildProfile.name,
                    templateChildProfile.nodeId());
            mergeProfiles(allChilds, newCreatedMergedChildProfile, planNodeMap);
            // RuntimeProfile has at least one counter named TotalTime, should exclude it.
            if (newCreatedMergedChildProfile.counterMap.size() > 1) {
                simpleProfile.addChildWithCheck(newCreatedMergedChildProfile, planNodeMap);
            }
        }
    }

    private static void mergeCounters(String parentCounterName, List<RuntimeProfile> profiles,
            RuntimeProfile simpleProfile) {
        if (profiles.size() == 0) {
            return;
        }
        RuntimeProfile templateProfile = profiles.get(0);
        Set<String> childCounterSet = templateProfile.childCounterMap.get(parentCounterName);
        if (childCounterSet == null) {
            return;
        }
        for (String childCounterName : childCounterSet) {
            Counter counter = templateProfile.counterMap.get(childCounterName);
            if (counter.getLevel() == 1) {
                Counter oldCounter = profiles.get(0).counterMap.get(childCounterName);
                AggCounter aggCounter = new AggCounter(oldCounter.getType());
                for (RuntimeProfile profile : profiles) {
                    Counter orgCounter = profile.counterMap.get(childCounterName);
                    aggCounter.addCounter(orgCounter);
                }
                if (simpleProfile.counterMap.containsKey(parentCounterName)) {
                    simpleProfile.addCounter(childCounterName, aggCounter, parentCounterName);
                } else {
                    simpleProfile.addCounter(childCounterName, aggCounter, ROOT_COUNTER);
                }
            }
            mergeCounters(childCounterName, profiles, simpleProfile);
        }
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
                        .append(counter.print()).append("\n");
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
            case NONE: {
                // Do nothing, it is just a label
                break;
            }
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
            case TIME_MS: {
                if (tmpValue >= DebugUtil.THOUSAND) {
                    // If the time is over a second, print it up to ms.
                    DebugUtil.printTimeMs(tmpValue, builder);
                } else {
                    builder.append(tmpValue).append("ms");
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
            Pair<RuntimeProfile, Boolean> pair = Pair.of(child, true);
            this.childList.add(pair);
        } finally {
            childLock.writeLock().unlock();
        }
    }

    public void addChildWithCheck(RuntimeProfile child, Map<Integer, String> planNodeMap) {
        // check name
        if (child.name.startsWith("PipelineTask") || child.name.startsWith("PipelineContext")) {
            return;
        }
        childLock.writeLock().lock();
        try {
            Pair<RuntimeProfile, Boolean> pair = Pair.of(child, true);
            this.childList.add(pair);
        } finally {
            childLock.writeLock().unlock();
        }
        // insert plan node info to profile strinfo
        if (planNodeMap == null || !planNodeMap.containsKey(child.nodeId())) {
            return;
        }
        child.addPlanNodeInfos(planNodeMap.get(child.nodeId()));
        planNodeMap.remove(child.nodeId());
    }

    public void addPlanNodeInfos(String infos) {
        String[] infoList = infos.split("\n");
        for (String info : infoList) {
            planNodeInfos.add(info);
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
            Pair<RuntimeProfile, Boolean> pair = Pair.of(child, true);
            this.childList.addFirst(pair);
        } finally {
            childLock.writeLock().unlock();
        }
    }

    // Because the profile of summary and child fragment is not a real parent-child
    // relationship
    // Each child profile needs to calculate the time proportion consumed by itself
    public void computeTimeInChildProfile() {
        childMap.values().forEach(RuntimeProfile::computeTimeInProfile);
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
        } catch (IllegalArgumentException e) {
            // This exception may be thrown if the counter total time of the child is
            // updated in the update method
            // during the sorting process. This sorting only affects the profile instance
            // display order, so this
            // exception is temporarily ignored here.
            if (LOG.isDebugEnabled()) {
                LOG.debug("sort child list error: ", e);
            }
        } finally {
            childLock.writeLock().unlock();
        }
    }

    public void addInfoString(String key, String value) {
        infoStringsLock.writeLock().lock();
        try {
            String target = this.infoStrings.get(key);
            this.infoStrings.put(key, value);
            if (target == null) {
                this.infoStringsDisplayOrder.add(key);
            }
        } finally {
            infoStringsLock.writeLock().unlock();
        }
    }

    // Returns the value to which the specified key is mapped;
    // or null if this map contains no mapping for the key.
    public String getInfoString(String key) {
        return infoStrings.getOrDefault(key, "");
    }

    public Map<String, String> getInfoStrings() {
        return infoStrings;
    }
}
