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

package org.apache.doris.common.profile;

import org.apache.doris.common.Pair;
import org.apache.doris.common.Reference;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TCounter;
import org.apache.doris.thrift.TPlanNodeRuntimeStatsItem;
import org.apache.doris.thrift.TRuntimeProfileNode;
import org.apache.doris.thrift.TRuntimeProfileTree;
import org.apache.doris.thrift.TUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * It is accessed by two kinds of thread, one is to create this RuntimeProfile
 * , named 'query thread', the other is to call
 * {@link org.apache.doris.common.proc.CurrentQueryInfoProvider}.
 */
public class RuntimeProfile {
    // TODO: 这里维护性太差了
    // BE 上的 OperatorXBase::init 里面有 Operator 的命名规则
    // 简而言之是把 XXX_NODE 平替为 XXX_OPERATOR
    // 所以这里应该把 gensrc/thrift/PlanNodes.thrift 里面定义的 NodeType
    // 手动转为了其对应 BE 上 operator 的名字
    // 有一些 Node 比较特殊，它在 BE 上是作为 Pipeline breaker 的，比如 AGGREGATION_NODE,
    // 也得手动把这类 operator 的名字添加到这里
    public static final ImmutableSet<String> PLAN_NODE_TYPE_MAP =
            ImmutableSet.<String>builder()
                    .add("OLAP_SCAN_OPERATOR")
                    .add("AGGREGATION_OPERATOR")
                    .add("AGGREGATION_SINK_OPERATOR")
                    .add("STREAMING_AGGREGATION_OPERATOR")
                    .add("HASH_JOIN_OPERATOR")
                    .add("HASH_JOIN_SINK_OPERATOR").build();
    private static final Logger LOG = LogManager.getLogger(RuntimeProfile.class);
    public static String ROOT_COUNTER = "";
    public static String MAX_TIME_PRE = "max ";
    public static String MIN_TIME_PRE = "min ";
    public static String AVG_TIME_PRE = "avg ";
    public static String SUM_TIME_PRE = "sum ";
    @SerializedName(value = "counterTotalTime")
    private Counter counterTotalTime;
    @SerializedName(value = "localTimePercent")
    private double localTimePercent = 0;
    @SerializedName(value = "infoStrings")
    private Map<String, String> infoStrings = Maps.newHashMap();
    @SerializedName(value = "infoStringsDisplayOrder")
    private List<String> infoStringsDisplayOrder = Lists.newArrayList();
    private transient ReentrantReadWriteLock infoStringsLock = new ReentrantReadWriteLock();

    @SerializedName(value = "counterMap")
    private Map<String, Counter> counterMap = Maps.newConcurrentMap();
    @SerializedName(value = "childCounterMap")
    private Map<String, TreeSet<String>> childCounterMap = Maps.newConcurrentMap();
    // protect TreeSet in ChildCounterMap
    private transient ReentrantReadWriteLock counterLock = new ReentrantReadWriteLock();
    @SerializedName(value = "childMap")
    private Map<String, RuntimeProfile> childMap = Maps.newConcurrentMap();
    @SerializedName(value = "childList")
    private LinkedList<Pair<RuntimeProfile, Boolean>> childList = Lists.newLinkedList();
    private transient ReentrantReadWriteLock childLock = new ReentrantReadWriteLock();
    @SerializedName(value = "planNodeInfos")
    private List<String> planNodeInfos = Lists.newArrayList();

    @SerializedName(value = "name")
    private String name = "";
    @SerializedName(value = "timestamp")
    private Long timestamp = -1L;
    @SerializedName(value = "isDone")
    private Boolean isDone = false;
    @SerializedName(value = "isCancel")
    private Boolean isCancel = false;
    // In pipelineX, we have explicitly split the Operator into sink and operator,
    // and we can distinguish them using tags.
    // In the old pipeline, we can only differentiate them based on their position
    // in the profile, which is quite tricky and only transitional.
    @SerializedName(value = "isSinkOperator")
    private Boolean isSinkOperator = false;
    @SerializedName(value = "nodeid")
    private int nodeid = -1;

    public Map<String, Long> rowsProducedMap = new HashMap<>();

    public RuntimeProfile() {
        init();
    }

    public RuntimeProfile(String name) {
        if (Strings.isNullOrEmpty(name)) {
            throw new RuntimeException("Profile name must not be null");
        }
        this.name = name;
        this.counterTotalTime = new Counter(TUnit.TIME_NS, 0, 1);
        this.counterMap.put("TotalTime", counterTotalTime);
        init();
    }

    public RuntimeProfile(String name, int nodeId) {
        if (Strings.isNullOrEmpty(name)) {
            throw new RuntimeException("Profile name must not be null");
        }
        this.name = name;
        this.nodeid = nodeId;
        this.counterTotalTime = new Counter(TUnit.TIME_NS, 0, 3);
        this.counterMap.put("TotalTime", counterTotalTime);
        init();
    }

    private void init() {
        this.infoStringsLock = new ReentrantReadWriteLock();
        this.childLock = new ReentrantReadWriteLock();
        this.counterLock = new ReentrantReadWriteLock();
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

        Preconditions.checkState(timestamp == -1 || node.timestamp != -1);
        // update this level's counters
        if (node.counters != null) {
            for (TCounter tcounter : node.counters) {
                // If different node has counter with the same name, it will lead to chaos.
                Counter counter = this.counterMap.get(tcounter.name);
                if (counter == null) {
                    if (tcounter.isSetDescription()) {
                        counterMap.put(tcounter.name, new Counter(tcounter.description));
                    } else {
                        counterMap.put(tcounter.name, new Counter(tcounter.type, tcounter.value, tcounter.level));
                    }
                } else {
                    counter.setLevel(tcounter.level);
                    if (counter.getType() != tcounter.type) {
                        LOG.error("Cannot update counters with the same name but different types"
                                + " type=" + tcounter.type);
                    } else if (tcounter.isSetDescription()) {
                        continue;
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

    // Print the profile:
    // 1. Profile Name
    // 2. Info Strings
    // 3. Counters
    // 4. Children
    public void prettyPrint(StringBuilder builder, String prefix) {
        // 1. profile name
        builder.append(prefix).append(name).append(":");

        builder.append("\n");

        // plan node info
        printPlanNodeInfo(prefix + "   ", builder);

        // 2. info String
        infoStringsLock.readLock().lock();
        try {
            for (String key : this.infoStringsDisplayOrder) {
                builder.append(prefix);
                if (SummaryProfile.EXECUTION_SUMMARY_KEYS_INDENTATION.containsKey(key)) {
                    for (int i = 0; i < SummaryProfile.EXECUTION_SUMMARY_KEYS_INDENTATION.get(key); i++) {
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
        try {
            printChildCounters(prefix, ROOT_COUNTER, builder);
        } catch (Exception e) {
            builder.append("print child counters error: ").append(e.getMessage());
        }


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

    private void printPlanNodeInfo(String prefix, StringBuilder builder) {
        if (planNodeInfos.isEmpty()) {
            return;
        }
        builder.append(prefix).append("- ").append("PlanInfo\n");

        for (String info : planNodeInfos) {
            builder.append(prefix).append("   - ").append(info).append("\n");
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
        prettyPrint(builder, "");
        return builder.toString();
    }

    boolean shouldBeIncluded() {
        if (Objects.equals(this.name, "CommonCounters") || Objects.equals(this.name, "CustomCounters")) {
            return true;
        } else {
            return this.name.matches(".*Pipeline.*") || this.name.matches(".*_OPERATOR.*");
        }
    }

    public static void mergeProfiles(List<RuntimeProfile> profiles,
            RuntimeProfile resultProfile, Map<Integer, String> planNodeMap) {
        mergeCounters(ROOT_COUNTER, profiles, resultProfile);
        if (profiles.isEmpty()) {
            return;
        }

        RuntimeProfile templateProfile = profiles.get(0);
        for (int i = 0; i < templateProfile.childList.size(); i++) {
            RuntimeProfile templateChildProfile = templateProfile.childList.get(i).first;
            // Traverse all profiles and get the child profile with the same name
            List<RuntimeProfile> allChilds = getChildListFromLists(templateChildProfile.name, profiles);
            RuntimeProfile newCreatedMergedChildProfile = new RuntimeProfile(templateChildProfile.name,
                    templateChildProfile.nodeId());
            mergeProfiles(allChilds, newCreatedMergedChildProfile, planNodeMap);
            if (newCreatedMergedChildProfile.shouldBeIncluded()) {
                resultProfile.addChildWithCheck(newCreatedMergedChildProfile, planNodeMap,
                                            templateProfile.childList.get(i).second);
                resultProfile.rowsProducedMap.putAll(newCreatedMergedChildProfile.rowsProducedMap);
            }
        }
    }

    static void mergeCounters(String parentCounterName, List<RuntimeProfile> profiles,
            RuntimeProfile simpleProfile) {
        if (profiles.isEmpty()) {
            return;
        }
        RuntimeProfile templateProfile = profiles.get(0);
        Map<String, Counter> templateCounterMap = templateProfile.counterMap;
        Pattern pattern = Pattern.compile("nereids_id=(\\d+)");
        Matcher matcher = pattern.matcher(templateProfile.getName());
        String nereidsId = null;
        if (matcher.find()) {
            nereidsId = matcher.group(1);
        }
        Set<String> childCounterSet = templateProfile.childCounterMap.get(parentCounterName);
        if (childCounterSet == null) {
            return;
        }
        for (String childCounterName : childCounterSet) {
            Counter counter = templateProfile.counterMap.get(childCounterName);
            if (counter == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Child counter {} of {} not found in profile {}", childCounterName, parentCounterName,
                            templateProfile.toString());
                }
                continue;
            }
            if (counter.getLevel() == 1) {
                Counter oldCounter = templateCounterMap.get(childCounterName);
                AggCounter aggCounter = new AggCounter(oldCounter.getType());
                for (RuntimeProfile profile : profiles) {
                    // orgCounter could be null if counter-structure is changed
                    // change of counter structure happens when NonZeroCounter is involved.
                    // So here we have to ignore the counter if it is not found in the profile.
                    Counter orgCounter = profile.counterMap.get(childCounterName);
                    aggCounter.addCounter(orgCounter);
                }
                if (nereidsId != null && childCounterName.equals("RowsProduced")) {
                    simpleProfile.rowsProducedMap.put(nereidsId, aggCounter.sum.getValue());
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
                if (counter != null) {
                    builder.append(prefix).append("   - ").append(childCounterName).append(": ")
                            .append(counter.print()).append("\n");
                    this.printChildCounters(prefix + "  ", childCounterName, builder);
                } else {
                    throw new RuntimeException("Child counter " + childCounterName + " of " + counterName
                            + " not found in profile");
                }
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

    public void addChild(RuntimeProfile child, boolean indent) {
        if (child == null) {
            return;
        }

        childLock.writeLock().lock();
        try {
            if (childMap.containsKey(child.name)) {
                // Pipeline/Instance has alread finished.
                // This could happen because the report profile rpc is async.
                if (childMap.get(child.name).getIsDone() || childMap.get(child.name).getIsCancel()) {
                    return;
                } else {
                    childList.removeIf(e -> e.first.name.equals(child.name));
                }
            }
            this.childMap.put(child.name, child);
            Pair<RuntimeProfile, Boolean> pair = Pair.of(child, indent);
            this.childList.add(pair);
        } finally {
            childLock.writeLock().unlock();
        }
    }

    public void addChildWithCheck(RuntimeProfile child, Map<Integer, String> planNodeMap, boolean indent) {
        // check name
        if (child.name.startsWith("PipelineTask") || child.name.startsWith("PipelineContext")) {
            return;
        }
        childLock.writeLock().lock();
        try {
            Pair<RuntimeProfile, Boolean> pair = Pair.of(child, indent);
            this.childList.add(pair);
        } finally {
            childLock.writeLock().unlock();
        }
        // insert plan node info to profile string info
        if (planNodeMap == null || !planNodeMap.containsKey(child.nodeId())) {
            return;
        }

        /*
         * The check for SINK_OPERATOR is performed because SINK_OPERATOR does not have
         * a corresponding plan node ID.
         * Currently, the plan node info is only printed for non-sink nodes in the merge
         * profile.
         */
        if (name.contains("_SINK_OPERATOR")) {
            child.addPlanNodeInfos(planNodeMap.get(child.nodeId()));
            planNodeMap.remove(child.nodeId());
        }
    }

    public void addPlanNodeInfos(String infos) {
        String[] infoList = infos.split("\n");
        for (String info : infoList) {
            planNodeInfos.add(info);
        }
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

    public void write(DataOutput output) throws IOException {
        Text.writeString(output, GsonUtils.GSON.toJson(this));
    }

    private TPlanNodeRuntimeStatsItem toTPlanNodeRuntimeStatsItem() {
        TPlanNodeRuntimeStatsItem item = new TPlanNodeRuntimeStatsItem();
        item.setNodeId(this.nodeId());
        boolean isBuildSinkOperator = this.getName().startsWith("HASH_JOIN_SINK_OPERATOR");
        for (Map.Entry<String, Counter> entry : this.counterMap.entrySet()) {
            if (entry.getValue() instanceof AggCounter) {
                AggCounter value = (AggCounter) entry.getValue();
                String key = entry.getKey();
                item.setInstanceNum(value.number);
                switch (key) {
                    case "RowsProduced":
                        item.setOutputRows(value.sum.getValue());
                        break;
                    case "InputRows":
                        item.setInputRows(value.sum.getValue());
                        if (isBuildSinkOperator) {
                            item.setJoinBuilderRows(value.sum.getValue());
                            if (value.sum.getValue() == 0 || value.sum.getValue() / value.number == 0) {
                                // builder side may be empty, set 1 directly for passing skew checking
                                item.setJoinBuilderSkewRatio(1);
                            } else {
                                long avgBuildValue = value.sum.getValue() / value.number;
                                item.setJoinBuilderSkewRatio((int) (value.max.getValue() / avgBuildValue));
                            }
                        }
                        break;
                    case "ProbeRows":
                        item.setJoinProbeRows(value.sum.getValue());
                        if (value.sum.getValue() == 0 || value.sum.getValue() / value.number == 0) {
                            // probe side may be empty, set 1 directly for passing skew checking
                            item.setJoinProberSkewRatio(1);
                        } else {
                            long avgProbeValue = value.sum.getValue() / value.number;
                            item.setJoinProberSkewRatio((int) (value.max.getValue() / avgProbeValue));
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        // Handle runtime profile structure
        /*
- RuntimeFilterInfo: sum 0, avg 0, max 0, min 0
- RF0 FilterRows: sum 1, avg 0, max 1, min 0
- RF0 InputRows: sum 390.144K (390144), avg 8.128K (8128), max 8.128K (8128), min 8.128K (8128)
- RF1 FilterRows: sum 9.349313M (9349313), avg 194.777K (194777), max 196.512K (196512), min 192.736K (192736)
- RF1 InputRows: sum 11.692823M (11692823), avg 243.6K (243600), max 245.822K (245822), min 240.939K (240939)
- RF2 FilterRows: sum 0, avg 0, max 0, min 0
- RF2 InputRows: sum 390.143K (390143), avg 8.127K (8127), max 8.128K (8128), min 8.127K (8127)
- RF3 FilterRows: sum 0, avg 0, max 0, min 0
- RF3 InputRows: sum 0, avg 0, max 0, min 0
- RF4 FilterRows: sum 0, avg 0, max 0, min 0
- RF4 InputRows: sum 0, avg 0, max 0, min 0
- RF5 FilterRows: sum 86.632484M (86632484), avg 1.804843M (1804843), max 1.809673M (1809673), min 1.7993M (1799300)
- RF5 InputRows: sum 144.419127M (144419127), avg 3.008731M (3008731), max 3.015499M (3015499), min 3.001376M (3001376)
- RF6 FilterRows: sum 0, avg 0, max 0, min 0
- RF6 InputRows: sum 361.696K (361696), avg 7.535K (7535), max 16.256K (16256), min 0
- RF7 FilterRows: sum 46.09382M (46093820), avg 960.287K (960287), max 962.989K (962989), min 956.812K (956812)
- RF7 InputRows: sum 57.786643M (57786643), avg 1.203888M (1203888), max 1.206715M (1206715), min 1.199902M (1199902)
         */
        for (Entry<String, TreeSet<String>> entry : this.childCounterMap.entrySet()) {
            if (entry.getKey().equals("RuntimeFilterInfo")) {
                AggCounter filteredAggCounter = new AggCounter(TUnit.UNIT);
                AggCounter inputAggCounter = new AggCounter(TUnit.UNIT);
                Set<String> childCounterSet = entry.getValue();
                for (String childCounterName : childCounterSet) {
                    Counter rhsAggCounter = this.counterMap.get(childCounterName);
                    if (rhsAggCounter == null) {
                        LOG.warn("Should not happen, childCounter {} not found.", childCounterName);
                        throw new RuntimeException("Invalid profile");
                    }
                    // Only merged profile will call toTPlanNodeRuntimeStatsItem()
                    if (!(rhsAggCounter instanceof AggCounter)) {
                        LOG.warn("Should not happen, invalid counter type, counter name: {}", childCounterName);
                        throw new RuntimeException("Invalid profile");
                    }

                    if (childCounterName.endsWith("FilterRows")) {
                        filteredAggCounter.mergeCounter((AggCounter) rhsAggCounter);
                    } else if (childCounterName.endsWith("InputRows")) {
                        inputAggCounter.mergeCounter((AggCounter) rhsAggCounter);
                    } else {
                        LOG.warn("Should not happen, childCounterName: " + childCounterName);
                    }
                }
                item.setRuntimeFilterInputRows(inputAggCounter.sum.getValue());
                item.setRuntimeFilterRows(filteredAggCounter.sum.getValue());
            }
        }
        return item;
    }

    public static List<TPlanNodeRuntimeStatsItem> toTPlanNodeRuntimeStatsItem(RuntimeProfile profile,
            List<TPlanNodeRuntimeStatsItem> itemsFromParent) {
        if (itemsFromParent == null) {
            itemsFromParent = new ArrayList<>();
        }

        // Recurse on children
        profile.childLock.readLock().lock();
        try {
            for (int i = 0; i < profile.childList.size(); i++) {
                List<TPlanNodeRuntimeStatsItem> childItems = new ArrayList<>();
                itemsFromParent.addAll(toTPlanNodeRuntimeStatsItem(profile.childList.get(i).first, childItems));
            }
        } finally {
            profile.childLock.readLock().unlock();
        }

        String name = profile.getName();
        if (!name.contains("_OPERATOR")) {
            return itemsFromParent;
        } else {
            int index = name.indexOf('(');
            name = name.substring(0, index).trim();
        }

        // Check if current node is valid for processing
        if (!PLAN_NODE_TYPE_MAP.contains(name)) {
            return itemsFromParent; // Skip current node
        }
        // Add stats for current node
        itemsFromParent.add(profile.toTPlanNodeRuntimeStatsItem());

        if (LOG.isDebugEnabled()) {
            List<TPlanNodeRuntimeStatsItem> currentItem = new ArrayList<TPlanNodeRuntimeStatsItem>();
            currentItem.add(profile.toTPlanNodeRuntimeStatsItem());
            LOG.debug("Current node {}({}) hbo items\n{},\nparent\n{}",
                    profile.getName(), profile.nodeid,
                    DebugUtil.prettyPrintPlanNodeRuntimeStatsItems(currentItem),
                    DebugUtil.prettyPrintPlanNodeRuntimeStatsItems(itemsFromParent));
        }

        return itemsFromParent;
    }

    public static List<TPlanNodeRuntimeStatsItem> mergeTPlanNodeRuntimeStatsItem(
            List<TPlanNodeRuntimeStatsItem> items) {
        Map<Integer, TPlanNodeRuntimeStatsItem> itemMap = new HashMap<>();
        for (TPlanNodeRuntimeStatsItem item : items) {
            int nodeId = item.getNodeId();
            if (itemMap.containsKey(nodeId)) {
                TPlanNodeRuntimeStatsItem oldItem = itemMap.get(nodeId);
                oldItem.setInputRows(oldItem.getInputRows() + item.getInputRows());
                oldItem.setOutputRows(oldItem.getOutputRows() + item.getOutputRows());
                oldItem.setJoinProbeRows(oldItem.getJoinProbeRows() + item.getJoinProbeRows());
                oldItem.setJoinProberSkewRatio(oldItem.getJoinProberSkewRatio() + item.getJoinProberSkewRatio());
                oldItem.setJoinBuilderRows(oldItem.getJoinBuilderRows() + item.getJoinBuilderRows());
                oldItem.setJoinBuilderSkewRatio(oldItem.getJoinBuilderSkewRatio() + item.getJoinBuilderSkewRatio());
                oldItem.setInstanceNum(oldItem.getInstanceNum() + item.getInstanceNum());
            } else {
                itemMap.put(nodeId, item);
            }
        }
        return new ArrayList<>(itemMap.values());
    }
}
