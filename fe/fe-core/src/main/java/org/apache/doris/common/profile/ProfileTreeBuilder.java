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
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Counter;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.thrift.TUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is used to build a tree from the query runtime profile
 * It will build tree for the entire query, and also tree for each instance,
 * So that user can view the profile tree by query id or by instance id.
 *
 * Each runtime profile of a query should be built once and be read every where.
 */
public class ProfileTreeBuilder {

    private static final String PROFILE_NAME_DATA_STREAM_SENDER = "DataStreamSender";
    private static final String PROFILE_NAME_DATA_BUFFER_SENDER = "DataBufferSender";
    private static final String PROFILE_NAME_OLAP_TABLE_SINK = "OlapTableSink";
    private static final String PROFILE_NAME_BLOCK_MGR = "BlockMgr";
    private static final String PROFILE_NAME_BUFFER_POOL = "Buffer pool";
    private static final String PROFILE_NAME_EXCHANGE_NODE = "EXCHANGE_NODE";
    public static final String FINAL_SENDER_ID = "-1";
    public static final String UNKNOWN_ID = "-2";

    private RuntimeProfile profileRoot;

    // auxiliary structure to link different fragments
    private List<ProfileTreeNode> exchangeNodes = Lists.newArrayList();
    private List<ProfileTreeNode> senderNodes = Lists.newArrayList();

    // fragment id -> instance id -> instance tree root
    private Map<String, Map<String, ProfileTreeNode>> instanceTreeMap = Maps.newHashMap();
    // fragment id -> (instance id, instance host, instance active time)
    private Map<String, List<Triple<String, String, Long>>> instanceActiveTimeMap = Maps.newHashMap();

    // the tree root of the entire query profile tree
    private ProfileTreeNode fragmentTreeRoot;

    // Match string like:
    // EXCHANGE_NODE (id=3):(Active: 103.899ms, % non-child: 2.27%)
    // Extract "EXCHANGE_NODE" and "3"
    private static final String EXEC_NODE_NAME_ID_PATTERN_STR = "^(.*) .*id=([0-9]+).*";
    private static final Pattern EXEC_NODE_NAME_ID_PATTERN;

    // Match string like:
    // Fragment 0:
    // Extract "0"
    private static final String FRAGMENT_ID_PATTERN_STR = "^Fragment ([0-9]+).*";
    private static final Pattern FRAGMENT_ID_PATTERN;

    // Match string like:
    // Instance e0f7390f5363419e-b416a2a7999608b6 (host=TNetworkAddress(hostname:192.168.1.1, port:9060)):(Active: 1s858ms, % non-child: 0.02%)
    // Extract "e0f7390f5363419e-b416a2a7999608b6", "192.168.1.1", "9060"
    private static final String INSTANCE_PATTERN_STR = "^Instance (.*) \\(.*hostname:(.*), port:([0-9]+).*";
    private static final Pattern INSTANCE_PATTERN;

    static {
        EXEC_NODE_NAME_ID_PATTERN = Pattern.compile(EXEC_NODE_NAME_ID_PATTERN_STR);
        FRAGMENT_ID_PATTERN = Pattern.compile(FRAGMENT_ID_PATTERN_STR);
        INSTANCE_PATTERN = Pattern.compile(INSTANCE_PATTERN_STR);
    }

    public ProfileTreeBuilder(RuntimeProfile root) {
        this.profileRoot = root;
    }

    public ProfileTreeNode getFragmentTreeRoot() {
        return fragmentTreeRoot;
    }

    public ProfileTreeNode getInstanceTreeRoot(String fragmentId, String instanceId) {
        if (!instanceTreeMap.containsKey(fragmentId)) {
            return null;
        }
        return instanceTreeMap.get(fragmentId).get(instanceId);
    }

    public List<Triple<String, String, Long>> getInstanceList(String fragmentId) {
        return instanceActiveTimeMap.get(fragmentId);
    }

    public void build() throws UserException {
        reset();
        checkProfile();
        analyzeAndBuildFragmentTrees();
        assembleFragmentTrees();
    }

    private void reset() {
        exchangeNodes.clear();
        senderNodes.clear();
        instanceTreeMap.clear();
        instanceActiveTimeMap.clear();
        fragmentTreeRoot = null;
    }

    private void checkProfile() throws UserException {
        if (!profileRoot.getName().startsWith(MultiProfileTreeBuilder.PROFILE_NAME_EXECUTION)) {
            throw new UserException("Invalid profile. Expected " + MultiProfileTreeBuilder.PROFILE_NAME_EXECUTION);
        }
    }

    private void analyzeAndBuildFragmentTrees() throws UserException {
        List<Pair<RuntimeProfile, Boolean>> childrenFragment = profileRoot.getChildList();
        for (Pair<RuntimeProfile, Boolean> pair : childrenFragment) {
            analyzeAndBuildFragmentTree(pair.first);
        }
    }

    private void analyzeAndBuildFragmentTree(RuntimeProfile fragmentProfile) throws UserException {
        String fragmentId = getFragmentId(fragmentProfile);
        List<Pair<RuntimeProfile, Boolean>> fragmentChildren = fragmentProfile.getChildList();
        if (fragmentChildren.isEmpty()) {
            throw new UserException("Empty instance in fragment: " + fragmentProfile.getName());
        }

        // 1. Get max active time of instances in this fragment
        List<Triple<String, String, Long>> instanceIdAndActiveTimeList = Lists.newArrayList();
        long maxActiveTimeNs = 0;
        for (Pair<RuntimeProfile, Boolean> pair : fragmentChildren) {
            Triple<String, String, Long> instanceIdAndActiveTime = getInstanceIdHostAndActiveTime(pair.first);
            maxActiveTimeNs = Math.max(instanceIdAndActiveTime.getRight(), maxActiveTimeNs);
            instanceIdAndActiveTimeList.add(instanceIdAndActiveTime);
        }
        instanceActiveTimeMap.put(fragmentId, instanceIdAndActiveTimeList);

        // 2. Build tree for all fragments
        //    All instance in a fragment are same, so use first instance to build the fragment tree
        RuntimeProfile instanceProfile = fragmentChildren.get(0).first;
        ProfileTreeNode instanceTreeRoot = buildSingleInstanceTree(instanceProfile, fragmentId, null);
        instanceTreeRoot.setMaxInstanceActiveTime(RuntimeProfile.printCounter(maxActiveTimeNs, TUnit.TIME_NS));
        if (instanceTreeRoot.id.equals(FINAL_SENDER_ID)) {
            fragmentTreeRoot = instanceTreeRoot;
        }

        // 2. Build tree for each single instance
        int i = 0;
        Map<String, ProfileTreeNode> instanceTrees = Maps.newHashMap();
        for (Pair<RuntimeProfile, Boolean> pair : fragmentChildren) {
            String instanceId = instanceIdAndActiveTimeList.get(i).getLeft();
            ProfileTreeNode singleInstanceTreeNode = buildSingleInstanceTree(pair.first, fragmentId, instanceId);
            instanceTrees.put(instanceId, singleInstanceTreeNode);
            i++;
        }
        this.instanceTreeMap.put(fragmentId, instanceTrees);
    }

    // If instanceId is null, which means this profile tree node is for building the entire fragment tree.
    // So that we need to add sender and exchange node to the auxiliary structure.
    private ProfileTreeNode buildSingleInstanceTree(RuntimeProfile instanceProfile, String fragmentId,
                                                    String instanceId) throws UserException {
        List<Pair<RuntimeProfile, Boolean>> instanceChildren = instanceProfile.getChildList();
        ProfileTreeNode senderNode = null;
        ProfileTreeNode execNode = null;
        for (Pair<RuntimeProfile, Boolean> pair : instanceChildren) {
            RuntimeProfile profile = pair.first;
            if (profile.getName().startsWith(PROFILE_NAME_DATA_STREAM_SENDER)
                    || profile.getName().startsWith(PROFILE_NAME_DATA_BUFFER_SENDER)
                    || profile.getName().startsWith(PROFILE_NAME_OLAP_TABLE_SINK)) {
                senderNode = buildTreeNode(profile, null, fragmentId, instanceId);
                if (instanceId == null) {
                    senderNodes.add(senderNode);
                }
            } else if (profile.getName().startsWith(PROFILE_NAME_BLOCK_MGR)
                    || profile.getName().startsWith(PROFILE_NAME_BUFFER_POOL)) {
                // skip BlockMgr nad Buffer pool profile
                continue;
            } else {
                // This should be an ExecNode profile
                execNode = buildTreeNode(profile, null, fragmentId, instanceId);
            }
        }
        if (senderNode == null || execNode == null) {
            throw new UserException("Invalid instance profile, without sender or exec node: " + instanceProfile);
        }
        senderNode.addChild(execNode);
        execNode.setParentNode(senderNode);

        senderNode.setFragmentAndInstanceId(fragmentId, instanceId);
        execNode.setFragmentAndInstanceId(fragmentId, instanceId);

        return senderNode;
    }

    private ProfileTreeNode buildTreeNode(RuntimeProfile profile, ProfileTreeNode root,
                                          String fragmentId, String instanceId) {
        String name = profile.getName();
        if (name.startsWith(PROFILE_NAME_BUFFER_POOL)) {
            // skip Buffer pool, and buffer pool does not has child
            return null;
        }
        String finalSenderName = checkAndGetFinalSenderName(name);
        Matcher m = EXEC_NODE_NAME_ID_PATTERN.matcher(name);
        String extractName;
        String extractId;
        if ((!m.find() && finalSenderName == null) || m.groupCount() != 2) {
            // DataStreamBuffer name like: "DataBufferSender (dst_fragment_instance_id=d95356f9219b4831-986b4602b41683ca):"
            // So it has no id.
            // Other profile should has id like:
            // EXCHANGE_NODE (id=3):(Active: 103.899ms, % non-child: 2.27%)
            // HASH_JOIN_NODE (id=2):(Active: 972.329us, % non-child: 0.00%)
            extractName = name;
            extractId = UNKNOWN_ID;
        } else {
            extractName = finalSenderName != null ? finalSenderName : m.group(1);
            extractId = finalSenderName != null ? FINAL_SENDER_ID : m.group(2);
        }
        Counter activeCounter = profile.getCounterTotalTime();
        ExecNodeNode node = new ExecNodeNode(extractName, extractId);
        node.setActiveTime(RuntimeProfile.printCounter(activeCounter.getValue(), activeCounter.getType()));
        try (Formatter fmt = new Formatter()) {
            node.setNonChild(fmt.format("%.2f", profile.getLocalTimePercent()).toString());
        }
        CounterNode rootCounterNode = new CounterNode();
        buildCounterNode(profile, RuntimeProfile.ROOT_COUNTER, rootCounterNode);
        node.setCounterNode(rootCounterNode);

        if (root != null) {
            root.addChild(node);
            node.setParentNode(root);
        }

        if (node.name.equals(PROFILE_NAME_EXCHANGE_NODE) && instanceId == null) {
            exchangeNodes.add(node);
        }

        // The children in profile is reversed, so traverse it from last to first
        List<Pair<RuntimeProfile, Boolean>> children = profile.getChildList();
        for (int i = children.size() - 1; i >= 0; i--) {
            Pair<RuntimeProfile, Boolean> pair = children.get(i);
            ProfileTreeNode execNode = buildTreeNode(pair.first, node, fragmentId, instanceId);
            if (execNode != null) {
                // For buffer pool profile, buildTreeNode will return null
                execNode.setFragmentAndInstanceId(fragmentId, instanceId);
            }
        }
        return node;
    }

    // Check if the given node name is from final node, like DATA_BUFFER_SENDER or OLAP_TABLE_SINK
    // If yes, return that name, if not, return null;
    private String checkAndGetFinalSenderName(String name) {
        if (name.startsWith(PROFILE_NAME_DATA_BUFFER_SENDER)) {
            return PROFILE_NAME_DATA_BUFFER_SENDER;
        } else if (name.startsWith(PROFILE_NAME_OLAP_TABLE_SINK)) {
            return PROFILE_NAME_OLAP_TABLE_SINK;
        } else {
            return null;
        }
    }

    private void buildCounterNode(RuntimeProfile profile, String counterName, CounterNode root) {
        Map<String, TreeSet<String>> childCounterMap = profile.getChildCounterMap();
        Set<String> childCounterSet = childCounterMap.get(counterName);
        if (childCounterSet == null) {
            return;
        }

        Map<String, Counter> counterMap = profile.getCounterMap();
        for (String childCounterName : childCounterSet) {
            Counter counter = counterMap.get(childCounterName);
            CounterNode counterNode = new CounterNode();
            if (root != null) {
                root.addChild(counterNode);
            }
            counterNode.setCounter(childCounterName, RuntimeProfile.printCounter(counter.getValue(), counter.getType()));
            buildCounterNode(profile, childCounterName, counterNode);
        }
        return;
    }

    private void assembleFragmentTrees() throws UserException {
        for (ProfileTreeNode senderNode : senderNodes) {
            if (senderNode.id.equals(FINAL_SENDER_ID)) {
                // this is result sender, skip it.
                continue;
            }
            ProfileTreeNode exchangeNode = findExchangeNode(senderNode.id);
            exchangeNode.addChild(senderNode);
            senderNode.setParentNode(exchangeNode);
        }
    }

    private ProfileTreeNode findExchangeNode(String senderId) throws UserException {
        for (ProfileTreeNode node : exchangeNodes) {
            if (node.id.equals(senderId)) {
                return node;
            }
        }
        throw new UserException("Failed to find fragment for sender id: " + senderId);
    }

    private String getFragmentId(RuntimeProfile fragmentProfile) throws UserException {
        String name = fragmentProfile.getName();
        Matcher m = FRAGMENT_ID_PATTERN.matcher(name);
        if (!m.find() || m.groupCount() != 1) {
            throw new UserException("Invalid fragment profile name: " + name);
        }
        return m.group(1);
    }

    private Triple<String, String, Long> getInstanceIdHostAndActiveTime(RuntimeProfile instanceProfile)
            throws UserException {
        long activeTimeNs = instanceProfile.getCounterTotalTime().getValue();
        String name = instanceProfile.getName();
        Matcher m = INSTANCE_PATTERN.matcher(name);
        if (!m.find() || m.groupCount() != 3) {
            throw new UserException("Invalid instance profile name: " + name);
        }
        return new ImmutableTriple<>(m.group(1), m.group(2) + ":" + m.group(3), activeTimeNs);
    }
}
