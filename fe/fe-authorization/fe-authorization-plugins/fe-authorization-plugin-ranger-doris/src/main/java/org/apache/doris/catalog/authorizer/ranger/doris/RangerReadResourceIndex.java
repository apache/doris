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

package org.apache.doris.catalog.authorizer.ranger.doris;

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator.RangerPolicyResourceEvaluator;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher.MatchType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A Boolean-only index for the static direct-user snapshots accepted by {@link RangerReadScopeSummary}.
 * Columns remain in Ranger's existing matchers. Raw Ranger results and audit callbacks are never synthesized.
 */
final class RangerReadResourceIndex {
    static final String ENABLED = ".read.resource.index.enabled";
    private static final int MAX_BINDINGS = 4_000_000;
    private static final byte SELECT = 1;
    private static final byte ADMIN = 2;
    private static final List<String> PREFIX = Arrays.asList("catalog", "database", "table");

    private final Map<String, Integer> users;
    private final Map<TableKey, TableAccess> tables;
    private final TableAccess broadResources;
    private final Set<String> nativeUsers;
    private final boolean[] ignoreCase;
    private final int bindings;

    private RangerReadResourceIndex(Map<String, Integer> users, Map<TableKey, TableAccess> tables,
            TableAccess broadResources, Set<String> nativeUsers, boolean[] ignoreCase, int bindings) {
        this.users = users;
        this.tables = tables;
        this.broadResources = broadResources;
        this.nativeUsers = Collections.unmodifiableSet(nativeUsers);
        this.ignoreCase = ignoreCase;
        this.bindings = bindings;
    }

    // This is called only after the complete service snapshot has passed the summary's eligibility checks.
    static RangerReadResourceIndex build(RangerPolicyEngine engine, RangerPluginConfig config,
            Map<String, String> effectiveConfig) {
        String configured = config.get(config.getPropertyPrefix() + ENABLED);
        if (configured != null && !"true".equals(configured) && !"false".equals(configured)) {
            throw new IllegalArgumentException("Expected true or false for " + config.getPropertyPrefix() + ENABLED);
        }
        if ("false".equals(configured) || !(engine instanceof RangerPolicyEngineImpl)
                || config.getBoolean(config.getPropertyPrefix() + ".supports.in.place.policy.updates", false)
                || Boolean.parseBoolean(effectiveConfig.get(
                        config.getPropertyPrefix() + ".supports.in.place.policy.updates"))) {
            return null;
        }
        boolean[] ignoreCase = new boolean[PREFIX.size()];
        for (RangerResourceDef definition : engine.getServiceDef().getResources()) {
            int level = PREFIX.indexOf(definition.getName());
            if (level < 0) {
                continue;
            }
            String option = definition.getMatcherOptions().get("ignoreCase");
            if (option != null && !"true".equalsIgnoreCase(option) && !"false".equalsIgnoreCase(option)) {
                return null;
            }
            ignoreCase[level] = option == null || Boolean.parseBoolean(option);
        }
        Map<String, Integer> users = new HashMap<>();
        Map<TableKey, TableBuilder> builders = new HashMap<>();
        TableBuilder broad = new TableBuilder();
        Set<String> nativeUsers = new HashSet<>();
        int bindings = 0;
        List<RangerPolicyEvaluator> policies = ((RangerPolicyEngineImpl) engine).getPolicyEngine()
                .getPolicyRepository().getPolicyEvaluators();
        for (RangerPolicyEvaluator evaluator : policies) {
            Map<String, Byte> actions = readActions(evaluator.getPolicy());
            if (actions == null) {
                return null;
            }
            if (actions.isEmpty()) {
                continue;
            }
            for (RangerPolicyResourceEvaluator resource : evaluator.getResourceEvaluators()) {
                if (!hasOnlyAsciiValues(resource.getPolicyResource())) {
                    // Native trie lookup and equalsIgnoreCase matchers can disagree on Unicode folds.
                    // Reusing the matcher alone must not grant an access rejected by native candidate lookup.
                    nativeUsers.addAll(actions.keySet());
                    continue;
                }
                TableKey key = policyKey(resource.getPolicyResource(), ignoreCase);
                if (actions.size() > MAX_BINDINGS - bindings) {
                    return null;
                }
                TableBuilder builder = broad;
                if (key != null) {
                    builder = builders.computeIfAbsent(key, ignored -> new TableBuilder());
                }
                for (Map.Entry<String, Byte> action : actions.entrySet()) {
                    int user = users.computeIfAbsent(action.getKey(), ignored -> users.size());
                    builder.add(user, action.getValue(), resource.getPolicyResourceMatcher());
                    bindings++;
                }
            }
        }
        Map<TableKey, TableAccess> tables = new HashMap<>();
        int[] positions = new int[users.size()];
        for (Map.Entry<TableKey, TableBuilder> builder : builders.entrySet()) {
            tables.put(builder.getKey(), builder.getValue().finish(positions));
        }
        return new RangerReadResourceIndex(users, tables, broad.finish(positions), nativeUsers, ignoreCase, bindings);
    }

    Boolean allows(String user, DorisAccessType action, RangerDorisResource resource) {
        String column = (String) resource.getValue("column");
        if (column == null || nativeUsers.contains(user) || !isAscii(column)) {
            return null;
        }
        TableKey key = requestKey(resource, ignoreCase);
        if (key == null) {
            return null;
        }
        TableAccess table = tables.get(key);
        Integer userId = users.get(user);
        if (userId == null) {
            return false;
        }
        byte required = action == DorisAccessType.SELECT ? SELECT : ADMIN;
        if (table != null && table.allows(userId, required, resource)) {
            return true;
        }
        return broadResources.allows(userId, required, resource);
    }

    int tableCount() {
        return tables.size();
    }

    int userCount() {
        return users.size();
    }

    int bindingCount() {
        return bindings;
    }

    int nativeUserCount() {
        return nativeUsers.size();
    }

    private static boolean hasOnlyAsciiValues(Map<String, RangerPolicyResource> resources) {
        // Native matcher initialization already uses this key set; avoid caching an extra values view per map.
        for (String name : resources.keySet()) {
            List<String> values = resources.get(name).getValues();
            for (int index = 0; index < values.size(); index++) {
                if (!isAscii(values.get(index))) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isAscii(String value) {
        for (int index = 0; index < value.length(); index++) {
            if (value.charAt(index) > 127) {
                return false;
            }
        }
        return true;
    }

    private static Map<String, Byte> readActions(RangerPolicy policy) {
        Map<String, Byte> result = new HashMap<>();
        for (RangerPolicyItem item : policy.getPolicyItems()) {
            byte actions = 0;
            for (RangerPolicyItemAccess access : item.getAccesses()) {
                if (Boolean.FALSE.equals(access.getIsAllowed())) {
                    continue;
                }
                if ("SELECT".equals(access.getType())) {
                    actions |= SELECT;
                } else if ("ADMIN".equals(access.getType())) {
                    actions |= ADMIN;
                } else if ("SELECT".equalsIgnoreCase(access.getType()) || "ADMIN".equalsIgnoreCase(access.getType())) {
                    return null;
                }
            }
            if (actions == 0) {
                continue;
            }
            final byte granted = actions;
            for (String user : item.getUsers()) {
                result.merge(user, granted, (first, second) -> (byte) (first | second));
            }
        }
        return result;
    }

    private static TableKey policyKey(Map<String, RangerPolicyResource> resource, boolean[] ignoreCase) {
        String[] values = new String[PREFIX.size()];
        for (int level = 0; level < values.length; level++) {
            RangerPolicyResource part = resource.get(PREFIX.get(level));
            if (part == null || part.getValues().size() != 1) {
                return null;
            }
            values[level] = normalize(part.getValues().get(0), ignoreCase[level]);
            if (values[level] == null) {
                return null;
            }
        }
        return new TableKey(values[0], values[1], values[2]);
    }

    private static TableKey requestKey(RangerDorisResource resource, boolean[] ignoreCase) {
        String[] values = new String[PREFIX.size()];
        for (int level = 0; level < values.length; level++) {
            Object part = resource.getValue(PREFIX.get(level));
            if (!(part instanceof String)) {
                return null;
            }
            values[level] = normalize((String) part, ignoreCase[level]);
            if (values[level] == null) {
                return null;
            }
        }
        return new TableKey(values[0], values[1], values[2]);
    }

    private static String normalize(String value, boolean ignoreCase) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character > 127 || character == '*' || character == '?' || character == '{' || character == '}') {
                return null;
            }
        }
        return ignoreCase ? value.toLowerCase(Locale.ROOT) : value;
    }

    private static final class TableKey {
        private final String catalog;
        private final String database;
        private final String table;

        private TableKey(String catalog, String database, String table) {
            this.catalog = catalog;
            this.database = database;
            this.table = table;
        }

        @Override
        public int hashCode() {
            return 31 * (31 * catalog.hashCode() + database.hashCode()) + table.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof TableKey)) {
                return false;
            }
            TableKey other = (TableKey) object;
            return catalog.equals(other.catalog) && database.equals(other.database) && table.equals(other.table);
        }
    }

    private static final class TableAccess {
        private final int[] users;
        private final byte[] actions;
        private final RangerPolicyResourceMatcher[] matchers;

        private TableAccess(int[] users, byte[] actions, RangerPolicyResourceMatcher[] matchers) {
            this.users = users;
            this.actions = actions;
            this.matchers = matchers;
        }

        private boolean allows(int user, byte action, RangerDorisResource resource) {
            int low = 0;
            int high = users.length;
            while (low < high) {
                int middle = (low + high) >>> 1;
                if (users[middle] < user) {
                    low = middle + 1;
                    continue;
                }
                high = middle;
            }
            for (int index = low; index < users.length && users[index] == user; index++) {
                if ((actions[index] & action) == 0) {
                    continue;
                }
                MatchType match = matchers[index].getMatchType(resource, Collections.emptyMap());
                if (match == MatchType.SELF || match == MatchType.SELF_AND_ALL_DESCENDANTS) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final class TableBuilder {
        private int[] users = new int[4];
        private byte[] actions = new byte[4];
        private RangerPolicyResourceMatcher[] matchers = new RangerPolicyResourceMatcher[4];
        private int size;

        private void add(int user, byte action, RangerPolicyResourceMatcher matcher) {
            if (size == users.length) {
                users = Arrays.copyOf(users, size * 2);
                actions = Arrays.copyOf(actions, size * 2);
                matchers = Arrays.copyOf(matchers, size * 2);
            }
            users[size] = user;
            actions[size] = action;
            matchers[size++] = matcher;
        }

        private TableAccess finish(int[] positions) {
            Arrays.fill(positions, 0);
            for (int index = 0; index < size; index++) {
                positions[users[index]]++;
            }
            int next = 0;
            for (int user = 0; user < positions.length; user++) {
                int count = positions[user];
                positions[user] = next;
                next += count;
            }
            int[] sortedUsers = new int[size];
            byte[] sortedActions = new byte[size];
            RangerPolicyResourceMatcher[] sortedMatchers = new RangerPolicyResourceMatcher[size];
            for (int index = 0; index < size; index++) {
                int destination = positions[users[index]]++;
                sortedUsers[destination] = users[index];
                sortedActions[destination] = actions[index];
                sortedMatchers[destination] = matchers[index];
            }
            return new TableAccess(sortedUsers, sortedActions, sortedMatchers);
        }
    }
}
