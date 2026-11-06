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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/** Reproducible engine-call benchmark; deliberately does not claim FE SQL end-to-end performance. */
public final class V2AuthorizationBenchmark {
    /** Dimensions and modes read from the benchmark configuration JSON. */
    public static final class Config {
        public int users = 260;
        public int tablesPerUser = 800;
        public int tables = 5000;
        public int columns = 3;
        public int warmups = 10;
        public int samples = 60;
        public long seed = 83;
        public String mode = "cascade";
        public String requirement = "read";
        public boolean refresh = true;
        public boolean controllerVerdictsOnly = false;
        public boolean bootstrapAdminPolicy = false;
        public String adapter = "";
        public String serviceDef;
    }

    /** Implement this in the same package to adapt the production controller/scope-summary path. */
    public interface Adapter extends AutoCloseable {

        void install(ServicePolicies policies);

        RangerAccessResult evaluate(RangerAccessRequest request);

        long version();

        String describe();

        default long nativeEngineCalls() {
            return -1;
        }

        void close();
    }

    /** Uses an unmodified native plugin initialized with the actual production constructor. */
    public static final class StockAdapter implements Adapter {
        private final RangerBasePlugin plugin;
        private long engineCalls;

        public StockAdapter(RangerPluginConfig config) {
            // The real constructor selects V2 and initializes super-user/audit defaults. No init/network.
            plugin = new RangerBasePlugin(config);
        }

        @Override
        public void install(ServicePolicies policies) {
            plugin.setPolicies(policies);
        }

        @Override
        public RangerAccessResult evaluate(RangerAccessRequest request) {
            engineCalls++;
            return plugin.isAccessAllowed(request, null);
        }

        @Override
        public long version() {
            return plugin.getPoliciesVersion();
        }

        @Override
        public String describe() {
            return "stock RangerBasePlugin.setPolicies/isAccessAllowed";
        }

        @Override
        public long nativeEngineCalls() {
            return engineCalls;
        }

        @Override
        public void close() {
            plugin.cleanup();
        }
    }

    private static final class HeapSampler implements AutoCloseable {
        private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        private final AtomicLong peak = new AtomicLong();
        private volatile boolean active = true;
        private final Thread thread;

        HeapSampler() {
            thread = new Thread(() -> {
                while (active) {
                    peak.accumulateAndGet(memory.getHeapMemoryUsage().getUsed(), Math::max);
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException stop) {
                        return;
                    }
                }
            }, "authorization-benchmark-heap-sampler");
            thread.setDaemon(true);
            thread.start();
        }

        void reset() {
            peak.set(memory.getHeapMemoryUsage().getUsed());
        }

        long peak() {
            return peak.get();
        }

        @Override
        public void close() {
            active = false;
            thread.interrupt();
        }
    }

    private final Config config;
    private final Adapter adapter;
    private long checks;
    private long fingerprint;
    private long requestFingerprint;

    private V2AuthorizationBenchmark(Config config, Adapter adapter) {
        this.config = config;
        this.adapter = adapter;
    }

    private static long retainedHeap() throws InterruptedException {
        System.gc();
        Thread.sleep(120);
        System.gc();
        Thread.sleep(120);
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    private static double mib(long bytes) {
        return bytes / 1048576.0;
    }

    private Map<String, RangerPolicyResource> resource(int table, String user) {
        Map<String, RangerPolicyResource> block = new HashMap<>();
        block.put("catalog", new RangerPolicyResource("internal"));
        block.put("database", new RangerPolicyResource("db"));
        block.put("table", new RangerPolicyResource("t" + table));
        List<String> columns = new ArrayList<>();
        for (int column = 0; column < config.columns; column++) {
            columns.add(column == 0 ? "c0" : "c" + table + "_" + column);
        }
        columns.add(user);
        block.put("column", new RangerPolicyResource(columns, false, false));
        return block;
    }

    private ServicePolicies snapshot(long version, boolean revokeFirstTable) throws Exception {
        List<RangerPolicy> policies = new ArrayList<>();
        if (config.bootstrapAdminPolicy) {
            RangerPolicy admin = new RangerPolicy();
            admin.setId(config.users + 1L);
            admin.setVersion(version);
            admin.setName("bootstrap_admin");
            admin.setService("authorization-benchmark");
            admin.setIsEnabled(true);
            admin.setIsAuditEnabled(true);
            admin.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
            admin.setResources(Map.of("global", new RangerPolicyResource("*")));
            RangerPolicyItem item = new RangerPolicyItem();
            item.setUsers(List.of("bootstrap-admin"));
            item.setAccesses(List.of(new RangerPolicyItemAccess("ADMIN", true)));
            admin.setPolicyItems(List.of(item));
            policies.add(admin);
        }
        for (int user = 0; user < config.users; user++) {
            String name = "ad_" + user;
            RangerPolicy policy = new RangerPolicy();
            policy.setId(user + 1L);
            policy.setVersion(version);
            policy.setName("user_" + user);
            policy.setService("authorization-benchmark");
            policy.setIsEnabled(true);
            policy.setIsAuditEnabled(true);
            policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
            RangerPolicyItem item = new RangerPolicyItem();
            item.setUsers(new ArrayList<>(List.of(name)));
            item.setAccesses(new ArrayList<>(List.of(new RangerPolicyItemAccess("SELECT", true))));
            policy.setPolicyItems(new ArrayList<>(List.of(item)));
            List<Map<String, RangerPolicyResource>> blocks = new ArrayList<>();
            for (int block = 0; block < config.tablesPerUser; block++) {
                int offset = revokeFirstTable && user == 0 ? block + 1 : block;
                blocks.add(resource((user * 791 + offset) % config.tables, name));
            }
            policy.setResources(blocks.remove(0));
            policy.setAdditionalResources(blocks);
            policies.add(policy);
        }
        ServicePolicies snapshot = new ServicePolicies();
        snapshot.setServiceId(1L);
        snapshot.setServiceName("authorization-benchmark");
        snapshot.setPolicyVersion(version);
        snapshot.setPolicyUpdateTime(new Date(1793847600000L + version));
        snapshot.setServiceDef(new ObjectMapper().readValue(
                Path.of(config.serviceDef).toFile(), RangerServiceDef.class));
        snapshot.setPolicies(policies);
        snapshot.setAuditMode("audit-default");
        return snapshot;
    }

    private boolean check(int user, String accessType, String... resources) {
        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        for (int index = 0; index < resources.length; index += 2) {
            resource.setValue(resources[index], resources[index + 1]);
        }
        RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, accessType, "ad_" + user,
                Collections.emptySet(), Collections.emptySet());
        request.setAccessTime(new Date(1793847600000L));
        request.setClientIPAddress("10.11.12.13");
        RangerAccessResult result = adapter.evaluate(request);
        if (result == null) {
            throw new IllegalStateException("Engine returned no authorization result");
        }
        checks++;
        if (!config.controllerVerdictsOnly) {
            fingerprint = fingerprint * 31 + result.getPolicyId();
            fingerprint = fingerprint * 31 + result.getAuditPolicyId();
        }
        fingerprint = fingerprint * 31 + (result.getIsAllowed() ? 1 : 0);
        if (!config.controllerVerdictsOnly) {
            fingerprint = fingerprint * 31 + (result.getIsAudited() ? 1 : 0);
        }
        return result.getIsAllowed();
    }

    private boolean ask(int user, String... resources) {
        // Both 4.1.4 PrivPredicate.SELECT and master AccessRequirements.SELECT are ANY(ADMIN, SELECT).
        if (config.requirement.equals("read") && check(user, "ADMIN", resources)) {
            return true;
        }
        return check(user, "SELECT", resources);
    }

    private boolean authorize(int user, int table) {
        if (config.mode.equals("column")) {
            return ask(user, "catalog", "internal", "database", "db", "table", "t" + table, "column", "c0");
        }
        if (config.mode.equals("sr-columns")) {
            // Isolate SR's per-column SELECT call pattern using identical Ranger policies and dependencies.
            // This is an engine-call model, not a StarRocks FE/SQL benchmark.
            for (int column = 0; column < config.columns; column++) {
                String name = column == 0 ? "c0" : "c" + table + "_" + column;
                if (!check(user, "SELECT", "catalog", "internal", "database", "db",
                        "table", "t" + table, "column", name)) {
                    return false;
                }
            }
            return true;
        }
        if (ask(user, "global", "*")) {
            return true;
        }
        if (ask(user, "catalog", "internal")) {
            return true;
        }
        if (ask(user, "catalog", "internal", "database", "db")) {
            return true;
        }
        if (ask(user, "catalog", "internal", "database", "db", "table", "t" + table)) {
            return true;
        }
        for (int column = 0; column < config.columns; column++) {
            String name = column == 0 ? "c0" : "c" + table + "_" + column;
            if (!ask(user, "catalog", "internal", "database", "db", "table", "t" + table, "column", name)) {
                return false;
            }
        }
        return true;
    }

    private boolean caseAt(Random random, int index) {
        int user = random.nextInt(config.users);
        boolean expected = (index & 1) == 0;
        int offset = expected ? random.nextInt(config.tablesPerUser)
                : config.tablesPerUser + random.nextInt(config.tables - config.tablesPerUser);
        int table = (user * 791 + offset) % config.tables;
        boolean allowed = authorize(user, table);
        if (allowed != expected) {
            throw new IllegalStateException("Wrong verdict for user=" + user
                    + " table=" + table + " expected=" + expected);
        }
        requestFingerprint = ((requestFingerprint * 31 + user) * 31 + table) * 31 + (allowed ? 1 : 0);
        return allowed;
    }

    private static double percentile(long[] values, double quantile) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[(int) Math.ceil(quantile * sorted.length) - 1] / 1_000_000.0;
    }

    private Map<String, Object> run() throws Exception {
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("config", config);
        output.put("adapter", adapter.describe());
        output.put("rangerJar", RangerBasePlugin.class.getProtectionDomain().getCodeSource().getLocation().toString());
        output.put("initialization", "RangerBasePlugin constructor, explicit null_safe.supplier=v2, no init/network");
        long baseline = retainedHeap();
        try (HeapSampler sampler = new HeapSampler()) {
            sampler.reset();
            long preparationStart = System.nanoTime();
            ServicePolicies policies = snapshot(1, false);
            long installStart = System.nanoTime();
            adapter.install(policies);
            policies = null;
            if (adapter.version() != 1L) {
                throw new IllegalStateException("Initial policy installation failed");
            }
            double installSeconds = (System.nanoTime() - installStart) / 1e9;
            long installed = retainedHeap();
            output.put("baselineMiB", mib(baseline));
            output.put("retainedPolicyMiB", mib(installed - baseline));
            output.put("initialInstallSeconds", installSeconds);
            output.put("initialPreparationAndInstallSeconds", (System.nanoTime() - preparationStart) / 1e9);
            output.put("initialSampledPeakHeapMiB", mib(sampler.peak()));
            Random warmups = new Random(config.seed - 1);
            for (int index = 0; index < config.warmups; index++) {
                caseAt(warmups, index);
            }
            Random random = new Random(config.seed);
            long[] times = new long[config.samples];
            List<Long> allowedTimes = new ArrayList<>();
            List<Long> deniedTimes = new ArrayList<>();
            long callsBefore = checks;
            long nativeCallsBefore = adapter.nativeEngineCalls();
            long thread = Thread.currentThread().getId();
            java.lang.management.ThreadMXBean cpu = ManagementFactory.getThreadMXBean();
            com.sun.management.ThreadMXBean allocated = (com.sun.management.ThreadMXBean) cpu;
            long allocatedBefore = allocated.getThreadAllocatedBytes(thread);
            long cpuBefore = cpu.getCurrentThreadCpuTime();
            for (int index = 0; index < times.length; index++) {
                long start = System.nanoTime();
                boolean allowed = caseAt(random, index);
                times[index] = System.nanoTime() - start;
                (allowed ? allowedTimes : deniedTimes).add(times[index]);
            }
            output.put("requestMainThreadCpuMs", (cpu.getCurrentThreadCpuTime() - cpuBefore) / 1e6);
            output.put("requestMainThreadAllocatedMiB",
                    mib(allocated.getThreadAllocatedBytes(thread) - allocatedBefore));
            output.put("requests", times.length);
            output.put("authorizationQuestions", checks - callsBefore);
            if (nativeCallsBefore >= 0) {
                output.put("nativeEngineCalls", adapter.nativeEngineCalls() - nativeCallsBefore);
            }
            output.put("p50Ms", percentile(times, .50));
            output.put("p95Ms", percentile(times, .95));
            output.put("p99Ms", percentile(times, .99));
            output.put("allowedP50Ms", percentile(allowedTimes.stream().mapToLong(Long::longValue).toArray(), .50));
            output.put("deniedP50Ms", percentile(deniedTimes.stream().mapToLong(Long::longValue).toArray(), .50));
            if (config.controllerVerdictsOnly) {
                output.put("authorizationQuestionVerdictFingerprint", fingerprint);
            } else {
                output.put("verdictAndAuditFingerprint", fingerprint);
            }
            output.put("requestVerdictFingerprint", requestFingerprint);
            output.put("resultSemantics",
                    config.controllerVerdictsOnly ? "controller-boolean" : "native-result-metadata");
            output.put("postRequestRetainedPolicyMiB", mib(retainedHeap() - baseline));
            if (config.refresh) {
                sampler.reset();
                long refreshStart = System.nanoTime();
                ServicePolicies next = snapshot(2, true);
                long replacementStart = System.nanoTime();
                adapter.install(next);
                next = null;
                if (adapter.version() != 2L) {
                    throw new IllegalStateException("Full refresh failed");
                }
                output.put("fullRefreshInstallSeconds", (System.nanoTime() - replacementStart) / 1e9);
                boolean revoked = !authorize(0, 0);
                boolean newGrant = authorize(0, config.tablesPerUser);
                if (!revoked || !newGrant) {
                    throw new IllegalStateException("Refresh/revocation verdict mismatch");
                }
                output.put("revocationVerified", true);
                output.put("postRefreshRetainedPolicyMiB", mib(retainedHeap() - baseline));
                output.put("fullRefreshPreparationAndInstallSeconds", (System.nanoTime() - refreshStart) / 1e9);
                output.put("fullRefreshSampledPeakHeapMiB", mib(sampler.peak()));
            }
        }
        return output;
    }

    /** Runs one isolated JVM workload and writes its measurements as JSON. */

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Config config = mapper.readValue(Path.of(args[0]).toFile(), Config.class);
        if (config.users < 1 || config.columns < 1 || config.tablesPerUser < 1
                || config.tablesPerUser >= config.tables || config.samples < 2
                || (!config.mode.equals("cascade") && !config.mode.equals("column")
                        && !config.mode.equals("sr-columns"))
                || (!config.requirement.equals("read") && !config.requirement.equals("select"))) {
            throw new IllegalArgumentException("Invalid benchmark dimensions or mode");
        }
        Properties settings = new Properties();
        settings.setProperty("ranger.plugin.doris.null_safe.supplier", "v2");
        settings.setProperty("ranger.plugin.doris.policyengine.option.disable.context.enrichers", "true");
        settings.setProperty("ranger.plugin.doris.use.rangerGroups", "false");
        RangerPluginConfig rangerConfig = new RangerPluginConfig(
                "doris", "authorization-benchmark", "authorization-benchmark", settings);
        Adapter adapter = config.adapter.isEmpty() ? new StockAdapter(rangerConfig)
                : (Adapter) Class.forName(config.adapter).getConstructor(RangerPluginConfig.class)
                        .newInstance(rangerConfig);
        try (Adapter ignored = adapter) {
            Map<String, Object> result = new V2AuthorizationBenchmark(config, adapter).run();
            mapper.writerWithDefaultPrettyPrinter().writeValue(Path.of(args[1]).toFile(), result);
            System.out.println(mapper.writeValueAsString(result));
        }
    }
}
