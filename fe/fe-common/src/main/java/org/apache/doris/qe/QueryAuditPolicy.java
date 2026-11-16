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

package org.apache.doris.qe;

import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.ConfigException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

/**
 * The rules of the FE config query_audit_always_on. While it is on, no parameter that ADMIN or a user can change at
 * runtime may disable query auditing or hollow it out: the audit switches stay on, and no audit limit is weaker than
 * its factory default. A limit whose smaller values drop events or cut statements short has its default as the
 * minimum, one whose larger values hold events back has it as the maximum, and a batching knob, harmful in both
 * directions, is fixed at it. The master also fits built-in audit load timeouts to its transaction and load sink
 * limits before beginning the transaction and planning the load. The config itself changes only on disk,
 * with a restart.
 *
 * <p>The rules are defined here only, for three sides:
 * <ul>
 *   <li>reading: the effective value of each protected variable. Old values reach the readers from the image, the
 *   edit log, forwarded sessions and sessions opened before a change, so reading is where the rules hold;</li>
 *   <li>writing: SET, SET_VAR, UNSET and runtime config changes reject a value that breaks a rule, so that the
 *   attempt fails visibly and is audited itself;</li>
 *   <li>startup: the values in fe.conf and fe_custom.conf, which loading never checks, are corrected in memory.</li>
 * </ul>
 *
 * <p>With the config off, every value takes effect as set, except a negative audit_plugin_max_sql_length: it counts
 * as 0 and SET rejects it, because truncating a statement by it threw and dropped the audit event.
 */
public final class QueryAuditPolicy {
    public static final String QUERY_AUDIT_ALWAYS_ON = "query_audit_always_on";

    // Factory defaults of the protected parameters, which are their bounds while the policy is on. GlobalVariable
    // and Config initialize the fields with them.
    public static final long DEFAULT_AUDIT_PLUGIN_MAX_BATCH_BYTES = 50 * 1024 * 1024;
    public static final long DEFAULT_AUDIT_PLUGIN_MAX_BATCH_INTERVAL_SEC = 60;
    public static final int DEFAULT_AUDIT_PLUGIN_MAX_SQL_LENGTH = 2097152;
    public static final int DEFAULT_AUDIT_PLUGIN_MAX_INSERT_STMT_LENGTH = Integer.MAX_VALUE;
    public static final int DEFAULT_AUDIT_PLUGIN_LOAD_TIMEOUT_S = 600;
    public static final int DEFAULT_AUDIT_EVENT_LOG_QUEUE_SIZE = 250000;
    public static final int DEFAULT_QUERY_AUDIT_LOG_TIMEOUT_MS = 5000;
    public static final int DEFAULT_BE_REPORT_QUERY_STATISTICS_TIMEOUT_MS = 60000;
    public static final int DEFAULT_WORKLOAD_RUNTIME_STATUS_THREAD_INTERVAL_MS = 2000;

    private static final Logger LOG = LogManager.getLogger(QueryAuditPolicy.class);

    // The name of a session variable, which SessionVariable in fe-core defines.
    private static final String ENABLE_PREPARED_STMT_AUDIT_LOG = "enable_prepared_stmt_audit_log";
    // The module of audit_log_modules that writes queries to fe.audit.log.
    private static final String QUERY_AUDIT_LOG_MODULE = "query";
    private static final String SKIP_AUDIT_USER_LIST = "skip_audit_user_list";
    private static final String AUDIT_LOG_MODULES = "audit_log_modules";

    private static final Protected MAX_SQL_LENGTH = new Protected(GlobalVariable.AUDIT_PLUGIN_MAX_SQL_LENGTH,
            Rule.AT_LEAST, DEFAULT_AUDIT_PLUGIN_MAX_SQL_LENGTH);
    // Its default is unlimited, so INSERT ... VALUES is truncated only by audit_plugin_max_sql_length.
    private static final Protected MAX_INSERT_STMT_LENGTH = new Protected(
            GlobalVariable.AUDIT_PLUGIN_MAX_INSERT_STMT_LENGTH, Rule.AT_LEAST,
            DEFAULT_AUDIT_PLUGIN_MAX_INSERT_STMT_LENGTH);
    // A batch whose load times out is dropped, not retried.
    private static final Protected LOAD_TIMEOUT = new Protected(GlobalVariable.AUDIT_PLUGIN_LOAD_TIMEOUT,
            Rule.AT_LEAST, DEFAULT_AUDIT_PLUGIN_LOAD_TIMEOUT_S);
    // Larger batches keep events in memory, which a restart loses; smaller ones load each event on its own, and the
    // loader's queue drops events once loads fall behind.
    private static final Protected MAX_BATCH_BYTES = new Protected(GlobalVariable.AUDIT_PLUGIN_MAX_BATCH_BYTES,
            Rule.FIXED, DEFAULT_AUDIT_PLUGIN_MAX_BATCH_BYTES);
    private static final Protected MAX_BATCH_INTERVAL = new Protected(
            GlobalVariable.AUDIT_PLUGIN_MAX_BATCH_INTERVAL_SEC, Rule.FIXED,
            DEFAULT_AUDIT_PLUGIN_MAX_BATCH_INTERVAL_SEC);

    private static final ImmutableMap<String, Protected> VARIABLES = ImmutableMap.<String, Protected>builder()
            .put(GlobalVariable.ENABLE_AUDIT_PLUGIN, Protected.forcedOn(GlobalVariable.ENABLE_AUDIT_PLUGIN))
            .put(ENABLE_PREPARED_STMT_AUDIT_LOG, Protected.forcedOn(ENABLE_PREPARED_STMT_AUDIT_LOG))
            .put(MAX_SQL_LENGTH.name, MAX_SQL_LENGTH)
            .put(MAX_INSERT_STMT_LENGTH.name, MAX_INSERT_STMT_LENGTH)
            .put(LOAD_TIMEOUT.name, LOAD_TIMEOUT)
            .put(MAX_BATCH_BYTES.name, MAX_BATCH_BYTES)
            .put(MAX_BATCH_INTERVAL.name, MAX_BATCH_INTERVAL)
            .build();

    // The mutable FE configs. They get no read-side check: their values come only from startup, which corrects them,
    // and from ConfigBase.setMutableConfig, whose ProtectedConfHandler rejects what breaks the rules.
    private static final ImmutableMap<String, ProtectedConfig> CONFIGS = ImmutableMap.<String, ProtectedConfig>builder()
            // A full queue drops events.
            .put("audit_event_log_queue_size", new ProtectedConfig(
                    new Protected("audit_event_log_queue_size", Rule.AT_LEAST, DEFAULT_AUDIT_EVENT_LOG_QUEUE_SIZE),
                    () -> Config.audit_event_log_queue_size, v -> Config.audit_event_log_queue_size = v))
            // An event waits this long for BE statistics before it is dispatched.
            .put("query_audit_log_timeout_ms", new ProtectedConfig(
                    new Protected("query_audit_log_timeout_ms", Rule.AT_MOST, DEFAULT_QUERY_AUDIT_LOG_TIMEOUT_MS),
                    () -> Config.query_audit_log_timeout_ms, v -> Config.query_audit_log_timeout_ms = v))
            .put("be_report_query_statistics_timeout_ms", new ProtectedConfig(
                    new Protected("be_report_query_statistics_timeout_ms", Rule.AT_MOST,
                            DEFAULT_BE_REPORT_QUERY_STATISTICS_TIMEOUT_MS),
                    () -> Config.be_report_query_statistics_timeout_ms,
                    v -> Config.be_report_query_statistics_timeout_ms = v))
            // The interval of the thread that dispatches the events.
            .put("workload_runtime_status_thread_interval_ms", new ProtectedConfig(
                    new Protected("workload_runtime_status_thread_interval_ms", Rule.AT_MOST,
                            DEFAULT_WORKLOAD_RUNTIME_STATUS_THREAD_INTERVAL_MS),
                    () -> Config.workload_runtime_status_thread_interval_ms,
                    v -> Config.workload_runtime_status_thread_interval_ms = v))
            .build();

    private QueryAuditPolicy() {
    }

    /** Whether query_audit_always_on is enabled on this FE. */
    public static boolean isAlwaysOn() {
        return Config.query_audit_always_on;
    }

    /** Whether the built-in audit loader loads events into __internal_schema.audit_log. */
    public static boolean isAuditLoaderEnabled() {
        return isAlwaysOn() || GlobalVariable.enableAuditLoader;
    }

    /**
     * Whether to audit a COM_STMT_EXECUTE. With the policy on, the FE that received the execution always does, and
     * the master skips an execution forwarded to it (isProxy), which the receiving FE audits; with it off, the
     * session's enable_prepared_stmt_audit_log decides.
     */
    public static boolean shouldAuditPreparedExecute(boolean sessionValue, boolean isProxy) {
        return isAlwaysOn() ? !isProxy : sessionValue;
    }

    /**
     * The byte length an audit record truncates a statement to. A negative audit_plugin_max_sql_length counts as 0
     * with the policy off as well, because truncating by it threw and dropped the record.
     */
    public static int maxSqlLength() {
        int value = GlobalVariable.auditPluginMaxSqlLength;
        return isAlwaysOn() ? (int) MAX_SQL_LENGTH.effective(value) : Math.max(0, value);
    }

    /** The byte length an audit record truncates an INSERT ... VALUES to, if smaller than maxSqlLength(). */
    public static int maxInsertStmtLength() {
        int value = GlobalVariable.auditPluginMaxInsertStmtLength;
        return isAlwaysOn() ? (int) MAX_INSERT_STMT_LENGTH.effective(value) : value;
    }

    /** The timeout, in seconds, of the stream load that writes a batch to the audit table. */
    public static int loadTimeoutS() {
        int value = GlobalVariable.auditPluginLoadTimeoutS;
        return isAlwaysOn() ? (int) LOAD_TIMEOUT.effective(value) : value;
    }

    /** The batch size, in bytes, at which the audit loader loads its batch. */
    public static long maxBatchBytes() {
        long value = GlobalVariable.auditPluginMaxBatchBytes;
        return isAlwaysOn() ? MAX_BATCH_BYTES.effective(value) : value;
    }

    /** The time, in seconds, after which the audit loader loads its batch. */
    public static long maxBatchIntervalSec() {
        long value = GlobalVariable.auditPluginMaxBatchInternalSec;
        return isAlwaysOn() ? MAX_BATCH_INTERVAL.effective(value) : value;
    }

    /**
     * Checks the value that a SET, a SET_VAR hint or an UNSET gives a variable: the default for SET ... = DEFAULT
     * and UNSET. Returns why the value is rejected, or empty when it is allowed. A value that is not a number or a
     * boolean is left to the variable's setter, which reports it.
     */
    public static Optional<String> checkVariableValue(String name, String value) {
        Protected param = VARIABLES.get(name);
        if (param == null) {
            return Optional.empty();
        }
        if (isAlwaysOn()) {
            return violation("Variable", param, value);
        }
        if (param == MAX_SQL_LENGTH) {
            Optional<Long> length = param.parse(value);
            if (length.isPresent() && length.get() < 0) {
                return Optional.of("Variable '" + name + "' can't be set to '" + value
                        + "', the value must not be negative");
            }
        }
        return Optional.empty();
    }

    /**
     * Checks a runtime change of an FE config, from ADMIN SET [ALL] FRONTEND(S) CONFIG or /api/_set_config. Returns
     * why the value is rejected, or empty when it is allowed.
     */
    public static Optional<String> checkConfigValue(String name, String value) {
        ProtectedConfig config = CONFIGS.get(name);
        if (config == null || !isAlwaysOn()) {
            return Optional.empty();
        }
        // ConfigBase trims a value before parsing it.
        return violation("Config", config.param, value.trim());
    }

    /** The names of the variables the policy protects. */
    public static ImmutableSet<String> protectedVariableNames() {
        return VARIABLES.keySet();
    }

    /** The names of the mutable FE configs the policy protects. */
    public static ImmutableSet<String> protectedConfigNames() {
        return CONFIGS.keySet();
    }

    /**
     * The value a protected variable takes effect with while the policy is on: its value if allowed, otherwise the
     * bound of its rule. Both are strings as VariableMgr reads and writes them, such as "true" or "2097152".
     */
    public static String effectiveVariableValue(String name, String value) {
        Protected param = VARIABLES.get(name);
        Preconditions.checkArgument(param != null, "%s does not protect variable %s", QUERY_AUDIT_ALWAYS_ON, name);
        if (param.rule == Rule.FORCED_ON) {
            return Boolean.TRUE.toString();
        }
        return Long.toString(param.effective(Long.parseLong(value)));
    }

    /**
     * Corrects, in memory, the values from fe.conf and fe_custom.conf that break the policy, since loading the files
     * never runs config callbacks: skip_audit_user_list is cleared, audit_log_modules gets the query module, and each
     * protected config outside its bound gets the bound, which covers a value persisted into fe_custom.conf through
     * /api/_set_config before the policy was on. Runs once both files are loaded, before logging and the audit
     * pipeline start with these values; logging is not up yet, so it returns the corrections for logStartup.
     */
    public static List<StartupOverride> applyStartupOverrides() {
        List<StartupOverride> overrides = new ArrayList<>();
        if (!isAlwaysOn()) {
            return overrides;
        }
        // The events of the users in the list would be dropped.
        if (!Config.skip_audit_user_list.isEmpty()) {
            overrides.add(new StartupOverride(SKIP_AUDIT_USER_LIST, Config.skip_audit_user_list, ""));
            Config.skip_audit_user_list = "";
        }
        if (!Arrays.asList(Config.audit_log_modules).contains(QUERY_AUDIT_LOG_MODULE)) {
            String[] modules = Arrays.copyOf(Config.audit_log_modules, Config.audit_log_modules.length + 1);
            modules[modules.length - 1] = QUERY_AUDIT_LOG_MODULE;
            overrides.add(new StartupOverride(AUDIT_LOG_MODULES, String.join(",", Config.audit_log_modules),
                    String.join(",", modules)));
            Config.audit_log_modules = modules;
        }
        for (ProtectedConfig config : CONFIGS.values()) {
            int value = config.getter.getAsInt();
            int effective = (int) config.param.effective(value);
            if (effective != value) {
                overrides.add(new StartupOverride(config.param.name, Integer.toString(value),
                        Integer.toString(effective)));
                config.setter.accept(effective);
            }
        }
        return overrides;
    }

    /** Logs, once logging is up, that the policy is on, what it protects, and what applyStartupOverrides replaced. */
    public static void logStartup(List<StartupOverride> overrides) {
        if (!isAlwaysOn()) {
            return;
        }
        List<String> configs = new ArrayList<>(CONFIGS.keySet());
        configs.add(SKIP_AUDIT_USER_LIST);
        configs.add(AUDIT_LOG_MODULES);
        LOG.info("{} is enabled, protected variables: {}, protected configs: {}", QUERY_AUDIT_ALWAYS_ON,
                VARIABLES.keySet(), configs);
        for (StartupOverride override : overrides) {
            LOG.warn("{} is enabled, ignoring {}={} from the config files and using {}", QUERY_AUDIT_ALWAYS_ON,
                    override.getName(), override.getConfiguredValue(), override.getEffectiveValue());
        }
    }

    private static Optional<String> violation(String kind, Protected param, String value) {
        Optional<Long> parsed = param.parse(value);
        if (!parsed.isPresent() || param.allows(parsed.get())) {
            return Optional.empty();
        }
        return Optional.of(kind + " '" + param.name + "' can't be set to '" + value + "' because "
                + QUERY_AUDIT_ALWAYS_ON + " is enabled" + param.requirement());
    }

    /**
     * Rejects a runtime change of a protected FE config that breaks the policy. ConfigBase.setMutableConfig runs it
     * for both ADMIN SET FRONTEND CONFIG and /api/_set_config, and a rejected value is neither applied nor persisted.
     */
    public static class ProtectedConfHandler extends ConfigBase.DefaultConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            Optional<String> error = checkConfigValue(field.getName(), confVal);
            if (error.isPresent()) {
                throw new ConfigException(error.get());
            }
            super.handle(field, confVal);
        }
    }

    /** A config value from fe.conf or fe_custom.conf that the policy replaced at startup. */
    public static final class StartupOverride {
        private final String name;
        private final String configuredValue;
        private final String effectiveValue;

        StartupOverride(String name, String configuredValue, String effectiveValue) {
            this.name = name;
            this.configuredValue = configuredValue;
            this.effectiveValue = effectiveValue;
        }

        public String getName() {
            return name;
        }

        public String getConfiguredValue() {
            return configuredValue;
        }

        public String getEffectiveValue() {
            return effectiveValue;
        }

        @Override
        public String toString() {
            return name + ": " + configuredValue + " -> " + effectiveValue;
        }
    }

    // How a protected parameter may differ from its factory default while the policy is on.
    private enum Rule {
        // A switch that stays on.
        FORCED_ON,
        // Smaller values drop events or cut statements short: the default is the minimum.
        AT_LEAST,
        // Larger values hold events back: the default is the maximum.
        AT_MOST,
        // A batching knob, harmful in both directions: the default is the only value.
        FIXED
    }

    // A protected variable or config, with the bound of its rule: the factory default, or 1 (true) for a switch.
    private static final class Protected {
        private final String name;
        private final Rule rule;
        private final long bound;

        private Protected(String name, Rule rule, long bound) {
            this.name = name;
            this.rule = rule;
            this.bound = bound;
        }

        private static Protected forcedOn(String name) {
            return new Protected(name, Rule.FORCED_ON, 1);
        }

        private boolean allows(long value) {
            switch (rule) {
                case AT_LEAST:
                    return value >= bound;
                case AT_MOST:
                    return value <= bound;
                default:
                    return value == bound;
            }
        }

        private long effective(long value) {
            return allows(value) ? value : bound;
        }

        // The value as the setter reads it: a switch as VariableMgr reads a boolean, 1 or 0. Empty when the setter
        // rejects it anyway.
        private Optional<Long> parse(String value) {
            if (rule == Rule.FORCED_ON) {
                if (value.equalsIgnoreCase("ON") || value.equalsIgnoreCase("TRUE") || value.equals("1")) {
                    return Optional.of(1L);
                }
                if (value.equalsIgnoreCase("OFF") || value.equalsIgnoreCase("FALSE") || value.equals("0")) {
                    return Optional.of(0L);
                }
                return Optional.empty();
            }
            try {
                return Optional.of(Long.parseLong(value));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        }

        // What a rejected value must be instead, for the error message.
        private String requirement() {
            switch (rule) {
                case AT_LEAST:
                    return ", the value must be at least " + bound;
                case AT_MOST:
                    return ", the value must be at most " + bound;
                case FIXED:
                    return ", the value must be " + bound;
                default:
                    return "";
            }
        }
    }

    // A protected FE config with access to its field, which startup corrects.
    private static final class ProtectedConfig {
        private final Protected param;
        private final IntSupplier getter;
        private final IntConsumer setter;

        private ProtectedConfig(Protected param, IntSupplier getter, IntConsumer setter) {
            this.param = param;
            this.getter = getter;
            this.setter = setter;
        }
    }
}
