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

package org.apache.doris.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ConfigTest {
    @BeforeAll
    public static void setUp() throws Exception {
        Config config = new Config();
        // create an empty config file to initialize Config
        Path tempFile = Files.createTempFile("fe_ut_", ".conf");
        tempFile.toFile().deleteOnExit();
        config.init(tempFile.toAbsolutePath().toString());
    }

    // A sensitive config (fe_meta_auth_token) must never be dumped in plaintext by any config
    // API: both Config.dump() and ConfigBase.getConfigInfo() return the mask instead of the value.
    @Test
    public void testSensitiveConfigIsMaskedWhenSet() {
        String old = Config.fe_meta_auth_token;
        try {
            Config.fe_meta_auth_token = "super-secret-token";

            Map<String, String> dumped = ConfigBase.dump();
            Assertions.assertEquals(ConfigBase.SENSITIVE_CONF_MASK, dumped.get("fe_meta_auth_token"));

            String value = configInfoValue("fe_meta_auth_token");
            Assertions.assertEquals(ConfigBase.SENSITIVE_CONF_MASK, value);
        } finally {
            Config.fe_meta_auth_token = old;
        }
    }

    // The legacy cluster secret auth_token is also marked sensitive, so it is masked by every
    // config dump API too (it leaks through /rest/v1/config/fe otherwise).
    @Test
    public void testAuthTokenIsMaskedWhenSet() {
        String old = Config.auth_token;
        try {
            Config.auth_token = "super-secret-auth-token";

            Assertions.assertEquals(ConfigBase.SENSITIVE_CONF_MASK, ConfigBase.dump().get("auth_token"));
            Assertions.assertEquals(ConfigBase.SENSITIVE_CONF_MASK, configInfoValue("auth_token"));
        } finally {
            Config.auth_token = old;
        }
    }

    // Every config holding an operator-supplied secret must keep @ConfField(sensitive = true) and
    // must be masked by both dump APIs. The names are listed explicitly on purpose: a check driven
    // off the annotation alone cannot notice the annotation being *removed*, which is the silent
    // regression worth guarding -- dropping it while editing a neighbouring line puts the secret
    // back into every config dump with nothing failing. #67338 marked the first two sensitive and
    // nothing tested them; mysql_ssl_default_ca_certificate_password,
    // mysql_ssl_default_server_certificate_password and initial_root_password had no coverage at all.
    @Test
    public void testSecretConfigsStaySensitiveAndMasked() throws Exception {
        List<String> secretConfigs = Arrays.asList(
                "tls_private_key_password",
                "key_store_password",
                "auth_token",
                "fe_meta_auth_token",
                "mysql_ssl_default_ca_certificate_password",
                "mysql_ssl_default_server_certificate_password",
                "initial_root_password");

        for (String name : secretConfigs) {
            Field field = Config.class.getField(name);
            ConfigBase.ConfField confField = field.getAnnotation(ConfigBase.ConfField.class);
            Assertions.assertNotNull(confField, name + " must be a @ConfField");
            Assertions.assertTrue(confField.sensitive(),
                    name + " holds a secret and must stay @ConfField(sensitive = true)");

            String old = (String) field.get(null);
            try {
                field.set(null, "super-secret-value-of-" + name);

                Assertions.assertEquals(ConfigBase.SENSITIVE_CONF_MASK, ConfigBase.dump().get(name),
                        name + " must be masked by ConfigBase.dump()");
                Assertions.assertEquals(ConfigBase.SENSITIVE_CONF_MASK, configInfoValue(name),
                        name + " must be masked by ConfigBase.getConfigInfo()");
            } finally {
                field.set(null, old);
            }
        }

        // Keeps the list above complete: a config marked sensitive later has to be added here too.
        for (Field field : Config.class.getFields()) {
            ConfigBase.ConfField confField = field.getAnnotation(ConfigBase.ConfField.class);
            if (confField != null && confField.sensitive() && field.getType() == String.class) {
                Assertions.assertTrue(secretConfigs.contains(field.getName()),
                        field.getName() + " is sensitive but is missing from secretConfigs in this test");
            }
        }
    }

    // An empty sensitive config is left as-is (no secret to hide), so "unset" stays visible.
    @Test
    public void testEmptySensitiveConfigIsNotMasked() {
        String old = Config.fe_meta_auth_token;
        try {
            Config.fe_meta_auth_token = "";

            Assertions.assertEquals("", ConfigBase.dump().get("fe_meta_auth_token"));
            Assertions.assertEquals("", configInfoValue("fe_meta_auth_token"));
        } finally {
            Config.fe_meta_auth_token = old;
        }
    }

    private static String configInfoValue(String key) {
        for (List<String> row : ConfigBase.getConfigInfo(null)) {
            if (row.get(0).equals(key)) {
                return row.get(1);
            }
        }
        throw new IllegalStateException("config not found: " + key);
    }

    @Test
    public void testOptionsConfigAcceptsItsOptionsOnly() throws ConfigException {
        String original = Config.mysql_caching_sha2_password_clients;
        try {
            ConfigBase.setMutableConfig("mysql_caching_sha2_password_clients", "ALL");
            Assertions.assertEquals("all", Config.mysql_caching_sha2_password_clients);
            Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("mysql_caching_sha2_password_clients", "bogus"));
            Assertions.assertEquals("all", Config.mysql_caching_sha2_password_clients);
        } finally {
            Config.mysql_caching_sha2_password_clients = original;
        }
    }

    @Test
    public void testOptionsConfigIsValidatedDuringStartup() throws Exception {
        String original = Config.mysql_caching_sha2_password_clients;
        int originalInterval = Config.cloud_warm_up_job_scheduler_interval_millisecond;
        try {
            Config config = new Config();
            Path file = configFile("mysql_caching_sha2_password_clients= ALL \n"
                    + "cloud_warm_up_job_scheduler_interval_millisecond=0\n");
            config.init(file.toString());
            Assertions.assertEquals("all", Config.mysql_caching_sha2_password_clients);
            // This unrelated callback rejects zero at runtime; startup must not dispatch it.
            Assertions.assertEquals(0, Config.cloud_warm_up_job_scheduler_interval_millisecond);
            config.init(configFile("").toString());
            Assertions.assertEquals("all", Config.mysql_caching_sha2_password_clients);
            for (String value : new String[] {"nnoe", "", " "}) {
                IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                        () -> config.init(configFile("mysql_caching_sha2_password_clients=" + value + "\n")
                                .toString()));
                Assertions.assertInstanceOf(ConfigException.class, error.getCause());
                Assertions.assertEquals("all", Config.mysql_caching_sha2_password_clients);
            }
        } finally {
            Config.mysql_caching_sha2_password_clients = original;
            Config.cloud_warm_up_job_scheduler_interval_millisecond = originalInterval;
        }
    }

    @Test
    public void testOptionsConfigIsValidatedInCustomStartupFile() throws Exception {
        String original = Config.mysql_caching_sha2_password_clients;
        try {
            Config config = new Config();
            config.init(configFile("mysql_caching_sha2_password_clients=auto\n").toString());
            Path custom = configFile("mysql_caching_sha2_password_clients= NONE \n");
            config.initCustom(custom.toString());
            Assertions.assertEquals("none", Config.mysql_caching_sha2_password_clients);
            Files.writeString(custom, "mysql_caching_sha2_password_clients=nnoe\n");
            IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> config.initCustom(custom.toString()));
            Assertions.assertInstanceOf(ConfigException.class, error.getCause());
            Assertions.assertEquals("none", Config.mysql_caching_sha2_password_clients);
        } finally {
            Config.mysql_caching_sha2_password_clients = original;
        }
    }

    @Test
    public void testNumericBoundsBeforeStartupAndRuntimeAssignment() throws Exception {
        Config config = new Config();
        // One property is enough to reproduce each failure. No worker pool or RPC is needed.
        for (String name : new String[] {
                "stream_load_default_timeout_second",
                "stream_load_default_precommit_timeout_second",
                "min_load_timeout_second",
                "max_load_timeout_second",
                "max_stream_load_timeout_second",
                "publish_version_timeout_second",
                "group_commit_interval_ms_default_value",
                "group_commit_data_bytes_default_value",
                "report_resend_batch_task_num_per_rpc",
                "backup_restore_batch_task_num_per_rpc",
                "max_get_kafka_meta_timeout_second",
                "remote_fragment_exec_timeout_ms",
                "streaming_cdc_light_rpc_timeout_sec",
                "streaming_cdc_heavy_rpc_timeout_sec",
                "dictionary_rpc_timeout_seconds",
                "point_query_timeout_ms",
                "min_clone_task_timeout_sec",
                "max_clone_task_timeout_sec",
                "routine_load_task_timeout_multiplier",
                "routine_load_task_min_timeout_sec",
                "max_routine_load_task_num_per_be",
                "max_routine_load_task_concurrent_num",
                "publish_thread_pool_num",
                "publish_queue_size",
                "tablet_report_thread_pool_num",
                "tablet_report_queue_size",
                "heartbeat_mgr_threads_num",
                "heartbeat_mgr_blocking_queue_size",
                "fe_session_mgr_threads_num",
                "fe_session_mgr_blocking_queue_size",
                "max_agent_task_threads_num",
                "external_scan_split_threads",
                "external_scan_metadata_threads",
                "external_scan_task_queue_capacity",
                "max_external_cache_loader_thread_pool_size",
                "backup_upload_snapshot_batch_size",
                "restore_download_snapshot_batch_size",
                "cloud_restore_create_tablet_batch_size",
                "cloud_txn_tablet_batch_size",
                "restore_task_concurrency_per_be",
                "stats_cache_size",
                "max_external_table_row_count_cache_num",
                "backup_job_default_timeout_ms",
                "agent_task_be_unavailable_heartbeat_timeout_second",
                "query_colocate_join_memory_limit_penalty_factor",
                "min_bytes_per_broker_scanner",
                "max_bytes_per_broker_scanner",
                "max_broker_concurrency",
                "file_cache_query_limit_max_percent"}) {
            Field field = Config.class.getField(name);
            Object original = field.get(null);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> config.init(configFile(name + "=-1\n").toString()), name);
            Assertions.assertEquals(original, field.get(null), name);
            if (field.getAnnotation(ConfigBase.ConfField.class).mutable()) {
                Assertions.assertThrows(ConfigException.class,
                        () -> ConfigBase.setMutableConfig(name, "-1"), name);
                Assertions.assertEquals(original, field.get(null), name);
            }
        }
    }

    @Test
    public void testExternalCacheQueueProductFitsInt() throws Exception {
        int original = Config.max_external_cache_loader_thread_pool_size;
        try {
            Config config = new Config();
            // Only parse the property. Do not create 214748 threads or queue entries.
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> config.init(configFile("max_external_cache_loader_thread_pool_size=214749\n")
                            .toString()));
            Assertions.assertEquals(original, Config.max_external_cache_loader_thread_pool_size);
            config.init(configFile("max_external_cache_loader_thread_pool_size=214748\n").toString());
            Assertions.assertEquals(214748, Config.max_external_cache_loader_thread_pool_size);
        } finally {
            Config.max_external_cache_loader_thread_pool_size = original;
        }
    }

    @Test
    public void testNumericCustomConfigAndZeroBoundary() throws Exception {
        Config config = new Config();
        int originalPercent = Config.file_cache_query_limit_max_percent;
        int originalBatch = Config.backup_upload_snapshot_batch_size;
        try {
            // A custom file uses the same bounds as the main file and dynamic SET.
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> config.initCustom(configFile("file_cache_query_limit_max_percent=101\n").toString()));
            Assertions.assertEquals(originalPercent, Config.file_cache_query_limit_max_percent);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> config.init(configFile("backup_upload_snapshot_batch_size=0\n").toString()));
            config.init(configFile("file_cache_query_limit_max_percent=1\n"
                    + "backup_upload_snapshot_batch_size=1\n").toString());
            Assertions.assertEquals(1, Config.file_cache_query_limit_max_percent);
            Assertions.assertEquals(1, Config.backup_upload_snapshot_batch_size);
        } finally {
            Config.file_cache_query_limit_max_percent = originalPercent;
            Config.backup_upload_snapshot_batch_size = originalBatch;
        }
    }

    @Test
    public void testZeroSeparatesPositiveAndNonnegativeConfigBounds() throws Exception {
        Config config = new Config();
        // Check zero independently of -1. A zero pool or batch cannot make progress.
        for (String name : new String[] {
                "min_clone_task_timeout_sec",
                "max_clone_task_timeout_sec",
                "group_commit_interval_ms_default_value",
                "group_commit_data_bytes_default_value",
                "report_resend_batch_task_num_per_rpc",
                "backup_restore_batch_task_num_per_rpc",
                "publish_thread_pool_num",
                "publish_queue_size",
                "tablet_report_thread_pool_num",
                "tablet_report_queue_size",
                "heartbeat_mgr_threads_num",
                "heartbeat_mgr_blocking_queue_size",
                "fe_session_mgr_threads_num",
                "fe_session_mgr_blocking_queue_size",
                "max_agent_task_threads_num",
                "external_scan_split_threads",
                "external_scan_metadata_threads",
                "external_scan_task_queue_capacity",
                "max_external_cache_loader_thread_pool_size",
                "backup_upload_snapshot_batch_size",
                "restore_download_snapshot_batch_size",
                "cloud_restore_create_tablet_batch_size",
                "cloud_txn_tablet_batch_size",
                "restore_task_concurrency_per_be",
                "query_colocate_join_memory_limit_penalty_factor",
                "min_bytes_per_broker_scanner",
                "max_bytes_per_broker_scanner",
                "max_broker_concurrency",
                "file_cache_query_limit_max_percent"}) {
            Field field = Config.class.getField(name);
            Object original = field.get(null);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> config.init(configFile(name + "=0\n").toString()), name);
            Assertions.assertEquals(original, field.get(null), name);
        }
        // Zero is a valid timeout, disabled capacity, or empty cache for these settings.
        // Parse one property at a time. Do not construct the resource that it controls.
        for (String name : new String[] {
                "stream_load_default_timeout_second",
                "stream_load_default_precommit_timeout_second",
                "min_load_timeout_second",
                "max_load_timeout_second",
                "max_stream_load_timeout_second",
                "publish_version_timeout_second",
                "max_get_kafka_meta_timeout_second",
                "remote_fragment_exec_timeout_ms",
                "streaming_cdc_light_rpc_timeout_sec",
                "streaming_cdc_heavy_rpc_timeout_sec",
                "dictionary_rpc_timeout_seconds",
                "point_query_timeout_ms",
                "routine_load_task_timeout_multiplier",
                "routine_load_task_min_timeout_sec",
                "max_routine_load_task_num_per_be",
                "max_routine_load_task_concurrent_num",
                "stats_cache_size",
                "max_external_table_row_count_cache_num",
                "backup_job_default_timeout_ms",
                "agent_task_be_unavailable_heartbeat_timeout_second"}) {
            Field field = Config.class.getField(name);
            Object original = field.get(null);
            try {
                config.init(configFile(name + "=0\n").toString());
                Assertions.assertEquals(0, ((Number) field.get(null)).longValue(), name);
            } finally {
                field.set(null, original);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
            "min_clone_task_timeout_sec, 0, 1",
            "max_clone_task_timeout_sec, 0, 1",
            "try_commit_lock_timeout_seconds, -1, 0",
            "remote_fragment_exec_timeout_ms, 9223372036854770808, 9223372036854770807",
            "remote_fragment_exec_timeout_ms, 9223372036854775807, 9223372036854770807"
    })
    public void testTimeoutBoundsBeforePublication(String name, String invalid, String valid) throws Exception {
        // Each case uses one property. Zero clone deadlines violate the scheduler invariant.
        // A negative wait cannot wait for a busy lock. A fragment timeout must leave room for 5000 ms.
        Field field = Config.class.getField(name);
        Object original = field.get(null);
        Config config = new Config();
        try {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> config.init(configFile(name + "=" + invalid + "\n").toString()), name);
            Assertions.assertEquals(original, field.get(null), name);
            Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig(name, invalid), name);
            Assertions.assertEquals(original, field.get(null), name);
            // The adjacent valid boundary must work at startup and during a runtime update.
            config.init(configFile(name + "=" + valid + "\n").toString());
            Assertions.assertEquals(Long.parseLong(valid), ((Number) field.get(null)).longValue(), name);
            field.set(null, original);
            ConfigBase.setMutableConfig(name, valid);
            Assertions.assertEquals(Long.parseLong(valid), ((Number) field.get(null)).longValue(), name);
        } finally {
            field.set(null, original);
        }
    }

    private static Path configFile(String contents) throws Exception {
        Path file = Files.createTempFile("fe_ut_options_", ".conf");
        file.toFile().deleteOnExit();
        Files.writeString(file, contents);
        return file;
    }

    @Test
    public void testSensitivePropertyRegexIsValidatedDuringStartupAndCustomLoading() throws Exception {
        String original = Config.datasource_sensitive_property_pattern;
        try {
            Config config = new Config();
            config.init(configFile("datasource_sensitive_property_pattern=custom[.]value\n").toString());
            Assertions.assertEquals("custom[.]value", Config.datasource_sensitive_property_pattern);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> config.init(configFile("datasource_sensitive_property_pattern=[\n").toString()));
            Assertions.assertEquals("custom[.]value", Config.datasource_sensitive_property_pattern);
            Path custom = configFile("datasource_sensitive_property_pattern=(?!)\n");
            config.initCustom(custom.toString());
            Assertions.assertEquals("(?!)", Config.datasource_sensitive_property_pattern);
            Files.writeString(custom, "datasource_sensitive_property_pattern=[\n");
            Assertions.assertThrows(IllegalArgumentException.class, () -> config.initCustom(custom.toString()));
            Assertions.assertEquals("(?!)", Config.datasource_sensitive_property_pattern);
            config.init(configFile("datasource_sensitive_property_pattern=\n").toString());
            Assertions.assertEquals("", Config.datasource_sensitive_property_pattern);
        } finally {
            Config.datasource_sensitive_property_pattern = original;
        }
    }

    @Test
    public void testRegexValidationUsesTheValueAfterConfigTrimming() throws Exception {
        String original = Config.datasource_sensitive_property_pattern;
        try {
            Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("datasource_sensitive_property_pattern", "\\ "));
            Assertions.assertEquals(original, Config.datasource_sensitive_property_pattern);
        } finally {
            Config.datasource_sensitive_property_pattern = original;
        }
    }

    @Test
    public void testSetEmptyArray() throws ConfigException {
        ConfigBase.setMutableConfig("mysql_compat_var_whitelist", "a,b,c");
        ConfigBase.setMutableConfig("mysql_compat_var_whitelist", "");
        Assertions.assertEquals(0, Config.mysql_compat_var_whitelist.length, "array length should be 0");
    }

    @Test
    public void testConfFieldDescriptionsAreEnglishStrings() throws Exception {
        for (Field field : Config.class.getFields()) {
            ConfigBase.ConfField confField = field.getAnnotation(ConfigBase.ConfField.class);
            if (confField == null) {
                continue;
            }
            Assertions.assertFalse(confField.description().matches(".*[\\u4e00-\\u9fff].*"), "Chinese description found in config: " + field.getName());
        }
    }

    // File-path and jdbc-driver security configs must only be settable in fe.conf (ops), never at runtime
    // via ADMIN SET FRONTEND CONFIG. setMutableConfig is exactly that runtime entrypoint, so it must reject them.
    @Test
    public void testSecurityPathConfigsAreNotRuntimeMutable() {
        String[] opsOnlyConfigs = {
                "jdbc_driver_url_white_list",
                "jdbc_drivers_dir",
                "jdbc_driver_secure_path",
                "tmp_dir",
                "plugin_dir",
                "s3_load_endpoint_white_list",
                "force_sqlserver_jdbc_encrypt_false",
        };
        for (String key : opsOnlyConfigs) {
            ConfigException e = Assertions.assertThrows(ConfigException.class, () -> ConfigBase.setMutableConfig(key, "x"), key + " should not be runtime-mutable");
            Assertions.assertTrue(e.getMessage().contains("is not mutable"));
        }
    }

    @Test
    public void testRejectDeprecatedInvertedIndexV1WithWhitespace() throws Exception {
        String originFormat = Config.inverted_index_storage_format;
        try {
            ConfigBase.setMutableConfig("inverted_index_storage_format", "V2");
            ConfigException dynamicException = Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("inverted_index_storage_format", " V1 "));
            Assertions.assertTrue(dynamicException.getMessage().contains("Inverted index V1 is deprecated"));
            Assertions.assertEquals("V2", Config.inverted_index_storage_format);

            Config.inverted_index_storage_format = "V2";
            ConfigException startupException = Assertions.assertThrows(ConfigException.class,
                    () -> InvertedIndexStorageFormatValidator.rejectStartupV1(" V1 "));
            Assertions.assertTrue(startupException.getMessage().contains("inverted_index_storage_format=V1"));
            Assertions.assertEquals("V2", Config.inverted_index_storage_format);
        } finally {
            Config.inverted_index_storage_format = originFormat;
        }
    }

    @Test
    public void testSetWebSqlMaxResultBytes() throws ConfigException {
        long original = Config.web_sql_max_result_bytes;
        try {
            ConfigBase.setMutableConfig("web_sql_max_result_bytes", "32");
            Assertions.assertEquals(32, Config.web_sql_max_result_bytes);
            Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("web_sql_max_result_bytes", "0"));
            Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("web_sql_max_result_bytes", "104857601"));
        } finally {
            Config.web_sql_max_result_bytes = original;
        }
    }

    @Test
    public void testStatisticsScannerConcurrencyConfig() throws Exception {
        for (String key : List.of("statistics_sql_max_scanners_concurrency",
                "statistics_sql_max_file_scanners_concurrency")) {
            Field field = Config.getField(key);
            int previous = field.getInt(null);
            try {
                ConfigBase.setMutableConfig(key, "256");
                Assertions.assertEquals(256, field.getInt(null));
                for (String invalid : List.of("-1", "257", "unknown")) {
                    Assertions.assertThrows(ConfigException.class, () -> ConfigBase.setMutableConfig(key, invalid));
                    Assertions.assertEquals(256, field.getInt(null));
                }
                ConfigBase.setMutableConfig(key, "0");
                Assertions.assertEquals(0, field.getInt(null));

                new Config().init(configFile(key + "=257").toString());
                ConfigException exception = Assertions.assertThrows(ConfigException.class,
                        Config::validateStatisticsScannerConcurrency);
                Assertions.assertTrue(exception.getMessage().contains(key));
            } finally {
                field.setInt(null, previous);
            }
        }
    }

    @Test
    public void testCloudWarmUpSchedulerIntervalMustBePositive() throws ConfigException {
        int original = Config.cloud_warm_up_job_scheduler_interval_millisecond;
        try {
            ConfigBase.setMutableConfig("cloud_warm_up_job_scheduler_interval_millisecond", "2000");
            Assertions.assertEquals(2000, Config.cloud_warm_up_job_scheduler_interval_millisecond);

            ConfigException zeroException = Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("cloud_warm_up_job_scheduler_interval_millisecond", "0"));
            Assertions.assertTrue(zeroException.getMessage().contains("must be greater than 0"));
            Assertions.assertEquals(2000, Config.cloud_warm_up_job_scheduler_interval_millisecond);

            ConfigException negativeException = Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("cloud_warm_up_job_scheduler_interval_millisecond", "-1"));
            Assertions.assertTrue(negativeException.getMessage().contains("must be greater than 0"));
            Assertions.assertEquals(2000, Config.cloud_warm_up_job_scheduler_interval_millisecond);
        } finally {
            Config.cloud_warm_up_job_scheduler_interval_millisecond = original;
        }
    }

    @Test
    public void testMtmvCacheManageNumRejectsNegative() throws Exception {
        int original = Config.mtmv_cache_manage_num;
        try {
            Config.mtmv_cache_manage_num = 100;
            // ADMIN SET FRONTEND CONFIG runs the annotation callback before the cache-reload handler,
            // so a negative maximum must be refused there and leave the field untouched.
            ConfigException negative = Assertions.assertThrows(ConfigException.class,
                    () -> ConfigBase.setMutableConfig("mtmv_cache_manage_num", "-1"));
            Assertions.assertTrue(negative.getMessage().contains("must not be negative"));
            Assertions.assertEquals(100, Config.mtmv_cache_manage_num);

            // 0 is the documented way to disable the cache.
            new Config.NonNegativeMtmvCacheNumConfHandler()
                    .handle(ConfigBase.getField("mtmv_cache_manage_num"), " 0 ");
            Assertions.assertEquals(0, Config.mtmv_cache_manage_num);
            Assertions.assertDoesNotThrow(Config::validateMtmvCacheConfig);

            // fe.conf assigns the field without running any callback, so startup validates it too.
            Config.mtmv_cache_manage_num = -1;
            Assertions.assertThrows(ConfigException.class, Config::validateMtmvCacheConfig);
        } finally {
            Config.mtmv_cache_manage_num = original;
        }
    }

    @Test
    public void testValidateWebSqlStartupConfig() throws ConfigException {
        int originalIdleTimeout = Config.web_sql_session_idle_timeout_seconds;
        int originalMaxSessions = Config.web_sql_max_sessions;
        long originalMaxResultBytes = Config.web_sql_max_result_bytes;
        try {
            Config.validateWebSqlConfig();

            Config.web_sql_session_idle_timeout_seconds = 0;
            Assertions.assertThrows(ConfigException.class, Config::validateWebSqlConfig);
            Config.web_sql_session_idle_timeout_seconds = originalIdleTimeout;

            Config.web_sql_max_sessions = 0;
            Assertions.assertThrows(ConfigException.class, Config::validateWebSqlConfig);
            Config.web_sql_max_sessions = originalMaxSessions;

            Config.web_sql_max_result_bytes = Config.WEB_SQL_MAX_RESULT_BYTES_UPPER_BOUND + 1;
            Assertions.assertThrows(ConfigException.class, Config::validateWebSqlConfig);
        } finally {
            Config.web_sql_session_idle_timeout_seconds = originalIdleTimeout;
            Config.web_sql_max_sessions = originalMaxSessions;
            Config.web_sql_max_result_bytes = originalMaxResultBytes;
        }
    }
}
