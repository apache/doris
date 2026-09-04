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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ParamRules;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Everything a user writes in {@code CREATE CATALOG} for a MaxCompute catalog, bound and checked.
 *
 * <p>fe-core parses no connector properties, so this class is where they are interpreted. It replaces a
 * constants class whose values were re-parsed at six call sites, each with its own
 * {@code getOrDefault} + {@code parseInt} pair.
 *
 * <p>{@link #of(Map)} binds, derives and validates in one step, so an instance that exists has valid
 * properties and every reader downstream uses a getter. It performs no I/O and is idempotent: it runs at
 * {@code CREATE CATALOG}, again on the merged candidate when {@code ALTER CATALOG} validates, and once
 * more every time the connector is rebuilt -- including on an FE replaying the edit log, where I/O would
 * mean an unreachable service could stop FE from starting. Checks that do need the service stay in
 * {@code MaxComputeDorisConnector#testConnection}.
 *
 * <p><b>Unknown keys are accepted, always.</b> The same map carries engine keys ({@code type},
 * {@code meta.cache.*}, ...) and storage keys, and {@code ALTER CATALOG} merges properties: it can
 * overwrite a key but never remove one, so a key refused here would leave a catalog that no statement
 * could repair. Bad <i>values</i> are refused; unrecognized <i>names</i> are not.
 */
public final class MCCatalogProperties {

    // -- connection --

    /** The MaxCompute project this catalog exposes as its default namespace. */
    public static final String PROJECT = "mc.default.project";

    /**
     * The MaxCompute service endpoint. Required of any new catalog; the three legacy spellings below are
     * still resolved for a catalog already in the image -- see {@link #checkCreateTimeOnlyRules()}.
     */
    public static final String ENDPOINT = "mc.endpoint";

    /** Legacy: the region an endpoint is derived from, via a URL template. Superseded by {@link #ENDPOINT}. */
    public static final String REGION = "mc.region";

    /** Legacy: pairs with {@link #REGION} to pick the public rather than the intranet endpoint. */
    public static final String PUBLIC_ACCESS = "mc.public_access";

    /** Legacy: a service endpoint used as written. Superseded by {@link #ENDPOINT}. */
    public static final String ODPS_ENDPOINT = "mc.odps_endpoint";

    /** Legacy: a tunnel endpoint the service endpoint is rewritten from. Superseded by {@link #ENDPOINT}. */
    public static final String TUNNEL_SDK_ENDPOINT = "mc.tunnel_endpoint";

    /** The compute quota to run under. */
    public static final String QUOTA = "mc.quota";

    // -- authentication --

    public static final String AUTH_TYPE = "mc.auth.type";
    public static final String ACCESS_KEY = "mc.access_key";
    public static final String SECRET_KEY = "mc.secret_key";
    public static final String RAM_ROLE_ARN = "mc.ram_role_arn";
    public static final String ECS_RAM_ROLE = "mc.ecs_ram_role";

    /**
     * Whether an account is identified by display name or by id. It selects the SDK's {@code AccountFormat},
     * which decides how MaxCompute resolves the principal this catalog acts as.
     */
    public static final String ACCOUNT_FORMAT = "mc.account_format";

    // -- namespace --

    /**
     * Expose MaxCompute's three-level project/schema/table namespace instead of flattening it: with it on,
     * a Doris database is a MaxCompute schema; with it off, a Doris database is a MaxCompute project.
     */
    public static final String ENABLE_NAMESPACE_SCHEMA = "mc.enable.namespace.schema";

    // -- scan splitting --

    /** Which of the two split sizings below applies: one of {@link SplitStrategy}. */
    public static final String SPLIT_STRATEGY = "mc.split_strategy";

    /** Target bytes per split under {@link SplitStrategy#BYTE_SIZE}. */
    public static final String SPLIT_BYTE_SIZE = "mc.split_byte_size";

    /** Rows per split under {@link SplitStrategy#ROW_COUNT}. */
    public static final String SPLIT_ROW_COUNT = "mc.split_row_count";

    /** Whether one split may span partitions. */
    public static final String SPLIT_CROSS_PARTITION = "mc.split_cross_partition";

    /** Whether datetime predicates are pushed to MaxCompute. */
    public static final String DATETIME_PREDICATE_PUSH_DOWN = "mc.datetime_predicate_push_down";

    // -- transport --

    public static final String CONNECT_TIMEOUT = "mc.connect_timeout";
    public static final String READ_TIMEOUT = "mc.read_timeout";
    public static final String RETRY_COUNT = "mc.retry_count";

    /** The largest single field the write path will send. */
    public static final String MAX_FIELD_SIZE = "mc.max_field_size_bytes";

    /**
     * The floor on {@link #SPLIT_BYTE_SIZE}. Below it the Storage API produces so many splits that planning,
     * not scanning, becomes the cost.
     */
    private static final long MIN_SPLIT_BYTE_SIZE = 10485760L;

    /** How a scan's splits are sized. */
    public enum SplitStrategy {
        BYTE_SIZE("byte_size"),
        ROW_COUNT("row_count");

        private final String value;

        SplitStrategy(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /** How MaxCompute identifies the account this catalog acts as. */
    public enum AccountFormat {
        NAME("name"),
        ID("id");

        private final String value;

        AccountFormat(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /** How credentials are supplied. */
    public enum AuthType {
        AK_SK("ak_sk"),
        RAM_ROLE_ARN("ram_role_arn"),
        ECS_RAM_ROLE("ecs_ram_role");

        private final String value;

        AuthType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @ConnectorProperty(names = {PROJECT}, description = "the MaxCompute project used as the default namespace")
    private String project;

    @ConnectorProperty(names = {ENDPOINT}, required = false,
            description = "the MaxCompute service endpoint; required of a new catalog")
    private String endpoint = "";

    @ConnectorProperty(names = {REGION}, required = false,
            description = "legacy: region an endpoint is derived from")
    private String region = "";

    @ConnectorProperty(names = {PUBLIC_ACCESS}, required = false,
            description = "legacy: with mc.region, pick the public endpoint instead of the intranet one")
    private boolean publicAccess;

    @ConnectorProperty(names = {ODPS_ENDPOINT}, required = false,
            description = "legacy: service endpoint used as written")
    private String odpsEndpoint = "";

    @ConnectorProperty(names = {TUNNEL_SDK_ENDPOINT}, required = false,
            description = "legacy: tunnel endpoint the service endpoint is rewritten from")
    private String tunnelEndpoint = "";

    @ConnectorProperty(names = {QUOTA}, required = false, description = "the compute quota to run under")
    private String quota = "pay-as-you-go";

    @ConnectorProperty(names = {AUTH_TYPE}, required = false,
            description = "ak_sk | ram_role_arn | ecs_ram_role")
    private String authType = "";

    @ConnectorProperty(names = {ACCESS_KEY}, required = false, sensitive = true,
            description = "AccessKey id")
    private String accessKey = "";

    @ConnectorProperty(names = {SECRET_KEY}, required = false, sensitive = true,
            description = "AccessKey secret")
    private String secretKey = "";

    @ConnectorProperty(names = {RAM_ROLE_ARN}, required = false,
            description = "the RAM role to assume under ram_role_arn auth")
    private String ramRoleArn = "";

    @ConnectorProperty(names = {ECS_RAM_ROLE}, required = false,
            description = "the ECS instance RAM role name under ecs_ram_role auth")
    private String ecsRamRole = "";

    @ConnectorProperty(names = {ACCOUNT_FORMAT}, required = false, description = "name | id")
    private String accountFormat = "";

    @ConnectorProperty(names = {ENABLE_NAMESPACE_SCHEMA}, required = false,
            description = "expose MaxCompute schemas as Doris databases instead of projects")
    private boolean enableNamespaceSchema;

    @ConnectorProperty(names = {SPLIT_STRATEGY}, required = false, description = "byte_size | row_count")
    private String splitStrategy = "";

    @ConnectorProperty(names = {SPLIT_BYTE_SIZE}, required = false,
            description = "target bytes per split under the byte_size strategy")
    private long splitByteSize = 268435456L;

    @ConnectorProperty(names = {SPLIT_ROW_COUNT}, required = false,
            description = "rows per split under the row_count strategy")
    private long splitRowCount = 1048576L;

    @ConnectorProperty(names = {SPLIT_CROSS_PARTITION}, required = false,
            description = "whether one split may span partitions")
    private boolean splitCrossPartition = true;

    @ConnectorProperty(names = {DATETIME_PREDICATE_PUSH_DOWN}, required = false,
            description = "whether datetime predicates are pushed to MaxCompute")
    private boolean dateTimePredicatePushDown = true;

    @ConnectorProperty(names = {CONNECT_TIMEOUT}, required = false, description = "seconds")
    private int connectTimeout = 10;

    @ConnectorProperty(names = {READ_TIMEOUT}, required = false, description = "seconds")
    private int readTimeout = 120;

    @ConnectorProperty(names = {RETRY_COUNT}, required = false, description = "REST retries")
    private int retryCount = 4;

    @ConnectorProperty(names = {MAX_FIELD_SIZE}, required = false,
            description = "the largest single field the write path will send, in bytes")
    private long maxFieldSize = 8388608L;

    private final Map<String, String> raw;
    private SplitStrategy splitStrategyValue;
    private AccountFormat accountFormatValue;
    private AuthType authTypeValue;
    private String resolvedEndpoint;

    private MCCatalogProperties(Map<String, String> properties) {
        this.raw = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    public static MCCatalogProperties of(Map<String, String> properties) {
        MCCatalogProperties p = new MCCatalogProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(p, properties);
        p.splitStrategyValue = parseSplitStrategy(p.splitStrategy);
        p.accountFormatValue = parseAccountFormat(p.accountFormat);
        p.authTypeValue = parseAuthType(p.authType);
        p.resolvedEndpoint = MCConnectorEndpoint.resolveEndpoint(p);
        new ParamRules()
                .require(p.project, "Required property '" + PROJECT + "' is missing")
                .require(p.resolvedEndpoint, "Required property '" + ENDPOINT + "' is missing")
                // ParamRules.check throws when the condition holds, so each states the FAILING case.
                .check(() -> p.splitStrategyValue == SplitStrategy.BYTE_SIZE
                                && p.splitByteSize < MIN_SPLIT_BYTE_SIZE,
                        SPLIT_BYTE_SIZE + " must be greater than or equal to " + MIN_SPLIT_BYTE_SIZE)
                .check(() -> p.splitStrategyValue == SplitStrategy.ROW_COUNT && p.splitRowCount <= 0,
                        SPLIT_ROW_COUNT + " must be greater than 0")
                .check(() -> p.connectTimeout <= 0, CONNECT_TIMEOUT + " must be greater than 0")
                .check(() -> p.readTimeout <= 0, READ_TIMEOUT + " must be greater than 0")
                .check(() -> p.retryCount <= 0, RETRY_COUNT + " must be greater than 0")
                .validate();
        p.checkAuthCompleteness();
        return p;
    }

    /**
     * The rules that apply to a statement a user is writing now, but not to a catalog already stored.
     *
     * <p>{@link #ENDPOINT} is required of any new catalog, while {@link #REGION}, {@link #ODPS_ENDPOINT}
     * and {@link #TUNNEL_SDK_ENDPOINT} go on resolving for catalogs created before it existed. Enforcing
     * that in {@link #of(Map)} would make those older catalogs unbuildable on the next FE restart, so it
     * lives here, where only the interactive {@code CREATE}/{@code ALTER} doors call it.
     */
    public MCCatalogProperties checkCreateTimeOnlyRules() {
        new ParamRules()
                .require(endpoint, "Required property '" + ENDPOINT + "' is missing")
                .validate();
        return this;
    }

    /**
     * Each auth type needs its own credentials present; without them the client is built and fails much
     * later, at the first request, saying only that the service refused it.
     */
    private void checkAuthCompleteness() {
        switch (authTypeValue) {
            case AK_SK:
                if (accessKey.isEmpty() || secretKey.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Missing access key or secret key for AK/SK auth type");
                }
                break;
            case RAM_ROLE_ARN:
                if (accessKey.isEmpty() || secretKey.isEmpty() || ramRoleArn.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Missing access key, secret key or role arn for RAM Role ARN auth type");
                }
                break;
            case ECS_RAM_ROLE:
                if (ecsRamRole.isEmpty()) {
                    throw new IllegalArgumentException(
                            "Missing role name for ECS RAM Role auth type");
                }
                break;
            default:
                throw new IllegalStateException("Unhandled auth type: " + authTypeValue);
        }
    }

    private static SplitStrategy parseSplitStrategy(String value) {
        if (value.isEmpty()) {
            return SplitStrategy.BYTE_SIZE;
        }
        for (SplitStrategy strategy : SplitStrategy.values()) {
            if (strategy.getValue().equals(value)) {
                return strategy;
            }
        }
        throw new IllegalArgumentException("property " + SPLIT_STRATEGY + " must be "
                + SplitStrategy.BYTE_SIZE.getValue() + " or " + SplitStrategy.ROW_COUNT.getValue());
    }

    private static AccountFormat parseAccountFormat(String value) {
        if (value.isEmpty()) {
            return AccountFormat.NAME;
        }
        for (AccountFormat format : AccountFormat.values()) {
            if (format.getValue().equals(value)) {
                return format;
            }
        }
        throw new IllegalArgumentException(
                "property " + ACCOUNT_FORMAT + " only support name and id");
    }

    private static AuthType parseAuthType(String value) {
        if (value.isEmpty()) {
            return AuthType.AK_SK;
        }
        for (AuthType type : AuthType.values()) {
            // Case-insensitive, as the pre-holder readers were; the other two enums above match exactly,
            // as theirs were. Aligning them would change which spellings an existing catalog accepts.
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported auth type: " + value);
    }

    public String getProject() {
        return project;
    }

    /**
     * The endpoint as spelled by {@link #ENDPOINT}, empty when the catalog carries only a legacy spelling.
     * Readers want {@link #getResolvedEndpoint()}; this one exists for {@link #checkCreateTimeOnlyRules()}.
     */
    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public boolean isPublicAccess() {
        return publicAccess;
    }

    public String getOdpsEndpoint() {
        return odpsEndpoint;
    }

    public String getTunnelEndpoint() {
        return tunnelEndpoint;
    }

    /** The service endpoint after the legacy spellings are resolved. Never blank. */
    public String getResolvedEndpoint() {
        return resolvedEndpoint;
    }

    public String getQuota() {
        return quota;
    }

    public AuthType getAuthType() {
        return authTypeValue;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getRamRoleArn() {
        return ramRoleArn;
    }

    public String getEcsRamRole() {
        return ecsRamRole;
    }

    public AccountFormat getAccountFormat() {
        return accountFormatValue;
    }

    public boolean isEnableNamespaceSchema() {
        return enableNamespaceSchema;
    }

    public SplitStrategy getSplitStrategy() {
        return splitStrategyValue;
    }

    public long getSplitByteSize() {
        return splitByteSize;
    }

    public long getSplitRowCount() {
        return splitRowCount;
    }

    public boolean isSplitCrossPartition() {
        return splitCrossPartition;
    }

    public boolean isDateTimePredicatePushDown() {
        return dateTimePredicatePushDown;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public long getMaxFieldSize() {
        return maxFieldSize;
    }

    /**
     * The catalog's properties as written, unmodifiable.
     *
     * <p><b>It contains the AccessKey secret.</b> It is handed to BE as part of the table descriptor and
     * the write sink, which need the credentials; pass it nowhere else that a user can read back.
     */
    public Map<String, String> getRaw() {
        return raw;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }
}
