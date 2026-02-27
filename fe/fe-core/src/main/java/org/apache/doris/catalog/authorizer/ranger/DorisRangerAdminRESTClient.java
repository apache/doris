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

package org.apache.doris.catalog.authorizer.ranger;

import org.apache.doris.common.Config;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.apache.ranger.plugin.util.RangerCommonConstants;
import org.apache.ranger.plugin.util.RangerPluginCapability;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerServiceNotFoundException;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.plugin.util.URLEncoderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;

/**
 * Custom RangerAdminRESTClient that supports fetching permissions from Ranger
 * Server
 * using secure (/secure/) URL paths with Basic Auth, without requiring actual
 * Kerberos credentials.
 *
 * <p>
 * Background: when Ranger Server has Kerberos enabled, it expects clients to
 * use /secure/ URL
 * paths. The parent class {@link RangerAdminRESTClient} couples /secure/ URL
 * usage with
 * {@code MiscUtil.executePrivilegedAction()}, which requires real Kerberos
 * credentials.
 * This class decouples the two: it uses /secure/ URLs but makes direct HTTP
 * requests with
 * Basic Auth instead of Kerberos SPNEGO authentication.
 *
 * <p>
 * When {@code Config.ranger_force_secure_url} is true, this client overrides
 * the 4 periodic
 * download methods to use secure URL paths with direct HTTP calls. Basic Auth
 * credentials
 * are configured via
 * {@code ranger.plugin.<type>.policy.rest.client.username/password} in
 * the ranger security XML configuration file (e.g., ranger-doris-security.xml).
 */
public class DorisRangerAdminRESTClient extends RangerAdminRESTClient {

    private static final Logger LOG = LoggerFactory.getLogger(DorisRangerAdminRESTClient.class);

    private boolean forceSecureUrl;

    // Fields for force-secure mode (initialized only when forceSecureUrl=true)
    private RangerRESTClient secureRestClient;
    private String serviceName;
    private String serviceNameUrlParam;
    private String pluginId;
    private String clusterName;
    private boolean supportsPolicyDeltas;
    private boolean supportsTagDeltas;
    private String pluginCapabilities;
    private boolean isRangerCookieEnabled;
    private String rangerAdminCookieName;
    private Cookie sessionId;

    @Override
    public void init(String serviceName, String appId, String propertyPrefix, Configuration config) {
        LOG.info("DorisRangerAdminRESTClient: loaded. serviceName={}, ranger_force_secure_url={}",
                serviceName, Config.ranger_force_secure_url);
        super.init(serviceName, appId, propertyPrefix, config);

        this.forceSecureUrl = Config.ranger_force_secure_url;
        if (!forceSecureUrl) {
            LOG.info("DorisRangerAdminRESTClient: force-secure-url is disabled, using default behavior.");
            return;
        }

        // Re-compute fields from init params (parent fields are private)
        this.serviceName = serviceName;
        this.pluginId = new RangerRESTUtils().getPluginId(serviceName, appId);
        this.pluginCapabilities = Long.toHexString(new RangerPluginCapability().getPluginCapabilities());

        try {
            this.serviceNameUrlParam = URLEncoderUtil.encodeURIParam(serviceName);
        } catch (UnsupportedEncodingException e) {
            LOG.warn("DorisRangerAdminRESTClient: failed to encode serviceName={}", serviceName);
            this.serviceNameUrlParam = serviceName;
        }

        // Read cluster name (same logic as parent)
        this.clusterName = config.get(propertyPrefix + ".access.cluster.name", "");
        if (StringUtil.isEmpty(this.clusterName)) {
            this.clusterName = config.get(propertyPrefix + ".ambari.cluster.name", "");
            if (StringUtil.isEmpty(this.clusterName) && config instanceof RangerPluginConfig) {
                this.clusterName = ((RangerPluginConfig) config).getClusterName();
            }
        }

        // Read config values (same defaults as parent)
        this.supportsPolicyDeltas = config.getBoolean(
                propertyPrefix + RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_POLICY_DELTA,
                RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_POLICY_DELTA_DEFAULT);
        this.supportsTagDeltas = config.getBoolean(
                propertyPrefix + RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_TAG_DELTA,
                RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_TAG_DELTA_DEFAULT);
        this.isRangerCookieEnabled = config.getBoolean(
                propertyPrefix + ".policy.rest.client.cookie.enabled",
                RangerCommonConstants.POLICY_REST_CLIENT_SESSION_COOKIE_ENABLED);
        this.rangerAdminCookieName = config.get(
                propertyPrefix + ".policy.rest.client.session.cookie.name",
                RangerCommonConstants.DEFAULT_COOKIE_NAME);

        // Create our own RangerRESTClient with same configuration.
        // The RangerRESTClient constructor auto-reads Basic Auth credentials from
        // config:
        // ranger.plugin.<type>.policy.rest.client.username
        // ranger.plugin.<type>.policy.rest.client.password
        // and sets up HTTPBasicAuthFilter on the Jersey Client.
        String url = config.get(propertyPrefix + ".policy.rest.url", "").trim();
        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        String sslConfigFileName = config.get(propertyPrefix + ".policy.rest.ssl.config.file");
        int connTimeOutMs = config.getInt(
                propertyPrefix + ".policy.rest.client.connection.timeoutMs", 120 * 1000);
        int readTimeOutMs = config.getInt(
                propertyPrefix + ".policy.rest.client.read.timeoutMs", 30 * 1000);
        int maxRetryAttempts = config.getInt(
                propertyPrefix + ".policy.rest.client.max.retry.attempts", 3);
        int retryIntervalMs = config.getInt(
                propertyPrefix + ".policy.rest.client.retry.interval.ms", 1000);

        this.secureRestClient = new RangerRESTClient(url, sslConfigFileName, config);
        this.secureRestClient.setRestClientConnTimeOutMs(connTimeOutMs);
        this.secureRestClient.setRestClientReadTimeOutMs(readTimeOutMs);
        this.secureRestClient.setMaxRetryAttempts(maxRetryAttempts);
        this.secureRestClient.setRetryIntervalMs(retryIntervalMs);

        boolean basicAuthConfigured = config.get(propertyPrefix + ".policy.rest.client.username") != null;

        LOG.info("DorisRangerAdminRESTClient: force-secure-url mode initialized."
                + " serviceName={}, url={}, basicAuthConfigured={}",
                serviceName, url, basicAuthConfigured);
        if (!basicAuthConfigured) {
            LOG.warn("DorisRangerAdminRESTClient: Basic Auth credentials are NOT configured. "
                    + "Please set '{}.policy.rest.client.username' and '{}.policy.rest.client.password' "
                    + "in ranger security XML config.", propertyPrefix, propertyPrefix);
        }
    }

    @Override
    public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion,
            long lastActivationTimeInMillis) throws Exception {
        if (!forceSecureUrl) {
            return super.getServicePoliciesIfUpdated(lastKnownVersion, lastActivationTimeInMillis);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> DorisRangerAdminRESTClient.getServicePoliciesIfUpdated({}, {})",
                    lastKnownVersion, lastActivationTimeInMillis);
        }

        final String relativeURL = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED
                + serviceNameUrlParam;

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION,
                Long.toString(lastKnownVersion));
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME,
                Long.toString(lastActivationTimeInMillis));
        queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
        queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, clusterName);
        queryParams.put(RangerRESTUtils.REST_PARAM_SUPPORTS_POLICY_DELTAS,
                Boolean.toString(supportsPolicyDeltas));
        queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

        if (LOG.isDebugEnabled()) {
            LOG.debug("DorisRangerAdminRESTClient: fetching policies using force-secure URL: {}", relativeURL);
        }

        ClientResponse response = secureRestClient.get(relativeURL, queryParams, sessionId);
        checkAndResetSessionCookie(response);

        final ServicePolicies ret;

        if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED
                || response.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
            if (response == null) {
                LOG.error("DorisRangerAdminRESTClient: Error getting policies; Received NULL response!!."
                        + " serviceName={}", serviceName);
            } else {
                if (LOG.isDebugEnabled()) {
                    RESTResponse resp = RESTResponse.fromClientResponse(response);
                    LOG.debug("DorisRangerAdminRESTClient: No change in policies."
                            + " response={}, serviceName={}, lastKnownVersion={}",
                            resp, serviceName, lastKnownVersion);
                }
            }
            ret = null;
        } else if (response.getStatus() == HttpServletResponse.SC_OK) {
            ret = JsonUtilsV2.readResponse(response, ServicePolicies.class);
        } else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
            ret = null;
            LOG.error("DorisRangerAdminRESTClient: Error getting policies; service not found."
                    + " httpStatus={}, serviceName={}", response.getStatus(), serviceName);
            String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;
            RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);
            LOG.warn("DorisRangerAdminRESTClient: Received 404 with body:[{}], Ignoring", exceptionMsg);
        } else {
            RESTResponse resp = RESTResponse.fromClientResponse(response);
            LOG.warn("DorisRangerAdminRESTClient: Error getting policies."
                    + " httpStatus={}, response={}, serviceName={}",
                    response.getStatus(), resp, serviceName);
            ret = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== DorisRangerAdminRESTClient.getServicePoliciesIfUpdated({}, {})",
                    lastKnownVersion, lastActivationTimeInMillis);
        }

        return ret;
    }

    @Override
    public RangerRoles getRolesIfUpdated(long lastKnownRoleVersion,
            long lastActivationTimeInMillis) throws Exception {
        if (!forceSecureUrl) {
            return super.getRolesIfUpdated(lastKnownRoleVersion, lastActivationTimeInMillis);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> DorisRangerAdminRESTClient.getRolesIfUpdated({}, {})",
                    lastKnownRoleVersion, lastActivationTimeInMillis);
        }

        final String relativeURL = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES
                + serviceNameUrlParam;

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_ROLE_VERSION,
                Long.toString(lastKnownRoleVersion));
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME,
                Long.toString(lastActivationTimeInMillis));
        queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
        queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, clusterName);
        queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

        if (LOG.isDebugEnabled()) {
            LOG.debug("DorisRangerAdminRESTClient: fetching roles using force-secure URL: {}", relativeURL);
        }

        ClientResponse response = secureRestClient.get(relativeURL, queryParams, sessionId);
        checkAndResetSessionCookie(response);

        final RangerRoles ret;

        if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED
                || response.getStatus() == HttpServletResponse.SC_NO_CONTENT) {
            if (response == null) {
                LOG.error("DorisRangerAdminRESTClient: Error getting roles; Received NULL response!!."
                        + " serviceName={}", serviceName);
            } else {
                if (LOG.isDebugEnabled()) {
                    RESTResponse resp = RESTResponse.fromClientResponse(response);
                    LOG.debug("DorisRangerAdminRESTClient: No change in roles."
                            + " response={}, serviceName={}, lastKnownRoleVersion={}",
                            resp, serviceName, lastKnownRoleVersion);
                }
            }
            ret = null;
        } else if (response.getStatus() == HttpServletResponse.SC_OK) {
            ret = JsonUtilsV2.readResponse(response, RangerRoles.class);
        } else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
            ret = null;
            LOG.error("DorisRangerAdminRESTClient: Error getting roles; service not found."
                    + " httpStatus={}, serviceName={}", response.getStatus(), serviceName);
            String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;
            RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);
            LOG.warn("DorisRangerAdminRESTClient: Received 404 with body:[{}], Ignoring", exceptionMsg);
        } else {
            RESTResponse resp = RESTResponse.fromClientResponse(response);
            LOG.warn("DorisRangerAdminRESTClient: Error getting roles."
                    + " httpStatus={}, response={}, serviceName={}",
                    response.getStatus(), resp, serviceName);
            ret = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== DorisRangerAdminRESTClient.getRolesIfUpdated({}, {})",
                    lastKnownRoleVersion, lastActivationTimeInMillis);
        }

        return ret;
    }

    @Override
    public ServiceTags getServiceTagsIfUpdated(long lastKnownVersion,
            long lastActivationTimeInMillis) throws Exception {
        if (!forceSecureUrl) {
            return super.getServiceTagsIfUpdated(lastKnownVersion, lastActivationTimeInMillis);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> DorisRangerAdminRESTClient.getServiceTagsIfUpdated({}, {})",
                    lastKnownVersion, lastActivationTimeInMillis);
        }

        final String relativeURL = RangerRESTUtils.REST_URL_GET_SECURE_SERVICE_TAGS_IF_UPDATED
                + serviceNameUrlParam;

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(RangerRESTUtils.LAST_KNOWN_TAG_VERSION_PARAM, Long.toString(lastKnownVersion));
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME,
                Long.toString(lastActivationTimeInMillis));
        queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
        queryParams.put(RangerRESTUtils.REST_PARAM_SUPPORTS_TAG_DELTAS,
                Boolean.toString(supportsTagDeltas));
        queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

        if (LOG.isDebugEnabled()) {
            LOG.debug("DorisRangerAdminRESTClient: fetching tags using force-secure URL: {}", relativeURL);
        }

        ClientResponse response = secureRestClient.get(relativeURL, queryParams, sessionId);
        checkAndResetSessionCookie(response);

        final ServiceTags ret;

        if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED) {
            if (response == null) {
                LOG.error("DorisRangerAdminRESTClient: Error getting tags; Received NULL response!!."
                        + " serviceName={}", serviceName);
            } else {
                if (LOG.isDebugEnabled()) {
                    RESTResponse resp = RESTResponse.fromClientResponse(response);
                    LOG.debug("DorisRangerAdminRESTClient: No change in tags."
                            + " response={}, serviceName={}, lastKnownVersion={}",
                            resp, serviceName, lastKnownVersion);
                }
            }
            ret = null;
        } else if (response.getStatus() == HttpServletResponse.SC_OK) {
            ret = JsonUtilsV2.readResponse(response, ServiceTags.class);
        } else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
            ret = null;
            LOG.error("DorisRangerAdminRESTClient: Error getting tags; service not found."
                    + " httpStatus={}, serviceName={}", response.getStatus(), serviceName);
            String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;
            RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);
            LOG.warn("DorisRangerAdminRESTClient: Received 404 with body:[{}], Ignoring", exceptionMsg);
        } else {
            RESTResponse resp = RESTResponse.fromClientResponse(response);
            LOG.warn("DorisRangerAdminRESTClient: Error getting tags."
                    + " httpStatus={}, response={}, serviceName={}",
                    response.getStatus(), resp, serviceName);
            ret = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== DorisRangerAdminRESTClient.getServiceTagsIfUpdated({}, {})",
                    lastKnownVersion, lastActivationTimeInMillis);
        }

        return ret;
    }

    @Override
    public RangerUserStore getUserStoreIfUpdated(long lastKnownUserStoreVersion,
            long lastActivationTimeInMillis) throws Exception {
        if (!forceSecureUrl) {
            return super.getUserStoreIfUpdated(lastKnownUserStoreVersion, lastActivationTimeInMillis);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> DorisRangerAdminRESTClient.getUserStoreIfUpdated({}, {})",
                    lastKnownUserStoreVersion, lastActivationTimeInMillis);
        }

        final String relativeURL = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USERSTORE
                + serviceNameUrlParam;

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_KNOWN_USERSTORE_VERSION,
                Long.toString(lastKnownUserStoreVersion));
        queryParams.put(RangerRESTUtils.REST_PARAM_LAST_ACTIVATION_TIME,
                Long.toString(lastActivationTimeInMillis));
        queryParams.put(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
        queryParams.put(RangerRESTUtils.REST_PARAM_CLUSTER_NAME, clusterName);
        queryParams.put(RangerRESTUtils.REST_PARAM_CAPABILITIES, pluginCapabilities);

        if (LOG.isDebugEnabled()) {
            LOG.debug("DorisRangerAdminRESTClient: fetching userStore using force-secure URL: {}", relativeURL);
        }

        ClientResponse response = secureRestClient.get(relativeURL, queryParams, sessionId);
        checkAndResetSessionCookie(response);

        final RangerUserStore ret;

        if (response == null || response.getStatus() == HttpServletResponse.SC_NOT_MODIFIED) {
            if (response == null) {
                LOG.error("DorisRangerAdminRESTClient: Error getting userStore; Received NULL response!!."
                        + " serviceName={}", serviceName);
            } else {
                if (LOG.isDebugEnabled()) {
                    RESTResponse resp = RESTResponse.fromClientResponse(response);
                    LOG.debug("DorisRangerAdminRESTClient: No change in userStore."
                            + " response={}, serviceName={}, lastKnownUserStoreVersion={}",
                            resp, serviceName, lastKnownUserStoreVersion);
                }
            }
            ret = null;
        } else if (response.getStatus() == HttpServletResponse.SC_OK) {
            ret = JsonUtilsV2.readResponse(response, RangerUserStore.class);
        } else if (response.getStatus() == HttpServletResponse.SC_NOT_FOUND) {
            ret = null;
            LOG.error("DorisRangerAdminRESTClient: Error getting userStore; service not found."
                    + " httpStatus={}, serviceName={}", response.getStatus(), serviceName);
            String exceptionMsg = response.hasEntity() ? response.getEntity(String.class) : null;
            RangerServiceNotFoundException.throwExceptionIfServiceNotFound(serviceName, exceptionMsg);
            LOG.warn("DorisRangerAdminRESTClient: Received 404 with body:[{}], Ignoring", exceptionMsg);
        } else {
            RESTResponse resp = RESTResponse.fromClientResponse(response);
            LOG.warn("DorisRangerAdminRESTClient: Error getting userStore."
                    + " httpStatus={}, response={}, serviceName={}",
                    response.getStatus(), resp, serviceName);
            ret = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== DorisRangerAdminRESTClient.getUserStoreIfUpdated({}, {})",
                    lastKnownUserStoreVersion, lastActivationTimeInMillis);
        }

        return ret;
    }

    /**
     * Replicate parent's private checkAndResetSessionCookie logic using our own
     * sessionId field.
     * When forceSecureUrl=true, all 4 overridden methods use this.sessionId
     * consistently,
     * so there's no conflict with the parent's sessionId.
     */
    private void checkAndResetSessionCookie(ClientResponse response) {
        if (!isRangerCookieEnabled) {
            return;
        }
        if (response == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("DorisRangerAdminRESTClient.checkAndResetSessionCookie(): "
                        + "RESETTING sessionId - response is null");
            }
            sessionId = null;
        } else {
            int status = response.getStatus();
            if (status == HttpServletResponse.SC_OK
                    || status == HttpServletResponse.SC_NO_CONTENT
                    || status == HttpServletResponse.SC_NOT_MODIFIED) {
                Cookie newCookie = null;
                for (NewCookie cookie : response.getCookies()) {
                    if (cookie.getName().equalsIgnoreCase(rangerAdminCookieName)) {
                        newCookie = cookie;
                        break;
                    }
                }
                if (sessionId == null || newCookie != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("DorisRangerAdminRESTClient.checkAndResetSessionCookie(): "
                                + "status={}, sessionIdCookie={}, newCookie={}",
                                status, sessionId, newCookie);
                    }
                    sessionId = newCookie;
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("DorisRangerAdminRESTClient.checkAndResetSessionCookie(): "
                            + "RESETTING sessionId - status={}", status);
                }
                sessionId = null;
            }
        }
    }
}
