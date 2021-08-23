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

package org.apache.doris.stack.model.response.config;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
public class SessionSettingResp {

    private String authType;

    // Normal configuration
    private String humanizationStrategy = "advanced";

    // has-sample-dataset?
    private boolean hasSampleDataset = true;

    // ssl-certificate-public-key
    private String sslCertificatePublicKey;

    // engines
    private String engines;

    // breakout-bin-width
    private int breakoutBinWidth = 10;

    // embedding-secret-key
    private String embeddingSecretKey;

    // ga-code
    private String gaCode = "UA-60817802-1";

    // enable-enhancements?
    private boolean enableEnhancements = false;

    // site-locale
    private String siteLocale = "zh";

    // report-timezone-short
    private String reportTimezoneShort = "CT";

    // application-name
    private String applicationName = "Doris Studio";

    // enable-xrays
    private boolean enableXrays = false;

    // query-caching-ttl-ratio
    private int queryCachingTtlRatio = 10;

    // embedding-app-origin
    private String embeddingAppOrigin;

    // ldap-sync-user-attributes-blacklist
    private List<String> ldapSyncUserAttributesVlacklist =
            Lists.newArrayList("userPassword", "dn", "distinguishedName");

    // admin-email
    private String adminEmail;

    // slack-token
    private String slackToken;

    // check-for-updates
    private boolean checkForUpdates = true;

    private Map<String, Set<String>> entities;

    // source-address-header
    private String sourceAddressHeader;

    // enable-nested-queries
    private boolean enableNestedQueries = true;

    // site-url
    private String siteUrl;

    // enable-password-login
    private boolean enablePasswordLogin = true;

    private Map<String, Set<String>> types;

    // start-of-week
    private String startOfWeek = "monday";

    // custom-geojson
    private Map<String, GeoInfo> customGeojson;

    // enable-public-sharing
    private boolean enablePublicSharing;

    // available-timezones
    private Set<String> availableTimezones;

    // hide-embed-branding?
    private boolean hideEmbedBranding = false;

    // enable-sandboxes?
    private boolean enableSandboxes = false;

    // available-locales
    private List<List<String>> availableLocales;

    // landing-page
    private String landingPage;

    // setup-token
    private String setupToken;

    // enable-embedding
    private boolean enableEmbedding = false;

    // application-colors
    private String applicationColors = "{}";

    // enable-audit-app?
    private boolean enableAuditApp = false;

    // version-info
    private VersionInfo versionInfo = new VersionInfo("v0.4.0", "2021-04-22", "for test");

    // anon-tracking-enabled
    private boolean anonTrackingEnabled = false;

    // application-logo-url
    private String applicationLogoUrl = "app/assets/img/logo.svg";

    // "application-favicon-url":"frontend_client/favicon.ico",
    private String applicationFaviconUrl = "frontend_client/favicon.ico";

    // show-homepage-xrays
    private boolean showHomepageXrays = true;

    // enable-whitelabeling?
    private boolean enableWhitelabeling = false;

    // show-homepage-data
    private boolean showHomepageData = true;

    // map-tile-server-url
    private String mapTileServerUrl = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";

    // site-name
    private String siteName = "Doris Studio";

    // enable-query-caching
    private boolean enableQueryCaching = false;

    // redirect-all-requests-to-https
    private boolean redirectAllRequestsToHttps = false;

    //
    private Version version = new Version();

    // password-complexity
    private PasswordComplexity passwordComplexity = new PasswordComplexity();

    // report-timezone
    private String reportTimezone;

    //query-caching-min-ttl
    private int queryCachingMinTtl = 60;

    // "query-caching-max-ttl":8640000,
    private long queryCachingMaxTtl = 8640000;

    // metabot-enabled
    private boolean metabotEnabled = false;

    // premium-embedding-token
    private String premiumEmbeddingToken;

    // breakout-bins-num
    private int breakoutBinsNum = 8;

    // enable-sso?
    private boolean enableSso = false;

    // query-caching-max-kb
    private int queryCachingMaxKb = 1000;

    // premium-features
    private PremiumFeatures premiumFeatures = new PremiumFeatures();

    // custom-formatting
    private Object customFormatting;

    // LDAP配置
    // ldap-configured?
    private boolean ldapConfigured;

    private String ldapAttributeEmail;

    private String ldapAttributeFirstName;

    private String ldapAttributeLastName;

    private String ldapBindDn;

    private Boolean ldapEnabled;

    private String ldapGroupBase;

    private Object ldapGroupMappings;

    private Boolean ldapGroupSync;

    private String ldapHost;

    private String ldapPassword;

    private Integer ldapPort;

    private String ldapSecurity;

    private List<String> ldapUserBase;

    private String ldapUserFilter;

    // Mailbox configuration
    private String smtpHost;

    private String smtpPort;

    private String smtpSecurity;

    private String smtpUsername;

    private String smtpPassword;

    private String fromAddress;

    private boolean emailConfigured;

    @JSONField(name = "humanization-strategy")
    @JsonProperty("humanization-strategy")
    public String getHumanizationStrategy() {
        return humanizationStrategy;
    }

    @JSONField(name = "humanization-strategy")
    @JsonProperty("humanization-strategy")
    public void setHumanizationStrategy(String humanizationStrategy) {
        this.humanizationStrategy = humanizationStrategy;
    }

    @JSONField(name = "email-smtp-host")
    @JsonProperty("email-smtp-host")
    public String getSmtpHost() {
        return smtpHost;
    }

    @JSONField(name = "email-smtp-host")
    @JsonProperty("email-smtp-host")
    public void setSmtpHost(String smtpHost) {
        this.smtpHost = smtpHost;
    }

    @JSONField(name = "email-smtp-port")
    @JsonProperty("email-smtp-port")
    public String getSmtpPort() {
        return smtpPort;
    }

    @JSONField(name = "email-smtp-port")
    @JsonProperty("email-smtp-port")
    public void setSmtpPort(String smtpPort) {
        this.smtpPort = smtpPort;
    }

    @JSONField(name = "email-smtp-security")
    @JsonProperty("email-smtp-security")
    public String getSmtpSecurity() {
        return smtpSecurity;
    }

    @JSONField(name = "email-smtp-security")
    @JsonProperty("email-smtp-security")
    public void setSmtpSecurity(String smtpSecurity) {
        this.smtpSecurity = smtpSecurity;
    }

    @JSONField(name = "email-smtp-username")
    @JsonProperty("email-smtp-username")
    public String getSmtpUsername() {
        return smtpUsername;
    }

    @JSONField(name = "email-smtp-username")
    @JsonProperty("email-smtp-username")
    public void setSmtpUsername(String smtpUsername) {
        this.smtpUsername = smtpUsername;
    }

    @JSONField(name = "email-smtp-password")
    @JsonProperty("email-smtp-password")
    public String getSmtpPassword() {
        return smtpPassword;
    }

    @JSONField(name = "email-smtp-password")
    @JsonProperty("email-smtp-password")
    public void setSmtpPassword(String smtpPassword) {
        this.smtpPassword = smtpPassword;
    }

    @JSONField(name = "email-from-address")
    @JsonProperty("email-from-address")
    public String getFromAddress() {
        return fromAddress;
    }

    @JSONField(name = "email-from-address")
    @JsonProperty("email-from-address")
    public void setFromAddress(String fromAddress) {
        this.fromAddress = fromAddress;
    }

    @JSONField(name = "email-configured?")
    @JsonProperty("email-configured?")
    public boolean isEmailConfigured() {
        return emailConfigured;
    }

    @JSONField(name = "email-configured?")
    @JsonProperty("email-configured?")
    public void setEmailConfigured(boolean emailConfigured) {
        this.emailConfigured = emailConfigured;
    }

    @JSONField(name = "has-sample-dataset?")
    @JsonProperty("has-sample-dataset?")
    public boolean isHasSampleDataset() {
        return hasSampleDataset;
    }

    @JSONField(name = "has-sample-dataset?")
    @JsonProperty("has-sample-dataset?")
    public void setHasSampleDataset(boolean hasSampleDataset) {
        this.hasSampleDataset = hasSampleDataset;
    }

    @JSONField(name = "ssl-certificate-public-key")
    @JsonProperty("ssl-certificate-public-key")
    public String getSslCertificatePublicKey() {
        return sslCertificatePublicKey;
    }

    @JSONField(name = "ssl-certificate-public-key")
    @JsonProperty("ssl-certificate-public-key")
    public void setSslCertificatePublicKey(String sslCertificatePublicKey) {
        this.sslCertificatePublicKey = sslCertificatePublicKey;
    }

    @JSONField(name = "breakout-bin-width")
    @JsonProperty("breakout-bin-width")
    public int getBreakoutBinWidth() {
        return breakoutBinWidth;
    }

    @JSONField(name = "breakout-bin-width")
    @JsonProperty("breakout-bin-width")
    public void setBreakoutBinWidth(int breakoutBinWidth) {
        this.breakoutBinWidth = breakoutBinWidth;
    }

    @JSONField(name = "embedding-secret-key")
    @JsonProperty("embedding-secret-key")
    public String getEmbeddingSecretKey() {
        return embeddingSecretKey;
    }

    @JSONField(name = "embedding-secret-key")
    @JsonProperty("embedding-secret-key")
    public void setEmbeddingSecretKey(String embeddingSecretKey) {
        this.embeddingSecretKey = embeddingSecretKey;
    }

    @JSONField(name = "ga-code")
    @JsonProperty("ga-code")
    public String getGaCode() {
        return gaCode;
    }

    @JSONField(name = "ga-code")
    @JsonProperty("ga-code")
    public void setGaCode(String gaCode) {
        this.gaCode = gaCode;
    }

    @JSONField(name = "enable-enhancements?")
    @JsonProperty("enable-enhancements?")
    public boolean isEnableEnhancements() {
        return enableEnhancements;
    }

    @JSONField(name = "enable-enhancements?")
    @JsonProperty("enable-enhancements?")
    public void setEnableEnhancements(boolean enableEnhancements) {
        this.enableEnhancements = enableEnhancements;
    }

    @JSONField(name = "site-locale")
    @JsonProperty("site-locale")
    public String getSiteLocale() {
        return siteLocale;
    }

    @JSONField(name = "site-locale")
    @JsonProperty("site-locale")
    public void setSiteLocale(String siteLocale) {
        this.siteLocale = siteLocale;
    }

    @JSONField(name = "report-timezone-short")
    @JsonProperty("report-timezone-short")
    public String getReportTimezoneShort() {
        return reportTimezoneShort;
    }

    @JSONField(name = "report-timezone-short")
    @JsonProperty("report-timezone-short")
    public void setReportTimezoneShort(String reportTimezoneShort) {
        this.reportTimezoneShort = reportTimezoneShort;
    }

    @JSONField(name = "application-name")
    @JsonProperty("application-name")
    public String getApplicationName() {
        return applicationName;
    }

    @JSONField(name = "application-name")
    @JsonProperty("application-name")
    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    @JSONField(name = "enable-xrays")
    @JsonProperty("enable-xrays")
    public boolean isEnableXrays() {
        return enableXrays;
    }

    @JSONField(name = "enable-xrays")
    @JsonProperty("enable-xrays")
    public void setEnableXrays(boolean enableXrays) {
        this.enableXrays = enableXrays;
    }

    @JSONField(name = "query-caching-ttl-ratio")
    @JsonProperty("query-caching-ttl-ratio")
    public int getQueryCachingTtlRatio() {
        return queryCachingTtlRatio;
    }

    @JSONField(name = "query-caching-ttl-ratio")
    @JsonProperty("query-caching-ttl-ratio")
    public void setQueryCachingTtlRatio(int queryCachingTtlRatio) {
        this.queryCachingTtlRatio = queryCachingTtlRatio;
    }

    @JSONField(name = "embedding-app-origin")
    @JsonProperty("embedding-app-origin")
    public String getEmbeddingAppOrigin() {
        return embeddingAppOrigin;
    }

    @JSONField(name = "embedding-app-origin")
    @JsonProperty("embedding-app-origin")
    public void setEmbeddingAppOrigin(String embeddingAppOrigin) {
        this.embeddingAppOrigin = embeddingAppOrigin;
    }

    @JSONField(name = "ldap-sync-user-attributes-blacklist")
    @JsonProperty("ldap-sync-user-attributes-blacklist")
    public List<String> getLdapSyncUserAttributesVlacklist() {
        return ldapSyncUserAttributesVlacklist;
    }

    @JSONField(name = "ldap-sync-user-attributes-blacklist")
    @JsonProperty("ldap-sync-user-attributes-blacklist")
    public void setLdapSyncUserAttributesVlacklist(List<String> ldapSyncUserAttributesVlacklist) {
        this.ldapSyncUserAttributesVlacklist = ldapSyncUserAttributesVlacklist;
    }

    @JSONField(name = "ldap-attribute-email")
    @JsonProperty("ldap-attribute-email")
    public String getLdapAttributeEmail() {
        return ldapAttributeEmail;
    }

    @JSONField(name = "ldap-attribute-email")
    @JsonProperty("ldap-attribute-email")
    public void setLdapAttributeEmail(String ldapAttributeEmail) {
        this.ldapAttributeEmail = ldapAttributeEmail;
    }

    @JSONField(name = "ldap-attribute-firstname")
    @JsonProperty("ldap-attribute-firstname")
    public String getLdapAttributeFirstName() {
        return ldapAttributeFirstName;
    }

    @JSONField(name = "ldap-attribute-firstname")
    @JsonProperty("ldap-attribute-firstname")
    public void setLdapAttributeFirstName(String ldapAttributeFirstName) {
        this.ldapAttributeFirstName = ldapAttributeFirstName;
    }

    @JSONField(name = "ldap-attribute-lastname")
    @JsonProperty("ldap-attribute-lastname")
    public String getLdapAttributeLastName() {
        return ldapAttributeLastName;
    }

    @JSONField(name = "ldap-attribute-lastname")
    @JsonProperty("ldap-attribute-lastname")
    public void setLdapAttributeLastName(String ldapAttributeLastName) {
        this.ldapAttributeLastName = ldapAttributeLastName;
    }

    @JSONField(name = "ldap-bind-dn")
    @JsonProperty("ldap-bind-dn")
    public String getLdapBindDn() {
        return ldapBindDn;
    }

    @JSONField(name = "ldap-bind-dn")
    @JsonProperty("ldap-bind-dn")
    public void setLdapBindDn(String ldapBindDn) {
        this.ldapBindDn = ldapBindDn;
    }

    @JSONField(name = "ldap-enabled")
    @JsonProperty("ldap-enabled")
    public Boolean getLdapEnabled() {
        return ldapEnabled;
    }

    @JSONField(name = "ldap-enabled")
    @JsonProperty("ldap-enabled")
    public void setLdapEnabled(Boolean ldapEnabled) {
        this.ldapEnabled = ldapEnabled;
    }

    @JSONField(name = "ldap-group-base")
    @JsonProperty("ldap-group-base")
    public String getLdapGroupBase() {
        return ldapGroupBase;
    }

    @JSONField(name = "ldap-group-base")
    @JsonProperty("ldap-group-base")
    public void setLdapGroupBase(String ldapGroupBase) {
        this.ldapGroupBase = ldapGroupBase;
    }

    @JSONField(name = "ldap-group-mappings")
    @JsonProperty("ldap-group-mappings")
    public Object getLdapGroupMappings() {
        return ldapGroupMappings;
    }

    @JSONField(name = "ldap-group-mappings")
    @JsonProperty("ldap-group-mappings")
    public void setLdapGroupMappings(Object ldapGroupMappings) {
        this.ldapGroupMappings = ldapGroupMappings;
    }

    @JSONField(name = "ldap-group-sync")
    @JsonProperty("ldap-group-sync")
    public Boolean getLdapGroupSync() {
        return ldapGroupSync;
    }

    @JSONField(name = "ldap-group-sync")
    @JsonProperty("ldap-group-sync")
    public void setLdapGroupSync(Boolean ldapGroupSync) {
        this.ldapGroupSync = ldapGroupSync;
    }

    @JSONField(name = "ldap-host")
    @JsonProperty("ldap-host")
    public String getLdapHost() {
        return ldapHost;
    }

    @JSONField(name = "ldap-host")
    @JsonProperty("ldap-host")
    public void setLdapHost(String ldapHost) {
        this.ldapHost = ldapHost;
    }

    @JSONField(name = "ldap-password")
    @JsonProperty("ldap-password")
    public String getLdapPassword() {
        return ldapPassword;
    }

    @JSONField(name = "ldap-password")
    @JsonProperty("ldap-password")
    public void setLdapPassword(String ldapPassword) {
        this.ldapPassword = ldapPassword;
    }

    @JSONField(name = "ldap-port")
    @JsonProperty("ldap-port")
    public Integer getLdapPort() {
        return ldapPort;
    }

    @JSONField(name = "ldap-port")
    @JsonProperty("ldap-port")
    public void setLdapPort(Integer ldapPort) {
        this.ldapPort = ldapPort;
    }

    @JSONField(name = "ldap-security")
    @JsonProperty("ldap-security")
    public String getLdapSecurity() {
        return ldapSecurity;
    }

    @JSONField(name = "ldap-security")
    @JsonProperty("ldap-security")
    public void setLdapSecurity(String ldapSecurity) {
        this.ldapSecurity = ldapSecurity;
    }

    @JSONField(name = "ldap-user-base")
    @JsonProperty("ldap-user-base")
    public List<String> getLdapUserBase() {
        return ldapUserBase;
    }

    @JSONField(name = "ldap-user-base")
    @JsonProperty("ldap-user-base")
    public void setLdapUserBase(List<String> ldapUserBase) {
        this.ldapUserBase = ldapUserBase;
    }

    @JSONField(name = "ldap-user-filter")
    @JsonProperty("ldap-user-filter")
    public String getLdapUserFilter() {
        return ldapUserFilter;
    }

    @JSONField(name = "ldap-user-filter")
    @JsonProperty("ldap-user-filter")
    public void setLdapUserFilter(String ldapUserFilter) {
        this.ldapUserFilter = ldapUserFilter;
    }

    @JSONField(name = "admin-email")
    @JsonProperty("admin-email")
    public String getAdminEmail() {
        return adminEmail;
    }

    @JSONField(name = "admin-email")
    @JsonProperty("admin-email")
    public void setAdminEmail(String adminEmail) {
        this.adminEmail = adminEmail;
    }

    @JSONField(name = "slack-token")
    @JsonProperty("slack-token")
    public String getSlackToken() {
        return slackToken;
    }

    @JSONField(name = "slack-token")
    @JsonProperty("slack-token")
    public void setSlackToken(String slackToken) {
        this.slackToken = slackToken;
    }

    @JSONField(name = "check-for-updates")
    @JsonProperty("check-for-updates")
    public boolean isCheckForUpdates() {
        return checkForUpdates;
    }

    @JSONField(name = "check-for-updates")
    @JsonProperty("check-for-updates")
    public void setCheckForUpdates(boolean checkForUpdates) {
        this.checkForUpdates = checkForUpdates;
    }

    @JSONField(name = "source-address-header")
    @JsonProperty("source-address-header")
    public String getSourceAddressHeader() {
        return sourceAddressHeader;
    }

    @JSONField(name = "source-address-header")
    @JsonProperty("source-address-header")
    public void setSourceAddressHeader(String sourceAddressHeader) {
        this.sourceAddressHeader = sourceAddressHeader;
    }

    @JSONField(name = "enable-nested-queries")
    @JsonProperty("enable-nested-queries")
    public boolean isEnableNestedQueries() {
        return enableNestedQueries;
    }

    @JSONField(name = "enable-nested-queries")
    @JsonProperty("enable-nested-queries")
    public void setEnableNestedQueries(boolean enableNestedQueries) {
        this.enableNestedQueries = enableNestedQueries;
    }

    @JSONField(name = "site-url")
    @JsonProperty("site-url")
    public String getSiteUrl() {
        return siteUrl;
    }

    @JSONField(name = "site-url")
    @JsonProperty("site-url")
    public void setSiteUrl(String siteUrl) {
        this.siteUrl = siteUrl;
    }

    @JSONField(name = "enable-password-login")
    @JsonProperty("enable-password-login")
    public boolean isEnablePasswordLogin() {
        return enablePasswordLogin;
    }

    @JSONField(name = "enable-password-login")
    @JsonProperty("enable-password-login")
    public void setEnablePasswordLogin(boolean enablePasswordLogin) {
        this.enablePasswordLogin = enablePasswordLogin;
    }

    @JSONField(name = "start-of-week")
    @JsonProperty("start-of-week")
    public String getStartOfWeek() {
        return startOfWeek;
    }

    @JSONField(name = "start-of-week")
    @JsonProperty("start-of-week")
    public void setStartOfWeek(String startOfWeek) {
        this.startOfWeek = startOfWeek;
    }

    @JSONField(name = "custom-geojson")
    @JsonProperty("custom-geojson")
    public Map<String, GeoInfo> getCustomGeojson() {
        return customGeojson;
    }

    @JSONField(name = "custom-geojson")
    @JsonProperty("custom-geojson")
    public void setCustomGeojson(Map<String, GeoInfo> customGeojson) {
        this.customGeojson = customGeojson;
    }

    @JSONField(name = "enable-public-sharing")
    @JsonProperty("enable-public-sharing")
    public boolean isEnablePublicSharing() {
        return enablePublicSharing;
    }

    @JSONField(name = "enable-public-sharing")
    @JsonProperty("enable-public-sharing")
    public void setEnablePublicSharing(boolean enablePublicSharing) {
        this.enablePublicSharing = enablePublicSharing;
    }

    @JSONField(name = "available-timezones")
    @JsonProperty("available-timezones")
    public Set<String> getAvailableTimezones() {
        return availableTimezones;
    }

    @JSONField(name = "available-timezones")
    @JsonProperty("available-timezones")
    public void setAvailableTimezones(Set<String> availableTimezones) {
        this.availableTimezones = availableTimezones;
    }

    @JSONField(name = "hide-embed-branding?")
    @JsonProperty("hide-embed-branding?")
    public boolean isHideEmbedBranding() {
        return hideEmbedBranding;
    }

    @JSONField(name = "hide-embed-branding?")
    @JsonProperty("hide-embed-branding?")
    public void setHideEmbedBranding(boolean hideEmbedBranding) {
        this.hideEmbedBranding = hideEmbedBranding;
    }

    @JSONField(name = "enable-sandboxes?")
    @JsonProperty("enable-sandboxes?")
    public boolean isEnableSandboxes() {
        return enableSandboxes;
    }

    @JSONField(name = "enable-sandboxes?")
    @JsonProperty("enable-sandboxes?")
    public void setEnableSandboxes(boolean enableSandboxes) {
        this.enableSandboxes = enableSandboxes;
    }

    @JSONField(name = "available-locales")
    @JsonProperty("available-locales")
    public List<List<String>> getAvailableLocales() {
        return availableLocales;
    }

    @JSONField(name = "available-locales")
    @JsonProperty("available-locales")
    public void setAvailableLocales(List<List<String>> availableLocales) {
        this.availableLocales = availableLocales;
    }

    @JSONField(name = "landing-page")
    @JsonProperty("landing-page")
    public String getLandingPage() {
        return landingPage;
    }

    @JSONField(name = "landing-page")
    @JsonProperty("landing-page")
    public void setLandingPage(String landingPage) {
        this.landingPage = landingPage;
    }

    @JSONField(name = "setup-token")
    @JsonProperty("setup-token")
    public String getSetupToken() {
        return setupToken;
    }

    @JSONField(name = "setup-token")
    @JsonProperty("setup-token")
    public void setSetupToken(String setupToken) {
        this.setupToken = setupToken;
    }

    @JSONField(name = "enable-embedding")
    @JsonProperty("enable-embedding")
    public boolean isEnableEmbedding() {
        return enableEmbedding;
    }

    @JSONField(name = "enable-embedding")
    @JsonProperty("enable-embedding")
    public void setEnableEmbedding(boolean enableEmbedding) {
        this.enableEmbedding = enableEmbedding;
    }

    @JSONField(name = "application-colors")
    @JsonProperty("application-colors")
    public String getApplicationColors() {
        return applicationColors;
    }

    @JSONField(name = "application-colors")
    @JsonProperty("application-colors")
    public void setApplicationColors(String applicationColors) {
        this.applicationColors = applicationColors;
    }

    @JSONField(name = "enable-audit-app?")
    @JsonProperty("enable-audit-app?")
    public boolean isEnableAuditApp() {
        return enableAuditApp;
    }

    @JSONField(name = "enable-audit-app?")
    @JsonProperty("enable-audit-app?")
    public void setEnableAuditApp(boolean enableAuditApp) {
        this.enableAuditApp = enableAuditApp;
    }

    @JSONField(name = "version-info")
    @JsonProperty("version-info")
    public VersionInfo getVersionInfo() {
        return versionInfo;
    }

    @JSONField(name = "version-info")
    @JsonProperty("version-info")
    public void setVersionInfo(VersionInfo versionInfo) {
        this.versionInfo = versionInfo;
    }

    @JSONField(name = "anon-tracking-enabled")
    @JsonProperty("anon-tracking-enabled")
    public boolean isAnonTrackingEnabled() {
        return anonTrackingEnabled;
    }

    @JSONField(name = "anon-tracking-enabled")
    @JsonProperty("anon-tracking-enabled")
    public void setAnonTrackingEnabled(boolean anonTrackingEnabled) {
        this.anonTrackingEnabled = anonTrackingEnabled;
    }

    @JSONField(name = "application-logo-url")
    @JsonProperty("application-logo-url")
    public String getApplicationLogoUrl() {
        return applicationLogoUrl;
    }

    @JSONField(name = "application-logo-url")
    @JsonProperty("application-logo-url")
    public void setApplicationLogoUrl(String applicationLogoUrl) {
        this.applicationLogoUrl = applicationLogoUrl;
    }

    @JSONField(name = "application-favicon-url")
    @JsonProperty("application-favicon-url")
    public String getApplicationFaviconUrl() {
        return applicationFaviconUrl;
    }

    @JSONField(name = "application-favicon-url")
    @JsonProperty("application-favicon-url")
    public void setApplicationFaviconUrl(String applicationFaviconUrl) {
        this.applicationFaviconUrl = applicationFaviconUrl;
    }

    @JSONField(name = "show-homepage-xrays")
    @JsonProperty("show-homepage-xrays")
    public boolean isShowHomepageXrays() {
        return showHomepageXrays;
    }

    @JSONField(name = "show-homepage-xrays")
    @JsonProperty("show-homepage-xrays")
    public void setShowHomepageXrays(boolean showHomepageXrays) {
        this.showHomepageXrays = showHomepageXrays;
    }

    @JSONField(name = "enable-whitelabeling?")
    @JsonProperty("enable-whitelabeling?")
    public boolean isEnableWhitelabeling() {
        return enableWhitelabeling;
    }

    @JSONField(name = "enable-whitelabeling?")
    @JsonProperty("enable-whitelabeling?")
    public void setEnableWhitelabeling(boolean enableWhitelabeling) {
        this.enableWhitelabeling = enableWhitelabeling;
    }

    @JSONField(name = "show-homepage-data")
    @JsonProperty("show-homepage-data")
    public boolean isShowHomepageData() {
        return showHomepageData;
    }

    @JSONField(name = "show-homepage-data")
    @JsonProperty("show-homepage-data")
    public void setShowHomepageData(boolean showHomepageData) {
        this.showHomepageData = showHomepageData;
    }

    @JSONField(name = "map-tile-server-url")
    @JsonProperty("map-tile-server-url")
    public String getMapTileServerUrl() {
        return mapTileServerUrl;
    }

    @JSONField(name = "map-tile-server-url")
    @JsonProperty("map-tile-server-url")
    public void setMapTileServerUrl(String mapTileServerUrl) {
        this.mapTileServerUrl = mapTileServerUrl;
    }

    @JSONField(name = "site-name")
    @JsonProperty("site-name")
    public String getSiteName() {
        return siteName;
    }

    @JSONField(name = "site-name")
    @JsonProperty("site-name")
    public void setSiteName(String siteName) {
        this.siteName = siteName;
    }

    @JSONField(name = "enable-query-caching")
    @JsonProperty("enable-query-caching")
    public boolean isEnableQueryCaching() {
        return enableQueryCaching;
    }

    @JSONField(name = "enable-query-caching")
    @JsonProperty("enable-query-caching")
    public void setEnableQueryCaching(boolean enableQueryCaching) {
        this.enableQueryCaching = enableQueryCaching;
    }

    @JSONField(name = "redirect-all-requests-to-https")
    @JsonProperty("redirect-all-requests-to-https")
    public boolean isRedirectAllRequestsToHttps() {
        return redirectAllRequestsToHttps;
    }

    @JSONField(name = "redirect-all-requests-to-https")
    @JsonProperty("redirect-all-requests-to-https")
    public void setRedirectAllRequestsToHttps(boolean redirectAllRequestsToHttps) {
        this.redirectAllRequestsToHttps = redirectAllRequestsToHttps;
    }

    @JSONField(name = "password-complexity")
    @JsonProperty("password-complexity")
    public PasswordComplexity getPasswordComplexity() {
        return passwordComplexity;
    }

    @JSONField(name = "password-complexity")
    @JsonProperty("password-complexity")
    public void setPasswordComplexity(PasswordComplexity passwordComplexity) {
        this.passwordComplexity = passwordComplexity;
    }

    @JSONField(name = "report-timezone")
    @JsonProperty("report-timezone")
    public String getReportTimezone() {
        return reportTimezone;
    }

    @JSONField(name = "report-timezone")
    @JsonProperty("report-timezone")
    public void setReportTimezone(String reportTimezone) {
        this.reportTimezone = reportTimezone;
    }

    @JSONField(name = "query-caching-min-ttl")
    @JsonProperty("query-caching-min-ttl")
    public int getQueryCachingMinTtl() {
        return queryCachingMinTtl;
    }

    @JSONField(name = "query-caching-min-ttl")
    @JsonProperty("query-caching-min-ttl")
    public void setQueryCachingMinTtl(int queryCachingMinTtl) {
        this.queryCachingMinTtl = queryCachingMinTtl;
    }

    @JSONField(name = "query-caching-max-ttl")
    @JsonProperty("query-caching-max-ttl")
    public long getQueryCachingMaxTtl() {
        return queryCachingMaxTtl;
    }

    @JSONField(name = "query-caching-max-ttl")
    @JsonProperty("query-caching-max-ttl")
    public void setQueryCachingMaxTtl(long queryCachingMaxTtl) {
        this.queryCachingMaxTtl = queryCachingMaxTtl;
    }

    @JSONField(name = "metabot-enabled")
    @JsonProperty("metabot-enabled")
    public boolean isMetabotEnabled() {
        return metabotEnabled;
    }

    @JSONField(name = "metabot-enabled")
    @JsonProperty("metabot-enabled")
    public void setMetabotEnabled(boolean metabotEnabled) {
        this.metabotEnabled = metabotEnabled;
    }

    @JSONField(name = "premium-embedding-token")
    @JsonProperty("premium-embedding-token")
    public String getPremiumEmbeddingToken() {
        return premiumEmbeddingToken;
    }

    @JSONField(name = "premium-embedding-token")
    @JsonProperty("premium-embedding-token")
    public void setPremiumEmbeddingToken(String premiumEmbeddingToken) {
        this.premiumEmbeddingToken = premiumEmbeddingToken;
    }

    @JSONField(name = "breakout-bins-num")
    @JsonProperty("breakout-bins-num")
    public int getBreakoutBinsNum() {
        return breakoutBinsNum;
    }

    @JSONField(name = "breakout-bins-num")
    @JsonProperty("breakout-bins-num")
    public void setBreakoutBinsNum(int breakoutBinsNum) {
        this.breakoutBinsNum = breakoutBinsNum;
    }

    @JSONField(name = "enable-sso?")
    @JsonProperty("enable-sso?")
    public boolean isEnableSso() {
        return enableSso;
    }

    @JSONField(name = "enable-sso?")
    @JsonProperty("enable-sso?")
    public void setEnableSso(boolean enableSso) {
        this.enableSso = enableSso;
    }

    @JSONField(name = "query-caching-max-kb")
    @JsonProperty("query-caching-max-kb")
    public int getQueryCachingMaxKb() {
        return queryCachingMaxKb;
    }

    @JSONField(name = "query-caching-max-kb")
    @JsonProperty("query-caching-max-kb")
    public void setQueryCachingMaxKb(int queryCachingMaxKb) {
        this.queryCachingMaxKb = queryCachingMaxKb;
    }

    @JSONField(name = "premium-features")
    @JsonProperty("premium-features")
    public PremiumFeatures getPremiumFeatures() {
        return premiumFeatures;
    }

    @JSONField(name = "premium-features")
    @JsonProperty("premium-features")
    public void setPremiumFeatures(PremiumFeatures premiumFeatures) {
        this.premiumFeatures = premiumFeatures;
    }

    @JSONField(name = "custom-formatting")
    @JsonProperty("custom-formatting")
    public Object getCustomFormatting() {
        return customFormatting;
    }

    @JSONField(name = "custom-formatting")
    @JsonProperty("custom-formatting")
    public void setCustomFormatting(Object customFormatting) {
        this.customFormatting = customFormatting;
    }

    @JSONField(name = "ldap-configured?")
    @JsonProperty("ldap-configured?")
    public boolean isLdapConfigured() {
        return ldapConfigured;
    }

    @JSONField(name = "ldap-configured?")
    @JsonProperty("ldap-configured?")
    public void setLdapConfigured(boolean ldapConfigured) {
        this.ldapConfigured = ldapConfigured;
    }

    @JSONField(name = "auth_type")
    @JsonProperty("auth_type")
    public String getAuthType() {
        return authType;
    }

    @JSONField(name = "auth_type")
    @JsonProperty("auth_type")
    public void setAuthType(String authType) {
        this.authType = authType;
    }
}
