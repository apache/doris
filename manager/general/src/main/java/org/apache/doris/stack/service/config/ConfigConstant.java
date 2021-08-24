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

package org.apache.doris.stack.service.config;

import com.alibaba.fastjson.JSON;
import org.apache.doris.stack.model.response.config.VersionInfo;
import org.apache.doris.stack.constant.EnvironmentDefine;
import org.apache.doris.stack.constant.PropertyDefine;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description：
 * The configuration name in the service (these configurations refer to the configurations that the user will
 * write to the database if configured). Some configurations are common to all spaces and are stored
 * in the setting table,
 *
 * Some configurations are configured separately for each user space and stored in studio_setting table,
 * associate the corresponding user space ID
 */
public class ConfigConstant {

    private ConfigConstant() {
        throw new UnsupportedOperationException();
    }

    // Public configuration item name
    // Initialize the configuration item, which can be written through the environment variable when
    // the system is started
    public static final String ADMIN_EMAIL_KEY = "admin-email";

    public static final String ANON_TRACKING_ENABLED_KEY = "anon-tracking-enabled";

    public static final String REDIRECT_HTTPS_KEY = "redirect-all-requests-to-https";

    public static final String SITE_LOCALE_KEY = "site-locale";

    public static final String SITE_NAME_KEY = "site-name";

    public static final String SITE_URL_KEY = "site-url";

    public static final String VERSION_INFO_KEY = "version-info";

    public static final String DEFAULT_GROUP_KEY = "default-group-id";

    public static final String ENABLE_PUBLIC_KEY = "enable-public-sharing";

    public static final String CUSTOM_FORMATTING_KEY = "custom-formatting";

    // Open the sample data provided by Stack
    public static final String SAMPLE_DATA_ENABLE_KEY = "sample-data-enable";

    // Engine data type
    public static final String DATABASE_TYPE_KEY = "database-type";

    public static final Map<String, ConfigItem> PUBLIC_CONFIGS;

    static {
        PUBLIC_CONFIGS = new HashMap<>();
        String mailDesc = "如果碰到问题，可以给服务运维邮箱发送邮件.";
        PUBLIC_CONFIGS.put(ADMIN_EMAIL_KEY, new ConfigItem(ADMIN_EMAIL_KEY, ConfigItem.Type.STRING,
                mailDesc, false, ConfigItem.Visibility.PUBLIC, EnvironmentDefine.ADMIN_EMAIL_ENV));

        String trackingDesc = "启用匿名使用数据的收集，以帮助改进Doris Studio.";
        PUBLIC_CONFIGS.put(ANON_TRACKING_ENABLED_KEY, new ConfigItem(ANON_TRACKING_ENABLED_KEY, ConfigItem.Type.BOOLEAN,
                trackingDesc, false, ConfigItem.Visibility.PUBLIC, EnvironmentDefine.ANON_TRACKING_ENABLED_ENV,
                "false"));

        String httpsDesc = "如果站点URL为HTTPS，则强制所有流量通过重定向使用HTTPS.";
        PUBLIC_CONFIGS.put(REDIRECT_HTTPS_KEY, new ConfigItem(REDIRECT_HTTPS_KEY, ConfigItem.Type.BOOLEAN,
                httpsDesc, false, ConfigItem.Visibility.PUBLIC, EnvironmentDefine.REDIRECT_HTTPS_ENV,
                "false"));

        String localeDesc = "Doris Stack服务的默认语言.";
        PUBLIC_CONFIGS.put(SITE_LOCALE_KEY, new ConfigItem(SITE_LOCALE_KEY, ConfigItem.Type.STRING,
                localeDesc, false, ConfigItem.Visibility.PUBLIC, EnvironmentDefine.SITE_LOCALE_ENV,
                "zh"));

        String siteNameDesc = "Doris Stack服务实例的名称.";
        PUBLIC_CONFIGS.put(SITE_NAME_KEY, new ConfigItem(SITE_NAME_KEY, ConfigItem.Type.STRING,
                siteNameDesc, false, ConfigItem.Visibility.PUBLIC, EnvironmentDefine.SITE_NAME_ENV));

        String siteUrlDesc = "Doris Stack服务实例的访问地址.";
        PUBLIC_CONFIGS.put(SITE_URL_KEY, new ConfigItem(SITE_URL_KEY, ConfigItem.Type.STRING,
                siteUrlDesc, false, ConfigItem.Visibility.PUBLIC, EnvironmentDefine.SITE_URL_ENV));

        String versionDesc = "Doris Stack服务的版本信息.";
        VersionInfo versionInfo = new VersionInfo("v1.0.0", "2021-08-20", "for test");
        PUBLIC_CONFIGS.put(VERSION_INFO_KEY, new ConfigItem(VERSION_INFO_KEY, ConfigItem.Type.JSON,
                versionDesc, false, ConfigItem.Visibility.PUBLIC, EnvironmentDefine.VERSION_INFO_ENV,
                JSON.toJSONString(versionInfo)));

        String defaultGroupDesc = "Doris Stack 默认用户池分组";
        PUBLIC_CONFIGS.put(DEFAULT_GROUP_KEY, new ConfigItem(DEFAULT_GROUP_KEY, ConfigItem.Type.INTEGER,
                defaultGroupDesc, false, ConfigItem.Visibility.PUBLIC, EnvironmentDefine.DEFAULT_GROUP_ID_ENV));

        String enablePublicDesc = "是否启用管理员来为查询和仪表盘创建公开的可浏览链接（并且可嵌入的iframes框架）.";
        PUBLIC_CONFIGS.put(ENABLE_PUBLIC_KEY, new ConfigItem(ENABLE_PUBLIC_KEY, ConfigItem.Type.BOOLEAN,
                enablePublicDesc, false, ConfigItem.Visibility.PUBLIC, EnvironmentDefine.ENABLE_PUBLIC_KEY_ENV,
                "false"));

        String formatingDesc = "按类型键入的对象包含格式设置.";
        PUBLIC_CONFIGS.put(CUSTOM_FORMATTING_KEY, new ConfigItem(CUSTOM_FORMATTING_KEY, ConfigItem.Type.STRING,
                formatingDesc, false, ConfigItem.Visibility.PUBLIC, EnvironmentDefine.CUSTOM_FORMATTING_KEY_ENV,
                ConfigValueDef.TYPE_TEMPORAL));

        String sampleDataEnableDesc = "是否自带样例数据.";
        PUBLIC_CONFIGS.put(SAMPLE_DATA_ENABLE_KEY, new ConfigItem(SAMPLE_DATA_ENABLE_KEY, ConfigItem.Type.BOOLEAN,
                sampleDataEnableDesc, false, ConfigItem.Visibility.PUBLIC,
                EnvironmentDefine.SAMPLE_DATA_ENABLE_KEY_ENV, "true"));

        String databaseTypeDesc = "服务存储引擎类型.";
        PUBLIC_CONFIGS.put(DATABASE_TYPE_KEY, new ConfigItem(DATABASE_TYPE_KEY, ConfigItem.Type.STRING,
                databaseTypeDesc, false, ConfigItem.Visibility.PUBLIC,
                EnvironmentDefine.DB_TYPE_ENV, PropertyDefine.JPA_DATABASE_MYSQL));
    }

    // Public configuration item
    // Mailbox configuration, which can not be written through environment variables, can only be configured by users
    public static final String EMAIL_CONFIGURED_KEY = "email-configured";

    public static final String EMAIL_ADDRESS_KEY = "email-from-address";

    public static final String EMAIL_PROTOCOL_KEY = "SMTP";

    public static final String EMAIL_HOST_KEY = "email-smtp-host";

    public static final String EMAIL_PORT_KEY = "email-smtp-port";

    public static final String EMAIL_SECURITY_KEY = "email-smtp-security";

    public static final String EMAIL_USER_NAME_KEY = "email-smtp-username";

    public static final String EMAIL_PASSWORD_KEY = "email-smtp-password";

    // General information for storing mailbox configuration items
    public static final Map<String, ConfigItem> EMAIL_CONFIGS;

    static {
        EMAIL_CONFIGS = new HashMap<>();
        String configuredDesc = "检查是否已启用SMTP电子邮件服务.";
        EMAIL_CONFIGS.put(EMAIL_CONFIGURED_KEY, new ConfigItem(EMAIL_CONFIGURED_KEY, ConfigItem.Type.BOOLEAN,
                configuredDesc, false, ConfigItem.Visibility.ADMIN));

        String addressDesc = "Doris Studio SMTP邮箱服务的邮件地址.";
        EMAIL_CONFIGS.put(EMAIL_ADDRESS_KEY, new ConfigItem(EMAIL_ADDRESS_KEY, ConfigItem.Type.STRING,
                addressDesc, false, ConfigItem.Visibility.ADMIN));

        String hostDesc = "Doris Studio SMTP服务器地址.";
        EMAIL_CONFIGS.put(EMAIL_HOST_KEY, new ConfigItem(EMAIL_HOST_KEY, ConfigItem.Type.STRING,
                hostDesc, false, ConfigItem.Visibility.ADMIN));

        String portDesc = "SMTP发送邮件使用的端口.";
        EMAIL_CONFIGS.put(EMAIL_PORT_KEY, new ConfigItem(EMAIL_PORT_KEY, ConfigItem.Type.INTEGER,
                portDesc, false, ConfigItem.Visibility.ADMIN));

        String secuDesc = "SMTP安全连接协议（tls，sll，starttls，或者没有）.";
        EMAIL_CONFIGS.put(EMAIL_SECURITY_KEY, new ConfigItem(EMAIL_SECURITY_KEY, ConfigItem.Type.STRING,
                secuDesc, false, ConfigItem.Visibility.ADMIN));

        String userDesc = "SMTP用户名.";
        EMAIL_CONFIGS.put(EMAIL_USER_NAME_KEY, new ConfigItem(EMAIL_USER_NAME_KEY, ConfigItem.Type.STRING,
                userDesc, false, ConfigItem.Visibility.ADMIN));

        String passwdDesc = "SMTP密码.";
        EMAIL_CONFIGS.put(EMAIL_PASSWORD_KEY, new ConfigItem(EMAIL_PASSWORD_KEY, ConfigItem.Type.STRING,
                passwdDesc, true, ConfigItem.Visibility.AUTHENTICATED));
    }

    // Public configuration item

    // Idaas configuration
    public static final String IDAAS_PROJECT_URL_KEY = "idaas-project-url";

    public static final String IDAAS_PROJECT_ID_KEY = "idaas-project-id";

    public static final String IDAAS_APP_ID_KEY = "idaas-app-id";

    public static final String IDAAS_CLIENT_ID_KEY = "idaas-client-id";

    public static final String IDAAS_CLIENT_SECRET_KEY = "idaas-client-secret";

    public static final Map<String, ConfigItem> IDAAS_CONFIGS;

    static {
        IDAAS_CONFIGS = new HashMap<>();

        // Now we judge by authentication, because we only support single authentication

        String urlDesc = "IDAAS 服务地址";
        IDAAS_CONFIGS.put(IDAAS_PROJECT_URL_KEY, new ConfigItem(IDAAS_PROJECT_URL_KEY, ConfigItem.Type.STRING,
                urlDesc, false, ConfigItem.Visibility.ADMIN));

        String projectIdDesc = "IDAAS 项目id";
        IDAAS_CONFIGS.put(IDAAS_PROJECT_ID_KEY, new ConfigItem(IDAAS_PROJECT_ID_KEY, ConfigItem.Type.STRING,
                projectIdDesc, false, ConfigItem.Visibility.ADMIN));

        String appIdDesc = "IDAAS 应用id";
        IDAAS_CONFIGS.put(IDAAS_APP_ID_KEY, new ConfigItem(IDAAS_APP_ID_KEY, ConfigItem.Type.STRING,
                appIdDesc, false, ConfigItem.Visibility.ADMIN));

        String clientIdDesc = "IDAAS 客户端id";
        IDAAS_CONFIGS.put(IDAAS_CLIENT_ID_KEY, new ConfigItem(IDAAS_CLIENT_ID_KEY, ConfigItem.Type.STRING,
                clientIdDesc, false, ConfigItem.Visibility.ADMIN));

        String clientSecretDesc = "IDAAS 客户端密钥";
        IDAAS_CONFIGS.put(IDAAS_CLIENT_SECRET_KEY, new ConfigItem(IDAAS_CLIENT_SECRET_KEY, ConfigItem.Type.STRING,
                clientSecretDesc, false, ConfigItem.Visibility.ADMIN));

    }

    // LDAP configuration, which cannot be written through environment variables, can only be configured by users
    public static final String LDAP_HOST_KEY = "ldap-host";

    public static final String LDAP_PORT_KEY = "ldap-port";

    public static final String LDAP_SECURITY_KEY = "ldap-security";

    public static final String LDAP_BIND_DN_KEY = "ldap-bind-dn";

    public static final String LDAP_PASSWORD_KEY = "ldap-password";

    public static final String LDAP_USER_BASE_KEY = "ldap-user-base";

    public static final String LDAP_USER_FILTER_KEY = "ldap-user-filter";

    public static final String LDAP_ATTRIBUTE_EMAIL_KEY = "ldap-attribute-email";

    public static final String LDAP_ATTRIBUTE_FIRSTNAME_KEY = "ldap-attribute-firstname";

    public static final String LDAP_ATTRIBUTE_LASTNAME_KEY = "ldap-attribute-lastname";

    public static final Map<String, ConfigItem> LDAP_CONFIGS;

    static {
        LDAP_CONFIGS = new HashMap<>();

        // Now we judge by authentication, because we only support single authentication

        String hostDesc = "LDAP 服务地址";
        LDAP_CONFIGS.put(LDAP_HOST_KEY, new ConfigItem(LDAP_HOST_KEY, ConfigItem.Type.STRING,
                hostDesc, false, ConfigItem.Visibility.ADMIN));

        String portDesc = "LDAP 服务端口";
        LDAP_CONFIGS.put(LDAP_PORT_KEY, new ConfigItem(LDAP_PORT_KEY, ConfigItem.Type.INTEGER,
                portDesc, false, ConfigItem.Visibility.ADMIN));

        String securityDesc = "LDAP 安全秘钥";
        LDAP_CONFIGS.put(LDAP_SECURITY_KEY, new ConfigItem(LDAP_SECURITY_KEY, ConfigItem.Type.STRING,
                securityDesc, false, ConfigItem.Visibility.ADMIN));

        String bindDNDesc = "LDAP Bind DN";
        LDAP_CONFIGS.put(LDAP_BIND_DN_KEY, new ConfigItem(LDAP_BIND_DN_KEY, ConfigItem.Type.STRING,
                bindDNDesc, false, ConfigItem.Visibility.ADMIN));

        String passwordDesc = "LDAP 服务密码";
        LDAP_CONFIGS.put(LDAP_PASSWORD_KEY, new ConfigItem(LDAP_PASSWORD_KEY, ConfigItem.Type.STRING,
                passwordDesc, false, ConfigItem.Visibility.ADMIN));

        String userBaseDesc = "LDAP中的搜索基础 (将采用递归检索)";
        LDAP_CONFIGS.put(LDAP_USER_BASE_KEY, new ConfigItem(LDAP_USER_BASE_KEY, ConfigItem.Type.STRING,
                userBaseDesc, false, ConfigItem.Visibility.ADMIN));

        String userFilterDesc = "LDAP中的用户查询筛选, 占位符{Login}将被用户提供的登录信息替换";
        LDAP_CONFIGS.put(LDAP_USER_FILTER_KEY, new ConfigItem(LDAP_USER_FILTER_KEY, ConfigItem.Type.STRING,
                userFilterDesc, false, ConfigItem.Visibility.ADMIN));

        String attributeEmailDesc = "LDAP 服务邮箱属性";
        LDAP_CONFIGS.put(LDAP_ATTRIBUTE_EMAIL_KEY, new ConfigItem(LDAP_ATTRIBUTE_EMAIL_KEY, ConfigItem.Type.STRING,
                attributeEmailDesc, false, ConfigItem.Visibility.ADMIN));

        String attributeFirstNameDesc = "LDAP 用户名属性";
        LDAP_CONFIGS.put(LDAP_ATTRIBUTE_FIRSTNAME_KEY, new ConfigItem(LDAP_ATTRIBUTE_FIRSTNAME_KEY, ConfigItem.Type.STRING,
                attributeFirstNameDesc, false, ConfigItem.Visibility.ADMIN));

        String attributeLastNameDesc = "LDAP 用户姓属性";
        LDAP_CONFIGS.put(LDAP_ATTRIBUTE_LASTNAME_KEY, new ConfigItem(LDAP_ATTRIBUTE_LASTNAME_KEY, ConfigItem.Type.STRING,
                attributeLastNameDesc, false, ConfigItem.Visibility.ADMIN));
    }

    // Public configuration item
    // Other configurations cannot be written through environment variables, but can only be configured by users
    public static final String AUTH_TYPE_KEY = "auth_type";

    public static final String DEPLOY_TYPE = "deploy-type";

    public static final Map<String, ConfigItem> CLUSTER_CONFIGS;

    static {
        CLUSTER_CONFIGS = new HashMap<>();

        String authTypeDesc = "Doris Studio服务认证方式";
        CLUSTER_CONFIGS.put(AUTH_TYPE_KEY, new ConfigItem(AUTH_TYPE_KEY, ConfigItem.Type.STRING,
                authTypeDesc, false, ConfigItem.Visibility.ADMIN));

        String deployNameDesc = "服务部署名称";
        CLUSTER_CONFIGS.put(DEPLOY_TYPE, new ConfigItem(DEPLOY_TYPE, ConfigItem.Type.STRING,
                deployNameDesc, false, ConfigItem.Visibility.ADMIN));
    }

    // All public configuration items
    public static final Map<String, ConfigItem> ALL_PUBLIC_CONFIGS;

    static {
        ALL_PUBLIC_CONFIGS = new HashMap<>();

        ALL_PUBLIC_CONFIGS.putAll(PUBLIC_CONFIGS);
        ALL_PUBLIC_CONFIGS.putAll(EMAIL_CONFIGS);
        ALL_PUBLIC_CONFIGS.putAll(LDAP_CONFIGS);
        ALL_PUBLIC_CONFIGS.putAll(IDAAS_CONFIGS);
        ALL_PUBLIC_CONFIGS.putAll(CLUSTER_CONFIGS);
    }

    // User space separate configuration
    // Query cache configuration item
    public static final String ENABLE_QUERY_CACHING = "enable-query-caching";

    public static final String QUERY_CACHING_TTL_TATIO = "query-caching-ttl-ratio";

    public static final String QUERY_CACHING_MIN_TTL = "query-caching-min-ttl";

    public static final String QUERY_CACHING_MAX_TTL = "query-caching-max-ttl";

    public static final String QUERY_CACHING_MAX_KB = "query-caching-max-kb";

    // Cache configuration item default information
    public static final Map<String, ConfigItem> CHCHE_CONFIGS;

    static {
        CHCHE_CONFIGS = new HashMap<>();

        String enableDesc = "是否开启缓存";
        CHCHE_CONFIGS.put(ENABLE_QUERY_CACHING, new ConfigItem(ENABLE_QUERY_CACHING, ConfigItem.Type.BOOLEAN,
                enableDesc, false, ConfigItem.Visibility.ADMIN, null, "false"));

        String queryCahceTtlTatioDesc = "缓存最大占比";
        CHCHE_CONFIGS.put(QUERY_CACHING_TTL_TATIO, new ConfigItem(QUERY_CACHING_TTL_TATIO, ConfigItem.Type.DOUBLE,
                queryCahceTtlTatioDesc, false, ConfigItem.Visibility.ADMIN, null, "10"));

        String queryCahceMinTtlDesc = "查询缓存最小执行时间（毫秒）";
        CHCHE_CONFIGS.put(QUERY_CACHING_MIN_TTL, new ConfigItem(QUERY_CACHING_MIN_TTL, ConfigItem.Type.INTEGER,
                queryCahceMinTtlDesc, false, ConfigItem.Visibility.ADMIN, null, "60"));

        String queryCahceMaxTtlDesc = "查询缓存最大执行时间（毫秒）";
        CHCHE_CONFIGS.put(QUERY_CACHING_MAX_TTL, new ConfigItem(QUERY_CACHING_MAX_TTL, ConfigItem.Type.INTEGER,
                queryCahceMaxTtlDesc, false, ConfigItem.Visibility.ADMIN, null, "8640000"));

        String queryCahceMaxKbDesc = "查询缓存最大容量（kb）";
        CHCHE_CONFIGS.put(QUERY_CACHING_MAX_KB, new ConfigItem(QUERY_CACHING_MAX_KB, ConfigItem.Type.INTEGER,
                queryCahceMaxKbDesc, false, ConfigItem.Visibility.ADMIN, null, "1000"));

    }

    // All user space configuration items
    public static final Map<String, ConfigItem> ALL_ADMIN_CONFIGS;

    static {
        ALL_ADMIN_CONFIGS = new HashMap<>();

        ALL_ADMIN_CONFIGS.putAll(CHCHE_CONFIGS);
    }

}
