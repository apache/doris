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

package org.apache.doris.kerberos;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class HadoopKerberosAuthenticator implements HadoopAuthenticator {
    private static final Logger LOG = LogManager.getLogger(HadoopKerberosAuthenticator.class);
    private final KerberosAuthenticationConfig config;
    private Subject subject;
    private long nextRefreshTime;
    private UserGroupInformation ugi;

    // Shared with Hadoop through the plugin parent loader; guarded by UserGroupInformation.class.
    // Hadoop can publish its auth mode before setConfiguration fails, so that mode alone cannot prove success.
    private static boolean authConfigurationFailed;

    // Hadoop's rule grammar keeps whitespace inside a regex or substitution distinct from rule separators.
    private static final Pattern AUTH_TO_LOCAL_RULE = Pattern.compile(
            "\\s*((DEFAULT)|(RULE:\\[(\\d*):([^\\]]*)](\\(([^)]*)\\))?"
                    + "(s/([^/]*)/([^/]*)/(g)?)?))/?(L)?");

    public HadoopKerberosAuthenticator(KerberosAuthenticationConfig config) {
        this.config = config;
    }

    public static void initializeAuthConfig(Configuration hadoopConf) {
        hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");
        String additionalRules = hadoopConf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL);
        String mechanism = hadoopConf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL_MECHANISM,
                KerberosName.DEFAULT_MECHANISM);
        // UGI changes its auth mode before parsing rules. Reject malformed input before it can leave
        // an enabled UGI with the permissive rules inherited from a previous SIMPLE initialization.
        Preconditions.checkArgument(KerberosName.MECHANISM_HADOOP.equalsIgnoreCase(mechanism)
                        || KerberosName.MECHANISM_MIT.equalsIgnoreCase(mechanism),
                "Invalid auth_to_local mechanism: %s", mechanism);
        Set<String> requestedRules = additionalRules == null
                ? Collections.emptySet() : parseAuthToLocalRules(additionalRules);
        // Connector and filesystem plugins share Hadoop and this helper through their parent loader.
        // Use Hadoop's monitor for both policy changes and the principal mapping that validates them.
        synchronized (UserGroupInformation.class) {
            // isSecurityEnabled() lazily initializes UGI and its Groups singleton with default settings.
            if (!UserGroupInformation.isInitialized() || authConfigurationFailed
                    || !UserGroupInformation.isSecurityEnabled()) {
                String previousRules = KerberosName.getRules();
                String previousMechanism = KerberosName.getRuleMechanism();
                try {
                    UserGroupInformation.setConfiguration(hadoopConf);
                    authConfigurationFailed = false;
                } catch (RuntimeException e) {
                    authConfigurationFailed = true;
                    KerberosName.setRules(previousRules);
                    KerberosName.setRuleMechanism(previousMechanism);
                    throw e;
                }
                return;
            }
            if (additionalRules == null) {
                return;
            }
            Preconditions.checkArgument(mechanism.equalsIgnoreCase(KerberosName.getRuleMechanism()),
                    "Kerberos catalogs sharing a Hadoop runtime must use the same auth_to_local mechanism "
                            + "(existing=%s, requested=%s)", KerberosName.getRuleMechanism(), mechanism);
            Set<String> rules = parseAuthToLocalRules(KerberosName.getRules());
            int previousSize = rules.size();
            // Append after all existing rules, including DEFAULT: a later realm must not change an
            // earlier principal's mapping. Replacing the global configuration would break its next login.
            if (rules.addAll(requestedRules)) {
                // MIT accepts an unmatched principal unchanged; extending its rules could rename a user.
                Preconditions.checkArgument(KerberosName.MECHANISM_HADOOP.equalsIgnoreCase(mechanism),
                        "MIT auth_to_local rules cannot be extended after Hadoop initialization; "
                                + "configure a common policy before initializing catalogs");
                KerberosName.setRules(String.join("\n", rules));
                LOG.debug("Extended JVM-global Kerberos auth_to_local rules from {} to {} rules",
                        previousSize, rules.size());
            }
        }
    }

    public static UserGroupInformation getUGIFromSubject(Configuration hadoopConf, Subject subject) throws IOException {
        synchronized (UserGroupInformation.class) {
            String previousRules = KerberosName.getRules();
            String previousMechanism = KerberosName.getRuleMechanism();
            boolean configuring = !UserGroupInformation.isInitialized() || authConfigurationFailed
                    || !UserGroupInformation.isSecurityEnabled();
            try {
                initializeAuthConfig(hadoopConf);
                return UserGroupInformation.getUGIFromSubject(subject);
            } catch (IOException | RuntimeException e) {
                // Some valid rule syntax (e.g. an out-of-range $ parameter) fails only when a principal
                // is mapped. A failed catalog must not leave that rule ahead of later corrected rules.
                KerberosName.setRules(previousRules);
                KerberosName.setRuleMechanism(previousMechanism);
                if (configuring) {
                    authConfigurationFailed = true;
                }
                throw e;
            }
        }
    }

    private static Set<String> parseAuthToLocalRules(String rawRules) {
        String rules = rawRules.trim();
        Set<String> result = new LinkedHashSet<>();
        Matcher matcher = AUTH_TO_LOCAL_RULE.matcher(rules);
        int position = 0;
        while (position < rules.length()) {
            matcher.region(position, rules.length());
            if (!matcher.lookingAt()) {
                throw new IllegalArgumentException("Invalid auth_to_local rule: " + rules.substring(position));
            }
            String rule = matcher.group(1);
            if (matcher.group(4) != null) {
                if (matcher.group(7) != null) {
                    Pattern.compile(matcher.group(7));
                }
                if (matcher.group(9) != null) {
                    Pattern.compile(matcher.group(9));
                }
                rule = "RULE:[" + Integer.parseInt(matcher.group(4)) + ":" + matcher.group(5) + "]"
                        + Objects.toString(matcher.group(6), "") + Objects.toString(matcher.group(8), "");
                if (matcher.group(12) != null) {
                    rule += "/L";
                }
            }
            result.add(rule);
            position = matcher.end();
        }
        return result;
    }

    @Override
    public synchronized UserGroupInformation getUGI() throws IOException {
        if (ugi == null) {
            subject = getSubject(config.getKerberosKeytab(), config.getKerberosPrincipal(), config.isPrintDebugLog());
            ugi = Objects.requireNonNull(login(subject), "login result is null");
            if (LOG.isDebugEnabled()) {
                Date lastTicketEndTime = getTicketEndTime(subject);
                LOG.debug("Kerberos principal: {}, last ticket end time: {}",
                        config.getKerberosPrincipal(), lastTicketEndTime);
            }
            return ugi;
        }
        if (nextRefreshTime < System.currentTimeMillis()) {
            long lastRefreshTime = nextRefreshTime;
            Subject existingSubject = subject;
            if (LOG.isDebugEnabled()) {
                Date lastTicketEndTime = getTicketEndTime(subject);
                LOG.debug("Current ticket expired time is {}", lastTicketEndTime);
            }
            // renew subject
            Subject newSubject = getSubject(config.getKerberosKeytab(), config.getKerberosPrincipal(),
                    config.isPrintDebugLog());
            Objects.requireNonNull(login(newSubject), "re-login result is null");
            // modify UGI instead of returning new UGI
            existingSubject.getPrincipals().addAll(newSubject.getPrincipals());
            Set<Object> privateCredentials = existingSubject.getPrivateCredentials();
            // clear the old credentials
            synchronized (privateCredentials) {
                privateCredentials.clear();
                privateCredentials.addAll(newSubject.getPrivateCredentials());
            }
            Set<Object> publicCredentials = existingSubject.getPublicCredentials();
            synchronized (publicCredentials) {
                publicCredentials.clear();
                publicCredentials.addAll(newSubject.getPublicCredentials());
            }
            nextRefreshTime = calculateNextRefreshTime(newSubject);
            if (LOG.isDebugEnabled()) {
                Date lastTicketEndTime = getTicketEndTime(newSubject);
                LOG.debug("Next ticket expired time is {}", lastTicketEndTime);
                LOG.debug("Refresh kerberos ticket succeeded, last time is {}, next time is {}",
                        lastRefreshTime, nextRefreshTime);
            }
        }
        if (LOG.isDebugEnabled()) {
            Date lastTicketEndTime = getTicketEndTime(subject);
            LOG.debug("Kerberos principal: {}, last ticket end time: {}",
                    config.getKerberosPrincipal(), lastTicketEndTime);
        }
        return ugi;
    }

    private UserGroupInformation login(Subject subject) throws IOException {
        // Login and get UGI when the catalog is initialized.
        String principal = config.getKerberosPrincipal();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Login by kerberos authentication with principal: {}", principal);
        }
        return getUGIFromSubject(config.getConf(), subject);
    }

    private static long calculateNextRefreshTime(Subject subject) {
        Preconditions.checkArgument(subject != null, "subject must be present in kerberos based UGI");
        KerberosTicket tgtTicket = KerberosTicketUtils.getTicketGrantingTicket(subject);
        return KerberosTicketUtils.getRefreshTime(tgtTicket);
    }

    private static Date getTicketEndTime(Subject subject) {
        Preconditions.checkArgument(subject != null, "subject must be present in kerberos based UGI");
        KerberosTicket tgtTicket = KerberosTicketUtils.getTicketGrantingTicket(subject);
        return tgtTicket.getEndTime();
    }

    private static Subject getSubject(String keytab, String principal, boolean printDebugLog) {
        Subject subject = new Subject(false, ImmutableSet.of(new KerberosPrincipal(principal)),
                Collections.emptySet(), Collections.emptySet());
        javax.security.auth.login.Configuration conf = getConfiguration(keytab, principal, printDebugLog);
        try {
            LoginContext loginContext = new LoginContext("", subject, null, conf);
            loginContext.login();
            return loginContext.getSubject();
        } catch (LoginException e) {
            throw new RuntimeException(e);
        }
    }

    private static javax.security.auth.login.Configuration getConfiguration(String keytab, String principal,
                                                                            boolean printDebugLog) {
        return new javax.security.auth.login.Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                        .put("doNotPrompt", "true")
                        .put("isInitiator", "true")
                        .put("useKeyTab", "true")
                        .put("storeKey", "true")
                        .put("keyTab", keytab)
                        .put("principal", principal);
                if (printDebugLog) {
                    builder.put("debug", "true");
                }
                Map<String, String> options = builder.build();
                return new AppConfigurationEntry[]{
                    new AppConfigurationEntry(
                        "com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options)};
            }
        };
    }
}
