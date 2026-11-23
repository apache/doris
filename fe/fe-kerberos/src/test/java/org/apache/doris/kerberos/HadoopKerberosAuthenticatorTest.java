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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

class HadoopKerberosAuthenticatorTest {
    private static final String AUTH_TO_LOCAL = "hadoop.security.auth_to_local";
    private static final String RULE_MECHANISM = "hadoop.security.auth_to_local.mechanism";
    private String originalRealm;
    private String originalKdc;

    @BeforeEach
    void setUp() {
        originalRealm = System.getProperty("java.security.krb5.realm");
        originalKdc = System.getProperty("java.security.krb5.kdc");
        System.setProperty("java.security.krb5.realm", "DEFAULT.EXAMPLE");
        System.setProperty("java.security.krb5.kdc", "localhost");
        UserGroupInformation.reset();
        UserGroupInformation.setConfiguration(configuration("FIRST.EXAMPLE"));
    }

    @AfterEach
    void tearDown() {
        UserGroupInformation.reset();
        restoreProperty("java.security.krb5.realm", originalRealm);
        restoreProperty("java.security.krb5.kdc", originalKdc);
    }

    @Test
    void laterRealmCanCreateUgiWithoutBreakingEarlierRealm() throws Exception {
        HadoopKerberosAuthenticator.initializeAuthConfig(configuration("FIRST.EXAMPLE"));
        assertUser("FIRST.EXAMPLE");

        HadoopKerberosAuthenticator.initializeAuthConfig(configuration("SECOND.EXAMPLE"));
        assertUser("SECOND.EXAMPLE");
        assertUser("FIRST.EXAMPLE");
    }

    @Test
    void firstKerberosCatalogInitializesSharedUgi() {
        UserGroupInformation.reset();
        HadoopKerberosAuthenticator.initializeAuthConfig(configuration("FIRST.EXAMPLE"));
        Assertions.assertTrue(UserGroupInformation.isSecurityEnabled());
        HadoopKerberosAuthenticator.initializeAuthConfig(configuration("SECOND.EXAMPLE"));
        assertUser("FIRST.EXAMPLE");
        assertUser("SECOND.EXAMPLE");
    }

    @Test
    void firstCatalogKeepsItsGroupMappingConfiguration() throws Exception {
        UserGroupInformation.reset();
        Groups.reset();
        try {
            Configuration conf = configuration("FIRST.EXAMPLE");
            conf.set("hadoop.user.group.static.mapping.overrides", "hdfs=analytics");
            HadoopKerberosAuthenticator.initializeAuthConfig(conf);
            Assertions.assertArrayEquals(new String[] {"analytics"}, user("FIRST.EXAMPLE").getGroupNames());
        } finally {
            Groups.reset();
        }
    }

    @Test
    void failedInitialConfigurationCanBeCorrected() throws Exception {
        UserGroupInformation.setConfiguration(new Configuration(false));
        Configuration invalid = configuration("FIRST.EXAMPLE");
        invalid.set(AUTH_TO_LOCAL, "RULE:[2:$1]s/^hdfs$/old-user/");
        invalid.set("hadoop.kerberos.min.seconds.before.relogin", "oops");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HadoopKerberosAuthenticator.initializeAuthConfig(invalid));

        Configuration corrected = configuration("FIRST.EXAMPLE");
        corrected.set(AUTH_TO_LOCAL, "RULE:[2:$1]s/^hdfs$/new-user/");
        HadoopKerberosAuthenticator.initializeAuthConfig(corrected);
        Assertions.assertEquals("new-user", user("FIRST.EXAMPLE").getShortUserName());
    }

    @Test
    void failedPrincipalMappingDoesNotPoisonLaterCatalogs() throws Exception {
        Configuration invalid = configuration("SECOND.EXAMPLE");
        invalid.set(AUTH_TO_LOCAL, "RULE:[2:$3]");
        String rules = KerberosName.getRules();
        Subject subject = new Subject(false, Set.of(new KerberosPrincipal("hdfs/host@SECOND.EXAMPLE")),
                Set.of(), Set.of());
        Assertions.assertThrows(KerberosAuthException.class,
                () -> HadoopKerberosAuthenticator.getUGIFromSubject(invalid, subject));
        Assertions.assertEquals(rules, KerberosName.getRules());

        HadoopKerberosAuthenticator.initializeAuthConfig(configuration("SECOND.EXAMPLE"));
        HadoopKerberosAuthenticator.initializeAuthConfig(configuration("THIRD.EXAMPLE"));
        assertUser("FIRST.EXAMPLE");
        assertUser("SECOND.EXAMPLE");
        assertUser("THIRD.EXAMPLE");
    }

    @Test
    void failedFirstPrincipalMappingCanBeCorrected() throws Exception {
        UserGroupInformation.reset();
        Configuration invalid = configuration("FIRST.EXAMPLE");
        invalid.set(AUTH_TO_LOCAL, "RULE:[2:$3]");
        Subject subject = new Subject(false, Set.of(new KerberosPrincipal("hdfs/host@FIRST.EXAMPLE")),
                Set.of(), Set.of());
        Assertions.assertThrows(KerberosAuthException.class,
                () -> HadoopKerberosAuthenticator.getUGIFromSubject(invalid, subject));
        Assertions.assertNull(KerberosName.getRules());
        Assertions.assertEquals("hdfs", HadoopKerberosAuthenticator
                .getUGIFromSubject(configuration("FIRST.EXAMPLE"), subject).getShortUserName());
    }

    @Test
    void reverseInitializationOrderKeepsBothRealms() {
        UserGroupInformation.setConfiguration(configuration("SECOND.EXAMPLE"));
        HadoopKerberosAuthenticator.initializeAuthConfig(configuration("FIRST.EXAMPLE"));
        assertUser("FIRST.EXAMPLE");
        assertUser("SECOND.EXAMPLE");
    }

    @Test
    void independentPluginCopiesDoNotReplaceSharedRules() throws Exception {
        URL classes = HadoopKerberosAuthenticator.class.getProtectionDomain().getCodeSource().getLocation();
        try (URLClassLoader first = pluginLoader(classes); URLClassLoader second = pluginLoader(classes)) {
            Class<?> firstAuth = first.loadClass(HadoopKerberosAuthenticator.class.getName());
            Class<?> secondAuth = second.loadClass(HadoopKerberosAuthenticator.class.getName());
            Assertions.assertNotSame(firstAuth, secondAuth);
            Assertions.assertSame(first.loadClass(UserGroupInformation.class.getName()),
                    second.loadClass(UserGroupInformation.class.getName()));
            firstAuth.getMethod("initializeAuthConfig", Configuration.class)
                    .invoke(null, configuration("FIRST.EXAMPLE"));
            secondAuth.getMethod("initializeAuthConfig", Configuration.class)
                    .invoke(null, configuration("SECOND.EXAMPLE"));

            assertUser("FIRST.EXAMPLE");
            assertUser("SECOND.EXAMPLE");
        }
    }

    @Test
    void concurrentInitializationAndRefreshKeepEveryRealm() throws Exception {
        int catalogs = 8;
        ExecutorService executor = Executors.newFixedThreadPool(catalogs);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < catalogs; i++) {
                String realm = "REALM" + i + ".EXAMPLE";
                futures.add(executor.submit(() -> {
                    Assertions.assertTrue(start.await(10, TimeUnit.SECONDS));
                    for (int refresh = 0; refresh < 20; refresh++) {
                        HadoopKerberosAuthenticator.initializeAuthConfig(configuration(realm));
                        assertUser(realm);
                        assertUser("FIRST.EXAMPLE");
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                Assertions.assertDoesNotThrow(() -> future.get(30, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void refreshDoesNotAccumulateDuplicateRules() throws Exception {
        Configuration conf = configuration("SECOND.EXAMPLE");
        conf.set(AUTH_TO_LOCAL, "  RULE:[02:$1@$0](.*@SECOND.EXAMPLE)s/@.*//  DEFAULT  ");
        HadoopKerberosAuthenticator.initializeAuthConfig(conf);
        String rules = KerberosName.getRules();
        assertUser("SECOND.EXAMPLE");
        for (int refresh = 0; refresh < 20; refresh++) {
            HadoopKerberosAuthenticator.initializeAuthConfig(conf);
        }
        Assertions.assertEquals(rules, KerberosName.getRules());
        Assertions.assertEquals(3, rules.lines().count());
    }

    @Test
    void lowercaseFlagsAndDefaultRulesAreCanonicalized() {
        Configuration conf = configuration("SECOND.EXAMPLE");
        conf.set(AUTH_TO_LOCAL, "RULE:[02:$1@$0](.*@SECOND.EXAMPLE)s/@.*//g/L DEFAULT/L");
        for (int refresh = 0; refresh < 20; refresh++) {
            HadoopKerberosAuthenticator.initializeAuthConfig(conf);
        }
        Assertions.assertEquals(3, KerberosName.getRules().lines().count());
        assertUser("FIRST.EXAMPLE");
        assertUser("SECOND.EXAMPLE");
    }

    @Test
    void whitespaceInsideSubstitutionIsPreserved() throws Exception {
        Configuration conf = configuration("SECOND.EXAMPLE");
        conf.set(AUTH_TO_LOCAL,
                "RULE:[2:$1@$0](.*@SECOND.EXAMPLE)s/(hdfs|user name)@.*/mapped-user/ DEFAULT");
        HadoopKerberosAuthenticator.initializeAuthConfig(conf);
        Assertions.assertEquals("mapped-user", user("SECOND.EXAMPLE").getShortUserName());
        String rules = KerberosName.getRules();
        HadoopKerberosAuthenticator.initializeAuthConfig(conf);
        Assertions.assertEquals(rules, KerberosName.getRules());
    }

    @Test
    void extendingMitRulesIsRejectedWithoutChangingFallbackMappings() throws Exception {
        Configuration first = configuration("FIRST.EXAMPLE");
        first.set(RULE_MECHANISM, "mit");
        UserGroupInformation.setConfiguration(first);
        Configuration second = configuration("SECOND.EXAMPLE");
        second.set(RULE_MECHANISM, "mit");
        String rules = KerberosName.getRules();
        String originalUser = user("SECOND.EXAMPLE").getShortUserName();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HadoopKerberosAuthenticator.initializeAuthConfig(second));
        Assertions.assertEquals(rules, KerberosName.getRules());
        Assertions.assertEquals(originalUser, user("SECOND.EXAMPLE").getShortUserName());
        Assertions.assertDoesNotThrow(() -> HadoopKerberosAuthenticator.initializeAuthConfig(first));
    }

    @Test
    void existingRulesKeepTheirPrecedence() throws Exception {
        Configuration conf = configuration("FIRST.EXAMPLE");
        conf.set(AUTH_TO_LOCAL, "RULE:[2:$1@$0](.*@FIRST.EXAMPLE)s/.*/another-user/ DEFAULT");
        HadoopKerberosAuthenticator.initializeAuthConfig(conf);

        assertUser("FIRST.EXAMPLE");
        Assertions.assertThrows(KerberosAuthException.class, () -> user("UNCONFIGURED.EXAMPLE"));
    }

    @Test
    void invalidRulesDoNotChangePublishedRules() {
        Configuration conf = configuration("SECOND.EXAMPLE");
        conf.set(AUTH_TO_LOCAL, "INVALID");
        String rules = KerberosName.getRules();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HadoopKerberosAuthenticator.initializeAuthConfig(conf));
        Assertions.assertEquals(rules, KerberosName.getRules());
    }

    @Test
    void invalidFirstCatalogCannotPublishPermissiveFallback() {
        Configuration invalidSyntax = configuration("FIRST.EXAMPLE");
        invalidSyntax.set(AUTH_TO_LOCAL, "INVALID");
        Configuration invalidRegex = configuration("FIRST.EXAMPLE");
        invalidRegex.set(AUTH_TO_LOCAL, "RULE:[2:$1@$0]s/[/x/");
        Configuration invalidMechanism = configuration("FIRST.EXAMPLE");
        invalidMechanism.set(RULE_MECHANISM, "unknown");
        for (Configuration invalid : List.of(invalidSyntax, invalidRegex, invalidMechanism)) {
            UserGroupInformation.reset();
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> HadoopKerberosAuthenticator.initializeAuthConfig(invalid));
            Assertions.assertFalse(UserGroupInformation.isSecurityEnabled());
            HadoopKerberosAuthenticator.initializeAuthConfig(configuration("SECOND.EXAMPLE"));
            assertUser("SECOND.EXAMPLE");
            Assertions.assertThrows(KerberosAuthException.class, () -> user("FIRST.EXAMPLE"));
        }
    }

    @Test
    void incompatibleRuleMechanismDoesNotChangePublishedRules() {
        Configuration conf = configuration("SECOND.EXAMPLE");
        conf.set(RULE_MECHANISM, "mit");
        String rules = KerberosName.getRules();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HadoopKerberosAuthenticator.initializeAuthConfig(conf));
        Assertions.assertEquals(rules, KerberosName.getRules());
        Assertions.assertEquals("hadoop", KerberosName.getRuleMechanism());
    }

    @Test
    void absentRulesDoNotBroadenExistingPolicy() throws Exception {
        Configuration conf = configuration("SECOND.EXAMPLE");
        conf.unset(AUTH_TO_LOCAL);
        String rules = KerberosName.getRules();
        HadoopKerberosAuthenticator.initializeAuthConfig(conf);

        Assertions.assertEquals(rules, KerberosName.getRules());
        assertUser("FIRST.EXAMPLE");
        Assertions.assertThrows(KerberosAuthException.class, () -> user("SECOND.EXAMPLE"));
    }

    private static Configuration configuration(String realm) {
        Configuration conf = new Configuration(false);
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set(AUTH_TO_LOCAL, "RULE:[2:$1@$0](.*@" + realm + ")s/@.*//\nDEFAULT");
        return conf;
    }

    private static void assertUser(String realm) {
        UserGroupInformation ugi = Assertions.assertDoesNotThrow(() -> user(realm));
        Assertions.assertEquals("hdfs", ugi.getShortUserName());
    }

    private static UserGroupInformation user(String realm) throws Exception {
        Subject subject = new Subject(false, Set.of(new KerberosPrincipal("hdfs/host@" + realm)),
                Set.of(), Set.of());
        synchronized (UserGroupInformation.class) {
            return UserGroupInformation.getUGIFromSubject(subject);
        }
    }

    private static URLClassLoader pluginLoader(URL classes) {
        return new URLClassLoader(new URL[] {classes}, HadoopKerberosAuthenticatorTest.class.getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                synchronized (getClassLoadingLock(name)) {
                    if (name.startsWith("org.apache.doris.kerberos.")) {
                        Class<?> loaded = findLoadedClass(name);
                        if (loaded == null) {
                            loaded = findClass(name);
                        }
                        if (resolve) {
                            resolveClass(loaded);
                        }
                        return loaded;
                    }
                    return super.loadClass(name, resolve);
                }
            }
        };
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }
}
