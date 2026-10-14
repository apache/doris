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

// Connectivity validation on CREATE CATALOG for type=jdbc.
//
// Two checks run, in this order (PluginDrivenExternalCatalog.checkWhenCreating):
//
//   1. FE->external, via JdbcDorisConnector.testConnection().  On failure the
//      message reads "Connectivity test failed for catalog '<name>': ...".
//   2. BE->external, via DefaultConnectorValidationContext.executePendingBeTests(),
//      which BRPCs the backend and lands on the (jdbc, connection-tester) factory.
//      On failure the message reads "BE connectivity test failed: ...".
//
// Step 1 short-circuits step 2, so a bad url/credential is always reported by FE.
// That makes the *successful* create the assertion that covers the BE factory:
// it only returns once executePendingBeTests() resolved (jdbc, connection-tester)
// and ran it.  If the factory name and the BE-side registry ever drift apart,
// every create below fails with "has no factory named" instead - which is exactly
// what the assertNoPluginWiringError() checks below are guarding.
suite("test_jdbc_connection_validation", "p0,external") {

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String mysql_port = context.config.otherConfigs.get("mysql_57_port")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    String driver_class = "com.mysql.cj.jdbc.Driver"
    String base_url = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false"

    // A failure must come from the JDBC stack, not from the BE plugin wiring. These
    // strings are what BE reports when (plugin, factory) addressing is broken; seeing
    // any of them means the connector never got as far as opening a connection.
    def assertNoPluginWiringError = { String label, Exception ex ->
        assertNotNull(ex, "${label}: expected the create to fail, but it succeeded")
        String msg = ex.toString()
        ["has no factory named", "is not deployed", "failed to load",
         "packages the SPI itself", "built against plugin API"].each { marker ->
            assertFalse(msg.contains(marker),
                    "${label}: BE plugin wiring error '${marker}' in: ${msg}")
        }
    }

    // Every create below must be the one that fails, so nothing may survive from an
    // earlier run - "catalog already exists" would mask the error under test.
    ["jdbc_conn_bad_password", "jdbc_conn_bad_port", "jdbc_conn_bad_driver_class",
     "jdbc_conn_missing_driver", "jdbc_conn_no_test", "jdbc_conn_ok"].each { name ->
        sql """ drop catalog if exists ${name} """
    }

    // 1. Right host, wrong password. FE's own test connects and is rejected by MySQL.
    test {
        sql """
            create catalog jdbc_conn_bad_password properties (
                "type"="jdbc",
                "user"="root",
                "password"="definitely-not-the-password",
                "jdbc_url" = "${base_url}",
                "driver_url" = "${driver_url}",
                "driver_class" = "${driver_class}"
            );
        """
        check { result, ex, startTime, endTime ->
            assertNoPluginWiringError("wrong password", ex)
            assertTrue(ex.toString().contains("Connectivity test failed for catalog"),
                    "wrong password: unexpected message: ${ex}")
            assertTrue(ex.toString().contains("Access denied for user"),
                    "wrong password: expected the driver's auth error, got: ${ex}")
        }
    }

    // 2. Nothing listening on the port at all.
    test {
        sql """
            create catalog jdbc_conn_bad_port properties (
                "type"="jdbc",
                "user"="root",
                "password"="123456",
                "jdbc_url" = "jdbc:mysql://${externalEnvIp}:3399/doris_test?useSSL=false",
                "driver_url" = "${driver_url}",
                "driver_class" = "${driver_class}"
            );
        """
        check { result, ex, startTime, endTime ->
            assertNoPluginWiringError("dead port", ex)
            assertTrue(ex.toString().contains("Connectivity test failed for catalog"),
                    "dead port: unexpected message: ${ex}")
        }
    }

    // 3. driver_class that the jar does not contain. The message names the classloaders
    //    that were searched, which is what tells a reader the lookup happened on the
    //    driver classloader chain rather than on some shared classpath.
    test {
        sql """
            create catalog jdbc_conn_bad_driver_class properties (
                "type"="jdbc",
                "user"="root",
                "password"="123456",
                "jdbc_url" = "${base_url}",
                "driver_url" = "${driver_url}",
                "driver_class" = "com.mysql.cj.jdbc.NoSuchDriver"
            );
        """
        check { result, ex, startTime, endTime ->
            assertNoPluginWiringError("bad driver_class", ex)
            assertTrue(ex.toString().contains("com.mysql.cj.jdbc.NoSuchDriver"),
                    "bad driver_class: expected the missing class to be named, got: ${ex}")
        }
    }

    // 4. A driver_url that resolves to nothing. Caught by preCreateValidation before
    //    either connectivity test runs.
    test {
        sql """
            create catalog jdbc_conn_missing_driver properties (
                "type"="jdbc",
                "user"="root",
                "password"="123456",
                "jdbc_url" = "${base_url}",
                "driver_url" = "no-such-driver-9.9.9.jar",
                "driver_class" = "${driver_class}"
            );
        """
        check { result, ex, startTime, endTime ->
            assertNoPluginWiringError("missing driver jar", ex)
        }
    }

    // 5. Control: the same bad password, with the check turned off. The catalog is
    //    created because neither connectivity test runs.
    sql """
        create catalog jdbc_conn_no_test properties (
            "type"="jdbc",
            "user"="root",
            "password"="definitely-not-the-password",
            "jdbc_url" = "${base_url}",
            "driver_url" = "${driver_url}",
            "driver_class" = "${driver_class}",
            "test_connection" = "false"
        );
    """
    def catalogs = sql """ show catalogs """
    assertTrue(catalogs.collect { it[1] }.contains("jdbc_conn_no_test"),
            "test_connection=false should have created the catalog despite bad credentials")

    // 6. The BE-side assertion. test_connection defaults to true, so returning from
    //    this statement means executePendingBeTests() reached (jdbc, connection-tester)
    //    on the backend and it reported OK. Reading a table afterwards confirms the
    //    catalog the tester validated is actually usable.
    sql """
        create catalog jdbc_conn_ok properties (
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "${base_url}",
            "driver_url" = "${driver_url}",
            "driver_class" = "${driver_class}"
        );
    """
    // 6b. Proof that 6 was not vacuous, checked BEFORE the first read of this catalog.
    //
    //    A successful CREATE CATALOG is only evidence that the BE test ran if the BE test ran at
    //    all: executePendingBeTests() returns silently when the payload it was handed is empty, so
    //    a regression that stops queueing the BE test leaves every create above passing and every
    //    "this covers the BE factory" claim in this file untrue. Loading the jdbc plugin is a side
    //    effect only the BE side has, and /api/jni_plugin_status reports it without starting
    //    anything.
    //
    //    HERE, between the CREATE and the first scan, and not at the end of the suite: BE serves
    //    the scanner and the connection tester out of the SAME jdbc plugin directory, and
    //    statusJson() reports per plugin rather than per factory. After a scan the plugin is
    //    loaded either way, so the check could no longer fail - which is what it was doing at the
    //    end of this file.
    //
    //    HONEST LIMIT, since it cannot be closed from here: this discriminates only while no
    //    earlier jdbc case has loaded the plugin on the same BE, so it is a check that can stop
    //    discriminating without saying so. Making it exact needs a per-factory counter on the BE
    //    side, which /api/jni_plugin_status does not report today.
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
    assertTrue(backendId_to_backendIP.size() > 0, "no backend to ask about its plugin state")
    def anyBackendLoadedJdbc = false
    backendId_to_backendIP.each { id, ip ->
        httpTest {
            // The endpoint requires ADMIN, which is what the regression user has.
            endpoint "${ip}:${backendId_to_backendHttpPort.get(id)}"
            uri "/api/jni_plugin_status"
            op "get"
            basicAuthorization "${context.config.feHttpUser}", "${context.config.feHttpPassword}"
            check { respCode, body ->
                logger.info("BE ${id} jni plugin status: ${respCode} ${body}")
                assertEquals(200, respCode)
                // Parsed, and read off "plugins" rather than "deployed": the latter is a directory
                // listing, present on every BE whether or not anything was ever loaded, so a
                // substring check for "jdbc" could not fail and proved nothing. "plugins" holds only
                // what this process has actually touched.
                def status = parseJson("${body}")
                if (status.plugins.any { it.name == "jdbc" && it.state == "READY" }) {
                    anyBackendLoadedJdbc = true
                }
            }
        }
    }
    assertTrue(anyBackendLoadedJdbc,
            "no backend has the jdbc plugin loaded, so the BE-side connection test never ran and "
            + "the successful creates above prove nothing about the (jdbc, connection-tester) factory")

    // Pinned rather than asserted non-empty: doris_test.ex_tb0 is created and filled by the docker
    // fixture with exactly five rows, so reading a different number means the catalog the tester
    // validated is not the one being read.
    qt_rows_after_default_test """ select count(*) from jdbc_conn_ok.doris_test.ex_tb0 """

    // 7. Same, but re-created explicitly with test_connection=true, so the BE test is
    //    requested rather than defaulted into.
    sql """ drop catalog if exists jdbc_conn_ok """
    sql """
        create catalog jdbc_conn_ok properties (
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "${base_url}",
            "driver_url" = "${driver_url}",
            "driver_class" = "${driver_class}",
            "test_connection" = "true"
        );
    """
    qt_rows_after_explicit_test """ select count(*) from jdbc_conn_ok.doris_test.ex_tb0 """

    // No drops here on purpose: every catalog this suite uses is dropped at the top instead, so a
    // failing run leaves its catalogs behind to be inspected.
}
