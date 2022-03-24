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

package org.apache.doris.regression

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import com.google.common.collect.Maps
import org.apache.commons.cli.CommandLine
import org.apache.doris.regression.util.FileUtils
import org.apache.doris.regression.util.JdbcUtils

import java.sql.Connection
import java.sql.DriverManager

import static org.apache.doris.regression.ConfigOptions.*

@Slf4j
@CompileStatic
class Config {
    public String jdbcUrl
    public String jdbcUser
    public String jdbcPassword
    public String defaultDb

    public String feHttpAddress
    public String feHttpUser
    public String feHttpPassword

    public String suitePath
    public String dataPath

    public String testGroups
    public String testSuites
    public boolean generateOutputFile
    public boolean forceGenerateOutputFile
    public boolean randomOrder

    public Properties otherConfigs = new Properties()

    public Set<String> suiteWildcard = new HashSet<>()
    public Set<String> groups = new HashSet<>()
    public InetSocketAddress feHttpInetSocketAddress
    public Integer parallel
    public Integer actionParallel
    public Integer times
    public boolean withOutLoadData

    Config() {}

    Config(String defaultDb, String jdbcUrl, String jdbcUser, String jdbcPassword,
           String feHttpAddress, String feHttpUser, String feHttpPassword,
           String suitePath, String dataPath, String testGroups, String testSuites) {
        this.defaultDb = defaultDb
        this.jdbcUrl = jdbcUrl
        this.jdbcUser = jdbcUser
        this.jdbcPassword = jdbcPassword
        this.feHttpAddress = feHttpAddress
        this.feHttpUser = feHttpUser
        this.feHttpPassword = feHttpPassword
        this.suitePath = suitePath
        this.dataPath = dataPath
        this.testGroups = testGroups
        this.testSuites = testSuites
    }

    static Config fromCommandLine(CommandLine cmd) {
        String confFilePath = cmd.getOptionValue(confFileOpt, "")
        File confFile = new File(confFilePath)
        Config config = new Config()
        if (confFile.exists() && confFile.isFile()) {
            log.info("Load config file ${confFilePath}".toString())
            def configSlurper = new ConfigSlurper()
            def systemProperties = Maps.newLinkedHashMap(System.getProperties())
            configSlurper.setBinding(systemProperties)
            ConfigObject configObj = configSlurper.parse(new File(confFilePath).toURI().toURL())
            config = Config.fromConfigObject(configObj)
        }

        fillDefaultConfig(config)

        config.suitePath = FileUtils.getCanonicalPath(cmd.getOptionValue(pathOpt, config.suitePath))
        config.dataPath = FileUtils.getCanonicalPath(cmd.getOptionValue(dataOpt, config.dataPath))
        config.suiteWildcard = cmd.getOptionValue(suiteOpt, config.testSuites)
                .split(",")
                .collect({g -> g.trim()})
                .findAll({g -> g != null && g.length() > 0})
                .toSet()
        config.groups = cmd.getOptionValue(groupsOpt, config.testGroups)
                .split(",")
                .collect({g -> g.trim()})
                .findAll({g -> g != null && g.length() > 0})
                .toSet()

        config.feHttpAddress = cmd.getOptionValue(feHttpAddressOpt, config.feHttpAddress)
        try {
            Inet4Address host = Inet4Address.getByName(config.feHttpAddress.split(":")[0]) as Inet4Address
            int port = Integer.valueOf(config.feHttpAddress.split(":")[1])
            config.feHttpInetSocketAddress = new InetSocketAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse stream load address: ${config.feHttpAddress}", t)
        }

        config.defaultDb = cmd.getOptionValue(jdbcOpt, config.defaultDb)
        config.jdbcUrl = cmd.getOptionValue(jdbcOpt, config.jdbcUrl)
        config.jdbcUser = cmd.getOptionValue(userOpt, config.jdbcUser)
        config.jdbcPassword = cmd.getOptionValue(passwordOpt, config.jdbcPassword)
        config.feHttpUser = cmd.getOptionValue(feHttpUserOpt, config.feHttpUser)
        config.feHttpPassword = cmd.getOptionValue(feHttpPasswordOpt, config.feHttpPassword)
        config.generateOutputFile = cmd.hasOption(genOutOpt)
        config.forceGenerateOutputFile = cmd.hasOption(forceGenOutOpt)
        config.parallel = Integer.parseInt(cmd.getOptionValue(parallelOpt, "1"))
        config.actionParallel = Integer.parseInt(cmd.getOptionValue(actionParallelOpt, "10"))
        config.times = Integer.parseInt(cmd.getOptionValue(timesOpt, "1"))
        config.randomOrder = cmd.hasOption(randomOrderOpt)
        config.withOutLoadData = cmd.hasOption(withOutLoadDataOpt)

        Properties props = cmd.getOptionProperties("conf")
        config.otherConfigs.putAll(props)

        config.tryCreateDbIfNotExist()
        config.buildUrlWithDefaultDb()

        return config
    }

    static Config fromConfigObject(ConfigObject obj) {
        def config = new Config(
            configToString(obj.defaultDb),
            configToString(obj.jdbcUrl),
            configToString(obj.jdbcUser),
            configToString(obj.jdbcPassword),
            configToString(obj.feHttpAddress),
            configToString(obj.feHttpUser),
            configToString(obj.feHttpPassword),
            configToString(obj.suitePath),
            configToString(obj.dataPath),
            configToString(obj.testGroups),
            configToString(obj.testSuites)
        )

        def declareFileNames = config.getClass()
                .getDeclaredFields()
                .collect({f -> f.name})
                .toSet()
        for (def kv : obj.toProperties().entrySet()) {
            String key = kv.getKey() as String
            if (!declareFileNames.contains(key)) {
                config.otherConfigs.put(key, kv.getValue())
            }
        }
        return config
    }

    static void fillDefaultConfig(Config config) {
        if (config.defaultDb == null) {
            config.defaultDb = "regression_test"
            log.info("Set defaultDb to '${config.defaultDb}' because not specify.".toString())
        }

        if (config.jdbcUrl == null) {
            config.jdbcUrl = "jdbc:mysql://127.0.0.1:9030"
            log.info("Set jdbcUrl to '${config.jdbcUrl}' because not specify.".toString())
        }

        if (config.jdbcUser == null) {
            config.jdbcUser = "root"
            log.info("Set jdbcUser to '${config.jdbcUser}' because not specify.".toString())
        }

        if (config.jdbcPassword == null) {
            config.jdbcPassword = ""
            log.info("Set jdbcPassword to empty because not specify.".toString())
        }

        if (config.feHttpAddress == null) {
            config.feHttpAddress = "127.0.0.1:8030"
            log.info("Set feHttpAddress to '${config.feHttpAddress}' because not specify.".toString())
        }

        if (config.feHttpUser == null) {
            config.feHttpUser = "root"
            log.info("Set feHttpUser to '${config.feHttpUser}' because not specify.".toString())
        }

        if (config.feHttpPassword == null) {
            config.feHttpPassword = ""
            log.info("Set feHttpPassword to empty because not specify.".toString())
        }

        if (config.suitePath == null) {
            config.suitePath = "regression-test/suites"
            log.info("Set suitePath to '${config.suitePath}' because not specify.".toString())
        }

        if (config.dataPath == null) {
            config.dataPath = "regression-test/suites"
            log.info("Set dataPath to '${config.dataPath}' because not specify.".toString())
        }

        if (config.testGroups == null) {
            config.testGroups = "default"
            log.info("Set testGroups to '${config.testGroups}' because not specify.".toString())
        }

        if (config.testSuites == null) {
            config.testSuites = ""
            log.info("Set testSuites to empty because not specify.".toString())
        }

        if (config.parallel == null) {
            config.parallel = 1
            log.info("Set parallel to 1 because not specify.".toString())
        }

        if (config.actionParallel == null) {
            config.actionParallel = 10
            log.info("Set actionParallel to 10 because not specify.".toString())
        }

        if (config.randomOrder == null) {
            config.randomOrder = false
            log.info("set randomOrder to false because not specify.".toString())
        }

        if (config.withOutLoadData == null) {
            config.withOutLoadData = false
            log.info("set withOutLoadData to false because not specify.".toString())
        }
    }
    
    static String configToString(Object obj) {
        return (obj instanceof String || obj instanceof GString) ? obj.toString() : null
    }

    void tryCreateDbIfNotExist() {
        // connect without specify default db
        try {
            String sql = "CREATE DATABASE IF NOT EXISTS ${defaultDb}"
            log.info("Try to create db, sql: ${sql}".toString())
            getConnection().withCloseable { conn ->
                JdbcUtils.executeToList(conn, sql)
            }
        } catch (Throwable t) {
            throw new IllegalStateException("Create database failed, jdbcUrl: ${jdbcUrl}", t)
        }
    }

    Connection getConnection() {
        return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
    }

    private void buildUrlWithDefaultDb() {
        String urlWithDb = jdbcUrl
        String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
        if (urlWithoutSchema.indexOf("/") >= 0) {
            if (jdbcUrl.contains("?")) {
                // e.g: jdbc:mysql://localhost:8080/?a=b
                urlWithDb = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/"))
                urlWithDb += ("/" + defaultDb) + jdbcUrl.substring(jdbcUrl.lastIndexOf("?"))
            } else {
                // e.g: jdbc:mysql://localhost:8080/
                urlWithDb += defaultDb
            }
        } else {
            // e.g: jdbc:mysql://localhost:8080
            urlWithDb += ("/" + defaultDb)
        }
        this.jdbcUrl = urlWithDb
        log.info("Reset jdbcUrl to ${jdbcUrl}".toString())

        // check connection with default db
        getConnection().close()
    }
}