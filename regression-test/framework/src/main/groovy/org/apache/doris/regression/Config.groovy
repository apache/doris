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
import java.util.function.Predicate

import static org.apache.doris.regression.ConfigOptions.*

import org.apache.doris.thrift.TNetworkAddress;

@Slf4j
@CompileStatic
class Config {
    public String jdbcUrl
    public String jdbcUser
    public String jdbcPassword
    public String defaultDb

    public String feSourceThriftAddress
    public String feTargetThriftAddress
    public String feSyncerUser
    public String feSyncerPassword
    public String syncerAddress

    public String feHttpAddress
    public String feHttpUser
    public String feHttpPassword

    public String metaServiceHttpAddress

    public String suitePath
    public String dataPath
    public String realDataPath
    public String cacheDataPath
    public boolean enableCacheData
    public String pluginPath
    public String sslCertificatePath
    public String dorisComposePath
    public String image
    public Boolean dockerEndDeleteFiles
    public Boolean excludeDockerTest

    public String testGroups
    public String excludeGroups
    public String testSuites
    public String excludeSuites
    public String testDirectories
    public String excludeDirectories
    public boolean generateOutputFile
    public boolean forceGenerateOutputFile
    public boolean randomOrder
    public boolean stopWhenFail
    public boolean dryRun

    public Properties otherConfigs = new Properties()

    public Set<String> suiteWildcard = new HashSet<>()
    public Set<String> groups = new HashSet<>()
    public Set<String> directories = new HashSet<>()

    public Set<String> excludeSuiteWildcard = new HashSet<>()
    public Set<String> excludeGroupSet = new HashSet<>()
    public Set<String> excludeDirectorySet = new HashSet<>()

    public TNetworkAddress feSourceThriftNetworkAddress
    public TNetworkAddress feTargetThriftNetworkAddress
    public TNetworkAddress syncerNetworkAddress
    public InetSocketAddress feHttpInetSocketAddress
    public InetSocketAddress metaServiceHttpInetSocketAddress
    public Integer parallel
    public Integer suiteParallel
    public Integer actionParallel
    public Integer times
    public boolean withOutLoadData

    Config() {}

    Config(String defaultDb, String jdbcUrl, String jdbcUser, String jdbcPassword,
           String feSourceThriftAddress, String feTargetThriftAddress, String feSyncerUser, String feSyncerPassword,
           String syncerPassword, String feHttpAddress, String feHttpUser, String feHttpPassword, String metaServiceHttpAddress,
           String suitePath, String dataPath, String realDataPath, String cacheDataPath, Boolean enableCacheData,
           String testGroups, String excludeGroups, String testSuites, String excludeSuites,
           String testDirectories, String excludeDirectories, String pluginPath, String sslCertificatePath) {
        this.defaultDb = defaultDb
        this.jdbcUrl = jdbcUrl
        this.jdbcUser = jdbcUser
        this.jdbcPassword = jdbcPassword
        this.feSourceThriftAddress = feSourceThriftAddress
        this.feTargetThriftAddress = feTargetThriftAddress
        this.feSyncerUser = feSyncerUser
        this.feSyncerPassword = feSyncerPassword
        this.syncerAddress = syncerAddress
        this.feHttpAddress = feHttpAddress
        this.feHttpUser = feHttpUser
        this.feHttpPassword = feHttpPassword
        this.metaServiceHttpAddress = metaServiceHttpAddress
        this.suitePath = suitePath
        this.dataPath = dataPath
        this.realDataPath = realDataPath
        this.cacheDataPath = cacheDataPath
        this.enableCacheData = enableCacheData
        this.testGroups = testGroups
        this.excludeGroups = excludeGroups
        this.testSuites = testSuites
        this.excludeSuites = excludeSuites
        this.testDirectories = testDirectories
        this.excludeDirectories = excludeDirectories
        this.pluginPath = pluginPath
        this.sslCertificatePath = sslCertificatePath
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
            String customConfFilePath = confFile.getParentFile().getPath() + "/regression-conf-custom.groovy"
            File custFile = new File(customConfFilePath)
            if (custFile.exists() && custFile.isFile()) {
                ConfigObject custConfigObj = configSlurper.parse(new File(customConfFilePath).toURI().toURL())
                configObj.merge(custConfigObj)
            }
            config = Config.fromConfigObject(configObj)
        }
        fillDefaultConfig(config)

        config.suitePath = FileUtils.getCanonicalPath(cmd.getOptionValue(pathOpt, config.suitePath))
        config.dataPath = FileUtils.getCanonicalPath(cmd.getOptionValue(dataOpt, config.dataPath))
        config.realDataPath = FileUtils.getCanonicalPath(cmd.getOptionValue(realDataOpt, config.realDataPath))
        config.cacheDataPath = cmd.getOptionValue(cacheDataOpt, config.cacheDataPath)
        config.enableCacheData = Boolean.parseBoolean(cmd.getOptionValue(enableCacheDataOpt, "true"))
        config.pluginPath = FileUtils.getCanonicalPath(cmd.getOptionValue(pluginOpt, config.pluginPath))
        config.sslCertificatePath = FileUtils.getCanonicalPath(cmd.getOptionValue(sslCertificateOpt, config.sslCertificatePath))
        config.dorisComposePath = FileUtils.getCanonicalPath(config.dorisComposePath)
        config.image = cmd.getOptionValue(imageOpt, config.image)
        config.suiteWildcard = cmd.getOptionValue(suiteOpt, config.testSuites)
                .split(",")
                .collect({s -> s.trim()})
                .findAll({s -> s != null && s.length() > 0})
                .toSet()
        config.groups = cmd.getOptionValue(groupsOpt, config.testGroups)
                .split(",")
                .collect({g -> g.trim()})
                .findAll({g -> g != null && g.length() > 0})
                .toSet()
        config.directories = cmd.getOptionValue(directoriesOpt, config.testDirectories)
                .split(",")
                .collect({d -> d.trim()})
                .findAll({d -> d != null && d.length() > 0})
                .toSet()
        config.excludeSuiteWildcard = cmd.getOptionValue(excludeSuiteOpt, config.excludeSuites)
                .split(",")
                .collect({s -> s.trim()})
                .findAll({s -> s != null && s.length() > 0})
                .toSet()
        config.excludeGroupSet = cmd.getOptionValue(excludeGroupsOpt, config.excludeGroups)
                .split(",")
                .collect({g -> g.trim()})
                .findAll({g -> g != null && g.length() > 0})
                .toSet()
        config.excludeDirectorySet = cmd.getOptionValue(excludeDirectoriesOpt, config.excludeDirectories)
                .split(",")
                .collect({d -> d.trim()})
                .findAll({d -> d != null && d.length() > 0})
                .toSet()

        if (!config.suiteWildcard && !config.groups && !config.directories && !config.excludeSuiteWildcard
            && !config.excludeGroupSet && !config.excludeDirectorySet) {
            log.info("no suites/directories/groups specified, set groups to p0".toString())
            config.groups = ["p0"].toSet()
        }

        config.feSourceThriftAddress = cmd.getOptionValue(feSourceThriftAddressOpt, config.feSourceThriftAddress)
        try {
            String host = config.feSourceThriftAddress.split(":")[0]
            int port = Integer.valueOf(config.feSourceThriftAddress.split(":")[1])
            config.feSourceThriftNetworkAddress = new TNetworkAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse fe thrift address: ${config.feSourceThriftAddress}", t)
        }

        config.feTargetThriftAddress = cmd.getOptionValue(feTargetThriftAddressOpt, config.feTargetThriftAddress)
        try {
            String host = config.feTargetThriftAddress.split(":")[0]
            int port = Integer.valueOf(config.feTargetThriftAddress.split(":")[1])
            config.feTargetThriftNetworkAddress = new TNetworkAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse fe thrift address: ${config.feTargetThriftAddress}", t)
        }

        config.syncerAddress = cmd.getOptionValue(syncerAddressOpt, config.syncerAddress)
        try {
            String host = config.syncerAddress.split(":")[0]
            int port = Integer.valueOf(config.syncerAddress.split(":")[1])
            config.syncerNetworkAddress = new TNetworkAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse syncer address: ${config.syncerAddress}", t)
        }

        config.feHttpAddress = cmd.getOptionValue(feHttpAddressOpt, config.feHttpAddress)
        try {
            Inet4Address host = Inet4Address.getByName(config.feHttpAddress.split(":")[0]) as Inet4Address
            int port = Integer.valueOf(config.feHttpAddress.split(":")[1])
            config.feHttpInetSocketAddress = new InetSocketAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse stream load address: ${config.feHttpAddress}", t)
        }

        config.metaServiceHttpAddress = cmd.getOptionValue(metaServiceHttpAddressOpt, config.metaServiceHttpAddress)
        try {
            Inet4Address host = Inet4Address.getByName(config.metaServiceHttpAddress.split(":")[0]) as Inet4Address
            int port = Integer.valueOf(config.metaServiceHttpAddress.split(":")[1])
            config.metaServiceHttpInetSocketAddress = new InetSocketAddress(host, port)
        } catch (Throwable t) {
            throw new IllegalStateException("Can not parse meta service address: ${config.metaServiceHttpAddress}", t)
        }
        log.info("msAddr : $config.metaServiceHttpAddress, socketAddr : $config.metaServiceHttpInetSocketAddress")

        config.defaultDb = cmd.getOptionValue(defaultDbOpt, config.defaultDb)
        config.jdbcUrl = cmd.getOptionValue(jdbcOpt, config.jdbcUrl)
        config.jdbcUser = cmd.getOptionValue(userOpt, config.jdbcUser)
        config.jdbcPassword = cmd.getOptionValue(passwordOpt, config.jdbcPassword)
        config.feSyncerUser = cmd.getOptionValue(feSyncerUserOpt, config.feSyncerUser)
        config.feSyncerPassword = cmd.getOptionValue(feSyncerPasswordOpt, config.feSyncerPassword)
        config.feHttpUser = cmd.getOptionValue(feHttpUserOpt, config.feHttpUser)
        config.feHttpPassword = cmd.getOptionValue(feHttpPasswordOpt, config.feHttpPassword)
        config.generateOutputFile = cmd.hasOption(genOutOpt)
        config.forceGenerateOutputFile = cmd.hasOption(forceGenOutOpt)
        config.parallel = Integer.parseInt(cmd.getOptionValue(parallelOpt, "10"))
        config.suiteParallel = Integer.parseInt(cmd.getOptionValue(suiteParallelOpt, "10"))
        config.actionParallel = Integer.parseInt(cmd.getOptionValue(actionParallelOpt, "10"))
        config.times = Integer.parseInt(cmd.getOptionValue(timesOpt, "1"))
        config.randomOrder = cmd.hasOption(randomOrderOpt)
        config.stopWhenFail = cmd.hasOption(stopWhenFailOpt)
        config.withOutLoadData = cmd.hasOption(withOutLoadDataOpt)
        config.dryRun = cmd.hasOption(dryRunOpt)

        log.info("randomOrder is ${config.randomOrder}".toString())
        log.info("stopWhenFail is ${config.stopWhenFail}".toString())
        log.info("withOutLoadData is ${config.withOutLoadData}".toString())
        log.info("dryRun is ${config.dryRun}".toString())

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
            configToString(obj.feSourceThriftAddress),
            configToString(obj.feTargetThriftAddress),
            configToString(obj.feSyncerUser),
            configToString(obj.feSyncerPassword),
            configToString(obj.syncerAddress),
            configToString(obj.feHttpAddress),
            configToString(obj.feHttpUser),
            configToString(obj.feHttpPassword),
            configToString(obj.metaServiceHttpAddress),
            configToString(obj.suitePath),
            configToString(obj.dataPath),
            configToString(obj.realDataPath),
            configToString(obj.cacheDataPath),
            configToBoolean(obj.enableCacheData),
            configToString(obj.testGroups),
            configToString(obj.excludeGroups),
            configToString(obj.testSuites),
            configToString(obj.excludeSuites),
            configToString(obj.testDirectories),
            configToString(obj.excludeDirectories),
            configToString(obj.pluginPath),
            configToString(obj.sslCertificatePath)
        )

        config.image = configToString(obj.image)
        config.dockerEndDeleteFiles = configToBoolean(obj.dockerEndDeleteFiles)
        config.excludeDockerTest = configToBoolean(obj.excludeDockerTest)

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
            //jdbcUrl needs parameter here. Refer to function: buildUrlWithDb(String jdbcUrl, String dbName)
            config.jdbcUrl = "jdbc:mysql://127.0.0.1:9030/?useLocalSessionState=true&allowLoadLocalInfile=true"
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

        if (config.feSourceThriftAddress == null) {
            config.feSourceThriftAddress = "127.0.0.1:9020"
            log.info("Set feThriftAddress to '${config.feSourceThriftAddress}' because not specify.".toString())
        }

        if (config.feTargetThriftAddress == null) {
            config.feTargetThriftAddress = "127.0.0.1:9020"
            log.info("Set feThriftAddress to '${config.feTargetThriftAddress}' because not specify.".toString())
        }

        if (config.feHttpAddress == null) {
            config.feHttpAddress = "127.0.0.1:8030"
            log.info("Set feHttpAddress to '${config.feHttpAddress}' because not specify.".toString())
        }

        if (config.metaServiceHttpAddress == null) {
            config.metaServiceHttpAddress = "127.0.0.1:5000"
            log.info("Set metaServiceHttpAddress to '${config.metaServiceHttpAddress}' because not specify.".toString())
        }

        if (config.feSyncerUser == null) {
            config.feSyncerUser = "root"
            log.info("Set feSyncerUser to '${config.feSyncerUser}' because not specify.".toString())
        }

        if (config.feSyncerPassword == null) {
            config.feSyncerPassword = ""
            log.info("Set feSyncerPassword to empty because not specify.".toString())
        }

        if (config.syncerAddress == null) {
            config.syncerAddress = "127.0.0.1:9190"
            log.info("Set syncerAddress to '${config.syncerAddress}' because not specify.".toString())
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
            config.dataPath = "regression-test/data"
            log.info("Set dataPath to '${config.dataPath}' because not specify.".toString())
        }

        if (config.realDataPath == null) {
            config.realDataPath = "regression-test/realData"
            log.info("Set realDataPath to '${config.realDataPath}' because not specify.".toString())
        }

        if (config.cacheDataPath == null) {
            config.cacheDataPath = "regression-test/cacheData"
            log.info("Set cacheDataPath to '${config.cacheDataPath}' because not specify.".toString())
        }

        if (config.enableCacheData == null) {
            config.enableCacheData = true
            log.info("Set enableCacheData to '${config.enableCacheData}' because not specify.".toString())
        }

        if (config.pluginPath == null) {
            config.pluginPath = "regression-test/plugins"
            log.info("Set dataPath to '${config.pluginPath}' because not specify.".toString())
        }

        if (config.sslCertificatePath == null) {
            config.sslCertificatePath = "regression-test/ssl_default_certificate"
            log.info("Set sslCertificatePath to '${config.sslCertificatePath}' because not specify.".toString())
        }

        if (config.dockerEndDeleteFiles == null) {
            config.dockerEndDeleteFiles = false
            log.info("Set dockerEndDeleteFiles to '${config.dockerEndDeleteFiles}' because not specify.".toString())
        }

        if (config.excludeDockerTest == null) {
            config.excludeDockerTest = true
            log.info("Set excludeDockerTest to '${config.excludeDockerTest}' because not specify.".toString())
        }

        if (config.dorisComposePath == null) {
            config.dorisComposePath = "docker/runtime/doris-compose/doris-compose.py"
            log.info("Set dorisComposePath to '${config.dorisComposePath}' because not specify.".toString())
        }

        if (config.testGroups == null) {
            config.testGroups = "default"
            log.info("Set testGroups to '${config.testGroups}' because not specify.".toString())
        }

        if (config.excludeGroups == null) {
            config.excludeGroups = ""
            log.info("Set excludeGroups to empty because not specify.".toString())
        }

        if (config.testDirectories == null) {
            config.testDirectories = ""
            log.info("Set testDirectories to empty because not specify.".toString())
        }

        if (config.excludeDirectories == null) {
            config.excludeDirectories = ""
            log.info("Set excludeDirectories to empty because not specify.".toString())
        }

        if (config.testSuites == null) {
            config.testSuites = ""
            log.info("Set testSuites to empty because not specify.".toString())
        }

        if (config.excludeSuites == null) {
            config.excludeSuites = ""
            log.info("Set excludeSuites to empty because not specify.".toString())
        }

        if (config.parallel == null) {
            config.parallel = 1
            log.info("Set parallel to 1 because not specify.".toString())
        }

        if (config.suiteParallel == null) {
            config.suiteParallel = 1
            log.info("Set suiteParallel to 1 because not specify.".toString())
        }

        if (config.actionParallel == null) {
            config.actionParallel = 10
            log.info("Set actionParallel to 10 because not specify.".toString())
        }
    }

    static String configToString(Object obj) {
        return (obj instanceof String || obj instanceof GString) ? obj.toString() : null
    }

    static Boolean configToBoolean(Object obj) {
        if (obj instanceof Boolean) {
            return (Boolean) obj
        } else if (obj instanceof String || obj instanceof GString) {
            String stringValue = obj.toString().trim()
            if (stringValue.equalsIgnoreCase("true")) {
                return true
            } else if (stringValue.equalsIgnoreCase("false")) {
                return false
            }
        }
        return null
    }

    void tryCreateDbIfNotExist(String dbName = defaultDb) {
        // connect without specify default db
        try {
            String sql = "CREATE DATABASE IF NOT EXISTS ${dbName}"
            log.info("Try to create db, sql: ${sql}".toString())
            if (!dryRun) {
                getConnection().withCloseable { conn ->
                    JdbcUtils.executeToList(conn, sql)
                }
            }
        } catch (Throwable t) {
            throw new IllegalStateException("Create database failed, jdbcUrl: ${jdbcUrl}", t)
        }
    }

    Connection getConnection() {
        return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
    }

    Connection getConnectionByDbName(String dbName) {
        String dbUrl = buildUrlWithDb(jdbcUrl, dbName)
        tryCreateDbIfNotExist(dbName)
        log.info("connect to ${dbUrl}".toString())
        return DriverManager.getConnection(dbUrl, jdbcUser, jdbcPassword)
    }

    String getDbNameByFile(File suiteFile) {
        String dir = new File(suitePath).relativePath(suiteFile.parentFile)
        // We put sql files under sql dir, so dbs and tables used by cases
        // under sql directory should be prepared by load.groovy under the
        // parent.
        //
        // e.g.
        // suites/tpcds_sf1/load.groovy
        // suites/tpcds_sf1/sql/q01.sql
        // suites/tpcds_sf1/sql/dir/q01.sql
        if (dir.indexOf(File.separator + "sql", dir.length() - 4) > 0 && dir.endsWith("sql")) {
            dir = dir.substring(0, dir.indexOf(File.separator + "sql", dir.length() - 4))
        }
        if (dir.indexOf(File.separator + "sql" + File.separator) > 0) {
            dir = dir.substring(0, dir.indexOf(File.separator + "sql" + File.separator))
        }

        dir = dir.replace('-', '_')
        dir = dir.replace('.', '_')

        return defaultDb + '_' + dir.replace(File.separator, '_')
    }

    Predicate<String> getDirectoryFilter() {
        return (Predicate<String>) { String directoryName ->
            if (directories.isEmpty() && excludeDirectorySet.isEmpty()) {
                return true
            }

            String relativePath = new File(suitePath).relativePath(new File(directoryName))
            List<String> allLevelPaths = new ArrayList<>()
            String parentPath = ""
            for (String pathName : relativePath.split(File.separator)) {
                String currentPath = parentPath + pathName
                allLevelPaths.add(currentPath)
                parentPath = currentPath + File.separator
            }

            if (!directories.isEmpty() && !allLevelPaths.any({directories.contains(it) })) {
                return false
            }
            if (!excludeDirectorySet.isEmpty() && allLevelPaths.any({ excludeDirectorySet.contains(it) })) {
                return false
            }
            return true
        }
    }

    public void buildUrlWithDefaultDb() {
        this.jdbcUrl = buildUrlWithDb(jdbcUrl, defaultDb)
        log.info("Reset jdbcUrl to ${jdbcUrl}".toString())
    }

    public static String buildUrlWithDb(String jdbcUrl, String dbName) {
        String urlWithDb = jdbcUrl
        String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
        if (urlWithoutSchema.indexOf("/") >= 0) {
            if (jdbcUrl.contains("?")) {
                // e.g: jdbc:mysql://locahost:8080/?a=b
                urlWithDb = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("?"))
                urlWithDb = urlWithDb.substring(0, urlWithDb.lastIndexOf("/"))
                urlWithDb += ("/" + dbName) + jdbcUrl.substring(jdbcUrl.lastIndexOf("?"))
            } else {
                // e.g: jdbc:mysql://locahost:8080/
                urlWithDb += dbName
            }
        } else {
            // e.g: jdbc:mysql://locahost:8080
            urlWithDb += ("/" + dbName)
        }
        urlWithDb = addSslUrl(urlWithDb);
        urlWithDb = addTimeoutUrl(urlWithDb);

        return urlWithDb
    }

    private static String addSslUrl(String url) {
        if (url.contains("TLS")) {
            return url
        }
        // ssl-mode = PREFERRED
        String useSsl = "true"
        String useSslConfig = "verifyServerCertificate=false&useSSL=" + useSsl + "&requireSSL=false"
        String tlsVersion = "TLSv1.2"
        String tlsVersionConfig = "&enabledTLSProtocols=" + tlsVersion
        String sslUrl = useSslConfig + tlsVersionConfig
        // e.g: jdbc:mysql://locahost:8080/dbname?
        if (url.charAt(url.length() - 1) == '?') {
            return url + sslUrl
            // e.g: jdbc:mysql://locahost:8080/dbname?a=b
        } else if (url.contains('?')) {
            return url + '&' + sslUrl
            // e.g: jdbc:mysql://locahost:8080/dbname
        } else {
            return url + '?' + sslUrl
        }
    }

    private static String addTimeoutUrl(String url) {
        if (url.contains("connectTimeout=") || url.contains("socketTimeout="))
        {
            return url
        }

        Integer connectTimeout = 5000
        Integer socketTimeout = 1000 * 60 * 30
        String s = String.format("connectTimeout=%d&socketTimeout=%d", connectTimeout, socketTimeout)
        if (url.charAt(url.length() - 1) == '?') {
            return url + s
            // e.g: jdbc:mysql://locahost:8080/dbname?a=b
        } else if (url.contains('?')) {
            return url + '&' + s
            // e.g: jdbc:mysql://locahost:8080/dbname
        } else {
            return url + '?' + s
        }
    }
}
