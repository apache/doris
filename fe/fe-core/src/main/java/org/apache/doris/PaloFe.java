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

package org.apache.doris;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.CommandLineOptions;
import org.apache.doris.common.Config;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.common.Log4jConfig;
import org.apache.doris.common.MetaReader;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.Version;
import org.apache.doris.common.util.JdkUtils;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.httpv2.HttpServer;
import org.apache.doris.journal.bdbje.BDBDebugger;
import org.apache.doris.journal.bdbje.BDBTool;
import org.apache.doris.journal.bdbje.BDBToolOptions;
import org.apache.doris.qe.QeService;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FeServer;
import org.apache.doris.service.FrontendOptions;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

public class PaloFe {
    private static final Logger LOG = LogManager.getLogger(PaloFe.class);

    public static final String DORIS_HOME_DIR = System.getenv("DORIS_HOME");
    public static final String PID_DIR = System.getenv("PID_DIR");

    public static void main(String[] args) {
        start(DORIS_HOME_DIR, PID_DIR, args);
    }

    // entrance for doris frontend
    public static void start(String dorisHomeDir, String pidDir, String[] args) {
    	if (System.getenv("DORIS_LOG_TO_STDERR") != null) {
    		Log4jConfig.foreground = true;
    	}
        if (Strings.isNullOrEmpty(dorisHomeDir)) {
            System.err.println("env DORIS_HOME is not set.");
            return;
        }

        if (Strings.isNullOrEmpty(pidDir)) {
            System.err.println("env PID_DIR is not set.");
            return;
        }

        CommandLineOptions cmdLineOpts = parseArgs(args);

        try {
            // pid file
            if (!createAndLockPidFile(pidDir + "/fe.pid")) {
                throw new IOException("pid file is already locked.");
            }

            // init config
            Config config = new Config();
            config.init(dorisHomeDir + "/conf/fe.conf");
            // Must init custom config after init config, separately.
            // Because the path of custom config file is defined in fe.conf
            config.initCustom(Config.custom_config_dir + "/fe_custom.conf");

            LdapConfig ldapConfig = new LdapConfig();
            if (new File(dorisHomeDir + "/conf/ldap.conf").exists()) {
                ldapConfig.init(dorisHomeDir + "/conf/ldap.conf");
            }

            // check it after Config is initialized, otherwise the config 'check_java_version' won't work.
            if (!JdkUtils.checkJavaVersion()) {
                throw new IllegalArgumentException("Java version doesn't match");
            }

            Log4jConfig.initLogging(dorisHomeDir + "/conf/");

            // set dns cache ttl
            java.security.Security.setProperty("networkaddress.cache.ttl", "60");

            // check command line options
            checkCommandLineOptions(cmdLineOpts);

            LOG.info("Palo FE starting...");

            FrontendOptions.init();

            // check all port
            checkAllPorts();

            if (Config.enable_bdbje_debug_mode) {
                // Start in BDB Debug mode
                BDBDebugger.get().startDebugMode(dorisHomeDir);
                return;
            }

            // init catalog and wait it be ready
            Catalog.getCurrentCatalog().initialize(args);
            Catalog.getCurrentCatalog().waitForReady();

            // init and start:
            // 1. HttpServer for HTTP Server
            // 2. FeServer for Thrift Server
            // 3. QeService for MySQL Server
            QeService qeService = new QeService(Config.query_port, Config.mysql_service_nio_enabled, ExecuteEnv.getInstance().getScheduler());
            FeServer feServer = new FeServer(Config.rpc_port);

            feServer.start();

            HttpServer httpServer = new HttpServer();
            httpServer.setPort(Config.http_port);
            httpServer.setMaxHttpPostSize(Config.jetty_server_max_http_post_size);
            httpServer.setAcceptors(Config.jetty_server_acceptors);
            httpServer.setSelectors(Config.jetty_server_selectors);
            httpServer.setWorkers(Config.jetty_server_workers);
            httpServer.setMaxThreads(Config.jetty_threadPool_maxThreads);
            httpServer.setMinThreads(Config.jetty_threadPool_minThreads);
            httpServer.start();
            
            qeService.start();

            ThreadPoolManager.registerAllThreadPoolMetric();

            while (true) {
                Thread.sleep(2000);
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private static void checkAllPorts() throws IOException {
        if (!NetUtils.isPortAvailable(FrontendOptions.getLocalHostAddress(), Config.edit_log_port,
                "Edit log port", NetUtils.EDIT_LOG_PORT_SUGGESTION)) {
            throw new IOException("port " + Config.edit_log_port + " already in use");
        }
        if (!NetUtils.isPortAvailable(FrontendOptions.getLocalHostAddress(), Config.http_port,
                "Http port", NetUtils.HTTP_PORT_SUGGESTION)) {
            throw new IOException("port " + Config.http_port + " already in use");
        }
        if (!NetUtils.isPortAvailable(FrontendOptions.getLocalHostAddress(), Config.query_port,
                "Query port", NetUtils.QUERY_PORT_SUGGESTION)) {
            throw new IOException("port " + Config.query_port + " already in use");
        }
        if (!NetUtils.isPortAvailable(FrontendOptions.getLocalHostAddress(), Config.rpc_port,
                "Rpc port", NetUtils.RPC_PORT_SUGGESTION)) {
            throw new IOException("port " + Config.rpc_port + " already in use");
        }
    }

    /*
     * -v --version
     *      Print the version of Palo Frontend
     * -h --helper
     *      Specify the helper node when joining a bdb je replication group
     * -i --image
     *      Check if the specified image is valid
     * -b --bdb
     *      Run bdbje debug tools
     *
     *      -l --listdb
     *          List all database names in bdbje
     *      -d --db
     *          Specify a database in bdbje
     *
     *          -s --stat
     *              Print statistic of a database, including count, first key, last key
     *          -f --from
     *              Specify the start scan key
     *          -t --to
     *              Specify the end scan key
     *          -m --metaversion
     *              Specify the meta version to decode log value
     *
     */
    private static CommandLineOptions parseArgs(String[] args) {
        CommandLineParser commandLineParser = new DefaultParser();
        Options options = new Options();
        options.addOption("v", "version", false, "Print the version of Palo Frontend");
        options.addOption("h", "helper", true, "Specify the helper node when joining a bdb je replication group");
        options.addOption("i", "image", true, "Check if the specified image is valid");
        options.addOption("b", "bdb", false, "Run bdbje debug tools");
        options.addOption("l", "listdb", false, "List databases in bdbje");
        options.addOption("d", "db", true, "Specify a database in bdbje");
        options.addOption("s", "stat", false, "Print statistic of a database, including count, first key, last key");
        options.addOption("f", "from", true, "Specify the start scan key");
        options.addOption("t", "to", true, "Specify the end scan key");
        options.addOption("m", "metaversion", true, "Specify the meta version to decode log value");

        CommandLine cmd = null;
        try {
            cmd = commandLineParser.parse(options, args);
        } catch (final ParseException e) {
            e.printStackTrace();
            System.err.println("Failed to parse command line. exit now");
            System.exit(-1);
        }

        // version
        if (cmd.hasOption('v') || cmd.hasOption("version")) {
            return new CommandLineOptions(true, "", null, "");
        }
        // helper
        if (cmd.hasOption('h') || cmd.hasOption("helper")) {
            String helperNode = cmd.getOptionValue("helper");
            if (Strings.isNullOrEmpty(helperNode)) {
                System.err.println("Missing helper node");
                System.exit(-1);
            }
            return new CommandLineOptions(false, helperNode, null, "");
        }
        // image
        if (cmd.hasOption('i') || cmd.hasOption("image")) {
            // get image path
            String imagePath = cmd.getOptionValue("image");
            if (Strings.isNullOrEmpty(imagePath)) {
                System.err.println("imagePath is not set");
                System.exit(-1);
            }
            return new CommandLineOptions(false, "", null, imagePath);
        }
        if (cmd.hasOption('b') || cmd.hasOption("bdb")) {
            if (cmd.hasOption('l') || cmd.hasOption("listdb")) {
                // list bdb je databases
                BDBToolOptions bdbOpts = new BDBToolOptions(true, "", false, "", "", 0);
                return new CommandLineOptions(false, "", bdbOpts, "");
            }
            if (cmd.hasOption('d') || cmd.hasOption("db")) {
                // specify a database
                String dbName = cmd.getOptionValue("db");
                if (Strings.isNullOrEmpty(dbName)) {
                    System.err.println("BDBJE database name is missing");
                    System.exit(-1);
                }
                if (cmd.hasOption('s') || cmd.hasOption("stat")) {
                    BDBToolOptions bdbOpts = new BDBToolOptions(false, dbName, true, "", "", 0);
                    return new CommandLineOptions(false, "", bdbOpts, "");
                }
                String fromKey = "";
                String endKey = "";
                int metaVersion = 0;
                if (cmd.hasOption('f') || cmd.hasOption("from")) {
                    fromKey = cmd.getOptionValue("from");
                    if (Strings.isNullOrEmpty(fromKey)) {
                        System.err.println("from key is missing");
                        System.exit(-1);
                    }
                }
                if (cmd.hasOption('t') || cmd.hasOption("to")) {
                    endKey = cmd.getOptionValue("to");
                    if (Strings.isNullOrEmpty(endKey)) {
                        System.err.println("end key is missing");
                        System.exit(-1);
                    }
                }
                if (cmd.hasOption('m') || cmd.hasOption("metaversion")) {
                    try {
                        metaVersion = Integer.parseInt(cmd.getOptionValue("metaversion"));
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid meta version format");
                        System.exit(-1);
                    }
                }

                BDBToolOptions bdbOpts = new BDBToolOptions(false, dbName, false, fromKey, endKey, metaVersion);
                return new CommandLineOptions(false, "", bdbOpts, "");

            } else {
                System.err.println("Invalid options when running bdb je tools");
                System.exit(-1);
            }
        }

        // helper node is null, means no helper node is specified
        return new CommandLineOptions(false, null, null, "");
    }

    private static void checkCommandLineOptions(CommandLineOptions cmdLineOpts) {
        if (cmdLineOpts.isVersion()) {
            System.out.println("Build version: " + Version.DORIS_BUILD_VERSION);
            System.out.println("Build time: " + Version.DORIS_BUILD_TIME);
            System.out.println("Build info: " + Version.DORIS_BUILD_INFO);
            System.out.println("Build hash: " + Version.DORIS_BUILD_HASH);
            System.out.println("Java compile version: " + Version.DORIS_JAVA_COMPILE_VERSION);
            System.exit(0);
        } else if (cmdLineOpts.runBdbTools()) {
            BDBTool bdbTool = new BDBTool(Catalog.getCurrentCatalog().getBdbDir(), cmdLineOpts.getBdbToolOpts());
            if (bdbTool.run()) {
                System.exit(0);
            } else {
                System.exit(-1);
            }
        } else if (cmdLineOpts.runImageTool()) {
            File imageFile = new File(cmdLineOpts.getImagePath());
            if (!imageFile.exists()) {
                System.out.println("image does not exist: " + imageFile.getAbsolutePath() + " . Please put an absolute path instead");
                System.exit(-1);
            } else {
                System.out.println("Start to load image: ");
                try {
                    MetaReader.read(imageFile, Catalog.getCurrentCatalog());
                    System.out.println("Load image success. Image file " + cmdLineOpts.getImagePath() + " is valid");
                } catch (Exception e) {
                    System.out.println("Load image failed. Image file " + cmdLineOpts.getImagePath() + " is invalid");
                    e.printStackTrace();
                } finally {
                    System.exit(0);
                }
            }
        }

        // go on
    }

    private static boolean createAndLockPidFile(String pidFilePath) throws IOException {
        File pid = new File(pidFilePath);
        RandomAccessFile file = new RandomAccessFile(pid, "rws");
        try {
            FileLock lock = file.getChannel().tryLock();
            if (lock == null) {
                return false;
            }

            pid.deleteOnExit();

            String name = ManagementFactory.getRuntimeMXBean().getName();
            file.setLength(0);
            file.write(name.split("@")[0].getBytes(Charsets.UTF_8));

            return true;
        } catch (OverlappingFileLockException e) {
            file.close();
            return false;
        } catch (IOException e) {
            file.close();
            throw e;
        }
    }
}


