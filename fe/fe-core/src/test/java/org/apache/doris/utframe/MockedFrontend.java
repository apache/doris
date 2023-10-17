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

package org.apache.doris.utframe;

import org.apache.doris.DorisFE;
import org.apache.doris.DorisFE.StartupOptions;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.PrintableMap;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;

/*
 * This class is used to start a Frontend process locally, for unit test.
 * Usage:
 *      MockedFrontend mockedFrontend = MockedFrontend.getInstance();
 *      mockedFrontend.init(confMap);
 *      mockedFrontend.start(new String[0]);
 *
 *      ...
 *
 * confMap is a instance of Map<String, String>.
 * Here you can add any FE configuration you want to add. For example:
 *      confMap.put("http_port", "8032");
 *
 * FrontendProcess already contains a minimal set of FE configurations.
 * Any configuration in confMap will form the final fe.conf file with this minimal set.
 *
 * 1 environment variable must be set:
 *      DORIS_HOME/
 *
 * The running dir is set when calling init();
 * There will be 3 directories under running dir/:
 *      running dir/conf/
 *      running dir/log/
 *      running dir/doris-meta/
 *
 *  All these 3 directories will be cleared first.
 *
 */
public class MockedFrontend {
    public static final String FE_PROCESS = "fe";

    // the running dir of this mocked frontend.
    // log/ doris-meta/ and conf/ dirs will be created under this dir.
    private String runningDir;
    // the min set of fe.conf.
    private static final Map<String, String> MIN_FE_CONF;

    private Map<String, String> finalFeConf;

    static {
        MIN_FE_CONF = Maps.newHashMap();
        MIN_FE_CONF.put("sys_log_level", "INFO");
        MIN_FE_CONF.put("http_port", "8030");
        MIN_FE_CONF.put("rpc_port", "9020");
        MIN_FE_CONF.put("query_port", "9030");
        MIN_FE_CONF.put("arrow_flight_sql_port", "9040");
        MIN_FE_CONF.put("edit_log_port", "9010");
        MIN_FE_CONF.put("priority_networks", "127.0.0.1/24");
        MIN_FE_CONF.put("sys_log_verbose_modules", "org");
    }

    public int getRpcPort() {
        return Integer.parseInt(finalFeConf.get("rpc_port"));
    }

    private boolean isInit = false;

    // init the fe process. This must be called before starting the frontend process.
    // 1. check if all necessary environment variables are set.
    // 2. clear and create 3 dirs: runningDir/log/, runningDir/doris-meta/, runningDir/conf/
    // 3. init fe.conf
    //      The content of "fe.conf" is a merge set of input `feConf` and MIN_FE_CONF
    public void init(String runningDir, Map<String, String> feConf) throws EnvVarNotSetException, IOException {
        if (isInit) {
            return;
        }

        if (Strings.isNullOrEmpty(runningDir)) {
            System.err.println("running dir is not set for mocked frontend");
            throw new EnvVarNotSetException("running dir is not set for mocked frontend");
        }

        this.runningDir = runningDir;
        System.out.println("mocked frontend running in dir: " + this.runningDir);

        // root running dir
        createAndClearDir(this.runningDir);
        // clear and create log dir
        createAndClearDir(runningDir + "/log/");
        // clear and create meta dir
        createAndClearDir(runningDir + "/doris-meta/");
        // clear and create conf dir
        createAndClearDir(runningDir + "/conf/");
        // init fe.conf
        initFeConf(runningDir + "/conf/", feConf);

        isInit = true;
    }

    private void initFeConf(String confDir, Map<String, String> feConf) throws IOException {
        finalFeConf = Maps.newHashMap(MIN_FE_CONF);
        // these 2 configs depends on running dir, so set them here.
        finalFeConf.put("LOG_DIR", this.runningDir + "/log");
        finalFeConf.put("meta_dir", this.runningDir + "/doris-meta");
        finalFeConf.put("sys_log_dir", this.runningDir + "/log");
        finalFeConf.put("audit_log_dir", this.runningDir + "/log");
        finalFeConf.put("tmp_dir", this.runningDir + "/temp_dir");
        // use custom config to add or override default config
        finalFeConf.putAll(feConf);

        PrintableMap<String, String> map = new PrintableMap<>(finalFeConf, "=", false, true, "");
        File confFile = new File(confDir + "fe.conf");
        if (!confFile.exists()) {
            confFile.createNewFile();
        }
        PrintWriter printWriter = new PrintWriter(confFile);
        try {
            printWriter.print(map.toString());
            printWriter.flush();
        } finally {
            printWriter.close();
        }
    }

    // clear the specified dir, and create a empty one
    private void createAndClearDir(String dir) throws IOException {
        File localDir = new File(dir);
        if (!localDir.exists()) {
            localDir.mkdirs();
        } else {
            Files.walk(Paths.get(dir)).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            if (!localDir.exists()) {
                localDir.mkdirs();
            }
        }
    }

    public String getRunningDir() {
        return runningDir;
    }

    private static class FERunnable implements Runnable {
        private MockedFrontend frontend;
        private String[] args;

        public FERunnable(MockedFrontend frontend, String[] args) {
            this.frontend = frontend;
            this.args = args;
        }

        @Override
        public void run() {
            StartupOptions options = new StartupOptions();
            // For FE unit tests, we don't need these 2 servers.
            // And it also cost time to start up.
            options.enableHttpServer = false;
            options.enableQeService = false;
            DorisFE.start(frontend.getRunningDir(), frontend.getRunningDir(), args, options);
        }
    }

    // must call init() before start.
    public void start(String[] args) throws FeStartException, NotInitException {
        if (!isInit) {
            throw new NotInitException("fe process is not initialized");
        }
        Thread feThread = new Thread(new FERunnable(this, args), FE_PROCESS);
        feThread.start();
        // wait the catalog to be ready until timeout (30 seconds)
        waitForCatalogReady(30 * 1000);
    }

    private void waitForCatalogReady(long timeoutMs) throws FeStartException {
        long left = timeoutMs;
        while (!Env.getCurrentEnv().isReady() && left > 0) {
            System.out.println("catalog is not ready");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            left -= 100;
        }

        if (left <= 0 && !Env.getCurrentEnv().isReady()) {
            throw new FeStartException("fe start failed");
        }
    }

    public static class FeStartException extends Exception {
        public FeStartException(String msg) {
            super(msg);
        }
    }

    public static class EnvVarNotSetException extends Exception {
        public EnvVarNotSetException(String msg) {
            super(msg);
        }
    }

    public static class NotInitException extends Exception {
        public NotInitException(String msg) {
            super(msg);
        }
    }

}
