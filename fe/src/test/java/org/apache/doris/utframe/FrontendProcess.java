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

import org.apache.doris.PaloFe;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.utframe.ThreadManager.ThreadAlreadyExistException;

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
 * This is a singleton class. There can be only one instance of this class globally.
 * Usage:
 *      FrontendProcess frontendProcess = FrontendProcess.getInstance();
 *      frontendProcess.init(confMap);
 *      frontendProcess.start(new String[0]);
 *      
 *      ...
 *      
 *      frontendProcess.stop();
 *      
 * confMap is a instance of Map<String, String>.
 * Here you can add any FE configuration you want to add. For example:
 *      confMap.put("http_port", "8032");
 *      
 * FrontendProcess already contains a minimal set of FE configurations.
 * Any configuration in confMap will form the final fe.conf file with this minimal set.
 *      
 * 2 environment variable must be set:
 *      DORIS_HOME/ and PID_DIR/
 *      
 * There will be 3 directories under DORIS_HOME/:
 *      DORIS_HOME/conf/
 *      DORIS_HOME/log/
 *      DORIS_HOME/palo-meta/
 *      
 *  All these 3 directories will be cleared first.
 *  
 *  PID_DIR/ is used to save fe.pid file.
 */
public class FrontendProcess {
    public static final String FE_PROCESS = "fe";
    
    // the min set of fe.conf.
    private static final Map<String, String> MIN_FE_CONF;

    static {
        MIN_FE_CONF = Maps.newHashMap();
        MIN_FE_CONF.put("LOG_DIR", "${DORIS_HOME}/log");
        MIN_FE_CONF.put("JAVA_OPTS", "\"-Xmx4096m -XX:+UseMembar -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=0 -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:$DORIS_HOME/log/fe.gc.log.$DATE\"");
        MIN_FE_CONF.put("sys_log_level", "INFO");
        MIN_FE_CONF.put("meta_dir", "${DORIS_HOME}/palo-meta");
        MIN_FE_CONF.put("http_port", "8030");
        MIN_FE_CONF.put("rpc_port", "9020");
        MIN_FE_CONF.put("query_port", "9030");
        MIN_FE_CONF.put("edit_log_port", "9010");
        MIN_FE_CONF.put("priority_networks", "127.0.0.1/24");
        MIN_FE_CONF.put("sys_log_verbose_modules", "org");
    }

    private static class SingletonHolder {
        private static final FrontendProcess INSTANCE = new FrontendProcess();
    }

    public static FrontendProcess getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private boolean isInit = false;

    // init the fe process. This must be called before starting the frontend process.
    // 1. check if all neccessary environment variables are set.
    // 2. clear and create 3 dirs: log/ palo-meta/ conf/
    // 3. init fe.conf
    //      The content of "fe.conf" is a merge set of input `feConf` and MIN_FE_CONF
    public void init(Map<String, String> feConf) throws EnvVarNotSetException, IOException {
        if (isInit) {
            return;
        }

        final String dorisHome = System.getenv("DORIS_HOME");
        if (Strings.isNullOrEmpty(dorisHome)) {
            throw new EnvVarNotSetException("env DORIS_HOME is not set.");
        }
        
        final String pidDir = System.getenv("PID_DIR");
        if (Strings.isNullOrEmpty(pidDir)) {
            throw new EnvVarNotSetException("env PID_DIR is not set.");
        }
        // clear and create log dir
        createAndClearDir(dorisHome + "/log/");
        // clear and create meta dir
        createAndClearDir(dorisHome + "/palo-meta/");
        // clear and create conf dir
        createAndClearDir(dorisHome + "/conf/");
        // init fe.conf
        initFeConf(feConf);

        isInit = true;
    }

    private void initFeConf(Map<String, String> feConf) throws IOException {
        Map<String, String> conf = Maps.newHashMap(MIN_FE_CONF);
        conf.putAll(feConf);

        final String dorisHome = System.getenv("DORIS_HOME");
        final String confDir = dorisHome + "/conf/";

        PrintableMap<String, String> map = new PrintableMap<>(conf, "=", false, true, "");
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

    private static class FERunnable implements Runnable {
        private String[] args;

        public FERunnable(String[] args) {
            this.args = args;
        }

        @Override
        public void run() {
            PaloFe.main(args);
        }
    }

    // must call init() before start.
    public void start(String[] args) throws ThreadAlreadyExistException, FeStartException, NotInitException {
        if (!isInit) {
            throw new NotInitException("fe process is not initialized");
        }
        Thread feThread = new Thread(new FERunnable(args), FE_PROCESS);
        feThread.start();
        ThreadManager.getInstance().registerThread(feThread);
        // wait the catalog to be ready until timeout (30 seconds)
        waitForCatalogReady(30 * 1000);
        System.out.println("Fe process is started");
    }

    private void waitForCatalogReady(long timeoutMs) throws FeStartException {
        long left = timeoutMs;
        while (!Catalog.getCurrentCatalog().isReady() && left > 0) {
            System.out.println("catalog is not ready");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            left -= 5000;
        }

        if (left <= 0 && !Catalog.getCurrentCatalog().isReady()) {
            throw new FeStartException("fe start failed");
        }
    }

    public void stop() throws IOException {
        Thread feThread = ThreadManager.getInstance().removeThread(FE_PROCESS);
        if (feThread != null) {
            feThread.interrupt();
        }
        
        // clear the running dir
        final String dorisHome = System.getenv("DORIS_HOME");
        createAndClearDir(dorisHome);
        System.out.println("Fe process is stopped");
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
