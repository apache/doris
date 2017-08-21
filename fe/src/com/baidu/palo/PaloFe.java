// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo;

import com.baidu.palo.common.Config;
import com.baidu.palo.common.Log4jConfig;
import com.baidu.palo.http.HttpServer;
import com.baidu.palo.qe.QeService;
import com.baidu.palo.service.ExecuteEnv;
import com.baidu.palo.service.FeServer;
import com.baidu.palo.service.FrontendOptions;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

/**
 * Created by zhaochun on 14-9-22.
 */
public class PaloFe {
    private static final Logger LOG = LogManager.getLogger(PaloFe.class);

    // entrance for palo frontend
    public static void main(String[] args) {
        try {
            final String paloHome = System.getenv("PALO_HOME");
            if (Strings.isNullOrEmpty(paloHome)) {
                System.out.println("env PALO_HOME is not set.");
                return;
            }

            // pid file
            if (!createAndLockPidFile(paloHome + "/bin/fe.pid")) {
                throw new IOException("pid file is already locked.");
            }

            // set dns cache ttl
            java.security.Security.setProperty("networkaddress.cache.ttl" , "60");

            // init config
            new Config().init(paloHome + "/conf/fe.conf");
            Log4jConfig.initLogging();

            LOG.info("Palo FE start");

            FrontendOptions.init();
            ExecuteEnv.setup();
            ExecuteEnv env = ExecuteEnv.getInstance();

            // setup qe server
            QeService qeService = new QeService(Config.query_port, env.getScheduler());

            // start query http server
            HttpServer httpServer = new HttpServer(qeService, Config.http_port);
            httpServer.setup();
            httpServer.start();

            // setup fe server
            // Http server must be started before setup and start feServer
            // Because the frontend need to download image file using http server from other frontends.
            FeServer feServer = new FeServer(Config.rpc_port);
            feServer.setup(args);

            // start qe server and fe server
            qeService.start();
            feServer.start();

            while (true) {
                Thread.sleep(2000);
            }
        } catch (Throwable exception) {
            exception.printStackTrace();
            System.exit(-1);
        }
    } // end PaloFe main()

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
