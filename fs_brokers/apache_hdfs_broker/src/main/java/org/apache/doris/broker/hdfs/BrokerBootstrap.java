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

package org.apache.doris.broker.hdfs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.apache.commons.codec.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TProcessor;

import org.apache.doris.thrift.TPaloBrokerService;
import org.apache.doris.common.ThriftServer;

public class BrokerBootstrap {

    public static void main(String[] args) {
        try {
            final String brokerHome = System.getenv("BROKER_HOME");
            if (brokerHome == null || StringUtils.isEmpty(brokerHome)) {
                System.out.println("BROKER_HOME is not set, exit");
                return;
            }
            System.setProperty("BROKER_LOG_DIR", System.getenv("BROKER_LOG_DIR"));
            PropertyConfigurator.configure(brokerHome + "/conf/log4j.properties");
            Logger logger = Logger.getLogger(BrokerBootstrap.class);
            logger.info("starting apache hdfs broker....");
            new BrokerConfig().init(brokerHome + "/conf/apache_hdfs_broker.conf");

            TProcessor tprocessor = new TPaloBrokerService.Processor<TPaloBrokerService.Iface>(
                    new HDFSBrokerServiceImpl());
            ThriftServer server = new ThriftServer(BrokerConfig.broker_ipc_port, tprocessor);
            server.start();
            logger.info("starting apache hdfs broker....succeed");
            while (true) {
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static boolean createAndLockPidFile(String pidFilePath)
            throws IOException {
        File pid = new File(pidFilePath);
        try (RandomAccessFile file = new RandomAccessFile(pid, "rws")) {
            FileLock lock = file.getChannel().tryLock();
            if (lock == null) {
                return false;
            }

            // if system exit abnormally, file will not be deleted
            pid.deleteOnExit();

            String name = ManagementFactory.getRuntimeMXBean().getName();
            file.write(name.split("@")[0].getBytes(Charsets.UTF_8));
            return true;
        } catch (OverlappingFileLockException e) {
            return false;
        } catch (IOException e) {
            throw e;
        }
    }
}
