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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.log4j.Logger;

import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerOperationStatusCode;

public class ClientContextManager {

    private static Logger logger = Logger
            .getLogger(ClientContextManager.class.getName());
    private ScheduledExecutorService executorService;
    private ConcurrentHashMap<String, ClientResourceContext> clientContexts;
    private ConcurrentHashMap<TBrokerFD, String> fdToClientMap;
    private int clientExpirationSeconds = BrokerConfig.client_expire_seconds;
    
    public ClientContextManager(ScheduledExecutorService executorService) {
        clientContexts = new ConcurrentHashMap<>();
        fdToClientMap = new ConcurrentHashMap<>();
        this.executorService = executorService;
        this.executorService.schedule(new CheckClientExpirationTask(), 0, TimeUnit.SECONDS);
    }
    
    public void onPing(String clientId) {
        if (!clientContexts.containsKey(clientId)) {
            clientContexts.putIfAbsent(clientId, new ClientResourceContext(clientId));
        }
        ClientResourceContext clientContext = clientContexts.get(clientId);
        clientContext.updateLastPingTime();
    }
    
    public synchronized void putNewOutputStream(String clientId, TBrokerFD fd, FSDataOutputStream fsDataOutputStream, 
            BrokerFileSystem brokerFileSystem) {
        if (!clientContexts.containsKey(clientId)) {
            clientContexts.putIfAbsent(clientId, new ClientResourceContext(clientId));
        }
        ClientResourceContext clientContext = clientContexts.get(clientId);
        clientContext.putOutputStream(fd, fsDataOutputStream, brokerFileSystem);
        fdToClientMap.putIfAbsent(fd, clientId);
    }
    
    public synchronized void putNewInputStream(String clientId, TBrokerFD fd, FSDataInputStream fsDataInputStream, 
            BrokerFileSystem brokerFileSystem) {
        if (!clientContexts.containsKey(clientId)) {
            clientContexts.putIfAbsent(clientId, new ClientResourceContext(clientId));
        }
        ClientResourceContext clientContext = clientContexts.get(clientId);
        clientContext.putInputStream(fd, fsDataInputStream, brokerFileSystem);
        fdToClientMap.putIfAbsent(fd, clientId);
    }
    
    public synchronized FSDataInputStream getFsDataInputStream(TBrokerFD fd) {
        String clientId = fdToClientMap.get(fd);
        if (clientId == null) {
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                    "the fd is not owned by client {}", clientId);
        }
        ClientResourceContext clientContext = clientContexts.get(clientId);
        FSDataInputStream fsDataInputStream = clientContext.getInputStream(fd);
        return fsDataInputStream;
    }
    
    public synchronized FSDataOutputStream getFsDataOutputStream(TBrokerFD fd) {
        String clientId = fdToClientMap.get(fd);
        if (clientId == null) {
            throw new BrokerException(TBrokerOperationStatusCode.TARGET_STORAGE_SERVICE_ERROR, 
                    "the fd is not owned by client {}", clientId);
        }
        ClientResourceContext clientContext = clientContexts.get(clientId);
        FSDataOutputStream fsDataOutputStream = clientContext.getOutputStream(fd);
        return fsDataOutputStream;
    }
    
    public synchronized void removeInputStream(TBrokerFD fd) {
        String clientId = fdToClientMap.remove(fd);
        if (clientId == null) {
            return;
        }
        ClientResourceContext clientContext = clientContexts.get(clientId);
        BrokerInputStream brokerInputStream = clientContext.inputStreams.remove(fd);
        try {
            if (brokerInputStream != null) {
                brokerInputStream.inputStream.close();
            }
        } catch (Exception e) {
            logger.error("errors while close file data input stream", e);
        }
    }
    
    public synchronized void removeOutputStream(TBrokerFD fd) {
        String clientId = fdToClientMap.remove(fd);
        if (clientId == null) {
            return;
        }
        ClientResourceContext clientContext = clientContexts.get(clientId);
        BrokerOutputStream brokerOutputStream = clientContext.outputStreams.remove(fd);
        try {
            if (brokerOutputStream != null) {
                brokerOutputStream.outputStream.close();
            }
        } catch (Exception e) {
            logger.error("errors while close file data output stream", e);
        }
    }
    
    class CheckClientExpirationTask implements Runnable {
        @Override
        public void run() {
            try {
                for (ClientResourceContext clientContext : clientContexts.values()) {
                    if (System.currentTimeMillis() - clientContext.lastPingTimestamp > clientExpirationSeconds * 1000) {
                        for (TBrokerFD fd : clientContext.inputStreams.keySet()) {
                            ClientContextManager.this.removeInputStream(fd);
                        }
                        for (TBrokerFD fd : clientContext.outputStreams.keySet()) {
                            ClientContextManager.this.removeOutputStream(fd);
                        }
                        clientContexts.remove(clientContext.clientId);
                        logger.info("client [" + clientContext.clientId 
                                + "] is expired, remove it from contexts. last ping time is " 
                                + clientContext.lastPingTimestamp);
                    }
                }
            } finally {
                ClientContextManager.this.executorService.schedule(this, 60, TimeUnit.SECONDS);
            }
        }
    }
    
    private static class BrokerOutputStream {
        
        private final FSDataOutputStream outputStream;
        private final BrokerFileSystem brokerFileSystem;
        
        public BrokerOutputStream(FSDataOutputStream outputStream, BrokerFileSystem brokerFileSystem) {
            this.outputStream = outputStream;
            this.brokerFileSystem = brokerFileSystem;
            this.brokerFileSystem.updateLastUpdateAccessTime();
        }
        
        public FSDataOutputStream getOutputStream() {
            this.brokerFileSystem.updateLastUpdateAccessTime();
            return outputStream;
        }
        
        public void updateLastUpdateAccessTime() {
            this.brokerFileSystem.updateLastUpdateAccessTime();
        }
    }
    
    private static class BrokerInputStream {
        
        private final FSDataInputStream inputStream;
        private final BrokerFileSystem brokerFileSystem;
        
        public BrokerInputStream(FSDataInputStream inputStream, BrokerFileSystem brokerFileSystem) {
            this.inputStream = inputStream;
            this.brokerFileSystem = brokerFileSystem;
            this.brokerFileSystem.updateLastUpdateAccessTime();
        }
        
        public FSDataInputStream getInputStream() {
            this.brokerFileSystem.updateLastUpdateAccessTime();
            return inputStream;
        }
        
        public void updateLastUpdateAccessTime() {
            this.brokerFileSystem.updateLastUpdateAccessTime();
        }
    }
    
    static class ClientResourceContext {

        private String clientId;
        private ConcurrentHashMap<TBrokerFD, BrokerInputStream> inputStreams;
        private ConcurrentHashMap<TBrokerFD, BrokerOutputStream> outputStreams;
        private long lastPingTimestamp;
        
        public ClientResourceContext(String clientId) {
            this.clientId = clientId;
            this.inputStreams = new ConcurrentHashMap<>();
            this.outputStreams = new ConcurrentHashMap<>();
            this.lastPingTimestamp = System.currentTimeMillis();
        }
        
        public void putInputStream(TBrokerFD fd, FSDataInputStream inputStream, BrokerFileSystem fileSystem) {
            inputStreams.putIfAbsent(fd, new BrokerInputStream(inputStream, fileSystem));
        }
        
        public void putOutputStream(TBrokerFD fd, FSDataOutputStream outputStream, BrokerFileSystem fileSystem) {
            outputStreams.putIfAbsent(fd, new BrokerOutputStream(outputStream, fileSystem));
        }
        
        public FSDataInputStream getInputStream(TBrokerFD fd) {
            BrokerInputStream brokerInputStream = inputStreams.get(fd);
            if (brokerInputStream != null) {
                return brokerInputStream.getInputStream();
            }
            return null;
        }
        
        public FSDataOutputStream getOutputStream(TBrokerFD fd) {
            BrokerOutputStream brokerOutputStream = outputStreams.get(fd);
            if (brokerOutputStream != null) {
                return brokerOutputStream.getOutputStream();
            }
            return null;
        }
        
        public void updateLastPingTime() {
            this.lastPingTimestamp = System.currentTimeMillis();
            // Should we also update the underline filesystem? maybe it is time cost
            for (BrokerInputStream brokerInputStream : inputStreams.values()) {
                brokerInputStream.updateLastUpdateAccessTime();
            }
            
            for (BrokerOutputStream brokerOutputStream : outputStreams.values()) {
                brokerOutputStream.updateLastUpdateAccessTime();
            }
        }
    }
}


