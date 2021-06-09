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

import junit.framework.TestCase;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestFileSystemManager extends TestCase {

    private final String testHdfsHost = "hdfs://host:port";
    
    private FileSystemManager fileSystemManager;
    
    protected void setUp() throws Exception {
        fileSystemManager = new FileSystemManager();
    }
    
    @Test
    public void testGetFileSystemSuccess() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "user");
        properties.put("password", "passwd");
        BrokerFileSystem fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties);
        assertNotNull(fs);
        fs.getDFSFileSystem().close();
    }
    
    @Test
    public void testGetFileSystemForhHA() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "user");
        properties.put("password", "passwd");
        properties.put("fs.defaultFS", "hdfs://palo");
        properties.put("dfs.nameservices", "palo");
        properties.put("dfs.ha.namenodes.palo", "nn1,nn2");
        properties.put("dfs.namenode.rpc-address.palo.nn1", "host1:port1");
        properties.put("dfs.namenode.rpc-address.palo.nn2", "host2:port2");
        properties.put("dfs.client.failover.proxy.provider.bdos",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        BrokerFileSystem fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties);
        assertNotNull(fs);
        fs.getDFSFileSystem().close();
    }

    @Test
    public void testGetFileSystemForHAWithNoNames() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "user");
        properties.put("password", "passwd");
        properties.put("fs.defaultFS", "hdfs://palo");
        properties.put("dfs.nameservices", "palo");
        properties.put("dfs.client.failover.proxy.provider.bdos",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        boolean haveException = false;
        try {
            BrokerFileSystem fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties);
        } catch (BrokerException be) {
            haveException = true;
        }
        assertEquals(true, haveException);
    }

    @Test
    public void testGetFileSystemForHAWithNoRpcConfig() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "user");
        properties.put("password", "passwd");
        properties.put("fs.defaultFS", "hdfs://palo");
        properties.put("dfs.nameservices", "palo");
        properties.put("dfs.ha.namenodes.palo", "nn1,nn2");
        properties.put("dfs.namenode.rpc-address.palo.nn1", "host1:port1");
        properties.put("dfs.client.failover.proxy.provider.bdos",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        boolean haveException = false;
        try {
            BrokerFileSystem fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties);
        } catch (BrokerException be) {
            haveException = true;
        }
        assertEquals(true, haveException);
    }

    @Test
    public void testGetFileSystemForHAWithNoProviderArguments() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "user");
        properties.put("password", "passwd");
        properties.put("fs.defaultFS", "hdfs://palo");
        properties.put("dfs.nameservices", "palo");
        properties.put("dfs.ha.namenodes.palo", "nn1,nn2");
        properties.put("dfs.namenode.rpc-address.palo.nn1", "host1:port1");
        properties.put("dfs.namenode.rpc-address.palo.nn2", "host2:port2");
        BrokerFileSystem fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties);
        assertNotNull(fs);
        fs.getDFSFileSystem().close();
    }
    
    @Test
    public void testGetFileSystemWithoutPassword() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "user");
        // properties.put("password", "changeit");
        boolean haveException = false;
        try {
            BrokerFileSystem fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties);
        } catch (BrokerException e) {
            haveException = true;
        }
        assertEquals(true, haveException);
    }
    
    @Test
    public void testListPaths() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "user");
        properties.put("password", "passwd");
        
        List<TBrokerFileStatus> files2 = fileSystemManager.listPath(testHdfsHost + "/data/abc/logs/*.out",
                false, properties);
        assertEquals(files2.size(), 2);
    }
    
    @Test
    public void testOpenFileStream() {
        String realClientId = "realClientId";
        String fokeClientId = "fokeClientId";
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "root");
        properties.put("password", "passwd");
        
        String tempFile = testHdfsHost + "/data/abc/logs/" + System.nanoTime() + ".txt";
        boolean isPathExist = fileSystemManager.checkPathExist(tempFile, properties);
        assertFalse(isPathExist);
        
        // test openwriter
        TBrokerFD writeFd = fileSystemManager.openWriter(realClientId, tempFile, properties);
        // test write
        byte[] dataBuf = new byte[1256];
        fileSystemManager.pwrite(writeFd, 0, dataBuf);
        // close writer
        fileSystemManager.closeWriter(writeFd);
        isPathExist = fileSystemManager.checkPathExist(tempFile, properties);
        assertTrue(isPathExist);
        
        // check file size
        List<TBrokerFileStatus> files = fileSystemManager.listPath(tempFile, false, properties);
        assertEquals(files.size(), 1);
        assertFalse(files.get(0).isDir);
        assertEquals(1256, files.get(0).size);
        
        // rename file
        String tempFile2 = testHdfsHost + "/data/abc/logs/" + System.nanoTime() + ".txt";
        fileSystemManager.renamePath(tempFile, tempFile2, properties);
        isPathExist = fileSystemManager.checkPathExist(tempFile, properties);
        assertFalse(isPathExist);
        isPathExist = fileSystemManager.checkPathExist(tempFile2, properties);
        assertTrue(isPathExist);
        
        // read file
        TBrokerFD readFd = fileSystemManager.openReader(realClientId, tempFile2, 0, properties);
        ByteBuffer readData = fileSystemManager.pread(readFd, 0, 2222);
        assertEquals(1256, readData.limit());
        
        // read with exception
        boolean readDataHasError = false;
        try {
            ByteBuffer readData2 = fileSystemManager.pread(readFd, 1, 2222);
        } catch (BrokerException e) {
            readDataHasError = true;
            assertEquals(TBrokerOperationStatusCode.INVALID_INPUT_OFFSET, e.errorCode);
        }
        assertEquals(true, readDataHasError);
        
        // delete file
        fileSystemManager.deletePath(tempFile2, properties);
        isPathExist = fileSystemManager.checkPathExist(tempFile2, properties);
        assertFalse(isPathExist);
    }

    @Test
    public void testGetFileSystemForS3aScheme() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("fs.s3a.access.key", "accessKey");
        properties.put("fs.s3a.secret.key", "secretKey");
        properties.put("fs.s3a.endpoint", "s3.test.com");
        BrokerFileSystem fs = fileSystemManager.getFileSystem("s3a://testbucket/data/abc/logs", properties);
        assertNotNull(fs);
        fs.getDFSFileSystem().close();
    }
}
