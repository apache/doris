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

package com.baidu.palo.broker.bos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.baidu.palo.broker.TBrokerFD;
import com.baidu.palo.broker.TBrokerFileStatus;
import com.baidu.palo.broker.TBrokerOperationStatusCode;
import com.baidu.palo.broker.bos.BrokerException;
import com.baidu.palo.broker.bos.BrokerFileSystem;
import com.baidu.palo.broker.bos.FileSystemManager;

import junit.framework.TestCase;

public class TestFileSystemManager extends TestCase {

    private final String testBosBucket = "bos://dir";
    private final String accessKey = "accessKey";
    private final String secretKey = "secretKey";
    private final String bosEndpoint = "http://uri";
    
    private FileSystemManager fileSystemManager;
    
    
    protected void setUp() throws Exception {
        fileSystemManager = new FileSystemManager();
    }
    
    @Test
    public void testGetFileSystemSuccess() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(FileSystemManager.BOS_ACCESS_KEY, accessKey);
        properties.put(FileSystemManager.BOS_SECRET_ACCESS_KEY, secretKey);
        properties.put(FileSystemManager.BOS_ENDPOINT, bosEndpoint);
        BrokerFileSystem fs = fileSystemManager.getFileSystem(testBosBucket 
                + "/star-schema-benchmark/customer.tbl", properties);
        assertNotNull(fs);
        fs.getDFSFileSystem().close();
    }
    
    @Test
    public void testGetFileSystemWithoutPassword() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "root");
        // properties.put("password", "changeit");
        boolean haveException = false;
        try {
            BrokerFileSystem fs = fileSystemManager.getFileSystem(testBosBucket + "/data/abc/logs", properties);
        } catch (BrokerException e) {
            haveException = true;
        }
        assertEquals(true, haveException);
    }
    
    @Test
    public void testListPaths() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(FileSystemManager.BOS_ACCESS_KEY, accessKey);
        properties.put(FileSystemManager.BOS_SECRET_ACCESS_KEY, secretKey);
        properties.put(FileSystemManager.BOS_ENDPOINT, bosEndpoint);
        
        List<TBrokerFileStatus> files2 = fileSystemManager.listPath(testBosBucket + "/elasticsearch-*", properties);
        assertEquals(files2.size(), 2);
    }
    
    // @Test
    public void testOpenFileStream() {
        String realClientId = "realClientId";
        String fokeClientId = "fokeClientId";
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(FileSystemManager.BOS_ACCESS_KEY, accessKey);
        properties.put(FileSystemManager.BOS_SECRET_ACCESS_KEY, secretKey);
        properties.put(FileSystemManager.BOS_ENDPOINT, bosEndpoint);
        
        String tempFile = testBosBucket + "/" + System.nanoTime() + ".txt";
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
        List<TBrokerFileStatus> files = fileSystemManager.listPath(tempFile, properties);
        assertEquals(files.size(), 1);
        assertFalse(files.get(0).isDir);
        assertEquals(1256, files.get(0).size);
        
        // rename file
        
        String tempFile2 = testBosBucket + "/" + System.nanoTime() + ".txt";
        fileSystemManager.renamePath(tempFile, tempFile2, properties);
        isPathExist = fileSystemManager.checkPathExist(tempFile, properties);
        assertFalse(isPathExist);
        isPathExist = fileSystemManager.checkPathExist(tempFile2, properties);
        assertTrue(isPathExist);
        
        // String tempFile2 = tempFile;
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
        assertEquals(false, readDataHasError);
        
        // delete file
        fileSystemManager.deletePath(tempFile2, properties);
        isPathExist = fileSystemManager.checkPathExist(tempFile2, properties);
        assertFalse(isPathExist);
    }
}
