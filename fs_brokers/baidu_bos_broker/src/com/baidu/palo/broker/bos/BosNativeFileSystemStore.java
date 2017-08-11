// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.broker.bos;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;

import com.baidubce.BceServiceException;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;



class BosNativeFileSystemStore implements NativeFileSystemStore {

    private BosClient bosClient;
    private String bucketName;

    private long multipartBlockSize;
    private boolean multipartEnabled;
    private int multiUploadThreadNum;
    private Configuration conf = null;
    static final long MAX_PART_SIZE = (long)5 * 1024 * 1024 * 1024;

    public static final Log LOG =
            LogFactory.getLog(BosNativeFileSystemStore.class);

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        this.conf = conf;
        String accessKey = null;
        String secretAccessKey = null;
        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            int index = userInfo.indexOf(':');
            if (index != -1) {
                accessKey = userInfo.substring(0, index);
                secretAccessKey = userInfo.substring(index + 1);
            } else {
                accessKey = userInfo;
            }
        }

        if (accessKey == null && secretAccessKey == null) {
            accessKey = conf.get("fs.bos.access.key");
            secretAccessKey = conf.get("fs.bos.secret.access.key");
        }

        if (accessKey == null || secretAccessKey == null) {
            throw new IllegalArgumentException("accessKey and secretAccessKey should not be null");
        }

        String endPoint = conf.get("fs.bos.endpoint");
        BosClientConfiguration config = new BosClientConfiguration();
        config.setCredentials(new DefaultBceCredentials(accessKey, secretAccessKey));
        config.setEndpoint(endPoint);
        int maxConnections = conf.getInt("fs.bos.max.connections", 1000);
        config.setMaxConnections(maxConnections);
        bosClient = new BosClient(config);

        multipartEnabled =
                conf.getBoolean("fs.bos.multipart.store.enabled", true);
        multipartBlockSize = Math.min(
                conf.getLong("fs.bos.multipart.store.block.size", 64 * 1024 * 1024),
                MAX_PART_SIZE);
        multiUploadThreadNum = conf.getInt("fs.bos.multipart.store.cocurrent.size", 5);

        bucketName = uri.getHost();
    }

    @Override
    public void storeFile(String key, File file)
            throws IOException {
        if (multipartEnabled && file.length() > multipartBlockSize) {
            storeLargeFile(key, file);
            return;
        }

        try {
            bosClient.putObject(bucketName, key, file);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
    }

    public void storeLargeFile(String key, File file)
            throws IOException {
        int partCount = calPartCount(file);
        if(partCount <= 1){
            throw new IllegalArgumentException("file size " + file.length()
                    + "is less than part size " + this.multipartBlockSize);
        }

        ExecutorService pool = null;
        try {
            String uploadId = initMultipartUpload(key);
            bosClient.listMultipartUploads((new ListMultipartUploadsRequest(bucketName)));
            pool = Executors.newFixedThreadPool(multiUploadThreadNum);
            List<PartETag> eTags = Collections.synchronizedList(new ArrayList<PartETag>());
            List<Future> futureList = new LinkedList<Future>();
            for (int i = 0; i < partCount; i++) {
                long start = multipartBlockSize * i;
                long curPartSize = multipartBlockSize < file.length() - start ? 
                        multipartBlockSize : file.length() - start;

                futureList.add(
                        pool.submit(new UploadPartThread(
                                bosClient,
                                bucketName,
                                key,
                                file,
                                uploadId,
                                i + 1,
                                multipartBlockSize * i,
                                curPartSize,
                                eTags,
                                this.conf)));
            }

            // wait UploadPartThread threads done
            try {
                for (Future future : futureList) {
                    future.get();
                }
            } catch (Exception e) {
                LOG.warn("catch exceptin when waiting UploadPartThread done: ", e);
            }

            if (eTags.size() != partCount) {
                throw new IllegalStateException("Multipart filed!");
            }

            bosClient.listParts(new ListPartsRequest(bucketName, key, uploadId));

            completeMultipartUpload(bucketName, key, uploadId, eTags);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        } finally {
            if (pool != null) {
                pool.shutdownNow();
            }
        }
    }

    private void completeMultipartUpload(String bucketName, String key, String uploadId, List<PartETag> eTags)
            throws BceServiceException {
        Collections.sort(eTags, new Comparator<PartETag>() {
            @Override
            public int compare(PartETag arg1, PartETag arg2) {
                PartETag part1 = arg1;
                PartETag part2 = arg2;
                return part1.getPartNumber() - part2.getPartNumber();
            }
        });
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucketName, key, uploadId, eTags);
        bosClient.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private static class UploadPartTask implements Callable<UploadPartResponse> {
        private BosClient client;
        private UploadPartRequest uploadPartRequest;

        public UploadPartTask(BosClient client, String bucket, String object,
                String uploadId, InputStream in, long size, int partId) {
            this.client = client;
            UploadPartRequest uploadPartRequest = new UploadPartRequest();
            uploadPartRequest.setBucketName(bucket);
            uploadPartRequest.setKey(object);
            uploadPartRequest.setUploadId(uploadId);
            uploadPartRequest.setInputStream(in);
            uploadPartRequest.setPartSize(size);
            uploadPartRequest.setPartNumber(partId);
            this.uploadPartRequest = uploadPartRequest;
        }

        @Override
        public UploadPartResponse call() throws Exception {
            return this.client.uploadPart(this.uploadPartRequest);
        }
    }

    private static class UploadPartThread implements Runnable {
        private Configuration conf;
        private File uploadFile;
        private String bucket;
        private String object;
        private long start;
        private long size;
        private List<PartETag> eTags;
        private int partId;
        private BosClient client;
        private String uploadId;
        private InputStream in;
        private Map<Integer, BosBlockBuffer> dataMap;
        private Map<Integer, Exception> exceptionMap;

        UploadPartThread(BosClient client,String bucket, String object,
                File uploadFile,String uploadId, int partId,
                long start, long partSize, List<PartETag> eTags, Configuration conf) throws IOException {
            this.uploadFile = uploadFile;
            this.bucket = bucket;
            this.object = object;
            this.start = start;
            this.size = partSize;
            this.eTags = eTags;
            this.partId = partId;
            this.client = client;
            this.uploadId = uploadId;
            this.in = new FileInputStream(this.uploadFile);
            this.in.skip(this.start);
            this.conf = conf;
        }

        UploadPartThread(BosClient client, String bucket, String object,
                 InputStream in, String uploadId, int partId,
                 long partSize, List<PartETag> eTags, Map<Integer, BosBlockBuffer> dataMap,
                 Map<Integer, Exception> exceptionMap, Configuration conf) throws IOException {
            this.bucket = bucket;
            this.object = object;
            this.size = partSize;
            this.eTags = eTags;
            this.partId = partId;
            this.client = client;
            this.uploadId = uploadId;
            this.in = in;
            this.dataMap = dataMap;
            this.exceptionMap = exceptionMap;
            this.conf = conf;
        }

        private int getAttempts() {
            return Math.max(this.conf.getInt("fs.bos.multipart.uploads.attempts", 5), 3);
        }

        private double getTimeoutFactor() {
            double minFactor = 10.0;
            return Math.max(this.conf.getFloat("fs.bos.multipart.uploads.factor", (float)minFactor), minFactor);
        }

        private long getUploadSpeed() {
            long defaultUploadSpeed = 10 * 1024 * 1024;
            long uploadSpeed = this.conf.getLong("fs.bos.multipart.uploads.speed", defaultUploadSpeed);
            return uploadSpeed <= 0 ? defaultUploadSpeed : uploadSpeed;
        }

        private long getTimeout() {
            long uploadSpeed = this.getUploadSpeed();
            double factor = this.getTimeoutFactor();
            long minTimeout = 10000;
            double uploadTime = (double)this.size * 1000 / uploadSpeed;
            return Math.max((long)(uploadTime * factor), minTimeout);
        }

        @Override
        public void run() {
            try {
                String partInfo = object + " with part id " + partId;
                LOG.info("start uploading " + partInfo);
                int attempts = this.getAttempts();
                long timeout = this.getTimeout();
                boolean isSuccess = false;
                UploadPartResponse uploadPartResult = null;
                for (int i = 0; i < attempts; i++) {
                    try {
                        in.reset();
                    } catch(Exception e) {
                        LOG.warn("failed to reset inputstream for " + partInfo);
                        break;
                    }
                    ExecutorService subPool = Executors.newSingleThreadExecutor();
                    UploadPartTask task = new UploadPartTask(this.client, bucket, object, uploadId, in, size, partId);
                    Future<UploadPartResponse> future = subPool.submit(task);
                    try {
                        LOG.debug("[upload attempt " + i + " timeout: " + timeout + "] start uploadPart " + partInfo);
                        uploadPartResult = future.get(timeout, TimeUnit.MILLISECONDS);
                        LOG.debug("[upload attempt " + i + " timeout: " + timeout + "] end uploadPart " + partInfo);
                        isSuccess = true;
                        break;
                    } catch (Exception e) {
                        future.cancel(true);
                        LOG.debug("[upload attempt " + i + "] failed to upload " + partInfo + ": " + e.getMessage());
                    } finally {
                        if (subPool != null) {
                            subPool.shutdownNow();
                        }
                    }
                }

                if (!isSuccess) {
                    BceServiceException e = new BceServiceException("failed to upload part " + partInfo);
                    exceptionMap.put(partId, e);
                    LOG.warn("failed to upload " + partInfo + " after " + attempts + " attempts");
                } else {
                    eTags.add(uploadPartResult.getPartETag());
                    if (dataMap != null) {
                        synchronized (dataMap) {
                            dataMap.get(partId).clear();
                            dataMap.notify();
                        }
                    }
                    LOG.info("end uploading key " + partInfo);
                }
            } catch (BceServiceException boe) {
                LOG.error("BOS Error code: " + boe.getErrorCode() + "; BOS Error message: " + boe.getErrorMessage());
                boe.printStackTrace();
                exceptionMap.put(partId, boe);
            } catch (Exception e) {
                LOG.error("Exception catched: ", e);
                e.printStackTrace();
                exceptionMap.put(partId, e);
            } finally {
                if (in != null) {
                    try { in.close();
                    } catch (Exception e) {}
                }
            }
        }
    }

    private int calPartCount(File file){
        int partCount = (int) (file.length() / multipartBlockSize);
        if(file.length() % multipartBlockSize != 0){
            partCount++;
        }
        return partCount;
    }

    private String initMultipartUpload(String key) throws BceServiceException {
        InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                new InitiateMultipartUploadRequest(bucketName, key);
        InitiateMultipartUploadResponse initiateMultipartUploadResult =
                bosClient.initiateMultipartUpload(initiateMultipartUploadRequest);
        return initiateMultipartUploadResult.getUploadId();
    }

    @Override
    public void storeEmptyFile(String key) throws IOException {
        InputStream in = null;
        try {
            in = new ByteArrayInputStream(new byte[0]);
            ObjectMetadata objectMeta = new ObjectMetadata();
            objectMeta.setContentType("binary/octet-stream");
            objectMeta.setContentLength(0);
            bosClient.putObject(bucketName, key, in, objectMeta);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
    }

    static abstract class BosOutputStream extends OutputStream {
        public String key;
        public Configuration conf;

        public int blockSize;
        public BosBlockBuffer currBlock;
        public int blkIndex = 1;
        public boolean closed;
        public long filePos = 0;
        public int bytesWrittenToBlock = 0;

        public ExecutorService pool = null;
        public String uploadId = null;
        public int concurrentUpload;
        public List<PartETag> eTags = Collections.synchronizedList(new ArrayList<PartETag>());
        public List<Future> futureList = new LinkedList<Future>();
        public final Map<Integer, BosBlockBuffer> blocksMap = new HashMap<Integer, BosBlockBuffer>();
        public Map<Integer, Exception> exceptionMap = new HashMap<Integer, Exception>();

        public BosOutputStream(String key, Configuration conf) {
            this.key = key;
            this.conf = conf;
            this.init();
        }

        public abstract void init();
    }

    @Override
    public OutputStream createFile(String key, Configuration conf, final int bufferSize)
            throws IOException {
        return new BosOutputStream(key, conf) {

            private void createBlockBufferIfNull() throws IOException {
                if (this.currBlock == null) {
                    LOG.debug("Ready to create Block Buffer ! key is " + this.key 
                            + ". blkIndex is " + this.blkIndex + ". blockSize is " + this.blockSize);

                    try {
                        this.currBlock = new BosBlockBuffer(this.key, this.blkIndex, this.blockSize);
                    } catch(Throwable throwable) {
                        LOG.warn("catch exception when allocating BosBlockBuffer: ", throwable);
                        throw new IOException("catch exception when allocating BosBlockBuffer: ", throwable);
                    }

                    this.bytesWrittenToBlock = 0;
                    this.blkIndex++;

                    LOG.debug("Block Buffer created ! key is " + this.key 
                            + ". blkIndex is " + this.blkIndex + ". blockSize is " + this.blockSize);
                }
            }

            @Override
            public synchronized void write(int b) throws IOException {
                if (this.closed) {
                    throw new IOException("Stream closed");
                }

                flush();
                createBlockBufferIfNull();

                this.currBlock.outBuffer.write(b);
                this.bytesWrittenToBlock++;
                this.filePos++;
            }

            @Override
            public synchronized void write(byte b[], int off, int len) throws IOException {
                if (this.closed) {
                    throw new IOException("Stream closed");
                }

                while (len > 0) {
                    flush();
                    createBlockBufferIfNull();

                    int remaining = this.blockSize - this.bytesWrittenToBlock;
                    int toWrite = Math.min(remaining, len);
                    this.currBlock.outBuffer.write(b, off, toWrite);
                    this.bytesWrittenToBlock += toWrite;
                    this.filePos += toWrite;
                    off += toWrite;
                    len -= toWrite;
                }
            }

            @Override
            public synchronized void flush() throws IOException {
                if (this.closed) {
                    throw new IOException("Stream closed");
                }

                if (this.bytesWrittenToBlock >= this.blockSize) {
                    endBlock();
                }
            }

            private synchronized void endBlock() throws IOException {
                if (this.currBlock == null) {
                    return;
                }

                LOG.debug("Enter endBlock() ! key is " + this.key 
                        + ". blkIndex is " + this.blkIndex + ". blockSize is " + this.blockSize 
                        + ". Size of eTags is " + this.eTags.size() 
                        + ". concurrentUpload is " + this.concurrentUpload);

                //
                // Move outBuffer to inBuffer
                //
                this.currBlock.outBuffer.close();
                this.currBlock.moveData();

                //
                // Block this when too many active UploadPartThread
                //
                if ((this.blkIndex - this.eTags.size()) > this.concurrentUpload) {
                    while (true) {
                        synchronized (this.blocksMap) {
                            try {
                                this.blocksMap.wait(10);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        if ((this.blkIndex - this.eTags.size()) <= this.concurrentUpload) {
                            break;
                        }
                        if (this.exceptionMap.size() > 0) {
                            //
                            // Exception happens during upload
                            //
                            throw new IOException("Exception happens during upload");
                        }
                    }
                }

                if (this.exceptionMap.size() > 0) {
                    //
                    // Exception happens during upload
                    //
                    throw new IOException("Exception happens during upload");
                }

                //
                // New a UploadPartThread and add it into ExecuteService pool
                //
                UploadPartThread upThread = new UploadPartThread(
                        bosClient,
                        bucketName,
                        this.key,
                        this.currBlock.inBuffer,
                        this.uploadId,
                        this.currBlock.getBlkId(),
                        this.currBlock.inBuffer.getLength(),
                        this.eTags,
                        this.blocksMap,
                        this.exceptionMap,
                        this.conf
                );
                this.futureList.add(pool.submit(upThread));

                //
                // Clear current BosBlockBuffer
                //
                this.blocksMap.put(this.currBlock.getBlkId(), this.currBlock);
                this.currBlock = null;
            }

            @Override
            public void init() {
                this.blockSize = this.conf.getInt("fs.bos.multipart.uploads.block.size", 10 * 1024 * 1024);
                this.concurrentUpload = this.conf.getInt("fs.bos.multipart.uploads.cocurrent.size", 10);
                this.uploadId = initMultipartUpload(this.key);
                this.pool = Executors.newFixedThreadPool(this.concurrentUpload);
            }

            @Override
            public synchronized void close() throws IOException {
                if (this.closed) {
                    return;
                }

                if (this.filePos == 0) {
                    storeEmptyFile(this.key);
                } else {
                    if (this.bytesWrittenToBlock != 0) {
                        endBlock();
                    }

                    LOG.debug("start to wait upload part threads done. futureList size: " + futureList.size());
                    // wait UploadPartThread threads done
                    try {
                        int index = 0;
                        for (Future future : this.futureList) {
                            future.get();
                            index += 1;
                            LOG.debug("future.get() index: " + index + " is done");
                        }
                    } catch (Exception e) {
                        LOG.warn("catch exceptin when waiting UploadPartThread done: ", e);
                    }

                    LOG.debug("success to wait upload part threads done");

                    LOG.debug("Size of eTags is " + this.eTags.size() + ". blkIndex is " + this.blkIndex);

                    if (this.eTags.size() != this.blkIndex - 1) {
                        throw new IllegalStateException("Multipart failed!");
                    }

                    try {
                        completeMultipartUpload(bucketName, this.key, this.uploadId, this.eTags);
                    } catch (BceServiceException e) {
                        handleBosServiceException(e);
                    } finally {
                        if (this.pool != null) {
                            this.pool.shutdownNow();
                        }
                    }
                }

                super.close();
                this.closed = true;
            }
        };
    }

    @Override
    public FileMetadata retrieveMetadata(String key) throws IOException {
        ObjectMetadata meta = null;
        try {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Getting metadata for key: " + key + " from bucket:" + bucketName);
            }
            meta = bosClient.getObjectMetadata(bucketName, key);
            return new FileMetadata(key, meta.getContentLength(), meta.getLastModified().getTime());
        } catch (BceServiceException e) {
            handleBosServiceException(key, e);
            return null; //never returned - keep compiler happy
        }
    }

    /**
     * @param key
     * The key is the object name that is being retrieved from the S3 bucket
     * @return
     * This method returns null if the key is not found
     * @throws IOException
     */
    @Override
    public InputStream retrieve(String key) throws IOException {
        try {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Getting key: " + key + " from bucket:" + bucketName);
            }
            BosObject object = bosClient.getObject(bucketName, key);
            return object.getObjectContent();
        } catch (BceServiceException e) {
            handleBosServiceException(key, e);
            return null; //return null if key not found
        }
    }

    /**
     *
     * @param key
     * The key is the object name that is being retrieved from the S3 bucket
     * @return
     * This method returns null if the key is not found
     * @throws IOException
     */

    @Override
    public InputStream retrieve(String key, long byteRangeStart)
            throws IOException {
        try {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Getting key: " + key + " from bucket:" 
                        + bucketName + " with byteRangeStart: " 
                        + byteRangeStart);
            }
            ObjectMetadata meta = bosClient.getObjectMetadata(bucketName, key);
            GetObjectRequest request = new GetObjectRequest(bucketName, key);
            //Due to the InvalidRange exception of bos, we shouldn't set range for empty file
            if (meta.getContentLength() != 0) {
              request.setRange(byteRangeStart, Long.MAX_VALUE);
            }
            BosObject object = bosClient.getObject(request);
            return object.getObjectContent();
        } catch (BceServiceException e) {
            handleBosServiceException(key, e);
            return null; //return null if key not found
        }
    }

    @Override
    public PartialListing list(String prefix, int maxListingLength)
            throws IOException {
        return list(prefix, maxListingLength, null);
    }

    public PartialListing list(String prefix, int maxListingLength, String priorLastKey) throws IOException {
        return list(prefix, BaiduBosFileSystem.PATH_DELIMITER, maxListingLength, priorLastKey);
    }

    public PartialListing list(String prefix, int maxListingLength, 
            String priorLastKey, String delimiter) throws IOException {
        return list(prefix, delimiter, maxListingLength, priorLastKey);
    }

    /**
     *
     * @return
     * This method returns null if the list could not be populated
     * due to S3 giving ServiceException
     * @throws IOException
     */

    private PartialListing list(String prefix, String delimiter,
            int maxListingLength, String priorLastKey) throws IOException {
        try {
            if (prefix != null && prefix.length() > 0 
                    && !prefix.endsWith(BaiduBosFileSystem.PATH_DELIMITER)) {
                prefix += BaiduBosFileSystem.PATH_DELIMITER;
            }

            ListObjectsRequest request = new ListObjectsRequest(bucketName);
            request.setPrefix(prefix);
            request.setMarker(priorLastKey);
            request.setDelimiter(delimiter);
            request.setMaxKeys(maxListingLength);

            ListObjectsResponse objects = bosClient.listObjects(request);

            List<FileMetadata> fileMetadata =
                    new LinkedList<FileMetadata>();
            for (int i = 0; i < objects.getContents().size(); i++) {
                BosObjectSummary object = objects.getContents().get(i);
                if (object.getKey() != null && object.getKey().length() > 0) {
                    fileMetadata.add(new FileMetadata(object.getKey(),
                            object.getSize(), object.getLastModified().getTime()));
                }
            }
            
            String[] commonPrefix = null;
            if(objects.getCommonPrefixes() != null) {
                commonPrefix = objects.getCommonPrefixes().toArray(new String[objects.getCommonPrefixes().size()]);
            }

            return new PartialListing(
                    objects.getNextMarker(),
                    fileMetadata.toArray(new FileMetadata[fileMetadata.size()]),
                    commonPrefix);
        } catch (BceServiceException e) {
            handleBosServiceException(e);
            return null; //never returned - keep compiler happy
        }
    }

    @Override
    public void delete(String key) throws IOException {
        try {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Deleting key:" + key + "from bucket" + bucketName);
            }

            bosClient.deleteObject(bucketName, key);
        } catch (BceServiceException e) {
            handleBosServiceException(key, e);
        }
    }
    
    @Override
    public void copy(String srcKey, String dstKey) throws IOException {
        try {
            if(LOG.isDebugEnabled()) {
                LOG.debug("Copying srcKey: " + srcKey + "to dstKey: "
                        + dstKey + "in bucket: " + bucketName);
            }

            bosClient.copyObject(bucketName, srcKey, bucketName, dstKey);
        } catch (BceServiceException e) {
            handleBosServiceException(srcKey, e);
        }
    }

    @Override
    public void rename(String srcKey, String dstKey) throws IOException {
        RenameObjectResponse response;
        try {
            LOG.debug("Renaming srcKey: " + srcKey + "to dstKey: "
                    + dstKey + "in bucket: " + bucketName);

            response = bosClient.renameObject(bucketName, srcKey, dstKey);
        } catch (BceServiceException e) {
            handleBosServiceException(srcKey, e);
        }
    }

    @Override
    public void purge(String prefix) throws IOException {
        try {

            ListObjectsResponse objects = bosClient.listObjects(bucketName, prefix);
            for (BosObjectSummary object : objects.getContents()) {
                bosClient.deleteObject(bucketName, object.getKey());
            }
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
    }

    @Override
    public void dump() throws IOException {
        StringBuilder sb = new StringBuilder("BOS Native Filesystem, ");
        sb.append(bucketName).append("\n");
        try {
            ListObjectsResponse objects = bosClient.listObjects(bucketName);
            for (BosObjectSummary object : objects.getContents()) {
                sb.append(object.getKey()).append("\n");
            }
        } catch (BceServiceException e) {
            handleBosServiceException(e);
        }
        System.out.println(sb);
    }

    private void handleBosServiceException(String key, BceServiceException e) throws IOException {
        if ("NoSuchKey".equalsIgnoreCase(e.getErrorCode()) ||
                e.getMessage().startsWith("Not Found")) {
            throw new FileNotFoundException("Key '" + key + "' does not exist in BOS");
        } else {
            handleBosServiceException(e);
        }
    }

    private void handleBosServiceException(BceServiceException e) throws IOException {
        if (e.getCause() instanceof IOException) {
            throw (IOException) e.getCause();
        }
        else {
            if(LOG.isDebugEnabled()) {
                LOG.debug("BOS Error code: " + e.getErrorCode() + "; BOS Error message: " + e.getErrorMessage());
            }
            throw new BosException(e);
        }
    }
}
