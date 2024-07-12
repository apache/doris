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

package org.apache.doris.cloud.storage;

import org.apache.doris.common.DdlException;

import cfjd.com.google.common.collect.Lists;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.Blob;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.BlobId;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.Bucket;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.Storage;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.StorageBatch;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.StorageException;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.StorageOptions;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GcsRemote extends RemoteBase {
    private static final Logger LOG = LogManager.getLogger(GcsRemote.class);
    private static final int BATCH_SIZE = 100;
    private Storage storage;
    private Bucket bucket;

    public GcsRemote(ObjectInfo obj) {
        super(obj);
    }

    private void initClient() {
        if (storage == null) {
            storage = StorageOptions.getDefaultInstance().getService();
        }
        if (bucket == null) {
            bucket = storage.get(obj.getBucket());
        }
    }

    @Override
    public String getPresignedUrl(String fileName) {
        initClient();
        BlobId blobId = BlobId.of(obj.getBucket(), normalizePrefix(fileName));
        Blob blob = storage.get(blobId);
        if (blob == null) {
            return null;
        }
        return blob.signUrl(SESSION_EXPIRE_SECOND, TimeUnit.SECONDS).toString();
    }

    @Override
    public ListObjectsResult listObjects(String continuationToken) throws DdlException {
        return listObjectsInner(normalizePrefix(), continuationToken);
    }

    @Override
    public ListObjectsResult listObjects(String subPrefix, String continuationToken) throws DdlException {
        return listObjectsInner(normalizePrefix(subPrefix), continuationToken);
    }

    @Override
    public ListObjectsResult headObject(String subKey) throws DdlException {
        initClient();
        String key = normalizePrefix(subKey);
        BlobId blobId = BlobId.of(obj.getBucket(), key);
        Blob blob = storage.get(blobId);
        if (blob == null) {
            LOG.warn("NoSuchKey when head object for GCS, subKey={}", subKey);
            return new ListObjectsResult(new ArrayList<>(), false, null);
        }
        ObjectFile objectFile = new ObjectFile(key, getRelativePath(key), blob.getEtag(), blob.getSize());
        return new ListObjectsResult(Lists.newArrayList(objectFile), false, null);
    }

    @Override
    public Triple<String, String, String> getStsToken() throws DdlException {
        return null;
    }

    @Override
    public void deleteObjects(List<String> keys) throws DdlException {
        initClient();
        List<StorageBatchResult<Boolean>> results = new ArrayList<>();
        IntStream.range(0, (keys.size() + BATCH_SIZE - 1) / BATCH_SIZE)
                .mapToObj(i -> keys.stream()
                        .skip(i * BATCH_SIZE)
                        .limit(BATCH_SIZE)
                        .collect(Collectors.toList()))
                .forEach(objs -> {
                    StorageBatch batch = storage.batch();
                    objs.forEach(key -> {
                        BlobId blob = BlobId.of(obj.getBucket(), normalizePrefix(key));
                        results.add(batch.delete(blob));
                    });
                    batch.submit();
                });
        for (StorageBatchResult<Boolean> result : results) {
            try {
                boolean ok = result.get();
                if (!ok) {
                    throw new DdlException("Failed to delete objects under GCS");
                }
            } catch (StorageException e) {
                throw new DdlException("Failed to delete objects under GCS because", e);
            }
        }
    }

    private ListObjectsResult listObjectsInner(String prefix, String continuationToken) throws DdlException {
        initClient();
        com.google.cloud.hadoop.repackaged.gcs.com.google.api.gax.paging.Page<Blob> blobs = bucket.list(
                BlobListOption.prefix(prefix), BlobListOption.pageToken(continuationToken));
        List<ObjectFile> objectFiles = new ArrayList<>();
        for (Blob blob : blobs.iterateAll()) {
            ObjectFile file = new ObjectFile(blob.getName(), getRelativePath(blob.getName()),
                    blob.getEtag(), blob.getSize());
            objectFiles.add(file);
        }
        return new ListObjectsResult(objectFiles, blobs.hasNextPage(),
                blobs.getNextPageToken());
    }
}
