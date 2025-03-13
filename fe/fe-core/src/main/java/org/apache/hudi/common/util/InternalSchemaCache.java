/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/* Copied From
 * https://github.com/apache/hudi/blob/release-0.15.0/hudi-common/src/main/java/org/apache/hudi/common/util/InternalSchemaCache.java
 * Doris Modification.
 * 1. Function `getHistoricalSchemas` converted from private to public.
 */

/**
 * An internal cache implementation for managing different version of schemas.
 * This is a Global cache; all threads in one container/executor share the same cache.
 * A map of (tablePath, HistorySchemas) is maintained.
 */
public class InternalSchemaCache {
    private static final Logger LOG = LoggerFactory.getLogger(InternalSchemaCache.class);
    // Use segment lock to reduce competition.
    // the lock size should be powers of 2 for better hash.
    private static Object[] lockList = new Object[16];

    static {
        for (int i = 0; i < lockList.length; i++) {
            lockList[i] = new Object();
        }
    }

    // historySchemas cache maintain a map about (tablePath, HistorySchemas).
    // this is a Global cache, all threads in one container/executor share the same cache.
    private static final Cache<String, TreeMap<Long, InternalSchema>>
            HISTORICAL_SCHEMA_CACHE = Caffeine.newBuilder().maximumSize(1000).weakValues().build();

    /**
     * Search internalSchema based on versionID.
     * first step: try to get internalSchema from hoodie commit files, we no need to add lock.
     * if we cannot get internalSchema by first step, then we try to get internalSchema from cache.
     *
     * @param versionID schema version_id need to search
     * @param metaClient current hoodie metaClient
     * @return internalSchema
     */
    public static InternalSchema searchSchemaAndCache(long versionID, HoodieTableMetaClient metaClient, boolean cacheEnable) {
        Option<InternalSchema> candidateSchema = getSchemaByReadingCommitFile(versionID, metaClient);
        if (candidateSchema.isPresent()) {
            return candidateSchema.get();
        }
        if (!cacheEnable) {
            // parse history schema and return directly
            return InternalSchemaUtils.searchSchema(versionID, getHistoricalSchemas(metaClient));
        }
        String tablePath = metaClient.getBasePath();
        // use segment lock to reduce competition.
        synchronized (lockList[tablePath.hashCode() & (lockList.length - 1)]) {
            TreeMap<Long, InternalSchema> historicalSchemas = HISTORICAL_SCHEMA_CACHE.getIfPresent(tablePath);
            if (historicalSchemas == null || InternalSchemaUtils.searchSchema(versionID, historicalSchemas) == null) {
                historicalSchemas = getHistoricalSchemas(metaClient);
                HISTORICAL_SCHEMA_CACHE.put(tablePath, historicalSchemas);
            } else {
                long maxVersionId = historicalSchemas.keySet().stream().max(Long::compareTo).get();
                if (versionID > maxVersionId) {
                    historicalSchemas = getHistoricalSchemas(metaClient);
                    HISTORICAL_SCHEMA_CACHE.put(tablePath, historicalSchemas);
                }
            }
            return InternalSchemaUtils.searchSchema(versionID, historicalSchemas);
        }
    }

    public static TreeMap<Long, InternalSchema> getHistoricalSchemas(HoodieTableMetaClient metaClient) {
        TreeMap<Long, InternalSchema> result = new TreeMap<>();
        FileBasedInternalSchemaStorageManager schemasManager = new FileBasedInternalSchemaStorageManager(metaClient);
        String historySchemaStr = schemasManager.getHistorySchemaStr();
        if (!StringUtils.isNullOrEmpty(historySchemaStr)) {
            result = SerDeHelper.parseSchemas(historySchemaStr);
        }
        return result;
    }

    private static Option<InternalSchema> getSchemaByReadingCommitFile(long versionID, HoodieTableMetaClient metaClient) {
        try {
            HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
            List<HoodieInstant> instants = timeline.getInstantsAsStream().filter(f -> f.getTimestamp().equals(String.valueOf(versionID))).collect(Collectors.toList());
            if (instants.isEmpty()) {
                return Option.empty();
            }
            byte[] data = timeline.getInstantDetails(instants.get(0)).get();
            HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(data, HoodieCommitMetadata.class);
            String latestInternalSchemaStr = metadata.getMetadata(SerDeHelper.LATEST_SCHEMA);
            return SerDeHelper.fromJson(latestInternalSchemaStr);
        } catch (Exception e) {
            throw new HoodieException("Failed to read schema from commit metadata", e);
        }
    }

    /**
     * Get internalSchema and avroSchema for compaction/cluster operation.
     *
     * @param metaClient current hoodie metaClient
     * @param compactionAndClusteringInstant first instant before current compaction/cluster instant
     * @return (internalSchemaStrOpt, avroSchemaStrOpt) a pair of InternalSchema/avroSchema
     */
    public static Pair<Option<String>, Option<String>> getInternalSchemaAndAvroSchemaForClusteringAndCompaction(HoodieTableMetaClient metaClient, String compactionAndClusteringInstant) {
        // try to load internalSchema to support Schema Evolution
        HoodieTimeline timelineBeforeCurrentCompaction = metaClient.getCommitsAndCompactionTimeline().findInstantsBefore(compactionAndClusteringInstant).filterCompletedInstants();
        Option<HoodieInstant> lastInstantBeforeCurrentCompaction =  timelineBeforeCurrentCompaction.lastInstant();
        if (lastInstantBeforeCurrentCompaction.isPresent()) {
            // try to find internalSchema
            byte[] data = timelineBeforeCurrentCompaction.getInstantDetails(lastInstantBeforeCurrentCompaction.get()).get();
            HoodieCommitMetadata metadata;
            try {
                metadata = HoodieCommitMetadata.fromBytes(data, HoodieCommitMetadata.class);
            } catch (Exception e) {
                throw new HoodieException(String.format("cannot read metadata from commit: %s", lastInstantBeforeCurrentCompaction.get()), e);
            }
            String internalSchemaStr = metadata.getMetadata(SerDeHelper.LATEST_SCHEMA);
            if (internalSchemaStr != null) {
                String existingSchemaStr = metadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
                return Pair.of(Option.of(internalSchemaStr), Option.of(existingSchemaStr));
            }
        }
        return Pair.of(Option.empty(), Option.empty());
    }

    /**
     * Give a schema versionId return its internalSchema.
     * This method will be called by spark tasks, we should minimize time cost.
     * We try our best to not use metaClient， since the initialization of metaClient is time cost
     * step1：
     * try to parser internalSchema from HoodieInstant directly
     * step2：
     * if we cannot parser internalSchema in step1， (eg: current versionId HoodieInstant has been archived)
     * try to find internalSchema in historySchema.
     * step3:
     * if we cannot parser internalSchema in step2  (eg: schema evolution is not enabled when we create hoodie table, however after some inserts we enable schema evolution)
     * try to convert table schema to internalSchema.
     *
     * @param versionId    the internalSchema version to be search.
     * @param tablePath    table path
     * @param storage      {@link HoodieStorage} instance.
     * @param validCommits current validate commits, use to make up the commit file path/verify the validity of the history schema files
     * @return a internalSchema.
     */
    public static InternalSchema getInternalSchemaByVersionId(long versionId, String tablePath, HoodieStorage storage, String validCommits) {
        String avroSchema = "";
        Set<String> commitSet = Arrays.stream(validCommits.split(",")).collect(Collectors.toSet());
        List<String> validateCommitList =
                commitSet.stream().map(HoodieInstant::extractTimestamp).collect(Collectors.toList());

        StoragePath hoodieMetaPath = new StoragePath(tablePath, HoodieTableMetaClient.METAFOLDER_NAME);
        //step1:
        StoragePath candidateCommitFile = commitSet.stream()
                .filter(fileName -> HoodieInstant.extractTimestamp(fileName).equals(versionId + ""))
                .findFirst().map(f -> new StoragePath(hoodieMetaPath, f)).orElse(null);
        if (candidateCommitFile != null) {
            try {
                byte[] data;
                try (InputStream is = storage.open(candidateCommitFile)) {
                    data = FileIOUtils.readAsByteArray(is);
                } catch (IOException e) {
                    throw e;
                }
                HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(data, HoodieCommitMetadata.class);
                String latestInternalSchemaStr = metadata.getMetadata(SerDeHelper.LATEST_SCHEMA);
                avroSchema = metadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
                if (latestInternalSchemaStr != null) {
                    return SerDeHelper.fromJson(latestInternalSchemaStr).orElse(null);
                }
            } catch (Exception e1) {
                // swallow this exception.
                LOG.warn(String.format(
                        "Cannot find internal schema from commit file %s. Falling back to parsing historical internal schema",
                        candidateCommitFile.toString()));
            }
        }
        // step2:
        FileBasedInternalSchemaStorageManager fileBasedInternalSchemaStorageManager =
                new FileBasedInternalSchemaStorageManager(storage, new StoragePath(tablePath));
        String latestHistorySchema =
                fileBasedInternalSchemaStorageManager.getHistorySchemaStrByGivenValidCommits(validateCommitList);
        if (latestHistorySchema.isEmpty()) {
            return InternalSchema.getEmptyInternalSchema();
        }
        InternalSchema fileSchema =
                InternalSchemaUtils.searchSchema(versionId, SerDeHelper.parseSchemas(latestHistorySchema));
        // step3:
        return fileSchema.isEmptySchema()
                ? StringUtils.isNullOrEmpty(avroSchema)
                ? InternalSchema.getEmptyInternalSchema()
                : AvroInternalSchemaConverter.convert(HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(avroSchema)))
                : fileSchema;
    }

    public static InternalSchema getInternalSchemaByVersionId(long versionId, HoodieTableMetaClient metaClient) {
        String validCommitLists = metaClient
                .getCommitsAndCompactionTimeline().filterCompletedInstants().getInstantsAsStream().map(HoodieInstant::getFileName).collect(Collectors.joining(","));
        return getInternalSchemaByVersionId(versionId, metaClient.getBasePathV2().toString(), metaClient.getStorage(), validCommitLists);
    }
}

