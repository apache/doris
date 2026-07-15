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

package org.apache.doris.backup;

import org.apache.doris.common.GZIPUtils;
import org.apache.doris.common.Pair;

import com.google.gson.annotations.SerializedName;

import java.io.IOException;

/**
 * Materialized local backup snapshot for getSnapshot RPC.
 *
 * <p>meta/jobInfo hold the response payload (raw or already gzip-compressed).
 * metaSize/jobInfoSize are the original on-disk sizes used by the 2GB RPC guard
 * and logging. After construction, callers must not access the original files.
 */
public class Snapshot {
    @SerializedName(value = "label")
    private String label = null;

    private byte[] meta = null;

    private byte[] jobInfo = null;

    private long metaSize = 0;

    private long jobInfoSize = 0;

    private boolean compressed = false;

    @SerializedName(value = "expired_at")
    private long expiredAt = 0;

    @SerializedName(value = "commitSeq")
    private long commitSeq = 0;

    public Snapshot() {
    }

    public Snapshot(String label, byte[] meta, byte[] jobInfo, long metaSize, long jobInfoSize,
            boolean compressed, long expiredAt, long commitSeq) {
        this.label = label;
        this.meta = meta;
        this.jobInfo = jobInfo;
        this.metaSize = metaSize;
        this.jobInfoSize = jobInfoSize;
        this.compressed = compressed;
        this.expiredAt = expiredAt;
        this.commitSeq = commitSeq;
    }

    public static Pair<BackupMeta, BackupJobInfo> readFromBytes(byte[] meta, byte[] jobInfo) throws IOException {
        BackupJobInfo backupJobInfo = BackupJobInfo.genFromJson(new String(jobInfo));
        BackupMeta backupMeta = BackupMeta.fromBytes(meta, backupJobInfo.metaVersion);
        return Pair.of(backupMeta, backupJobInfo);
    }

    public static Pair<BackupMeta, BackupJobInfo> readFromCompressedBytes(byte[] meta, byte[] jobInfo)
            throws IOException {
        BackupJobInfo backupJobInfo = BackupJobInfo.fromInputStream(GZIPUtils.lazyDecompress(jobInfo));
        BackupMeta backupMeta = BackupMeta.fromInputStream(GZIPUtils.lazyDecompress(meta), backupJobInfo.metaVersion);
        return Pair.of(backupMeta, backupJobInfo);
    }

    public static boolean isCompressed(byte[] meta, byte[] jobInfo) {
        return GZIPUtils.isGZIPCompressed(jobInfo) || GZIPUtils.isGZIPCompressed(meta);
    }

    /**
     * Original on-disk meta size, not necessarily {@code meta.length}.
     * Used by the uncompressed 2GB RPC guard before any content is returned.
     */
    public long getMetaSize() {
        return metaSize;
    }

    /**
     * Original on-disk job info size, not necessarily {@code jobInfo.length}.
     * Used by the uncompressed 2GB RPC guard before any content is returned.
     */
    public long getJobInfoSize() {
        return jobInfoSize;
    }

    public byte[] getMeta() {
        return meta;
    }

    public byte[] getJobInfo() {
        return jobInfo;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public long getExpiredAt() {
        return expiredAt;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() >= expiredAt;
    }

    public long getCommitSeq() {
        return commitSeq;
    }

    @Override
    public String toString() {
        return "Snapshot{"
                + "label='" + label + '\''
                + ", metaSize=" + metaSize
                + ", jobInfoSize=" + jobInfoSize
                + ", compressed=" + compressed
                + ", expiredAt=" + expiredAt
                + ", commitSeq=" + commitSeq
                + '}';
    }
}
