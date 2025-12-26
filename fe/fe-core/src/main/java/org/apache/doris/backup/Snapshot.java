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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class Snapshot {
    @SerializedName(value = "label")
    private String label = null;

    private File meta = null;

    private File jobInfo = null;

    @SerializedName(value = "expired_at")
    private long expiredAt = 0;

    @SerializedName(value = "commitSeq")
    private long commitSeq = 0;

    public Snapshot() {
    }

    public Snapshot(String label, File meta, File jobInfo, long expiredAt, long commitSeq) {
        this.label = label;
        this.meta = meta;
        this.jobInfo = jobInfo;
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

    public long getMetaSize() {
        return meta != null ? meta.length() : 0;
    }

    public long getJobInfoSize() {
        return jobInfo != null ? jobInfo.length() : 0;
    }

    public byte[] getCompressedMeta() throws IOException {
        return GZIPUtils.compress(meta);
    }

    public byte[] getCompressedJobInfo() throws IOException {
        return GZIPUtils.compress(jobInfo);
    }

    public byte[] getMeta() throws IOException {
        return Files.readAllBytes(meta.toPath());
    }

    public byte[] getJobInfo() throws IOException {
        return Files.readAllBytes(jobInfo.toPath());
    }

    public long getExpiredAt() {
        return expiredAt;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > expiredAt;
    }

    public long getCommitSeq() {
        return commitSeq;
    }

    @Override
    public String toString() {
        return "Snapshot{"
                + "label='" + label + '\''
                + ", expiredAt=" + expiredAt
                + ", commitSeq=" + commitSeq
                + '}';
    }
}
