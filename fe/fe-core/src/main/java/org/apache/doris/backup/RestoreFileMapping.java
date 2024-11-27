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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class RestoreFileMapping implements Writable {

    public static class IdChain {
        // tblId, partId, idxId, tabletId, replicaId
        @SerializedName("c")
        private Long[] chain;

        private IdChain() {
            // for persist
        }

        public IdChain(Long... ids) {
            Preconditions.checkState(ids.length == 6);
            chain = ids;
        }

        public Long getTblId() {
            return chain[0];
        }

        public long getPartId() {
            return chain[1];
        }

        public long getIdxId() {
            return chain[2];
        }

        public long getTabletId() {
            return chain[3];
        }

        public long getReplicaId() {
            return chain[4];
        }

        public boolean hasRefTabletId() {
            return chain.length >= 6 && chain[5] != -1L;
        }

        public long getRefTabletId() {
            return chain[5];
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            sb.append(Joiner.on("-").join(chain));
            sb.append("]");
            return sb.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof IdChain)) {
                return false;
            }

            if (((IdChain) obj).chain.length != chain.length) {
                return false;
            }

            IdChain other = (IdChain) obj;
            for (int i = 0; i < chain.length; i++) {
                // DO NOT use ==, Long_1 != Long_2
                if (!chain[i].equals(other.chain[i])) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public int hashCode() {
            int code = chain[0].hashCode();
            for (int i = 1; i < chain.length; i++) {
                code ^= chain[i].hashCode();
            }
            return code;
        }

        public void readFields(DataInput in) throws IOException {
            int size = in.readInt();
            chain = new Long[size];
            for (int i = 0; i < size; i++) {
                chain[i] = in.readLong();
            }
        }

        public static IdChain read(DataInput in) throws IOException {
            IdChain chain = new IdChain();
            chain.readFields(in);
            return chain;
        }
    }

    // catalog ids -> repository ids
    @SerializedName("m")
    private Map<IdChain, IdChain> mapping = Maps.newHashMap();
    // tablet id -> is overwrite
    @SerializedName("o")
    private Map<Long, Boolean> overwriteMap = Maps.newHashMap();

    public RestoreFileMapping() {

    }

    public void putMapping(IdChain key, IdChain value, boolean overwrite) {
        mapping.put(key, value);
        overwriteMap.put(key.getTabletId(), overwrite);
    }

    public IdChain get(IdChain key) {
        return mapping.get(key);
    }

    public Map<IdChain, IdChain> getMapping() {
        return mapping;
    }

    public boolean isOverwrite(long tabletId) {
        if (overwriteMap.containsKey(tabletId)) {
            return overwriteMap.get(tabletId);
        }
        return false;
    }

    public static RestoreFileMapping read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_135) {
            RestoreFileMapping mapping = new RestoreFileMapping();
            mapping.readFields(in);
            return mapping;
        } else {
            return GsonUtils.GSON.fromJson(Text.readString(in), RestoreFileMapping.class);
        }
    }

    public void clear() {
        mapping.clear();
        overwriteMap.clear();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            IdChain key = IdChain.read(in);
            IdChain val = IdChain.read(in);
            mapping.put(key, val);
        }

        size = in.readInt();
        for (int i = 0; i < size; i++) {
            long tabletId = in.readLong();
            boolean overwrite = in.readBoolean();
            overwriteMap.put(tabletId, overwrite);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<IdChain, IdChain> entry : mapping.entrySet()) {
            sb.append(entry.getKey()).append(" : ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }
}
