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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

public class RestoreFileMapping {

    public static class IdChain {
        // tblId, partId, idxId, tabletId, replicaId, (refTabletId)
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

    public void clear() {
        mapping.clear();
        overwriteMap.clear();
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
