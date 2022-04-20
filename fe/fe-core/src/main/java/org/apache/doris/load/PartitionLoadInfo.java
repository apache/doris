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

package org.apache.doris.load;

import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PartitionLoadInfo implements Writable {
    private long version;
    private List<Source> sources;
    private boolean needLoad;

    public PartitionLoadInfo() {
        this(new ArrayList<Source>());
    }

    public PartitionLoadInfo(List<Source> sources) {
        this.version = -1L;
        this.sources = sources;
        this.needLoad = true;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getVersion() {
        return version;
    }

    public List<Source> getSources() {
        return sources;
    }

    public boolean isNeedLoad() {
        return needLoad;
    }

    public void setNeedLoad(boolean needLoad) {
        this.needLoad = needLoad;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(version);
        // Versionhash useless just for compatible
        out.writeLong(0L);

        int count = 0;
        if (sources == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            count = sources.size();
            out.writeInt(count);
            for (int i = 0; i < count; ++i) {
                sources.get(i).write(out);
            }
        }

        out.writeBoolean(needLoad);
    }

    public void readFields(DataInput in) throws IOException {
        version = in.readLong();
        // Versionhash useless just for compatible
        in.readLong();
        int count = 0;

        if (in.readBoolean()) {
            count = in.readInt();
            for (int i = 0; i < count; i++) {
                Source source = new Source();
                source.readFields(in);
                sources.add(source);
            }
        }

        needLoad = in.readBoolean();
    }

    @Override
    public String toString() {
        return "PartitionLoadInfo{version=" + version + ", needLoad=" + needLoad + "}";
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof PartitionLoadInfo)) {
            return false;
        }

        PartitionLoadInfo info = (PartitionLoadInfo) obj;

        if (sources != info.sources) {
            if (sources == null || info.sources == null) {
                return false;
            }
            if (sources.size() != info.sources.size()) {
                return false;
            }
            for (Source source : sources) {
                if (!info.sources.contains(source)) {
                    return false;
                }
            }
        }

        return version == info.version && needLoad == info.needLoad;
    }

    public int hashCode() {
        int ret = (int) (version);
        ret ^= sources.size();
        return ret;
    }
}
