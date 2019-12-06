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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TResourceGroup;
import org.apache.doris.thrift.TResourceType;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

// Resource group to contain
public class ResourceGroup implements Writable {
    private EnumMap<ResourceType, Integer> quotaByType;

    // Used for readIn
    private ResourceGroup() {
        quotaByType = Maps.newEnumMap(ResourceType.class);
    }

    private ResourceGroup(int cpuShare) {
        quotaByType = Maps.newEnumMap(ResourceType.class);
        quotaByType.put(ResourceType.CPU_SHARE, cpuShare);
    }

    public ResourceGroup getCopiedResourceGroup() {
        ResourceGroup resourceGroup = new ResourceGroup();
        resourceGroup.quotaByType = Maps.newEnumMap(quotaByType);
        return resourceGroup;
    }

    public void updateByDesc(String desc, int value) throws DdlException {
        ResourceType type = ResourceType.fromDesc(desc);
        if (type == null) {
            throw new DdlException("Unknown resource type(" + desc + ")");
        }
        if (type == ResourceType.CPU_SHARE || type == ResourceType.IO_SHARE) {
            if (value < 100 || value > 1000) {
                throw new DdlException("Value for resource type(" 
                                       + desc + ") has to be in [100, 1000]");
            }
        }
        quotaByType.put(type, value);
    }

    public int getByDesc(String desc) throws DdlException {
        ResourceType type = ResourceType.fromDesc(desc);
        if (type == null) {
            throw new DdlException("Unknown resource type(" + desc + ")");
        }
        return quotaByType.get(type);
    }

    public Map<ResourceType, Integer> getQuotaMap() {
        return quotaByType;
    }
    public static Builder builder() {
        return new Builder();
    }

    public TResourceGroup toThrift() {
        TResourceGroup tgroup = new TResourceGroup();
        for (EnumMap.Entry<ResourceType, Integer> entry : quotaByType.entrySet()) {
            tgroup.putToResourceByType(entry.getKey().toThrift(), entry.getValue());
        }
        return tgroup;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int idx = 0;
        for (EnumMap.Entry<ResourceType, Integer> entry : quotaByType.entrySet()) {
            if (idx++ != 0) {
                sb.append(", ");
            }
            sb.append(entry.getKey().toString()).append(" = ").append(entry.getValue());
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(quotaByType.size());
        for (Map.Entry<ResourceType, Integer> entry : quotaByType.entrySet()) {
            out.writeInt(entry.getKey().toThrift().getValue());
            out.writeInt(entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        int numResource = in.readInt();
        for (int i = 0; i < numResource; ++i) {
            int code = in.readInt();
            int value = in.readInt();

            quotaByType.put(ResourceType.fromThrift(TResourceType.findByValue(code)), value);
        }
    }

    public static ResourceGroup readIn(DataInput in) throws IOException {
        ResourceGroup group = new ResourceGroup();
        group.readFields(in);
        return group;
    }

    public static class Builder {
        private int cpuShare;

        public Builder() {
            cpuShare = ResourceType.CPU_SHARE.getDefaultValue();
        }

        public ResourceGroup build() {
            return new ResourceGroup(cpuShare);
        }

        public Builder cpuShare(int share) {
            this.cpuShare = share;
            return this;
        }
    }
}
