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

package org.apache.doris.mysql.privilege;

import org.apache.doris.catalog.ResourceGroup;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TUserResource;

import com.google.common.collect.ImmutableSortedMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

// Resource belong to one user
public class UserResource implements Writable {
    public static final String LOW = "low";
    public static final String NORMAL = "normal";
    public static final String HIGH = "high";
    // TODO(zhaochun): move this to config
    private static int defaultLowShare = 100;
    private static int defaultNormalShare = 400;
    private static int defaultHighShare = 800;

    // This resource group is used to extend
    private ResourceGroup resource;
    // Use atomic integer for modify in place.
    private ImmutableSortedMap<String, AtomicInteger> shareByGroup;

    // Used to readIn
    private UserResource() {
    }

    public UserResource(int cpuShare) {
        resource = ResourceGroup.builder().cpuShare(cpuShare).build();

        ImmutableSortedMap.Builder<String, AtomicInteger> builder =
                ImmutableSortedMap.orderedBy(String.CASE_INSENSITIVE_ORDER);

        // Low, Normal and High.
        builder.put(LOW, new AtomicInteger(defaultLowShare));
        builder.put(NORMAL, new AtomicInteger(defaultNormalShare));
        builder.put(HIGH, new AtomicInteger(defaultHighShare));

        shareByGroup = builder.build();
    }

    public UserResource getCopiedUserResource() {
        UserResource userResource = new UserResource();
        userResource.resource = resource.getCopiedResourceGroup();
        userResource.shareByGroup = ImmutableSortedMap.copyOf(shareByGroup);
        return userResource;
    }

    public static boolean isValidGroup(String group) {
        if (group.equalsIgnoreCase(LOW) || group.equalsIgnoreCase(NORMAL) || group.equalsIgnoreCase(HIGH)) {
            return true;
        }
        return false;
    }

    public void updateResource(String desc, int quota) throws DdlException {
        resource.updateByDesc(desc, quota);
    }

    public void updateGroupShare(String groupName, int newShare) throws DdlException {
        AtomicInteger share = shareByGroup.get(groupName);
        if (share == null) {
            throw new DdlException("Unknown resource(" + groupName + ")");
        }
        share.set(newShare);
    }

    public ResourceGroup getResource() {
        return resource;
    }

    public Map<String, AtomicInteger> getShareByGroup() {
        return shareByGroup;
    }

    public TUserResource toThrift() {
        TUserResource tResource = new TUserResource();
        tResource.setResource(resource.toThrift());
        for (Map.Entry<String, AtomicInteger> entry : shareByGroup.entrySet()) {
            tResource.putToShareByGroup(entry.getKey(), entry.getValue().get());
        }
        return tResource;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TotalQuota(").append(resource.toString()).append(")\n");
        int idx = 0;
        for (Map.Entry<String, AtomicInteger> entry : shareByGroup.entrySet()) {
            if (idx++ != 0) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append("(").append(entry.getValue()).append(")");
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        resource.write(out);
        out.writeInt(shareByGroup.size());
        for (Map.Entry<String , AtomicInteger> entry : shareByGroup.entrySet()) {
            Text.writeString(out, entry.getKey());
            out.writeInt(entry.getValue().get());
        }
    }

    public void readFields(DataInput in) throws IOException {
        resource = ResourceGroup.readIn(in);
        ImmutableSortedMap.Builder<String, AtomicInteger> builder =
                ImmutableSortedMap.orderedBy(String.CASE_INSENSITIVE_ORDER);
        int numGroup = in.readInt();
        for (int i = 0; i < numGroup; ++i) {
            String name = Text.readString(in);
            AtomicInteger value = new AtomicInteger(in.readInt());

            builder.put(name, value);
        }
        shareByGroup = builder.build();
    }

    public static UserResource readIn(DataInput in) throws IOException {
        UserResource userResource = new UserResource();
        userResource.readFields(in);
        return userResource;
    }
}
