package org.apache.doris.catalog;

import org.apache.doris.resource.TagSet;

import com.google.common.collect.Maps;

import java.util.Map;

public class ReplicaAllocation {
    public enum AllocationType {
        LOCAL, REMOTE
    }

    private Map<AllocationType, Short> typeToNum = Maps.newHashMap();
    private Map<AllocationType, TagSet> typeToTag = Maps.newHashMap();

    public ReplicaAllocation() {

    }

    public void setReplica(AllocationType type, TagSet tagSet, short num) {
        typeToNum.put(type, num);
        typeToTag.put(type, tagSet);
    }

    public short getReplicaNumByType(AllocationType type) {
        return typeToNum.getOrDefault(type, (short) 0);
    }
}
