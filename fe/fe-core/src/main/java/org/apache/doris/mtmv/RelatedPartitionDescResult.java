package org.apache.doris.mtmv;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.catalog.PartitionItem;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

public class RelatedPartitionDescResult {
    // PartitionKeyDesc to relatedTable partition ids(Different partitions may have the same PartitionKeyDesc)
    private Map<PartitionKeyDesc, Set<Long>> descs;
    private Map<Long, PartitionItem> items;

    public RelatedPartitionDescResult() {
        this.descs = Maps.newHashMap();
        this.items = Maps.newHashMap();
    }

    public Map<PartitionKeyDesc, Set<Long>> getDescs() {
        return descs;
    }

    public void setDescs(Map<PartitionKeyDesc, Set<Long>> descs) {
        this.descs = descs;
    }

    public Map<Long, PartitionItem> getItems() {
        return items;
    }

    public void setItems(Map<Long, PartitionItem> items) {
        this.items = items;
    }
}
