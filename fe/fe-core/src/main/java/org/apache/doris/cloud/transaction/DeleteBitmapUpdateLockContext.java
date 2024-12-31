package org.apache.doris.cloud.transaction;

import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TabletMeta;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeleteBitmapUpdateLockContext {
    private Map<Long, Long> baseCompactionCnts;
    private Map<Long, Long> cumulativeCompactionCnts;
    private Map<Long, Long> cumulativePoints;
    private Map<Long, Set<Long>> tableToPartitions;
    private Map<Long, Partition> partitions;
    private Map<Long, Map<Long, Set<Long>>> backendToPartitionTablets;
    private Map<Long, List<Long>> tableToTabletList;
    private Map<Long, TabletMeta> tabletToTabletMeta;

    public DeleteBitmapUpdateLockContext() {
        baseCompactionCnts = Maps.newHashMap();
        cumulativeCompactionCnts = Maps.newHashMap();
        cumulativePoints = Maps.newHashMap();
        tableToPartitions = Maps.newHashMap();
        partitions = Maps.newHashMap();
        backendToPartitionTablets = Maps.newHashMap();
        tableToTabletList = Maps.newHashMap();
        tabletToTabletMeta = Maps.newHashMap();
    }

    public Map<Long, List<Long>> getTableToTabletList() {
        return tableToTabletList;
    }

    public Map<Long, Long> getBaseCompactionCnts() {
        return baseCompactionCnts;
    }

    public Map<Long, Long> getCumulativeCompactionCnts() {
        return cumulativeCompactionCnts;
    }

    public Map<Long, Long> getCumulativePoints() {
        return cumulativePoints;
    }

    public Map<Long, Map<Long, Set<Long>>> getBackendToPartitionTablets() {
        return backendToPartitionTablets;
    }

    public Map<Long, Partition> getPartitions() {
        return partitions;
    }

    public Map<Long, Set<Long>> getTableToPartitions() {
        return tableToPartitions;
    }

    public Map<Long, TabletMeta> getTabletToTabletMeta() {
        return tabletToTabletMeta;
    }

}
