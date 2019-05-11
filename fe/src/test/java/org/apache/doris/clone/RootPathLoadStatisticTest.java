package org.apache.doris.clone;

import org.apache.doris.catalog.DiskInfo.DiskState;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class RootPathLoadStatisticTest {

    @Test
    public void test() {
        RootPathLoadStatistic usageLow = new RootPathLoadStatistic(0L, "/home/disk1", 12345L, TStorageMedium.HDD, 4096L,
                1024L, DiskState.ONLINE);
        RootPathLoadStatistic usageHigh = new RootPathLoadStatistic(0L, "/home/disk2", 67890L, TStorageMedium.HDD,
                4096L, 2048L, DiskState.ONLINE);

        List<RootPathLoadStatistic> list = Lists.newArrayList();
        list.add(usageLow);
        list.add(usageHigh);

        // low usage should be ahead
        Collections.sort(list);
        Assert.assertTrue(list.get(0).getPathHash() == usageLow.getPathHash());
    }

}
