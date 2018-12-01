package org.apache.doris.clone;

import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.clone.TabletInfo.Priority;
import org.apache.doris.thrift.TStorageMedium;

import org.junit.Assert;
import org.junit.Test;

import java.util.PriorityQueue;

public class TabletInfoTest {

    @Test
    public void testPriorityCompare() {
        // equal priority, but info3's last visit time is earlier than info2 and info1, so info1 should ranks ahead
        PriorityQueue<TabletInfo> pendingTablets = new PriorityQueue<>();
        TabletInfo info1 = new TabletInfo(TabletStatus.REPLICA_MISSING, "default_cluster",
                1, 2, 3, 4, 1000, 6, TStorageMedium.SSD, System.currentTimeMillis());
        info1.setOrigPriority(Priority.NORMAL);
        info1.setLastVisitedTime(2);

        TabletInfo info2 = new TabletInfo(TabletStatus.REPLICA_MISSING, "default_cluster",
                1, 2, 3, 4, 1001, 6, TStorageMedium.SSD, System.currentTimeMillis());
        info2.setOrigPriority(Priority.NORMAL);
        info2.setLastVisitedTime(3);

        TabletInfo info3 = new TabletInfo(TabletStatus.REPLICA_MISSING, "default_cluster",
                1, 2, 3, 4, 1001, 6, TStorageMedium.SSD, System.currentTimeMillis());
        info3.setOrigPriority(Priority.NORMAL);
        info3.setLastVisitedTime(1);

        pendingTablets.add(info1);
        pendingTablets.add(info2);
        pendingTablets.add(info3);

        TabletInfo expectedInfo = pendingTablets.poll();
        Assert.assertNotNull(expectedInfo);
        Assert.assertEquals(info3.getTabletId(), expectedInfo.getTabletId());

        // priority is not equal, info2 is HIGH, should ranks ahead
        pendingTablets.clear();
        info1.setOrigPriority(Priority.NORMAL);
        info2.setOrigPriority(Priority.HIGH);
        info1.setLastVisitedTime(2);
        info2.setLastVisitedTime(2);
        pendingTablets.add(info2);
        pendingTablets.add(info1);
        expectedInfo = pendingTablets.poll();
        Assert.assertNotNull(expectedInfo);
        Assert.assertEquals(info2.getTabletId(), expectedInfo.getTabletId());

        // add info2 back to priority queue, and it should ranks ahead still.
        pendingTablets.add(info2);
        expectedInfo = pendingTablets.poll();
        Assert.assertNotNull(expectedInfo);
        Assert.assertEquals(info2.getTabletId(), expectedInfo.getTabletId());


    }

}
