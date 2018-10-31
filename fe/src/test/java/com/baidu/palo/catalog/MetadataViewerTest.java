package com.baidu.palo.catalog;

import com.baidu.palo.analysis.BinaryPredicate.Operator;
import com.baidu.palo.backup.CatalogMocker;
import com.baidu.palo.catalog.Replica.ReplicaStatus;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.system.SystemInfoService;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import mockit.Mocked;
import mockit.NonStrictExpectations;

public class MetadataViewerTest {
    
    private static Method getTabletStatusMethod;
    private static Method getTabletDistributionMethod;
    
    @Mocked
    private Catalog catalog;

    @Mocked
    private SystemInfoService infoService;

    private static Database db;

    @BeforeClass
    public static void setUp() throws NoSuchMethodException, SecurityException, InstantiationException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException, AnalysisException {
        Class[] argTypes = new Class[] { String.class, String.class, List.class, ReplicaStatus.class, Operator.class };
        getTabletStatusMethod = MetadataViewer.class.getDeclaredMethod("getTabletStatus", argTypes);
        getTabletStatusMethod.setAccessible(true);

        argTypes = new Class[] { String.class, String.class, List.class };
        getTabletDistributionMethod = MetadataViewer.class.getDeclaredMethod("getTabletDistribution", argTypes);
        getTabletDistributionMethod.setAccessible(true);

        db = CatalogMocker.mockDb();
    }

    @Before
    public void before() {

        new NonStrictExpectations() {
            {
                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDb(anyString);
                result = db;
            }
        };

        new NonStrictExpectations() {
            {
                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = infoService;

                infoService.getBackendIds(anyBoolean);
                result = Lists.newArrayList(10000L, 10001L, 10002L);
            }
        };
    }

    @Test
    public void testGetTabletStatus()
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        List<String> partitions = Lists.newArrayList();
        Object[] args = new Object[] { CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions, null,
                null };
        List<List<String>> result = (List<List<String>>) getTabletStatusMethod.invoke(null, args);
        Assert.assertEquals(3, result.size());
        System.out.println(result);

        args = new Object[] { CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions, ReplicaStatus.DEAD,
                Operator.EQ };
        result = (List<List<String>>) getTabletStatusMethod.invoke(null, args);
        Assert.assertEquals(3, result.size());

        args = new Object[] { CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions, ReplicaStatus.DEAD,
                Operator.NE };
        result = (List<List<String>>) getTabletStatusMethod.invoke(null, args);
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testGetTabletDistribution()
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        List<String> partitions = Lists.newArrayList();
        Object[] args = new Object[] { CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME, partitions };
        List<List<String>> result = (List<List<String>>) getTabletDistributionMethod.invoke(null, args);
        Assert.assertEquals(3, result.size());
        System.out.println(result);
    }

}
