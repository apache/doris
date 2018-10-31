package com.baidu.palo.catalog;

import com.baidu.palo.common.FeMetaVersion;

import mockit.Mock;
import mockit.MockUp;

public class FakeCatalog extends MockUp<Catalog> {
    
    private static Catalog catalog;
    
    public static void setCatalog(Catalog catalog) {
        FakeCatalog.catalog = catalog;
    }
    
    @Mock
    public int getJournalVersion() {
        return FeMetaVersion.VERSION_45;
    }
    
    @Mock
    private static Catalog getCurrentCatalog() {
        System.out.println("fake get current catalog is called");
        return catalog;
    }
    
    @Mock
    public static Catalog getInstance() {
        return catalog;
    }
}