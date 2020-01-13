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

import mockit.Mock;
import mockit.MockUp;
import org.apache.doris.system.SystemInfoService;

public class FakeCatalog extends MockUp<Catalog> {

    private static Catalog catalog;
    private static int metaVersion;
    private static SystemInfoService systemInfo = new SystemInfoService();

    public static void setCatalog(Catalog catalog) {
        FakeCatalog.catalog = catalog;
    }

    public static void setMetaVersion(int metaVersion) {
        FakeCatalog.metaVersion = metaVersion;
    }

    public static void setSystemInfo(SystemInfoService systemInfo) {
        FakeCatalog.systemInfo = systemInfo;
    }

    // @Mock
    // public int getJournalVersion() {
    // return FeMetaVersion.VERSION_45;
    // }

    @Mock
    public static Catalog getCurrentCatalog() {
        System.out.println("fake get current catalog is called");
        return catalog;
    }

    @Mock
    public static Catalog getInstance() {
        return catalog;
    }

    @Mock
    public static int getCurrentCatalogJournalVersion() {
        return metaVersion;
    }

    @Mock
    public static SystemInfoService getCurrentSystemInfo() {
        return systemInfo;
    }

}