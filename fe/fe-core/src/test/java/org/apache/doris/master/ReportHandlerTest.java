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

package org.apache.doris.master;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ReportHandlerTest {

    @Test
    public void testLegacyDirectRowTtlCannotRecoverWithEmptyTablet() {
        boolean originalRecoverWithEmptyTablet = Config.recover_with_empty_tablet;
        OlapTable table = Mockito.mock(OlapTable.class);
        try {
            Config.recover_with_empty_tablet = true;
            Mockito.when(table.isLegacyDirectRowTtl()).thenReturn(true);
            Assertions.assertFalse(ReportHandler.canRecoverWithEmptyTablet(table));

            Mockito.when(table.isLegacyDirectRowTtl()).thenReturn(false);
            Assertions.assertTrue(ReportHandler.canRecoverWithEmptyTablet(table));

            Config.recover_with_empty_tablet = false;
            Assertions.assertFalse(ReportHandler.canRecoverWithEmptyTablet(table));
        } finally {
            Config.recover_with_empty_tablet = originalRecoverWithEmptyTablet;
        }
    }
}
