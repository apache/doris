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

package org.apache.doris.regression.util

import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.assertEquals
import static org.junit.jupiter.api.Assertions.assertThrows

class RowsetInfoUtilsTest {
    @Test
    void parsesLegacyRowsetInfo() {
        def rowsetInfo = "[2-4] 3 DATA NONOVERLAPPING 0200000000000001 1.500 MB level=1"

        assertEquals(["1.500", "MB"], RowsetInfoUtils.dataSizeFields(rowsetInfo))
    }

    @Test
    void parsesExplicitSegmentListRowsetInfo() {
        def rowsetInfo =
                "[2-4] 3 DATA NONOVERLAPPING 0200000000000001 1.500 MB level=1 [100,101,200]"

        assertEquals(["1.500", "MB"], RowsetInfoUtils.dataSizeFields(rowsetInfo))
    }

    @Test
    void rejectsUnexpectedRowsetInfo() {
        def rowsetInfo = "[2-4] 3 DATA NONOVERLAPPING 1.500 MB"

        assertThrows(IllegalArgumentException.class) {
            RowsetInfoUtils.dataSizeFields(rowsetInfo)
        }
    }

    @Test
    void rejectsUnexpectedSegmentList() {
        def rowsetInfo =
                "[2-4] 3 DATA NONOVERLAPPING 0200000000000001 1.500 MB level=1 segment_ids"

        assertThrows(IllegalArgumentException.class) {
            RowsetInfoUtils.dataSizeFields(rowsetInfo)
        }
    }

    @Test
    void rejectsUnexpectedCompactionLevel() {
        def rowsetInfo =
                "[2-4] 3 DATA NONOVERLAPPING 0200000000000001 1.500 MB compaction_level=1"

        assertThrows(IllegalArgumentException.class) {
            RowsetInfoUtils.dataSizeFields(rowsetInfo)
        }
    }
}
