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

package org.apache.doris.datasource.iceberg.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IcebergDeleteFileFilterTest {
    @Test
    public void testValidateDeletionVectorMetadataAcceptsLongRange() {
        Assertions.assertDoesNotThrow(() -> IcebergDeleteFileFilter.validateDeletionVectorMetadata(
                "puffin.dv", 1L << 40, (1L << 32) + 17, (1L << 30) + 19));
    }

    @Test
    public void testValidateDeletionVectorMetadataRejectsInvalidRange() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergDeleteFileFilter.validateDeletionVectorMetadata("puffin.dv", 100, null, 1L));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergDeleteFileFilter.validateDeletionVectorMetadata("puffin.dv", 100, 1L, null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergDeleteFileFilter.validateDeletionVectorMetadata("puffin.dv", -1, 1L, 1L));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergDeleteFileFilter.validateDeletionVectorMetadata("puffin.dv", 100, -1L, 1L));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergDeleteFileFilter.validateDeletionVectorMetadata("puffin.dv", 100, 1L, -1L));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergDeleteFileFilter.validateDeletionVectorMetadata(
                        "puffin.dv", Long.MAX_VALUE, Long.MAX_VALUE, 1L));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergDeleteFileFilter.validateDeletionVectorMetadata("puffin.dv", 100, 90L, 11L));
    }
}
