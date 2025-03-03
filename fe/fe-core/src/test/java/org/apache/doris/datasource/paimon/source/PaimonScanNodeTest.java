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

package org.apache.doris.datasource.paimon.source;

import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.Split;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PaimonScanNodeTest {

    @Test
    public void testCalcuteTableLevelCount() {
        List<Split> splits = new ArrayList<>();

        // Create mock splits with row count and deletion files
        Split split1 = new Split() {
            @Override
            public long rowCount() {
                return 100;
            }

            @Override
            public Optional<List<DeletionFile>> deletionFiles() {
                List<DeletionFile> deletionFiles = new ArrayList<>();
                deletionFiles.add(new DeletionFile("path1", 0, 10, 10L));
                deletionFiles.add(new DeletionFile("path2", 0, 20, 20L));
                return Optional.of(deletionFiles);
            }
        };

        Split split2 = new Split() {
            @Override
            public long rowCount() {
                return 200;
            }

            @Override
            public Optional<List<DeletionFile>> deletionFiles() {
                List<DeletionFile> deletionFiles = new ArrayList<>();
                deletionFiles.add(new DeletionFile("path3", 0, 30, 30L));
                deletionFiles.add(new DeletionFile("path4", 0, 40, 40L));
                return Optional.of(deletionFiles);
            }
        };

        splits.add(split1);
        splits.add(split2);

        Optional<Long> result = PaimonScanNode.calcuteTableLevelCount(splits);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(200, result.get().longValue());
    }

    @Test
    public void testCalcuteTableLevelCountWithNullDeletionFile() {
        List<Split> splits = new ArrayList<>();

        // Create mock splits with row count and null deletion files
        Split split1 = new Split() {
            @Override
            public long rowCount() {
                return 100;
            }

            @Override
            public Optional<List<DeletionFile>> deletionFiles() {
                List<DeletionFile> deletionFiles = new ArrayList<>();
                deletionFiles.add(null);
                deletionFiles.add(new DeletionFile("path2", 0, 20, 20L));
                return Optional.of(deletionFiles);
            }
        };

        Split split2 = new Split() {
            @Override
            public long rowCount() {
                return 200;
            }

            @Override
            public Optional<List<DeletionFile>> deletionFiles() {
                return Optional.empty();
            }
        };

        splits.add(split1);
        splits.add(split2);

        Optional<Long> result = PaimonScanNode.calcuteTableLevelCount(splits);
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(280, result.get().longValue());
    }

    @Test
    public void testCalcuteTableLevelCountWithNullCardinality() {
        List<Split> splits = new ArrayList<>();

        // Create mock splits with row count and deletion files with null cardinality
        Split split1 = new Split() {
            @Override
            public long rowCount() {
                return 100;
            }

            @Override
            public Optional<List<DeletionFile>> deletionFiles() {
                List<DeletionFile> deletionFiles = new ArrayList<>();
                deletionFiles.add(new DeletionFile("path1", 0, 10, null));
                deletionFiles.add(new DeletionFile("path2", 0, 20, 20L));
                return Optional.of(deletionFiles);
            }
        };

        Split split2 = new Split() {
            @Override
            public long rowCount() {
                return 200;
            }

            @Override
            public Optional<List<DeletionFile>> deletionFiles() {
                List<DeletionFile> deletionFiles = new ArrayList<>();
                deletionFiles.add(new DeletionFile("path3", 0, 30, 30L));
                deletionFiles.add(null);
                return Optional.of(deletionFiles);
            }
        };

        splits.add(split1);
        splits.add(split2);

        Optional<Long> result = PaimonScanNode.calcuteTableLevelCount(splits);
        Assert.assertFalse(result.isPresent());
    }
}
