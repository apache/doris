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

package org.apache.doris.datasource.hive;

import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;
import org.apache.doris.datasource.property.storage.LocalProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.LocalDfsFileSystem;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class HiveAcidTest {

    private static final Map<StorageProperties.Type, StorageProperties> storagePropertiesMap = ImmutableMap.of(
            StorageProperties.Type.LOCAL, new LocalProperties(new HashMap<>())
    );

    @Test
    public void testOriginalDeltas() throws Exception {
        LocalDfsFileSystem localDFSFileSystem = new LocalDfsFileSystem();
        Path tempPath = Files.createTempDirectory("tbl");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/000000_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/000001_1");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/000002_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/random");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/_done");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/subdir/000000_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_025_025");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_029_029");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_025_030");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_050_100");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_101_101");

        Map<String, String> txnValidIds = new HashMap<>();
        txnValidIds.put(
                AcidUtil.VALID_TXNS_KEY,
                new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
        txnValidIds.put(
                AcidUtil.VALID_WRITEIDS_KEY,
            new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":").writeToString());

        HivePartition partition = new HivePartition(NameMapping.createForTest("", "tbl"),
                false, "", "file://" + tempPath.toAbsolutePath() + "",
                new ArrayList<>(), new HashMap<>());
        try {
            AcidUtil.getAcidState(localDFSFileSystem, partition, txnValidIds, new HashMap<>(), true);
        } catch (UnsupportedOperationException e) {
            Assert.assertTrue(e.getMessage().contains("For no acid table convert to acid, please COMPACT 'major'."));
        }
    }


    @Test
    public void testObsoleteOriginals() throws Exception {
        LocalDfsFileSystem localDFSFileSystem = new LocalDfsFileSystem();
        Path tempPath = Files.createTempDirectory("tbl");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/base_10/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/base_5/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/000000_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/000001_1");

        Map<String, String> txnValidIds = new HashMap<>();
        txnValidIds.put(
                AcidUtil.VALID_TXNS_KEY,
                new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
        txnValidIds.put(
                AcidUtil.VALID_WRITEIDS_KEY,
                new ValidReaderWriteIdList("tbl:150:" + Long.MAX_VALUE + ":").writeToString());

        HivePartition partition = new HivePartition(
                NameMapping.createForTest("", "tbl"),
                false,
                "",
                "file://" + tempPath.toAbsolutePath() + "",
                new ArrayList<>(),
                new HashMap<>());
        try {
            AcidUtil.getAcidState(localDFSFileSystem, partition, txnValidIds, storagePropertiesMap, true);
        } catch (UnsupportedOperationException e) {
            Assert.assertTrue(e.getMessage().contains("For no acid table convert to acid, please COMPACT 'major'."));
        }
    }


    @Test
    public void testOverlapingDelta() throws Exception {
        LocalDfsFileSystem localDFSFileSystem = new LocalDfsFileSystem();
        Path tempPath = Files.createTempDirectory("tbl");

        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_0000063_63/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_000062_62/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_00061_61/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_40_60/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_0060_60/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_052_55/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/base_50/bucket_0");

        Map<String, String> txnValidIds = new HashMap<>();
        txnValidIds.put(
                AcidUtil.VALID_TXNS_KEY,
                new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
        txnValidIds.put(
                AcidUtil.VALID_WRITEIDS_KEY,
                new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":").writeToString());

        HivePartition partition = new HivePartition(
                NameMapping.createForTest("", "tbl"),
                false,
                "",
                "file://" + tempPath.toAbsolutePath() + "",
                new ArrayList<>(),
                new HashMap<>());

        FileCacheValue fileCacheValue =
                AcidUtil.getAcidState(localDFSFileSystem, partition, txnValidIds, storagePropertiesMap, true);

        List<String> readFile =
                fileCacheValue.getFiles().stream().map(x -> x.path.getNormalizedLocation()).collect(Collectors.toList());


        List<String> resultReadFile = Arrays.asList(
                "file:" + tempPath.toAbsolutePath() + "/base_50/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_40_60/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_00061_61/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_000062_62/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_0000063_63/bucket_0"
        );
        Assert.assertTrue(resultReadFile.containsAll(readFile) && readFile.containsAll(resultReadFile));
    }


    @Test
    public void testOverlapingDelta2() throws Exception {
        LocalDfsFileSystem localDFSFileSystem = new LocalDfsFileSystem();
        Path tempPath = Files.createTempDirectory("tbl");

        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_0000063_63_0/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_000062_62_0/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_000062_62_3/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_00061_61_0/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_40_60/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_0060_60_1/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_0060_60_4/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_0060_60_7/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_052_55/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_058_58/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/base_50/bucket_0");

        Map<String, String> txnValidIds = new HashMap<>();
        txnValidIds.put(
                AcidUtil.VALID_TXNS_KEY,
                new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
        txnValidIds.put(
                AcidUtil.VALID_WRITEIDS_KEY,
                new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":").writeToString());

        HivePartition partition = new HivePartition(
                NameMapping.createForTest("", "tbl"),
                false,
                "",
                "file://" + tempPath.toAbsolutePath() + "",
                new ArrayList<>(),
                new HashMap<>());

        FileCacheValue fileCacheValue =
                AcidUtil.getAcidState(localDFSFileSystem, partition, txnValidIds, storagePropertiesMap, true);


        List<String> readFile =
                fileCacheValue.getFiles().stream().map(x -> x.path.getNormalizedLocation()).collect(Collectors.toList());


        List<String> resultReadFile = Arrays.asList(
                "file:" + tempPath.toAbsolutePath() + "/base_50/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_40_60/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_00061_61_0/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_000062_62_0/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_000062_62_3/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_0000063_63_0/bucket_0"

        );

        Assert.assertTrue(resultReadFile.containsAll(readFile) && readFile.containsAll(resultReadFile));
    }


    @Test
    public void deltasWithOpenTxnInRead() throws Exception {
        LocalDfsFileSystem localDFSFileSystem = new LocalDfsFileSystem();
        Path tempPath = Files.createTempDirectory("tbl");

        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_1_1/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_2_5/bucket_0");

        Map<String, String> txnValidIds = new HashMap<>();

        txnValidIds.put(
                AcidUtil.VALID_TXNS_KEY,
                new ValidReadTxnList(new long[] {50}, new BitSet(), 1000, 55).writeToString());
        txnValidIds.put(
                AcidUtil.VALID_WRITEIDS_KEY,
                new ValidReaderWriteIdList("tbl:100:4:4").writeToString());

        HivePartition partition = new HivePartition(
                NameMapping.createForTest("", "tbl"),
                false,
                "",
                "file://" + tempPath.toAbsolutePath() + "",
                new ArrayList<>(),
                new HashMap<>());


        FileCacheValue fileCacheValue =
                AcidUtil.getAcidState(localDFSFileSystem, partition, txnValidIds, storagePropertiesMap, true);


        List<String> readFile =
                fileCacheValue.getFiles().stream().map(x -> x.path.getNormalizedLocation()).collect(Collectors.toList());


        List<String> resultReadFile = Arrays.asList(
                "file:" + tempPath.toAbsolutePath() + "/delta_1_1/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_2_5/bucket_0"
        );

        Assert.assertTrue(resultReadFile.containsAll(readFile) && readFile.containsAll(resultReadFile));

    }


    @Test
    public void deltasWithOpenTxnInRead2() throws Exception {
        LocalDfsFileSystem localDFSFileSystem = new LocalDfsFileSystem();
        Path tempPath = Files.createTempDirectory("tbl");

        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_1_1/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_2_5/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_4_4_1/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_4_4_3/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_101_101_1/bucket_0");

        Map<String, String> txnValidIds = new HashMap<>();

        txnValidIds.put(
                AcidUtil.VALID_TXNS_KEY,
                new ValidReadTxnList(new long[] {50}, new BitSet(), 1000, 55).writeToString());
        txnValidIds.put(
                AcidUtil.VALID_WRITEIDS_KEY,
                new ValidReaderWriteIdList("tbl:100:4:4").writeToString());

        HivePartition partition = new HivePartition(
                NameMapping.createForTest("", "tbl"),
                false,
                "",
                "file://" + tempPath.toAbsolutePath() + "",
                new ArrayList<>(),
                new HashMap<>());

        FileCacheValue fileCacheValue =
                AcidUtil.getAcidState(localDFSFileSystem, partition, txnValidIds, storagePropertiesMap, true);


        List<String> readFile =
                fileCacheValue.getFiles().stream().map(x -> x.path.getNormalizedLocation()).collect(Collectors.toList());


        List<String> resultReadFile = Arrays.asList(
                "file:" + tempPath.toAbsolutePath() + "/delta_1_1/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_2_5/bucket_0"
        );

        Assert.assertTrue(resultReadFile.containsAll(readFile) && readFile.containsAll(resultReadFile));
    }

    @Test
    public void testBaseWithDeleteDeltas() throws Exception {
        LocalDfsFileSystem localDFSFileSystem = new LocalDfsFileSystem();
        Path tempPath = Files.createTempDirectory("tbl");

        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/base_5/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/base_10/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/base_49/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_025_025/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_029_029/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delete_delta_029_029/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_025_030/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delete_delta_025_030/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_050_105/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delete_delta_050_105/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delete_delta_110_110/bucket_0");

        Map<String, String> txnValidIds = new HashMap<>();
        txnValidIds.put(
                AcidUtil.VALID_TXNS_KEY,
                new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
        txnValidIds.put(
                AcidUtil.VALID_WRITEIDS_KEY,
                new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":").writeToString());

        // Map<String, String> tableProps = new HashMap<>();
        // tableProps.put(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        //         AcidUtils.AcidOperationalProperties.getDefault().toString());

        HivePartition partition = new HivePartition(
                NameMapping.createForTest("", "tbl"),
                false,
                "",
                "file://" + tempPath.toAbsolutePath() + "",
                new ArrayList<>(),
                new HashMap<>());

        FileCacheValue fileCacheValue =
                AcidUtil.getAcidState(localDFSFileSystem, partition, txnValidIds, storagePropertiesMap, true);


        List<String> readFile =
                fileCacheValue.getFiles().stream().map(x -> x.path.getNormalizedLocation()).collect(Collectors.toList());


        List<String> resultReadFile = Arrays.asList(
                "file:" + tempPath.toAbsolutePath() + "/base_49/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_050_105/bucket_0"
        );

        Assert.assertTrue(resultReadFile.containsAll(readFile) && readFile.containsAll(resultReadFile));

        List<String> resultDelta = Arrays.asList(
                "file://" + tempPath.toAbsolutePath() + "/delete_delta_050_105/bucket_0"
        );

        List<String> deltaFiles = new ArrayList<>();
        fileCacheValue.getAcidInfo().getDeleteDeltas().forEach(
                deltaInfo -> {
                    String loc = deltaInfo.getDirectoryLocation();
                    deltaInfo.getFileNames().forEach(
                            fileName -> deltaFiles.add(loc + "/" + fileName)
                    );
                }
        );

        Assert.assertTrue(resultDelta.containsAll(deltaFiles) && deltaFiles.containsAll(resultDelta));
    }


    @Test
    public void testOverlapingDeltaAndDeleteDelta() throws Exception {
        LocalDfsFileSystem localDFSFileSystem = new LocalDfsFileSystem();
        Path tempPath = Files.createTempDirectory("tbl");

        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_0000063_63/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_000062_62/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_00061_61/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delete_delta_00064_64/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_40_60/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delete_delta_40_60/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_0060_60/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_052_55/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delete_delta_052_55/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/base_50/bucket_0");

        Map<String, String> txnValidIds = new HashMap<>();
        txnValidIds.put(
                AcidUtil.VALID_TXNS_KEY,
                new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
        txnValidIds.put(
                AcidUtil.VALID_WRITEIDS_KEY,
                new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":").writeToString());


        Map<String, String> tableProps = new HashMap<>();
        // tableProps.put(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        //         AcidUtils.AcidOperationalProperties.getDefault().toString());

        HivePartition partition = new HivePartition(
                NameMapping.createForTest("", "tbl"),
                false,
                "",
                "file://" + tempPath.toAbsolutePath() + "",
                new ArrayList<>(),
                tableProps);

        FileCacheValue fileCacheValue =
                AcidUtil.getAcidState(localDFSFileSystem, partition, txnValidIds, storagePropertiesMap, true);



        List<String> readFile =
                fileCacheValue.getFiles().stream().map(x -> x.path.getNormalizedLocation()).collect(Collectors.toList());


        List<String> resultReadFile = Arrays.asList(
                "file:" + tempPath.toAbsolutePath() + "/base_50/bucket_0",

                "file:" + tempPath.toAbsolutePath() + "/delta_40_60/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_00061_61/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_000062_62/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_0000063_63/bucket_0"

        );

        Assert.assertTrue(resultReadFile.containsAll(readFile) && readFile.containsAll(resultReadFile));

        List<String> resultDelta = Arrays.asList(

                "file://" + tempPath.toAbsolutePath() + "/delete_delta_40_60/bucket_0",
                "file://" + tempPath.toAbsolutePath() + "/delete_delta_00064_64/bucket_0"
        );

        List<String> deltaFiles = new ArrayList<>();
        fileCacheValue.getAcidInfo().getDeleteDeltas().forEach(
                deltaInfo -> {
                    String loc = deltaInfo.getDirectoryLocation();
                    deltaInfo.getFileNames().forEach(
                            fileName -> deltaFiles.add(loc + "/" + fileName)
                    );
                }
        );

        Assert.assertTrue(resultDelta.containsAll(deltaFiles) && deltaFiles.containsAll(resultDelta));
    }

    @Test
    public void testMinorCompactedDeltaMakesInBetweenDelteDeltaObsolete() throws Exception {
        LocalDfsFileSystem localDFSFileSystem = new LocalDfsFileSystem();
        Path tempPath = Files.createTempDirectory("tbl");


        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_40_60/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delete_delta_50_50/bucket_0");

        Map<String, String> txnValidIds = new HashMap<>();
        txnValidIds.put(
                AcidUtil.VALID_TXNS_KEY,
                new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
        txnValidIds.put(
                AcidUtil.VALID_WRITEIDS_KEY,
                new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":").writeToString());

        Map<String, String> tableProps = new HashMap<>();
        HivePartition partition = new HivePartition(
                NameMapping.createForTest("", "tbl"),
                false,
                "",
                "file://" + tempPath.toAbsolutePath() + "",
                new ArrayList<>(),
                tableProps);

        FileCacheValue fileCacheValue =
                AcidUtil.getAcidState(localDFSFileSystem, partition, txnValidIds, storagePropertiesMap, true);

        List<String> readFile =
                fileCacheValue.getFiles().stream().map(x -> x.path.getNormalizedLocation()).collect(Collectors.toList());


        List<String> resultReadFile = Arrays.asList(
                "file:" + tempPath.toAbsolutePath() + "/delta_40_60/bucket_0"
        );

        Assert.assertTrue(resultReadFile.containsAll(readFile) && readFile.containsAll(resultReadFile));
    }

    @Test
    public void deleteDeltasWithOpenTxnInRead() throws Exception {
        LocalDfsFileSystem localDFSFileSystem = new LocalDfsFileSystem();
        Path tempPath = Files.createTempDirectory("tbl");

        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_1_1/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_2_5/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delete_delta_2_5/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delete_delta_3_3/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_4_4_1/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_4_4_3/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_101_101_1/bucket_0");


        Map<String, String> txnValidIds = new HashMap<>();
        txnValidIds.put(
                AcidUtil.VALID_TXNS_KEY,
                new ValidReadTxnList(new long[] {50}, new BitSet(), 1000, 55).writeToString());
        txnValidIds.put(
                AcidUtil.VALID_WRITEIDS_KEY,
                new ValidReaderWriteIdList("tbl:100:4:4").writeToString());


        Map<String, String> tableProps = new HashMap<>();
        HivePartition partition = new HivePartition(
                NameMapping.createForTest("", "tbl"),
                false,
                "",
                "file://" + tempPath.toAbsolutePath() + "",
                new ArrayList<>(),
                tableProps);

        FileCacheValue fileCacheValue =
                AcidUtil.getAcidState(localDFSFileSystem, partition, txnValidIds, storagePropertiesMap, true);


        List<String> readFile =
                fileCacheValue.getFiles().stream().map(x -> x.path.getNormalizedLocation()).collect(Collectors.toList());


        List<String> resultReadFile = Arrays.asList(
                "file:" + tempPath.toAbsolutePath() + "/delta_1_1/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_2_5/bucket_0"
        );

        Assert.assertTrue(resultReadFile.containsAll(readFile) && readFile.containsAll(resultReadFile));
        List<String> resultDelta = Arrays.asList(
                "file://" + tempPath.toAbsolutePath() + "/delete_delta_2_5/bucket_0"
        );

        List<String> deltaFiles = new ArrayList<>();
        fileCacheValue.getAcidInfo().getDeleteDeltas().forEach(
                deltaInfo -> {
                    String loc = deltaInfo.getDirectoryLocation();
                    deltaInfo.getFileNames().forEach(
                            fileName -> deltaFiles.add(loc + "/" + fileName)
                    );
                }
        );

        Assert.assertTrue(resultDelta.containsAll(deltaFiles) && deltaFiles.containsAll(resultDelta));
    }

    @Test
    public void testBaseDeltas() throws Exception {
        LocalDfsFileSystem localDFSFileSystem = new LocalDfsFileSystem();
        Path tempPath = Files.createTempDirectory("tbl");

        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/base_5/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/base_10/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/base_49/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_025_025/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_029_029/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_025_030/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_050_105/bucket_0");
        localDFSFileSystem.createFile("file://" + tempPath.toAbsolutePath() + "/delta_90_120/bucket_0");

        Map<String, String> txnValidIds = new HashMap<>();
        txnValidIds.put(
                AcidUtil.VALID_TXNS_KEY,
                new ValidReadTxnList(new long[0], new BitSet(), 1000, Long.MAX_VALUE).writeToString());
        txnValidIds.put(
                AcidUtil.VALID_WRITEIDS_KEY,
                new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":").writeToString());

        Map<String, String> tableProps = new HashMap<>();

        HivePartition partition = new HivePartition(
                NameMapping.createForTest("", "tbl"),
                false,
                "",
                "file://" + tempPath.toAbsolutePath() + "",
                new ArrayList<>(),
                tableProps);

        FileCacheValue fileCacheValue =
                AcidUtil.getAcidState(localDFSFileSystem, partition, txnValidIds, storagePropertiesMap, true);

        List<String> readFile =
                fileCacheValue.getFiles().stream().map(x -> x.path.getNormalizedLocation()).collect(Collectors.toList());



        List<String> resultReadFile = Arrays.asList(
                "file:" + tempPath.toAbsolutePath() + "/base_49/bucket_0",
                "file:" + tempPath.toAbsolutePath() + "/delta_050_105/bucket_0"
        );
        Assert.assertTrue(resultReadFile.containsAll(readFile) && readFile.containsAll(resultReadFile));
    }
}
