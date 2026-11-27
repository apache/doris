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

package org.apache.doris.datasource.tvf.source;

import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.scan.FileQueryScanNode;
import org.apache.doris.datasource.split.FileSplit;
import org.apache.doris.datasource.split.FileSplitter;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPushAggOp;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TVFScanNodeTest {
    private static final long MB = 1024L * 1024L;

    private ConnectContext previousContext;

    @BeforeEach
    public void setUpContext() {
        previousContext = ConnectContext.get();
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    public void restoreContext() {
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    private static Stream<Arguments> formatCases() {
        return Stream.of(
                Arguments.of("csv", Map.of(), true),
                Arguments.of("csv", Map.of("enclose", ""), true),
                Arguments.of("csv", Map.of("enclose", "\""), false),
                Arguments.of("csv", Map.of("enclose", "0"), false),
                Arguments.of("csv", Map.of("skip_lines", "0"), true),
                Arguments.of("csv", Map.of("skip_lines", "1"), true),
                Arguments.of("csv", Map.of("skip_lines", "20"), false),
                Arguments.of("hive_text", Map.of("skip_lines", "0"), true),
                Arguments.of("hive_text", Map.of("skip_lines", "1"), true),
                Arguments.of("hive_text", Map.of("skip_lines", "20"), false),
                Arguments.of("csv_with_names", Map.of(), true),
                Arguments.of("csv_with_names", Map.of("skip_lines", "20"), true),
                Arguments.of("csv_with_names_and_types", Map.of(), false),
                Arguments.of("csv_with_names_and_types", Map.of("skip_lines", "0"), false),
                Arguments.of("csv_with_names_and_types", Map.of("skip_lines", "1"), false),
                Arguments.of("json", Map.of(), true),
                Arguments.of("json", Map.of("read_json_by_line", "true"), true),
                Arguments.of("json", Map.of("read_json_by_line", "false"), false),
                Arguments.of("json", Map.of("strip_outer_array", "true"), false),
                Arguments.of("json", Map.of("strip_outer_array", "false"), false),
                Arguments.of("json", Map.of("read_json_by_line", "true", "strip_outer_array", "true"), true));
    }

    @ParameterizedTest
    @MethodSource("formatCases")
    public void testSplitsPreserveFormatState(String format, Map<String, String> options, boolean splittable)
            throws Exception {
        FileFormatProperties properties = FileFormatProperties.createFileFormatProperties(format);
        properties.analyzeFileFormatProperties(new HashMap<>(options), true);
        TFileAttributes attributes = properties.toTFileAttributes();
        for (TFileType storage : List.of(TFileType.FILE_S3, TFileType.FILE_HDFS)) {
            for (String suffix : List.of("", ".gz")) {
                for (TFileCompressType compression : List.of(TFileCompressType.PLAIN, TFileCompressType.UNKNOWN)) {
                    SessionVariable sv = new SessionVariable();
                    sv.setFileSplitSize(1024);
                    String path = (storage == TFileType.FILE_S3 ? "s3://example-bucket/" : "hdfs://example.test/")
                            + "input" + suffix;
                    TVFScanNode node = fileScanNode(sv, storage, properties.getFileFormatType(),
                            splittableFile(path, 4096), compression, attributes);
                    List<Split> splits = node.getSplits(1);
                    boolean compressed = compression == TFileCompressType.UNKNOWN && suffix.equals(".gz");
                    int expected = splittable && !compressed ? 4 : 1;
                    Assertions.assertEquals(expected, splits.size(), format + options + suffix + compression);
                    Assertions.assertEquals(0, splits.get(0).getStart());
                    Assertions.assertEquals(4096, splits.stream().mapToLong(Split::getLength).sum());
                    if (expected == 1) {
                        Assertions.assertEquals(4096, splits.get(0).getLength());
                    }
                }
            }
        }
    }

    private static Stream<Arguments> statefulFormatCases() {
        return formatCases().filter(args -> !(Boolean) args.get()[2]);
    }

    @ParameterizedTest
    @MethodSource("statefulFormatCases")
    public void testCountPushdownCannotEnableStatefulSplits(String format,
            Map<String, String> options, boolean ignored) throws Exception {
        FileFormatProperties properties = FileFormatProperties.createFileFormatProperties(format);
        properties.analyzeFileFormatProperties(new HashMap<>(options), true);
        for (boolean countStar : List.of(false, true)) {
            SessionVariable sv = new SessionVariable();
            sv.setFileSplitSize(1024);
            sv.parallelExecInstanceNum = 1;
            TVFScanNode node = fileScanNode(sv, TFileType.FILE_S3, properties.getFileFormatType(),
                    splittableFile("s3://example-bucket/input.gz", 4096), TFileCompressType.PLAIN,
                    properties.toTFileAttributes());
            node.setPushDownAggNoGrouping(TPushAggOp.COUNT);
            node.setPushDownCountSlotIds(countStar ? Collections.emptyList() : List.of(new SlotId(7)));
            // Two backends make the independent COUNT(*) policy permit splitting this one file.
            Assertions.assertEquals(1, node.getSplits(2).size(), format + options);
        }
    }

    @Test
    public void testStreamDoesNotInspectFileAttributesOrSplitFiles() throws Exception {
        TVFScanNode node = fileScanNode(new SessionVariable(), TFileType.FILE_STREAM,
                TFileFormatType.FORMAT_JSON, splittableFile("stream", 4096), TFileCompressType.PLAIN, null);
        Assertions.assertTrue(node.getSplits(1).isEmpty());
    }

    private static Stream<Arguments> compressionCases() {
        Stream.Builder<Arguments> cases = Stream.builder();
        for (TFileType storage : List.of(TFileType.FILE_S3, TFileType.FILE_HDFS)) {
            for (TFileFormatType format : List.of(TFileFormatType.FORMAT_CSV_PLAIN,
                    TFileFormatType.FORMAT_JSON, TFileFormatType.FORMAT_TEXT)) {
                for (String suffix : List.of("", ".gz")) {
                    for (TFileCompressType compression : List.of(TFileCompressType.UNKNOWN,
                            TFileCompressType.PLAIN, TFileCompressType.GZ)) {
                        boolean gzip = compression == TFileCompressType.GZ
                                || (compression == TFileCompressType.UNKNOWN && suffix.equals(".gz"));
                        cases.add(Arguments.of(storage, format, suffix, compression, gzip));
                    }
                }
            }
        }
        return cases.build();
    }

    @ParameterizedTest
    @MethodSource("compressionCases")
    public void testSplitsUseEffectiveCompression(TFileType storage, TFileFormatType format,
            String suffix, TFileCompressType compression, boolean gzip) throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setFileSplitSize(1024);
        String path = (storage == TFileType.FILE_S3 ? "s3://example-bucket/" : "hdfs://example.test/")
                + "input" + suffix;
        TVFScanNode node = fileScanNode(sv, storage, format, splittableFile(path, 4096), compression);
        List<Split> splits = node.getSplits(1);
        Assertions.assertEquals(gzip ? 1 : 4, splits.size());
        long nextOffset = 0;
        for (Split split : splits) {
            Assertions.assertEquals(nextOffset, split.getStart());
            Assertions.assertEquals(gzip ? 4096 : 1024, split.getLength());
            Assertions.assertEquals(path, ((FileSplit) split).getPathString());
            Assertions.assertEquals(gzip ? TFileCompressType.GZ : TFileCompressType.PLAIN,
                    node.getFileCompressType((FileSplit) split));
            nextOffset += split.getLength();
        }
        Assertions.assertEquals(4096, nextOffset);
    }

    @Test
    public void testExplicitPlainPreservesUnsplittableAndCountStarGates() throws Exception {
        for (boolean countStar : List.of(false, true)) {
            SessionVariable sv = new SessionVariable();
            sv.setFileSplitSize(1024);
            sv.parallelExecInstanceNum = 1;
            TBrokerFileStatus file = splittableFile("s3://example-bucket/input.gz", 4096);
            file.setIsSplitable(countStar);
            TVFScanNode node = fileScanNode(sv, TFileType.FILE_S3, TFileFormatType.FORMAT_CSV_PLAIN,
                    file, TFileCompressType.PLAIN);
            if (countStar) {
                node.setPushDownAggNoGrouping(TPushAggOp.COUNT);
                node.setPushDownCountSlotIds(Collections.emptyList());
            }
            List<Split> splits = node.getSplits(1);
            Assertions.assertEquals(1, splits.size());
            Assertions.assertEquals(0, splits.get(0).getStart());
            Assertions.assertEquals(4096, splits.get(0).getLength());
        }
    }

    @ParameterizedTest
    @EnumSource(value = TFileFormatType.class, names = {"FORMAT_PARQUET", "FORMAT_ORC"})
    public void testColumnarFilesWithoutEnvelopeCodecKeepCoarseSplits(TFileFormatType format) throws Exception {
        for (TFileType storage : List.of(TFileType.FILE_S3, TFileType.FILE_HDFS)) {
            SessionVariable sv = new SessionVariable();
            sv.setFileSplitSize(16 * MB);
            String path = storage == TFileType.FILE_S3
                    ? "s3://example-bucket/input" : "hdfs://example.test/input";
            TVFScanNode node = fileScanNode(sv, storage, format, splittableFile(path, 1536 * MB), null);
            List<Split> splits = node.getSplits(1);
            Assertions.assertEquals(3, splits.size());
            for (int i = 0; i < splits.size(); ++i) {
                Assertions.assertEquals(i * 512 * MB, splits.get(i).getStart());
                Assertions.assertEquals(512 * MB, splits.get(i).getLength());
            }
        }
    }

    @Test
    public void testCoarseRangesHonorMaxFileSplitNum() throws Exception {
        for (TFileFormatType format : List.of(TFileFormatType.FORMAT_PARQUET, TFileFormatType.FORMAT_ORC)) {
            SessionVariable sv = new SessionVariable();
            sv.setMaxFileSplitNum(1);
            TBrokerFileStatus file = splittableFile("s3://bucket/large", 2048 * MB);
            file.setModificationTime(17);
            TVFScanNode node = fileScanNode(sv, TFileType.FILE_S3, format, file);
            List<Split> splits = node.getSplits(1);
            Assertions.assertEquals(1, splits.size());
            Assertions.assertEquals(2048 * MB, splits.get(0).getLength());
        }
    }

    @Test
    public void testHttpRetainsFineRangesWhileStableStorageUsesCoarseRanges() throws Exception {
        for (TFileFormatType format : List.of(TFileFormatType.FORMAT_PARQUET, TFileFormatType.FORMAT_ORC)) {
            for (long explicitSize : new long[] {0, 16 * MB}) {
                SessionVariable sv = new SessionVariable();
                sv.setFileSplitSize(explicitSize);
                TBrokerFileStatus httpFile = splittableFile("http://example.test/data", 256 * MB);
                TVFScanNode http = fileScanNode(sv, TFileType.FILE_HTTP, format, httpFile);
                Assertions.assertEquals(explicitSize > 0 ? 16 : 8, http.getSplits(1).size());
                for (TFileType type : List.of(TFileType.FILE_S3, TFileType.FILE_HDFS)) {
                    String path = type == TFileType.FILE_S3 ? "s3://bucket/data" : "hdfs://host/data";
                    TBrokerFileStatus stableFile = splittableFile(path, 256 * MB);
                    stableFile.setModificationTime(17);
                    TVFScanNode stable = fileScanNode(sv, type, format, stableFile);
                    Assertions.assertEquals(1, stable.getSplits(1).size());
                }
            }
        }
    }

    private static TVFScanNode fileScanNode(SessionVariable sv, TFileType type,
            TFileFormatType format, TBrokerFileStatus file) throws Exception {
        return fileScanNode(sv, type, format, file, null);
    }

    private static TVFScanNode fileScanNode(SessionVariable sv, TFileType type,
            TFileFormatType format, TBrokerFileStatus file, TFileCompressType compression) throws Exception {
        TFileAttributes defaults = new TFileAttributes();
        defaults.setReadJsonByLine(true);
        defaults.setTextParams(new TFileTextScanRangeParams());
        return fileScanNode(sv, type, format, file, compression, defaults);
    }

    private static TVFScanNode fileScanNode(SessionVariable sv, TFileType type,
            TFileFormatType format, TBrokerFileStatus file, TFileCompressType compression,
            TFileAttributes attributes) throws Exception {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        FunctionGenTable table = Mockito.mock(FunctionGenTable.class);
        ExternalFileTableValuedFunction tvf = Mockito.mock(ExternalFileTableValuedFunction.class);
        Mockito.when(table.getTvf()).thenReturn(tvf);
        Mockito.when(tvf.getTFileType()).thenReturn(type);
        Mockito.when(tvf.getTFileFormatType()).thenReturn(format);
        Mockito.when(tvf.getTFileCompressType()).thenReturn(compression);
        Mockito.when(tvf.getFileAttributes()).thenReturn(attributes);
        Mockito.when(tvf.getFileStatuses()).thenReturn(List.of(file));
        desc.setTable(table);
        TVFScanNode node = new TVFScanNode(new PlanNodeId(0), desc, false, sv, ScanContext.EMPTY);
        setFileSplitter(node, new FileSplitter(32 * MB, 32 * MB, 0));
        return node;
    }

    @Test
    public void testDetermineTargetFileSplitSizeUsesCoarseSizeForParquet() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setFileSplitSize(16 * MB);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        FunctionGenTable table = Mockito.mock(FunctionGenTable.class);
        ExternalFileTableValuedFunction tvf = Mockito.mock(ExternalFileTableValuedFunction.class);
        Mockito.when(table.getTvf()).thenReturn(tvf);
        Mockito.when(tvf.getTFileFormatType()).thenReturn(org.apache.doris.thrift.TFileFormatType.FORMAT_PARQUET);
        desc.setTable(table);
        TVFScanNode node = new TVFScanNode(new PlanNodeId(0), desc, false, sv, ScanContext.EMPTY);
        Method method = TVFScanNode.class.getDeclaredMethod("determineTargetFileSplitSize", List.class);
        method.setAccessible(true);
        List<TBrokerFileStatus> statuses = List.of(splittableFile("s3://bucket/large.parquet", 1024 * MB));
        Assertions.assertEquals(512 * MB, (long) method.invoke(node, statuses));
        sv.maxFileScannersConcurrency = 1;
        Assertions.assertEquals(16 * MB, (long) method.invoke(node, statuses));
    }

    @Test
    public void testCountColumnKeepsNormalFileSplitting() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.parallelExecInstanceNum = 1;
        sv.setFileSplitSize(32 * MB);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        FunctionGenTable table = Mockito.mock(FunctionGenTable.class);
        ExternalFileTableValuedFunction tvf = Mockito.mock(ExternalFileTableValuedFunction.class);
        Mockito.when(table.getTvf()).thenReturn(tvf);
        Mockito.when(tvf.getTFileType()).thenReturn(TFileType.FILE_LOCAL);
        Mockito.when(tvf.getTFileCompressType()).thenReturn(TFileCompressType.UNKNOWN);
        Mockito.when(tvf.getFileStatuses()).thenReturn(List.of(
                splittableFile("file:///tmp/count_col_1.parquet", 128 * MB),
                splittableFile("file:///tmp/count_col_2.parquet", 128 * MB)));
        desc.setTable(table);

        TVFScanNode countColumnNode = new TVFScanNode(
                new PlanNodeId(0), desc, false, sv, ScanContext.EMPTY);
        setFileSplitter(countColumnNode, new FileSplitter(32 * MB, 32 * MB, 0));
        countColumnNode.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        countColumnNode.setPushDownCountSlotIds(Collections.singletonList(new SlotId(7)));

        List<Split> countColumnSplits = countColumnNode.getSplits(1);
        Assertions.assertEquals(8, countColumnSplits.size());

        TVFScanNode countStarNode = new TVFScanNode(
                new PlanNodeId(1), desc, false, sv, ScanContext.EMPTY);
        setFileSplitter(countStarNode, new FileSplitter(32 * MB, 32 * MB, 0));
        countStarNode.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        countStarNode.setPushDownCountSlotIds(Collections.emptyList());

        List<Split> countStarSplits = countStarNode.getSplits(1);
        Assertions.assertEquals(2, countStarSplits.size());
    }

    @Test
    public void testDetermineTargetFileSplitSizeHonorsMaxFileSplitNum() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        FunctionGenTable table = Mockito.mock(FunctionGenTable.class);
        ExternalFileTableValuedFunction tvf = Mockito.mock(ExternalFileTableValuedFunction.class);
        Mockito.when(table.getTvf()).thenReturn(tvf);
        desc.setTable(table);
        TVFScanNode node = new TVFScanNode(new PlanNodeId(0), desc, false, sv, ScanContext.EMPTY);

        TBrokerFileStatus status = new TBrokerFileStatus();
        status.setSize(10_000L * MB);
        List<TBrokerFileStatus> statuses = Collections.singletonList(status);

        Method method = TVFScanNode.class.getDeclaredMethod("determineTargetFileSplitSize", List.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, statuses);
        Assertions.assertEquals(100 * MB, target);
    }

    private static TBrokerFileStatus splittableFile(String path, long size) {
        TBrokerFileStatus status = new TBrokerFileStatus();
        status.setPath(path);
        status.setSize(size);
        status.setModificationTime(0);
        status.setIsSplitable(true);
        return status;
    }

    private static void setFileSplitter(TVFScanNode node, FileSplitter splitter) throws Exception {
        java.lang.reflect.Field field = FileQueryScanNode.class.getDeclaredField("fileSplitter");
        field.setAccessible(true);
        field.set(node, splitter);
    }
}
