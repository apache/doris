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

package org.apache.doris.common.proc;

import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.TableIndexes;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexesProcNodeTest {

    @Test
    public void testFetchResult() throws AnalysisException {
        List<Index> indexes = new ArrayList<>();
        Index indexBitmap = new Index(1, "bitmap_index", Lists.newArrayList("col_1"),
                IndexType.BITMAP, null, "bitmap index on col_1");
        Map<String, String> invertedProperties = new HashMap<>();
        invertedProperties.put("parser", "unicode");
        Index indexInverted = new Index(2, "inverted_index", Lists.newArrayList("col_2"),
                        IndexType.INVERTED, invertedProperties, "inverted index on col_2");
        Index indexBf = new Index(3, "bloomfilter_index", Lists.newArrayList("col_3"),
                IndexType.BLOOMFILTER, null, "bloomfilter index on col_3");
        Map<String, String> ngramProperties = new HashMap<>();
        ngramProperties.put("gram_size", "3");
        ngramProperties.put("bf_size", "256");
        Index indexNgramBf = new Index(4, "ngram_bf_index", Lists.newArrayList("col_4"),
                        IndexType.NGRAM_BF, ngramProperties, "ngram_bf index on col_4");
        indexes.add(indexBitmap);
        indexes.add(indexInverted);
        indexes.add(indexBf);
        indexes.add(indexNgramBf);

        OlapTable table = new OlapTable(1, "tbl_test_indexes_proc", Lists.newArrayList(new Column()), KeysType.DUP_KEYS, new PartitionInfo(),
                new HashDistributionInfo(), new TableIndexes(indexes));

        IndexesProcNode indexesProcNode = new IndexesProcNode(table);
        ProcResult procResult = indexesProcNode.fetchResult();

        Assert.assertEquals(4, procResult.getRows().size());
        Assert.assertEquals(procResult.getRows().get(0).get(0), "tbl_test_indexes_proc");
        Assert.assertEquals(procResult.getRows().get(0).get(1), "1");
        Assert.assertEquals(procResult.getRows().get(0).get(3), "bitmap_index");
        Assert.assertEquals(procResult.getRows().get(0).get(5), "col_1");
        Assert.assertEquals(procResult.getRows().get(0).get(11), "BITMAP");
        Assert.assertEquals(procResult.getRows().get(0).get(12), "bitmap index on col_1");
        Assert.assertEquals(procResult.getRows().get(0).get(13), "");

        Assert.assertEquals(procResult.getRows().get(1).get(0), "tbl_test_indexes_proc");
        Assert.assertEquals(procResult.getRows().get(1).get(1), "2");
        Assert.assertEquals(procResult.getRows().get(1).get(3), "inverted_index");
        Assert.assertEquals(procResult.getRows().get(1).get(5), "col_2");
        Assert.assertEquals(procResult.getRows().get(1).get(11), "INVERTED");
        Assert.assertEquals(procResult.getRows().get(1).get(12), "inverted index on col_2");
        Assert.assertEquals(procResult.getRows().get(1).get(13), "(\"parser\" = \"unicode\", \"lower_case\" = \"true\", \"support_phrase\" = \"true\")");

        Assert.assertEquals(procResult.getRows().get(2).get(0), "tbl_test_indexes_proc");
        Assert.assertEquals(procResult.getRows().get(2).get(1), "3");
        Assert.assertEquals(procResult.getRows().get(2).get(3), "bloomfilter_index");
        Assert.assertEquals(procResult.getRows().get(2).get(5), "col_3");
        Assert.assertEquals(procResult.getRows().get(2).get(11), "BLOOMFILTER");
        Assert.assertEquals(procResult.getRows().get(2).get(12), "bloomfilter index on col_3");
        Assert.assertEquals(procResult.getRows().get(2).get(13), "");

        Assert.assertEquals(procResult.getRows().get(3).get(0), "tbl_test_indexes_proc");
        Assert.assertEquals(procResult.getRows().get(3).get(1), "4");
        Assert.assertEquals(procResult.getRows().get(3).get(3), "ngram_bf_index");
        Assert.assertEquals(procResult.getRows().get(3).get(5), "col_4");
        Assert.assertEquals(procResult.getRows().get(3).get(11), "NGRAM_BF");
        Assert.assertEquals(procResult.getRows().get(3).get(12), "ngram_bf index on col_4");
        Assert.assertEquals(procResult.getRows().get(3).get(13), "(\"gram_size\" = \"3\", \"bf_size\" = \"256\")");

    }
}
