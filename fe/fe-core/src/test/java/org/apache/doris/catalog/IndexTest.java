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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class IndexTest {

    @Test
    public void testIndexSerde() throws IOException {
        List<IndexDef.IndexType> indexTypes = Arrays.asList(IndexDef.IndexType.NGRAM_BF, IndexDef.IndexType.BITMAP);
        List<List<Expr>> arguments = Arrays.asList(
                Arrays.asList(new IntLiteral(3), new IntLiteral(256)),
                new ArrayList<>()
        );
        for (int i = 0; i < indexTypes.size(); i++) {
            Index index1 = new Index("idx_name", Collections.singletonList("column_a"), indexTypes.get(i),
                                     arguments.get(i), "comment a idx");
            ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
            DataOutput output = ByteStreams.newDataOutput(byteArrayOutput);
            index1.write(output);

            byte[] bytes = byteArrayOutput.toByteArray();
            DataInput input = ByteStreams.newDataInput(bytes);
            Index index2 = Index.read(input);
            Assert.assertEquals(index1.toSql(), index2.toSql());
        }
    }

    @Test
    public void testEmptyArgumentSerde() throws IOException {
        Index index1 = new Index("idx_name", Collections.singletonList("column_a"), IndexDef.IndexType.BITMAP,
                null, "comment a idx");

        GsonUtils.GSON.toJson(index1);

        Map<String, Object> map = new HashMap<>();
        map.put("indexName", index1.getIndexName());
        map.put("columns", index1.getColumns());
        map.put("indexType", index1.getIndexType());
        map.put("comment", index1.getComment());
        String json = GsonUtils.GSON.toJson(map);

        ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
        DataOutput output = ByteStreams.newDataOutput(byteArrayOutput);
        Text.writeString(output, json);

        byte[] bytes = byteArrayOutput.toByteArray();
        DataInput input = ByteStreams.newDataInput(bytes);
        Index index2 = Index.read(input);
        Assert.assertEquals(index1.toSql(), index2.toSql());
        Assert.assertEquals(index1.hashCode(), index2.hashCode());
        Assert.assertEquals(index1.clone().hashCode(), index2.hashCode());
    }
}
