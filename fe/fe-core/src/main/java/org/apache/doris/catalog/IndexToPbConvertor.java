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

import org.apache.doris.proto.OlapFile;

import java.util.List;
import java.util.Map;

public class IndexToPbConvertor {
    public static OlapFile.TabletIndexPB toPb(Index index,
            Map<Integer, Column> columnMap, List<Integer> indexColumnUniqueIds) {
        OlapFile.TabletIndexPB.Builder builder = OlapFile.TabletIndexPB.newBuilder();
        builder.setIndexId(index.getIndexId());
        builder.setIndexName(index.getIndexName());

        for (Integer columnUniqueId : indexColumnUniqueIds) {
            Column column = columnMap.get(columnUniqueId);
            if (column != null) {
                builder.addColUniqueId(column.getUniqueId());
            }
        }

        switch (index.getIndexType()) {
            case BITMAP:
                builder.setIndexType(OlapFile.IndexType.BITMAP);
                break;

            case INVERTED:
                builder.setIndexType(OlapFile.IndexType.INVERTED);
                break;

            case NGRAM_BF:
                builder.setIndexType(OlapFile.IndexType.NGRAM_BF);
                break;

            case BLOOMFILTER:
                builder.setIndexType(OlapFile.IndexType.BLOOMFILTER);
                break;

            case ANN:
                builder.setIndexType(OlapFile.IndexType.ANN);
                break;

            default:
                throw new RuntimeException("indexType " + index.getIndexType() + " is not processed in toPb");
        }

        if (index.getProperties() != null) {
            builder.putAllProperties(index.getProperties());
        }

        return builder.build();
    }
}
