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

package org.apache.doris.cdcclient.source.reader;

import java.util.Iterator;
import java.util.Map;
import lombok.Data;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * 保存从 split 中读取的结果。 使用泛型支持不同类型的 Split 和 SplitState。 使用迭代器模式，避免将所有数据加载到内存。
 *
 * @param <Split> Split 类型（如 MySqlSplit）
 * @param <SplitState> SplitState 类型（如 MySqlSplitState）
 */
@Data
public class SplitReadResult<Split, SplitState> {
    private Iterator<SourceRecord> recordIterator; // 使用迭代器，支持流式处理
    private Map<String, String> lastMeta;
    private SplitState splitState; // Split state，由具体的 reader 管理
    private boolean readBinlog; // 是否读取 binlog（对于支持 binlog 的数据源）
    private boolean pureBinlogPhase; // 是否处于纯 binlog 阶段
    private Split split; // Split，由具体的 reader 管理
    private String splitId; // split ID
    private Map<String, String> defaultOffset; // 默认的 offset（用于没有数据时）

    /** 检查是否有数据（延迟检查，因为迭代器可能还没有被消费） */
    public boolean isEmpty() {
        return recordIterator == null || !recordIterator.hasNext();
    }
}
