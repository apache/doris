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

package org.apache.doris.journal;

import org.apache.doris.common.io.DataOutputBuffer;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.OperationType;

import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;

public class JournalBatch {
    private static final int OUTPUT_BUFFER_INIT_SIZE = 128;

    private ArrayList<Entity> entities;

    @Getter
    private long size = 0;

    public JournalBatch() {
        entities = new ArrayList<>();
    }

    public JournalBatch(int cap) {
        entities = new ArrayList<>(cap);
    }

    // Add a writable data into journal batch.
    //
    // The writable data will be serialized and saved in the journal batch with an internal
    // representation, so it is safety to update the data object once this function returned.
    //
    // If the batch is too large, it may cause a latency spike. Generally, we recommend controlling
    // the number of batch entities to less than 32 and the batch data size to less than 640KB.
    public void addJournal(short op, Writable data) throws IOException {
        if (op == OperationType.OP_TIMESTAMP) {
            // OP_TIMESTAMP is not supported, see `BDBJEJournal.write` for details.
            throw new RuntimeException("JournalBatch.addJournal is not supported OP_TIMESTAMP");
        }

        JournalEntity entity = new JournalEntity();
        entity.setOpCode(op);
        entity.setData(data);

        DataOutputBuffer buffer = new DataOutputBuffer(OUTPUT_BUFFER_INIT_SIZE);
        entity.write(buffer);
        size += buffer.size();

        entities.add(new Entity(op, buffer));
    }

    public ArrayList<Entity> getJournalEntities() {
        return entities;
    }

    public static class Entity {
        short op;
        DataOutputBuffer data;

        Entity(short op, DataOutputBuffer data) {
            this.op = op;
            this.data = data;
        }

        public short getOpCode() {
            return op;
        }

        public byte[] getBinaryData() {
            return data.getData();
        }
    }
}
