// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.broker.bos;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

public class BosBlockBuffer {

    private String key;
    private int blkId;

    DataOutputBuffer outBuffer;
    DataInputBuffer inBuffer = new DataInputBuffer();

    public BosBlockBuffer(String key, int blkId, int size) {
        this.key = key;
        this.blkId = blkId;
        outBuffer = new DataOutputBuffer(size);
    }

    public String getKey() {
        return key;
    }

    public int getBlkId() {
        return blkId;
    }

    public void setBlkId(int blkId) {
        this.blkId = blkId;
    }

    void moveData() {
        inBuffer.reset(outBuffer.getData(), outBuffer.getLength());
        outBuffer = new DataOutputBuffer(1);
    }

    void clear() {
        inBuffer.reset(null, 0);
    }
}