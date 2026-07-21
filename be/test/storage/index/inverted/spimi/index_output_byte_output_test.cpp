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

#include "storage/index/inverted/spimi/index_output_byte_output.h"

#include <CLucene.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/RAMDirectory.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/segment_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

std::vector<uint8_t> SlurpRamFile(lucene::store::RAMDirectory* dir, const char* name) {
    lucene::store::IndexInput* in = nullptr;
    CLuceneError err;
    EXPECT_TRUE(dir->openInput(name, in, err));
    if (in == nullptr) {
        return {};
    }
    const int64_t size = in->length();
    std::vector<uint8_t> bytes(static_cast<size_t>(size));
    if (size > 0) {
        in->readBytes(bytes.data(), size);
    }
    in->close();
    _CLDELETE(in);
    return bytes;
}

} // namespace

TEST(IndexOutputByteOutputTest, ForwardsPrimitivesToCLuceneIndexOutput) {
    lucene::store::RAMDirectory ram;
    auto* out = ram.createOutput("primitives.bin");
    {
        IndexOutputByteOutput adapter(out);
        adapter.WriteInt(0x01020304);
        adapter.WriteLong(-1);
        adapter.WriteVInt(300);
        adapter.WriteVLong(0x0102030405060708LL);
        EXPECT_GT(adapter.FilePointer(), 0);
    }
    out->close();
    _CLDELETE(out);

    // Reference: same primitives through MemoryByteOutput.
    MemoryByteOutput mem;
    mem.WriteInt(0x01020304);
    mem.WriteLong(-1);
    mem.WriteVInt(300);
    mem.WriteVLong(0x0102030405060708LL);

    EXPECT_EQ(SlurpRamFile(&ram, "primitives.bin"), mem.bytes());
}

TEST(IndexOutputByteOutputTest, SegmentWriterEmitsThroughCLuceneDirectory) {
    lucene::store::RAMDirectory ram;
    auto* tis = ram.createOutput("_0.tis");
    auto* tii = ram.createOutput("_0.tii");
    auto* frq = ram.createOutput("_0.frq");
    auto* prx = ram.createOutput("_0.prx");
    {
        IndexOutputByteOutput tis_out(tis);
        IndexOutputByteOutput tii_out(tii);
        IndexOutputByteOutput frq_out(frq);
        IndexOutputByteOutput prx_out(prx);
        SegmentWriter w(&tis_out, &tii_out, &frq_out, &prx_out);

        SpimiPostingBuffer buffer;
        buffer.Append("apple", 0, 0);
        buffer.Append("apple", 2, 1);
        buffer.Append("banana", 3, 5);
        buffer.Sort();
        w.Emit(buffer, 0);
        w.Close();
    }
    for (auto* o : {tis, tii, frq, prx}) {
        o->close();
        _CLDELETE(o);
    }

    // Reference: same buffer through MemoryByteOutputs.
    MemoryByteOutput m_tis, m_tii, m_frq, m_prx;
    SegmentWriter mw(&m_tis, &m_tii, &m_frq, &m_prx);
    SpimiPostingBuffer buffer;
    buffer.Append("apple", 0, 0);
    buffer.Append("apple", 2, 1);
    buffer.Append("banana", 3, 5);
    buffer.Sort();
    mw.Emit(buffer, 0);
    mw.Close();

    EXPECT_EQ(SlurpRamFile(&ram, "_0.tis"), m_tis.bytes());
    EXPECT_EQ(SlurpRamFile(&ram, "_0.tii"), m_tii.bytes());
    EXPECT_EQ(SlurpRamFile(&ram, "_0.frq"), m_frq.bytes());
    EXPECT_EQ(SlurpRamFile(&ram, "_0.prx"), m_prx.bytes());
}

} // namespace doris::segment_v2::inverted_index::spimi
