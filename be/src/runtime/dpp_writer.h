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

#ifndef DORIS_BE_RUNTIME_DPP_WRITER_H
#define DORIS_BE_RUNTIME_DPP_WRITER_H

#include "common/status.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/file_helper.h"

namespace doris {

class ExprContext;
class TupleRow;
class RowBatch;

// Class used to convert to DPP output
// this is used for don't change code in storage.
class DppWriter {
public:
    DppWriter(int32_t schema_hash, const std::vector<ExprContext*>& output_expr, FileHandler* fp);

    ~DppWriter();

    // Some prepare work complete here.
    Status open();

    // Add one batch to this writer
    Status add_batch(RowBatch* row_batch);

    // Close this writer
    // Write needed header to this file.
    Status close();

private:
    Status write_header();
    void reset_buf();
    void append_to_buf(const void* ptr, int len);
    void increase_buf(int len);
    Status append_one_row(TupleRow* row);

    // pass by client, data write to this file
    // owned by client, doesn't care about this
    int32_t _schema_hash;
    int _num_null_slots;
    int _num_null_bytes;
    const std::vector<ExprContext*>& _output_expr_ctxs;
    FileHandler* _fp;

    char* _buf;
    char* _end;
    char* _pos;

    int64_t _write_len;
    uint32_t _content_adler32;

    FileHeader<OLAPRawDeltaHeaderMessage, int32_t, FileHandler> _header;
};

} // namespace doris

#endif
