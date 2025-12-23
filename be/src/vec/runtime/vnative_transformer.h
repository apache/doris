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

#pragma once

#include <gen_cpp/PlanNodes_types.h>

#include <cstdint>

#include "common/status.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vfile_format_transformer.h"

namespace doris::io {
class FileWriter;
} // namespace doris::io

namespace doris::vectorized {
class Block;

#include "common/compile_check_begin.h"

// Doris Native format writer.
// It serializes vectorized Blocks into Doris Native binary format.
class VNativeTransformer final : public VFileFormatTransformer {
public:
    // |compress_type| controls how the PBlock is compressed on disk (ZSTD, LZ4, etc).
    // Defaults to ZSTD to preserve the previous behavior.
    VNativeTransformer(RuntimeState* state, doris::io::FileWriter* file_writer,
                       const VExprContextSPtrs& output_vexpr_ctxs, bool output_object_data,
                       TFileCompressType::type compress_type = TFileCompressType::ZSTD);

    ~VNativeTransformer() override = default;

    Status open() override;

    Status write(const Block& block) override;

    Status close() override;

    int64_t written_len() override;

private:
    doris::io::FileWriter* _file_writer; // not owned
    int64_t _written_len = 0;
    // Compression type used for Block::serialize (PBlock compression).
    segment_v2::CompressionTypePB _compression_type {segment_v2::CompressionTypePB::ZSTD};
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
