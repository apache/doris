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

#include <gen_cpp/DataSinks_types.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/file_writer.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/runtime/vfile_format_transformer.h"
#include "vec/runtime/vfile_format_transformer_factory.h"
#include "vec/sink/writer/async_result_writer.h"

namespace doris {
class RuntimeState;
class RuntimeProfile;

namespace vectorized {
class Block;

/**
 * VTVFTableWriter writes query result blocks to files (local/s3/hdfs)
 * via the TVF sink path: INSERT INTO tvf_name(properties) SELECT ...
 *
 * It inherits from AsyncResultWriter to perform IO in a separate thread pool,
 * avoiding blocking the pipeline execution engine.
 */
class VTVFTableWriter final : public AsyncResultWriter {
public:
    VTVFTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                    std::shared_ptr<pipeline::Dependency> dep,
                    std::shared_ptr<pipeline::Dependency> fin_dep);

    ~VTVFTableWriter() override = default;

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status write(RuntimeState* state, vectorized::Block& block) override;

    Status close(Status status) override;

private:
    Status _create_file_writer(const std::string& file_name);
    Status _create_next_file_writer();
    Status _close_file_writer(bool done);
    Status _create_new_file_if_exceed_size();
    Status _get_next_file_name(std::string* file_name);

    TTVFTableSink _tvf_sink;
    RuntimeState* _state = nullptr;

    std::unique_ptr<doris::io::FileWriter> _file_writer_impl;
    std::unique_ptr<VFileFormatTransformer> _vfile_writer;

    int64_t _current_written_bytes = 0;
    int64_t _max_file_size_bytes = 0;
    int _file_idx = 0;
    std::string _file_path;

    // profile counters
    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    RuntimeProfile::Counter* _written_data_bytes = nullptr;
    RuntimeProfile::Counter* _file_write_timer = nullptr;
    RuntimeProfile::Counter* _writer_close_timer = nullptr;
};

} // namespace vectorized
} // namespace doris
