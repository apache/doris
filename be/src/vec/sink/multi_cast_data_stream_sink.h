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

#include "pipeline/exec/multi_cast_data_streamer.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris::vectorized {

class MultiCastDataStreamSink : public DataSink {
public:
    MultiCastDataStreamSink(std::shared_ptr<pipeline::MultiCastDataStreamer>& streamer)
            : DataSink(streamer->row_desc()), _multi_cast_data_streamer(streamer) {
        _profile = _multi_cast_data_streamer->profile();
        init_sink_common_profile();
    };

    ~MultiCastDataStreamSink() override = default;

    Status send(RuntimeState* state, Block* block, bool eos = false) override {
        static_cast<void>(_multi_cast_data_streamer->push(state, block, eos));
        return Status::OK();
    };

    Status open(doris::RuntimeState* state) override { return Status::OK(); };

    // use sink to check can_write, now always true after we support spill to disk
    bool can_write() override { return _multi_cast_data_streamer->can_write(); }

    std::shared_ptr<pipeline::MultiCastDataStreamer>& get_multi_cast_data_streamer() {
        return _multi_cast_data_streamer;
    }

    Status close(RuntimeState* state, Status exec_status) override {
        _multi_cast_data_streamer->set_eos();
        return DataSink::close(state, exec_status);
    }

private:
    std::shared_ptr<pipeline::MultiCastDataStreamer> _multi_cast_data_streamer;
};
} // namespace doris::vectorized