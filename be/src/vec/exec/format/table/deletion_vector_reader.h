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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/file_factory.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "roaring/roaring64map.hh"
#include "util/profile_collector.h"
#include "util/slice.h"
#include "vec/exec/format/generic_reader.h"

namespace io {
struct IOContext;
} // namespace io

namespace doris {
namespace vectorized {
class DeletionVectorReader {
    ENABLE_FACTORY_CREATOR(DeletionVectorReader);

public:
    DeletionVectorReader(RuntimeState* state, RuntimeProfile* profile,
                         const TFileScanRangeParams& params, const TFileRangeDesc& range,
                         io::IOContext* io_ctx)
            : _state(state), _profile(profile), _range(range), _params(params), _io_ctx(io_ctx) {}
    ~DeletionVectorReader() = default;
    Status open();
    Status read_at(size_t offset, Slice result);

private:
    void _init_system_properties();
    void _init_file_description();
    Status _create_file_reader();

private:
    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;
    const TFileRangeDesc& _range;
    const TFileScanRangeParams& _params;
    io::IOContext* _io_ctx = nullptr;

    io::FileSystemProperties _system_properties;
    io::FileDescription _file_description;
    io::FileReaderSPtr _file_reader;
    int64_t _file_size = 0;
    bool _is_opened = false;
};
} // namespace vectorized
} // namespace doris
