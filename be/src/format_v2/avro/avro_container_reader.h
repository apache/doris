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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "io/fs/file_reader_writer_fwd.h"

namespace avro {
class GenericDatum;
class ValidSchema;
} // namespace avro

namespace doris {
class RuntimeProfile;
}

namespace doris::io {
struct IOContext;
}

namespace doris::format::avro {

// Doris owns container framing and allocation admission; upstream Avro owns schema/value types.
class AvroContainerReader {
public:
    using Reserve = std::function<void(size_t)>;
    using Workspace = std::function<std::shared_ptr<void>(size_t)>;
    using Checkpoint = std::function<void(const ::avro::GenericDatum&, bool)>;
    AvroContainerReader(io::FileReaderSPtr file, std::shared_ptr<io::IOContext> io_context,
                        Reserve reserve, Checkpoint checkpoint, Workspace workspace,
                        RuntimeProfile* profile);
    ~AvroContainerReader();
    const ::avro::ValidSchema& dataSchema() const;
    std::optional<std::string> metadata(const std::string& key) const;
    size_t scratchBytes() const;
    size_t position() const;
    int64_t seek_range(int64_t start, bool absolute_positions);
    void init(const ::avro::ValidSchema& schema, int64_t offset);
    bool hasMoreBefore(int64_t end);
    std::unique_ptr<::avro::GenericDatum> make_datum();
    void read(::avro::GenericDatum& datum);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace doris::format::avro
