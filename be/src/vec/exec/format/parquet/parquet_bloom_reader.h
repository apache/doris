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

#include "io/fs/file_reader.h"

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris {
namespace io {
class FileSystem;
struct IOContext;
} // namespace io

namespace vectorized {
class FileMetaData;
class VExprContext;
class BloomFilter;
} // namespace vectorized
struct TypeDescriptor;
} // namespace doris

namespace doris::vectorized {

class RowGroupBloomFilterReader {
public:
    virtual ~RowGroupBloomFilterReader() = default;

    virtual std::unique_ptr<BloomFilter> get_column_bloom_filter(int i) = 0;
};

class BloomFilterReader {
public:
    virtual ~BloomFilterReader() = default;

    /// \brief Create a BloomFilterReader instance.
    /// \returns a BloomFilterReader instance.
    /// WARNING: The returned BloomFilterReader references to all the input parameters, so
    /// it must not outlive all of the input parameters. Usually these input parameters
    /// come from the same ParquetFileReader object, so it must not outlive the reader
    /// that creates this BloomFilterReader.
    static std::unique_ptr<BloomFilterReader> make(io::FileReaderSPtr file_reader,
                                                   const tparquet::FileMetaData* file_metadata,
                                                   io::IOContext* io_context);

    /// \brief Get the bloom filter reader of a specific row group.
    /// \param[in] i row group ordinal to get bloom filter reader.
    /// \returns RowGroupBloomFilterReader of the specified row group. A nullptr may or may
    ///          not be returned if the bloom filter for the row group is unavailable. It
    ///          is the caller's responsibility to check the return value of follow-up calls
    ///          to the RowGroupBloomFilterReader.
    /// \throws ParquetException if the index is out of bound.
    virtual std::shared_ptr<RowGroupBloomFilterReader> row_group(int i) = 0;
    virtual std::shared_ptr<RowGroupBloomFilterReader> row_group(
            const tparquet::RowGroup* row_group) = 0;
};

}; // namespace doris::vectorized
