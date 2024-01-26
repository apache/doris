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

#include "parquet_bloom_reader.h"

#include "parquet_bloom.h"

namespace doris::vectorized {

class RowGroupBloomFilterReaderImpl final : public RowGroupBloomFilterReader {
public:
    RowGroupBloomFilterReaderImpl(io::FileReaderSPtr file_reader,
                                  const tparquet::RowGroup* row_group,
                                  const io::IOContext* io_context)
            : file_reader_(std::move(file_reader)),
              row_group_(row_group),
              io_context_(io_context) {}

    std::unique_ptr<BloomFilter> get_column_bloom_filter(int i) override;

private:
    io::FileReaderSPtr file_reader_ = nullptr;

    const tparquet::RowGroup* row_group_;

    const io::IOContext* io_context_;
};

std::unique_ptr<BloomFilter> RowGroupBloomFilterReaderImpl::get_column_bloom_filter(const int i) {
    const auto& meta_data = row_group_->columns[i].meta_data;
    if (!meta_data.__isset.bloom_filter_offset) {
        return nullptr;
    }

    const auto bloom_filter_offset = meta_data.bloom_filter_offset;
    const auto file_size = file_reader_->size();
    if (file_size <= bloom_filter_offset) {
        return nullptr;
    }

    auto bloom_filter =
            BlockSplitBloomFilter::deserialize(file_reader_, bloom_filter_offset, io_context_);
    return std::make_unique<BlockSplitBloomFilter>(std::move(bloom_filter));
}

class BloomFilterReaderImpl final : public BloomFilterReader {
public:
    BloomFilterReaderImpl(io::FileReaderSPtr file_reader,
                          const tparquet::FileMetaData* file_metadata,
                          const io::IOContext* io_context)
            : file_reader_(std::move(file_reader)),
              file_metadata_(file_metadata),
              io_context_(io_context) {}

    std::shared_ptr<RowGroupBloomFilterReader> row_group(int i) override {
        if (i < 0 || i >= file_metadata_->num_rows) {
            //throw ParquetException("Invalid row group ordinal: ", i);
            return nullptr;
        }

        const tparquet::RowGroup* row_group = &file_metadata_->row_groups[i];
        return std::make_shared<RowGroupBloomFilterReaderImpl>(file_reader_, row_group,
                                                               io_context_);
    }

    std::shared_ptr<RowGroupBloomFilterReader> row_group(
            const tparquet::RowGroup* row_group) override {
        return std::make_shared<RowGroupBloomFilterReaderImpl>(file_reader_, row_group,
                                                               io_context_);
    }

private:
    /// The input stream that can perform random read.
    io::FileReaderSPtr file_reader_;

    /// The file metadata to get row group metadata.
    const tparquet::FileMetaData* file_metadata_;

    const io::IOContext* io_context_;
};

std::unique_ptr<BloomFilterReader> BloomFilterReader::make(
        io::FileReaderSPtr file_reader, const tparquet::FileMetaData* file_metadata,
        io::IOContext* io_context) {
    return std::make_unique<BloomFilterReaderImpl>(file_reader, file_metadata, io_context);
}

} // namespace doris::vectorized
