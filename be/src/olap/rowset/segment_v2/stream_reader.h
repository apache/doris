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

#include <memory>

#include "olap/rowset/segment_v2/column_reader.h"

namespace doris::segment_v2 {

// This file Defined ColumnIterator and ColumnReader for reading variant subcolumns. The types from read schema and from storage are
// different, so we need to wrap the ColumnIterator from execution phase and storage column reading phase.And we also
// maintain the tree structure to get the full JSON structure for variants.

// Wrapped ColumnIterator from execution phase, the type is from read schema
struct SubstreamIterator {
    vectorized::MutableColumnPtr column;
    std::unique_ptr<ColumnIterator> iterator;
    std::shared_ptr<const vectorized::IDataType> type;
    bool inited = false;
    size_t rows_read = 0;
    SubstreamIterator() = default;
    SubstreamIterator(vectorized::MutableColumnPtr&& col, std::unique_ptr<ColumnIterator>&& it,
                      std::shared_ptr<const vectorized::IDataType> t)
            : column(std::move(col)), iterator(std::move(it)), type(t) {}
};

// path -> StreamReader
using SubstreamReaderTree = vectorized::SubcolumnsTree<SubstreamIterator>;

// Reader for the storage layer, the file_column_type indicates the read type of the column in segment file
struct SubcolumnReader {
    std::unique_ptr<ColumnReader> reader;
    std::shared_ptr<const vectorized::IDataType> file_column_type;
};
using SubcolumnColumnReaders = vectorized::SubcolumnsTree<SubcolumnReader>;

} // namespace doris::segment_v2