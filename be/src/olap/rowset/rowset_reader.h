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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_H

#include <memory>
#include <unordered_map>

#include "gen_cpp/olap_file.pb.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_reader_context.h"
#include "vec/core/block.h"

namespace doris {

namespace vectorized {
class Block;
}

class RowBlock;
class RowsetReader;
using RowsetReaderSharedPtr = std::shared_ptr<RowsetReader>;

class RowsetReader {
public:
    virtual ~RowsetReader() {}

    // reader init
    virtual OLAPStatus init(RowsetReaderContext* read_context) = 0;

    // read next block data into *block.
    // Returns
    //      OLAP_SUCCESS when read successfully.
    //      OLAP_ERR_DATA_EOF and set *block to null when there is no more block.
    //      Others when error happens.
    virtual OLAPStatus next_block(RowBlock** block) = 0;

    virtual OLAPStatus next_block(vectorized::Block* block) = 0;

    virtual bool delete_flag() = 0;

    virtual Version version() = 0;

    virtual RowsetSharedPtr rowset() = 0;

    virtual int64_t filtered_rows() = 0;

    virtual RowsetTypePB type() const = 0;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_READER_H
