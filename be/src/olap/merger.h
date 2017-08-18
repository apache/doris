// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_OLAP_MERGER_H
#define BDG_PALO_BE_SRC_OLAP_MERGER_H

#include "olap/olap_define.h"
#include "olap/olap_table.h"

namespace palo {

class OLAPIndex;
class IData;

class Merger {
public:
    // parameter index is created by caller, and it is empty.
    Merger(SmartOLAPTable table, OLAPIndex* index, ReaderType type);

    virtual ~Merger() {};

    // @brief read from multiple OLAPData and OLAPIndex, then write into single OLAPData and
    // OLAPIndex. When use_simple_merge is true, check weather to create hard link.
    // @return  OLAPStatus: OLAP_SUCCESS or FAIL
    // @note it will take long time to finish.
    OLAPStatus merge(
            const std::vector<IData*>& olap_data_arr,
            bool use_simple_merge,
            uint64_t* merged_rows,
            uint64_t* filted_rows);

    // 获取在做merge过程中累积的行数
    uint64_t row_count() {
        return _row_count;
    }
    // 获取前缀组合的selectivity
    const std::vector<uint32_t>& selectivities() {
        return _selectivities;
    }

private:
    OLAPStatus _merge(
            const std::vector<IData*>& olap_data_arr,
            uint64_t* merged_rows,
            uint64_t* filted_rows);

    bool _check_simple_merge(const std::vector<IData*>& olap_data_arr);

    OLAPStatus _create_hard_link();

    SmartOLAPTable _table;
    OLAPIndex* _index;
    ReaderType _reader_type;
    uint64_t _row_count;
    std::vector<uint64_t> _uniq_keys;      // 存储每一种前缀组合的独特值个数
    std::vector<uint32_t> _selectivities;  // 保存每一种前缀组合的selectivity
    Version _simple_merge_version;

    DISALLOW_COPY_AND_ASSIGN(Merger);
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_MERGER_H
