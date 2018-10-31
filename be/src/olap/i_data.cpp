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

#include "olap/i_data.h"

#include "olap/column_file/column_data.h"
#include "olap/olap_data.h"
#include "olap/rowset.h"

namespace palo {

IData* IData::create(Rowset* index) {
    IData* data = NULL;
    DataFileType file_type = index->table()->data_file_type();

    switch (file_type) {
    case OLAP_DATA_FILE:
        data = new(std::nothrow) OLAPData(index);
        break;

    case COLUMN_ORIENTED_FILE:
        data = new(std::nothrow) column_file::ColumnData(index);
        break;

    default:
        OLAP_LOG_WARNING("unknown data file type. [type=%s]",
                         DataFileType_Name(file_type).c_str());
    }

    return data;
}

bool IData::delta_pruning_filter() {
    if (empty() || zero_num_rows()) {
        return true;
    }
            
    if (!_olap_index->has_column_statistics()) {
        return false;
    }
    
    return _conditions->delta_pruning_filter(_olap_index->get_column_statistics());
}

int IData::delete_pruning_filter() {
    if (empty() || zero_num_rows()) {
        // should return DEL_NOT_SATISFIED, because that when creating rollup table,
        // the delete version file should preserved for filter data.
        return DEL_NOT_SATISFIED;
    }

    if (false == _olap_index->has_column_statistics()) {
        /*
         * if olap_index has no column statistics, we cannot judge whether the data can be filtered or not
         */
        return DEL_PARTIAL_SATISFIED;
    }

    /*
     * the relationship between delete condition A and B is A || B.
     * if any delete condition is satisfied, the data can be filtered.
     * elseif all delete condition is not satifsified, the data can't be filtered.
     * else is the partial satisfied.
    */
    int ret = DEL_PARTIAL_SATISFIED;
    bool del_partial_stastified = false;
    bool del_stastified = false;
    for (auto& delete_condtion : _delete_handler.get_delete_conditions()) {
        if (delete_condtion.filter_version <= _olap_index->version().first) {
            continue;
        }

        Conditions* del_cond = delete_condtion.del_cond;
        int del_ret = del_cond->delete_pruning_filter(_olap_index->get_column_statistics());
        if (DEL_SATISFIED == del_ret) {
            del_stastified = true;
            break;
        } else if (DEL_PARTIAL_SATISFIED == del_ret) {
            del_partial_stastified = true;
        } else {
            continue;
        }
    }

    if (true == del_stastified) {
        ret = DEL_SATISFIED;
    } else if (true == del_partial_stastified) {
        ret = DEL_PARTIAL_SATISFIED;
    } else {
        ret = DEL_NOT_SATISFIED;
    }

    return ret;
}

}  // namespace palo
