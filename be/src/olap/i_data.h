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

#ifndef BDG_PALO_BE_SRC_OLAP_I_DATA_H
#define BDG_PALO_BE_SRC_OLAP_I_DATA_H

#include <string>
#include <vector>

#include "exprs/expr.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/delete_handler.h"
#include "olap/olap_common.h"
#include "olap/olap_cond.h"
#include "olap/olap_index.h"
#include "util/runtime_profile.h"

namespace palo {

class OLAPTable;
class OLAPIndex;
class RowBlock;
class RowCursor;
class Conditions;

// 抽象数据访问接口
// 提供对不同数据文件类型的统一访问接口
class IData {
public:
    // 工厂方法, 生成IData对象, 调用者获得新建的对象, 并负责delete释放
    static IData* create(OLAPIndex* olap_index);
    virtual ~IData() {}

    // 为了与之前兼容, 暴露部分index的接口
    Version version() const {
        return _olap_index->version();
    }
    VersionHash version_hash() const {
        return _olap_index->version_hash();
    }
    bool delete_flag() const {
        return _olap_index->delete_flag();
    }
    uint32_t num_segments() const {
        return _olap_index->num_segments();
    }
    time_t max_timestamp() const {
        return _olap_index->max_timestamp();
    }

    // 查询数据文件类型
    DataFileType data_file_type() {
        return _data_file_type;
    }

    // 下面这些函数的注释见OLAPData的注释
    virtual OLAPStatus init() = 0;

    virtual void set_conjuncts(std::vector<ExprContext*>* query_conjuncts, 
                               std::vector<ExprContext*>* delete_conjuncts) = 0;

    virtual const RowCursor* get_first_row() = 0;
    virtual const RowCursor* get_current_row() = 0;
    virtual const RowCursor* get_next_row() = 0;

    virtual const RowCursor* find_row(
            const RowCursor& key,
            bool find_last_key,
            bool is_end_key) = 0;

    virtual OLAPStatus set_end_key(const RowCursor* end_key, bool find_last_end_key) = 0;

    // 下面两个接口用于schema_change.cpp, 我们需要改功能继续做roll up,
    // 所以继续暴露该接口
    virtual OLAPStatus get_first_row_block(RowBlock** row_block) = 0;
    virtual OLAPStatus get_next_row_block(RowBlock** row_block) = 0;

    // 设置读取数据的参数, 这是一个后加入的接口, IData的实现可以根据这个接口提供
    // 信息做更多的优化. OLAPData不需要这个接口, ColumnData通过这个接口获取更多
    // 的上层信息以减少不必须要的数据读取.
    // Input:
    //   returns_columns - 设置RowCursor需要返回的列
    //   conditions - 设置查询的过滤条件
    //   begin_keys - 查询会使用的begin keys
    //   end_keys - 查询会使用的end keys
    virtual void set_read_params(const std::vector<uint32_t>& return_columns,
                                 const std::set<uint32_t>& load_bf_columns,
                                 const Conditions& conditions,
                                 const std::vector<RowCursor*>& start_keys,
                                 const std::vector<RowCursor*>& end_keys,
                                 bool is_using_cache,
                                 RuntimeState* runtime_state) {
        _conditions = &conditions;
        _runtime_state = runtime_state;
    }

    virtual void set_delete_handler(const DeleteHandler& delete_handler) {
        _delete_handler = delete_handler;
    }

    virtual void set_delete_status(const DelCondSatisfied delete_status) {
        _delete_status = delete_status;
    }

    void set_profile(RuntimeProfile* profile) {
        _profile = profile;
    }

    // 开放接口查询_eof，让外界知道数据读取是否正常终止
    // 因为这个函数被频繁访问, 从性能考虑, 放在基类而不是虚函数
    bool eof() {
        return _eof;
    }

    void set_eof(bool eof) {
        _eof = eof;
    }

    bool* eof_ptr() {
        return &_eof;
    }

    bool empty() const {
        return _olap_index->empty();
    }

    bool delta_pruning_filter();

    int delete_pruning_filter();

    virtual uint64_t get_filted_rows() {
        return 0;
    }

    OLAPIndex* olap_index() const {
        return _olap_index;
    }

    void set_olap_index(OLAPIndex* olap_index) {
        _olap_index = olap_index;
    }

    int64_t num_rows() const {
        return _olap_index->num_rows();
    }

    // pickle接口
    virtual OLAPStatus pickle() = 0;
    virtual OLAPStatus unpickle() = 0;

protected:
    // 基类必须指定data_file_type, 也必须关联一个OLAPIndex
    IData(DataFileType data_file_type, OLAPIndex* olap_index):
        _data_file_type(data_file_type),
        _olap_index(olap_index),
        _eof(false),
        _conditions(NULL),
        _delete_status(DEL_NOT_SATISFIED),
        _profile(NULL),
        _runtime_state(NULL) {
    }

protected:
    DataFileType _data_file_type;
    OLAPIndex* _olap_index;
    // 当到达文件末尾或者到达end key时设置此标志
    bool _eof;
    const Conditions* _conditions;
    DeleteHandler _delete_handler;
    DelCondSatisfied _delete_status;
    RuntimeProfile* _profile;
    RuntimeState* _runtime_state;

private:
    DISALLOW_COPY_AND_ASSIGN(IData);
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_I_DATA_H

