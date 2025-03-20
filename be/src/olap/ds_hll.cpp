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

#include "olap/ds_hll.h"
#include "vec/common/string_ref.h"

#include "DataSketches/HllUtil.hpp"

namespace doris {
const std::unordered_map<std::string, ds_hll_type> DSHyperLogLog::ds_hll_map = {
        {"HLL_4", ds_hll_type::HLL_4},
        {"HLL_6", ds_hll_type::HLL_6},
        {"HLL_8", ds_hll_type::HLL_8}
};
const std::string DSHyperLogLog::DEFAULT_HLL_TYPE = "HLL_8";

DSHyperLogLog::DSHyperLogLog(uint8_t lg_k, std::string hll_type_str) : _lg_config_k(lg_k) {
    _check_lg_k();
    auto it = ds_hll_map.find(hll_type_str);
    if (it == ds_hll_map.end()) {
        throw std::invalid_argument("invalid ds hll map type: " + hll_type_str);
    }
    _hll_type = it->second;
}

DSHyperLogLog::DSHyperLogLog(uint8_t lg_k, ds_hll_type hll_type)
        : _lg_config_k(lg_k), _hll_type(hll_type){
    _check_lg_k();
}

DSHyperLogLog::DSHyperLogLog(const Slice& slice) {
    deserialize(slice);
}

void DSHyperLogLog::update(const void* data, size_t length_bytes) {
        get_sketch<true>()->update(data, length_bytes);
}

template <bool create_if_not_exists>
ds_hll_sketch* DSHyperLogLog::get_sketch() const {
    if (this->_sketch) {
        return _sketch.get();
    }
    if (this->_sketch_union) {
        this->_sketch = std::make_unique<ds_hll_sketch>(
                this->_sketch_union->get_result(_hll_type));
        this->_sketch_union.reset();
        return _sketch.get();
    }
    if constexpr (create_if_not_exists) {
        _sketch = std::make_unique<ds_hll_sketch >(_lg_config_k, _hll_type);
        return _sketch.get();
    } else {
        return nullptr;
    }
}

void DSHyperLogLog::merge(const DSHyperLogLog& other) {
    auto* other_sketch = other.get_sketch();
    if (!other_sketch) {
        return;
    }

    if (!_sketch_union) {
        _sketch_union = std::make_unique<ds_hll_union>(_lg_config_k);
    }
    if (_sketch) {
        _sketch_union->update(std::move(*_sketch));
        _sketch->reset();
    }
    _sketch_union->update(*other_sketch);
}

size_t DSHyperLogLog::serialized_size() const {
    auto* sketch = get_sketch();
    if (sketch) {
        return sketch->get_compact_serialization_bytes();
    }
    return 0;
}

void DSHyperLogLog::serialize(ds_vector_bytes& dst) const {
    auto* sketch = this->get_sketch();
    if (sketch) {
        dst = sketch->serialize_compact();
    }
}

bool DSHyperLogLog::deserialize(const Slice& slice) {
    try {
        _sketch = std::make_unique<ds_hll_sketch>(
                ds_hll_sketch::deserialize((uint8_t*)slice.data, slice.size));
        this->_lg_config_k = _sketch->get_lg_config_k();
        this->_hll_type = _sketch->get_target_type();
    } catch (std::invalid_argument& e) {
        LOG(WARNING) << "Get invalid_argument when deserialize ds_hll: " << e.what();
        return false;
    } catch (std::logic_error& e) {
        LOG(WARNING) << "Get logic_error when deserialize ds_hll: " << e.what();
        return false;
    }
    return true;
}

} // namespace doris end
