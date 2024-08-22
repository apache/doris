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

#include <algorithm>
#include <queue>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_writer.h"
#include "io/fs/file_reader.h"
#include "sparse_utils.h"
#include "sparse_inverted_index_config.h"
#include "bitsetview.h"

namespace doris::segment_v2::sparse {

class BufferReader {
public:
    static const size_t DEFAULT_BUFFER_SIZE = 256 * 1024;

    explicit BufferReader(io::FileReaderSPtr& reader) :
                          reader(reader),
                          buffer_size(DEFAULT_BUFFER_SIZE),
                          buffer(buffer_size) {}

    Status next(char* data, size_t size, size_t* bytes_read) {
        size_t total_read = 0;

        while (total_read < size) {
            // Check if we have enough data in the buffer
            if (bytes_in_buffer > 0) {
                size_t to_copy = std::min(size - total_read, bytes_in_buffer);
                std::memcpy(data + total_read, buffer.data() + offset, to_copy);
                total_read += to_copy;
                offset += to_copy;
                bytes_in_buffer -= to_copy;

                // If we have read enough data, return
                if (total_read >= size) {
                    *bytes_read = total_read;
                    return Status::OK();
                }
            }

            // If buffer is empty, read more data from the file
            size_t bytes_read_from_file = 0;
            RETURN_IF_ERROR(reader->read_at(file_offset, Slice(buffer.data(), buffer_size), &bytes_read_from_file));
            file_offset += bytes_read_from_file;

            // Update the buffer state
            offset = 0; // Reset offset for the new data
            bytes_in_buffer = bytes_read_from_file;

            // If we read 0 bytes, it means we reached the end of the file
            if (bytes_read_from_file == 0) {
                break; // No more data to read
            }
        }

        *bytes_read = total_read;
        return Status::OK();
    }

private:
    io::FileReaderSPtr& reader;
    const size_t buffer_size;
    std::vector<char> buffer;
    size_t file_offset = 0;
    size_t offset = 0;           // current read offset of the buffer
    size_t bytes_in_buffer = 0;  // valid bytes in buffer
}; //class BufferReader


class BufferWriter {
public:
    static const size_t DEFAULT_BUFFER_SIZE = 256 * 1024;

    explicit BufferWriter(io::FileWriterPtr& writer) :
                          writer(writer),
                          buffer(DEFAULT_BUFFER_SIZE) {}

    // write
    Status append(const Slice& data) {
        size_t remaining = data.size;
        const char* current_data = data.data;

        while (remaining > 0) {
            size_t space_in_buffer = buffer.size() - offset;

            if (remaining < space_in_buffer) {
                std::memcpy(buffer.data() + offset, current_data, remaining);
                offset += remaining;
                break;
            } else {
                std::memcpy(buffer.data() + offset, current_data, space_in_buffer);
                offset += space_in_buffer;
                RETURN_IF_ERROR(flush());
                current_data += space_in_buffer;
                remaining -= space_in_buffer;
            }
        }

        return Status::OK();
    }

    Status flush() {
        if (offset > 0) {
            RETURN_IF_ERROR(writer->append({buffer.data(), offset}));
            offset = 0;
        }
        return Status::OK();
    }

private:
    io::FileWriterPtr& writer; // file_writer
    std::vector<char> buffer;
    size_t offset = 0; // write offset in buffer
};

class BaseInvertedIndex {
public:
    virtual ~BaseInvertedIndex() = default;

    virtual Status Save(io::FileWriterPtr& writer) = 0;

    virtual Status Load(io::FileReaderSPtr& reader) = 0;

    virtual Status Train(const SparseRow* data,
                         size_t rows,
                         float drop_ratio_build) = 0;

    virtual Status Add(const SparseRow* data,
                       size_t rows,
                       int64_t dim,
                       uint32_t* row_ids) = 0;

    virtual void Search(const SparseRow& query,
                        size_t k,
                        float drop_ratio_search,
                        float* distances,
                        label_t* labels,
                        size_t refine_factor,
                        const BitsetView& bitset,
                        const DocValueComputer& computer) const = 0;

    virtual std::vector<float> GetAllDistances(const SparseRow& query,
                                               float drop_ratio_search,
                                               const BitsetView& bitset,
                                               const DocValueComputer& computer) const = 0;

    virtual void GetVectorById(const label_t id, SparseRow& output) const = 0;

    virtual DocValueComputer GetDocValueComputer(const SparseInvertedIndexConfig& cfg) const = 0;

    [[nodiscard]] virtual size_t size() const = 0;

    [[nodiscard]] virtual size_t n_rows() const = 0;

    [[nodiscard]] virtual size_t n_cols() const = 0;
}; //class BaseInvertedIndex

template <bool use_wand = false>
class InvertedIndex : public BaseInvertedIndex {
public:
    explicit InvertedIndex() {}

    DocValueComputer GetDocValueComputer(const SparseInvertedIndexConfig& cfg) const override {
        return GetDocValueOriginalComputer();
    }

    Status Save(io::FileWriterPtr& writer) override {
        std::shared_lock<std::shared_mutex> lock(mu_);

        BufferWriter buffer_writer(writer);

        // write version
        size_t current_version = 1;
        RETURN_IF_ERROR(buffer_writer.append({reinterpret_cast<const char*>(&current_version), sizeof(current_version)}));

        // write max dim
        RETURN_IF_ERROR(buffer_writer.append({reinterpret_cast<const char*>(&max_dim_), sizeof(max_dim_)}));

        // write max rowid
        RETURN_IF_ERROR(buffer_writer.append({reinterpret_cast<const char*>(&max_rowid_), sizeof(max_rowid_)}));

        // write unordered_map size
        size_t map_size = inverted_lut_.size();
        RETURN_IF_ERROR(buffer_writer.append({reinterpret_cast<const char*>(&map_size), sizeof(map_size)}));

        // write posting vector item size
        size_t item_size = sizeof(SparseIdVal);
        RETURN_IF_ERROR(buffer_writer.append({reinterpret_cast<const char*>(&item_size), sizeof(item_size)}));

        for (const auto& pair : inverted_lut_) {
            // write posting key
            RETURN_IF_ERROR(buffer_writer.append({reinterpret_cast<const char*>(&pair.first), sizeof(pair.first)}));

            // write max_score_in_key
            float max_score_in_dim = 0.0;
            auto it = max_score_in_dim_.find(pair.first);
            if (it != max_score_in_dim_.end()) {
                max_score_in_dim = it->second;
            }
            RETURN_IF_ERROR(buffer_writer.append({reinterpret_cast<const char*>(&max_score_in_dim), sizeof(max_score_in_dim)}));

            // write posting vector size
            size_t vec_size = pair.second.size();
            RETURN_IF_ERROR(buffer_writer.append({reinterpret_cast<const char*>(&vec_size), sizeof(vec_size)}));

            // write posting value
            if (vec_size > 0) {
                RETURN_IF_ERROR(buffer_writer.append({reinterpret_cast<const char*>(pair.second.data()), vec_size * item_size}));
            }
        }

        RETURN_IF_ERROR(buffer_writer.flush());

        return Status::OK();
    }

    Status Load(io::FileReaderSPtr& reader) override {
        std::unique_lock<std::shared_mutex> lock(mu_);

        BufferReader buffer_reader(reader);
        size_t bytes_read = 0;
        Status st;

        // read file format version
        size_t version;
        st = buffer_reader.next(reinterpret_cast<char*>(&version), sizeof(version), &bytes_read);
        if (!st.ok() || bytes_read < sizeof(version)) {
            return Status::Corruption("Not enough data to read version");
        }

        // read max_dim
        st = buffer_reader.next(reinterpret_cast<char*>(&max_dim_), sizeof(max_dim_), &bytes_read);
        if (!st.ok() || bytes_read < sizeof(max_dim_)) {
            return Status::Corruption("Not enough data to read max dim");
        }

        // read max_rowid
        st = buffer_reader.next(reinterpret_cast<char*>(&max_rowid_), sizeof(max_rowid_), &bytes_read);
        if (!st.ok() || bytes_read < sizeof(max_rowid_)) {
            return Status::Corruption("Not enough data to read max rowid");
        }

        // read unordered_map size
        size_t map_size;
        st = buffer_reader.next(reinterpret_cast<char*>(&map_size), sizeof(map_size), &bytes_read);
        if (!st.ok() || bytes_read < sizeof(map_size)) {
            return Status::Corruption("Not enough data to read map size");
        }

        // read posting vector item size
        size_t item_size;
        st = buffer_reader.next(reinterpret_cast<char*>(&item_size), sizeof(item_size), &bytes_read);
        if (!st.ok() || bytes_read < sizeof(item_size)) {
            return Status::Corruption("Not enough data to read item size");
        }

        for (size_t i = 0; i < map_size; ++i) {
            table_t key;
            st = buffer_reader.next(reinterpret_cast<char*>(&key), sizeof(key), &bytes_read);
            if (!st.ok() || bytes_read < sizeof(key)) {
                return Status::Corruption("Not enough data to read key, key_offset:{}, expect bytes:{}, actual bytes:{}", i, sizeof(key), bytes_read);
            }

            // read max_score_in_dim
            float max_score_in_dim = 0.0f;
            st = buffer_reader.next(reinterpret_cast<char*>(&max_score_in_dim), sizeof(max_score_in_dim), &bytes_read);
            if (!st.ok() || bytes_read < sizeof(max_score_in_dim)) {
                return Status::Corruption("Not enough data to read max score in dim, key_offset:{}, expect bytes:{}, actual bytes:{}", i, sizeof(key), bytes_read);
            }
            max_score_in_dim_[key] = max_score_in_dim;

            // read posting vector size
            size_t vec_size;
            st = buffer_reader.next(reinterpret_cast<char*>(&vec_size), sizeof(vec_size), &bytes_read);
            if (!st.ok() || bytes_read < sizeof(vec_size)) {
                return Status::Corruption("Not enough data to read vector size, key_offset:{}, expect bytes:{}, actual bytes:{}", i, sizeof(vec_size), bytes_read);
            }

            std::vector<SparseIdVal> vec(vec_size);
            if (vec_size > 0) {
                st = buffer_reader.next(reinterpret_cast<char*>(vec.data()), vec_size * item_size, &bytes_read);
                if (!st.ok() || bytes_read < vec_size * item_size) {
                    return Status::Corruption("Not enough data to read vector elements, key_offset:{}, expect bytes:{}, actual bytes:{}", i, sizeof(vec_size * item_size), bytes_read);
                }
            }

            inverted_lut_.emplace(key, std::move(vec));
        }

        return Status::OK();
    }

    // Non zero drop ratio is only supported for static index, i.e. data should
    // include all rows that'll be added to the index.
    Status Train(const SparseRow* data, size_t rows, float drop_ratio_build) override {
        if (drop_ratio_build == 0.0f) {
            return Status::OK();
        }
        // TODO: maybe i += 10 to down sample to speed up.
        size_t amount = 0;
        for (size_t i = 0; i < rows; ++i) {
            amount += data[i].size();
        }
        if (amount == 0) {
            return Status::OK();
        }
        std::vector<float> vals;
        vals.reserve(amount);
        for (size_t i = 0; i < rows; ++i) {
            for (size_t j = 0; j < data[i].size(); ++j) {
                vals.push_back(fabs(data[i][j].val));
            }
        }
        auto pos = vals.begin() + static_cast<size_t>(drop_ratio_build * vals.size());
        // pos may be vals.end() if drop_ratio_build is 1.0, in that case we use
        // the largest value as the threshold.
        if (pos == vals.end()) {
            pos--;
        }
        std::nth_element(vals.begin(), pos, vals.end());

        std::unique_lock<std::shared_mutex> lock(mu_);
        value_threshold_ = *pos;
        drop_during_build_ = true;
        return Status::OK();
    }

    Status Add(const SparseRow* data, size_t rows, int64_t dim, uint32_t* row_ids) override {
        std::unique_lock<std::shared_mutex> lock(mu_);

        if ((size_t)dim > max_dim_) {
            max_dim_ = dim;
        }

        for (size_t i = 0; i < rows; ++i) {
            add_row_to_index(data[i], row_ids[i]);

            max_rowid_ = std::max(row_ids[i], max_rowid_);
        }

        return Status::OK();
    }

    void Search(const SparseRow& query, size_t k, float drop_ratio_search,
                float* distances, label_t* labels, size_t refine_factor,
                const BitsetView& bitset, const DocValueComputer& computer) const override {
        // initially set result distances to NaN and labels to -1
        std::fill(distances, distances + k, std::numeric_limits<float>::quiet_NaN());
        std::fill(labels, labels + k, -1);
        if (query.size() == 0) {
            return;
        }

        std::vector<float> values(query.size());
        for (size_t i = 0; i < query.size(); ++i) {
            values[i] = std::abs(query[i].val);
        }
        auto pos = values.begin() + static_cast<size_t>(drop_ratio_search * values.size());
        std::nth_element(values.begin(), pos, values.end());
        auto q_threshold = *pos;

        std::shared_lock<std::shared_mutex> lock(mu_);
        // if no data was dropped during both build and search, no refinement is
        // needed.
        if (!drop_during_build_ && drop_ratio_search == 0) {
            refine_factor = 1;
        }
        MaxMinHeap<float> heap(k * refine_factor);
        if constexpr (!use_wand) {
            search_brute_force(query, q_threshold, heap, bitset, computer);
        } else {
            search_wand(query, q_threshold, heap, bitset, computer);
        }

        if (refine_factor == 1) {
            collect_result(heap, distances, labels);
        } else {
            refine_and_collect(query, heap, k, distances, labels, computer);
        }
    }

    std::vector<float> GetAllDistances(const SparseRow& query, float drop_ratio_search,
                                       const BitsetView& bitset,
                                       const DocValueComputer& computer) const override {
        if (query.size() == 0) {
            return {};
        }
        std::vector<float> values(query.size());
        for (size_t i = 0; i < query.size(); ++i) {
            values[i] = std::abs(query[i].val);
        }
        auto pos = values.begin() + static_cast<size_t>(drop_ratio_search * values.size());
        std::nth_element(values.begin(), pos, values.end());
        auto q_threshold = *pos;
        std::shared_lock<std::shared_mutex> lock(mu_);
        auto distances = compute_all_distances(query, q_threshold, computer);
        for (size_t i = 0; i < distances.size(); ++i) {
            if (bitset.contains(i)) {
                continue;
            }
            distances[i] = 0.0f;
        }
        return distances;
    }

    void GetVectorById(const label_t id, SparseRow& output) const override {
        output = raw_data_[id];
    }

    [[nodiscard]] size_t size() const override {
        // TODO:
        std::shared_lock<std::shared_mutex> lock(mu_);
        size_t res = sizeof(*this);
        res += sizeof(SparseRow) * n_rows_internal();
        for (auto& row : raw_data_) {
            res += row.memory_usage();
        }

        res += (sizeof(table_t) + sizeof(std::vector<SparseIdVal>)) * inverted_lut_.size();
        for (const auto& [idx, lut] : inverted_lut_) {
            res += sizeof(SparseIdVal) * lut.capacity();
        }
        if constexpr (use_wand) {
            res += (sizeof(table_t) + sizeof(float)) * max_score_in_dim_.size();
        }
        return res;
    }

    [[nodiscard]] size_t n_rows() const override {
        std::shared_lock<std::shared_mutex> lock(mu_);
        return n_rows_internal();
    }

    [[nodiscard]] size_t n_cols() const override {
        std::shared_lock<std::shared_mutex> lock(mu_);
        return n_cols_internal();
    }

private:
    size_t n_rows_internal() const {
        return raw_data_.size();
    }

    size_t n_cols_internal() const {
        return max_dim_;
    }

    size_t max_rowid_internal() const {
        return max_rowid_;
    }

    std::vector<float> compute_all_distances(const SparseRow& q_vec, float q_threshold,
                                             const DocValueComputer& computer) const {
        std::vector<float> scores(n_rows_internal(), 0.0f);
        for (size_t idx = 0; idx < q_vec.size(); ++idx) {
            auto [i, v] = q_vec[idx];
            if (v < q_threshold || i >= n_cols_internal()) {
                continue;
            }
            auto lut_it = inverted_lut_.find(i);
            if (lut_it == inverted_lut_.end()) {
                continue;
            }
            // TODO: improve with SIMD
            auto& lut = lut_it->second;
            for (size_t j = 0; j < lut.size(); j++) {
                auto [doc_id, val] = lut[j];
                float val_sum = 0;
                scores[doc_id] += v * computer(val, val_sum);
            }
        }
        return scores;
    }

    // find the top-k candidates using brute force search, k as specified by the capacity of the heap.
    // any value in q_vec that is smaller than q_threshold and any value with dimension >= n_cols() will be ignored.
    // TODO: may switch to row-wise brute force if filter rate is high. Benchmark needed.
    void search_brute_force(const SparseRow& q_vec, float q_threshold, MaxMinHeap<float>& heap,
                            const BitsetView& bitset, const DocValueComputer& computer) const {
        auto scores = compute_all_distances(q_vec, q_threshold, computer);
        for (size_t i = 0; i < n_rows_internal(); ++i) {
            if (bitset.contains(i) && scores[i] != 0) {
                heap.push(i, scores[i]);
            }
        }
    }

    // LUT supports size() and operator[] which returns an SparseIdVal.
    template<typename LUT> class Cursor {
    public:
        Cursor(const LUT& lut, size_t num_vec, float max_score, float q_value, const BitsetView bitset)
            : lut_(lut), num_vec_(num_vec), max_score_(max_score), q_value_(q_value), bitset_(bitset) {
            while (loc_ < lut_.size() && !bitset_.contains(cur_vec_id())) {
                loc_++;
            }
        }
        Cursor(const Cursor& rhs) = delete;

        void next() {
            loc_++;
            while (loc_ < lut_.size() && !bitset_.contains(cur_vec_id())) {
                loc_++;
            }
        }

        // advance loc until cur_vec_id() >= vec_id
        void seek(table_t vec_id) {
            while (loc_ < lut_.size() && cur_vec_id() < vec_id) {
                next();
            }
        }

        [[nodiscard]] table_t cur_vec_id() const {
            if (is_end()) {
                return num_vec_;
            }
            return lut_[loc_].id;
        }

        float cur_vec_val() const {
            return lut_[loc_].val;
        }

        [[nodiscard]] bool is_end() const {
            return loc_ >= size();
        }

        [[nodiscard]] float q_value() const {
            return q_value_;
        }

        [[nodiscard]] size_t size() const {
            return lut_.size();
        }

        [[nodiscard]] float max_score() const {
            return max_score_;
        }

    private:
        const LUT& lut_;
        size_t loc_ = 0;
        size_t num_vec_ = 0;
        float max_score_ = 0.0f;
        float q_value_ = 0.0f;
        const BitsetView bitset_;
    };  // class Cursor

    // any value in q_vec that is smaller than q_threshold will be ignored.
    void search_wand(const SparseRow& q_vec, float q_threshold, MaxMinHeap<float>& heap,
                     const BitsetView& bitset, const DocValueComputer& computer) const {
        auto q_dim = q_vec.size();
        std::vector<std::shared_ptr<Cursor<std::vector<SparseIdVal>>>> cursors(q_dim);
        auto valid_q_dim = 0;
        for (size_t i = 0; i < q_dim; ++i) {
            auto [idx, val] = q_vec[i];
            if (std::abs(val) < q_threshold || idx >= max_dim_) {
                continue;
            }
            auto lut_it = inverted_lut_.find(idx);
            if (lut_it == inverted_lut_.end()) {
                continue;
            }
            auto& lut = lut_it->second;
            cursors[valid_q_dim++] = std::make_shared<Cursor<std::vector<SparseIdVal>>>(
                    lut, max_rowid_ + 1, max_score_in_dim_.find(idx)->second * val,
                    val, bitset);
        }
        if (valid_q_dim == 0) {
            return;
        }
        cursors.resize(valid_q_dim);
        auto sort_cursors = [&cursors] {
            std::sort(cursors.begin(), cursors.end(),
                      [](auto& x, auto& y) { return x->cur_vec_id() < y->cur_vec_id(); });
        };
        sort_cursors();
        auto score_above_threshold = [&heap](float x) { return !heap.full() || x > heap.top().val; };
        while (true) {
            float upper_bound = 0;
            size_t pivot;
            bool found_pivot = false;
            for (pivot = 0; pivot < cursors.size(); ++pivot) {
                if (cursors[pivot]->is_end()) {
                    break;
                }
                upper_bound += cursors[pivot]->max_score();
                if (score_above_threshold(upper_bound)) {
                    found_pivot = true;
                    break;
                }
            }
            if (!found_pivot) {
                break;
            }
            table_t pivot_id = cursors[pivot]->cur_vec_id();
            if (pivot_id == cursors[0]->cur_vec_id()) {
                float score = 0;
                for (auto& cursor : cursors) {
                    if (cursor->cur_vec_id() != pivot_id) {
                        break;
                    }
                    float cur_vec_sum = 0;
                    score += cursor->q_value() * computer(cursor->cur_vec_val(), cur_vec_sum);
                    cursor->next();
                }
                heap.push(pivot_id, score);
                sort_cursors();
            } else {
                size_t next_list = pivot;
                for (; cursors[next_list]->cur_vec_id() == pivot_id; --next_list) {
                }
                cursors[next_list]->seek(pivot_id);
                for (size_t i = next_list + 1; i < cursors.size(); ++i) {
                    if (cursors[i]->cur_vec_id() >= cursors[i - 1]->cur_vec_id()) {
                        break;
                    }
                    std::swap(cursors[i], cursors[i - 1]);
                }
            }
        }
    }

    void refine_and_collect(const SparseRow& q_vec, MaxMinHeap<float>& inaccurate, size_t k,
                            float* distances, label_t* labels,
                            const DocValueComputer& computer) const {
        std::priority_queue<SparseIdVal, std::vector<SparseIdVal>, std::greater<SparseIdVal>> heap;

        while (!inaccurate.empty()) {
            auto [u, d] = inaccurate.top();
            inaccurate.pop();

            float u_sum = 0;

            auto dist_acc = q_vec.dot(raw_data_[u], computer, u_sum);
            if (heap.size() < k) {
                heap.emplace(u, dist_acc);
            } else if (heap.top().val < dist_acc) {
                heap.pop();
                heap.emplace(u, dist_acc);
            }
        }
        collect_result(heap, distances, labels);
    }

    template <typename HeapType> void collect_result(HeapType& heap, float* distances,
                                                     label_t* labels) const {
        int cnt = heap.size();
        for (auto i = cnt - 1; i >= 0; --i) {
            labels[i] = heap.top().id;
            distances[i] = heap.top().val;
            heap.pop();
        }
    }

    void add_row_to_index(const SparseRow& row, table_t id) {
        [[maybe_unused]] float row_sum = 0;
        for (size_t j = 0; j < row.size(); ++j) {
            auto [idx, val] = row[j];

            // Skip values equals to or close enough to zero(which contributes
            // little to the total IP score).
            if (val == 0 || (drop_during_build_ && fabs(val) < value_threshold_)) {
                continue;
            }
            if (inverted_lut_.find(idx) == inverted_lut_.end()) {
                inverted_lut_[idx];
                if constexpr (use_wand) {
                    max_score_in_dim_[idx] = 0;
                }
            }
            inverted_lut_[idx].emplace_back(id, val);
            if constexpr (use_wand) {
                auto score = val;
                max_score_in_dim_[idx] = std::max(max_score_in_dim_[idx], score);
            }
        }
    }

    std::vector<SparseRow> raw_data_;
    mutable std::shared_mutex mu_;

    std::unordered_map<table_t, std::vector<SparseIdVal>> inverted_lut_;
    // If we want to drop small values during build, we must first train the
    // index with all the data to compute value_threshold_.
    bool drop_during_build_ = false;
    // when drop_during_build_ is true, any value smaller than value_threshold_
    // will not be added to inverted_lut_. value_threshold_ is set to the
    // drop_ratio_build-th percentile of all absolute values in the index.
    float value_threshold_ = 0.0f;
    std::unordered_map<table_t, float> max_score_in_dim_;
    size_t max_dim_ = 0;
    uint32_t max_rowid_ = 0;
};  // class InvertedIndex

}