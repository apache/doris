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

#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/ListShardsResult.h>
#include <aws/kinesis/model/ChildShard.h>

#include <map>
#include <string>
#include <vector>

namespace doris {

// A deterministic upstream used by BE Kinesis tests. It models the three AWS calls used by
// routine load instead of bypassing the AWS response types at the caller.
class KinesisFakeClient final : public Aws::Kinesis::KinesisClient {
public:
    struct Shard {
        std::string id;
        std::string parent;
        std::string adjacent_parent;
        bool closed = false;
    };

    struct RecordsPage {
        std::vector<std::string> sequences;
        std::string next_iterator;
        int64_t millis_behind_latest = 0;
        std::map<std::string, std::vector<std::string>> child_parents;
    };

    void set_list_shards_pages(std::vector<std::vector<Shard>> pages) {
        _list_shards_pages = std::move(pages);
    }

    void set_records_pages(const std::string& shard_id, std::vector<RecordsPage> pages) {
        _records_pages[shard_id] = std::move(pages);
    }

    void set_initial_iterator(const std::string& iterator, const std::string& shard_id) {
        _iterator_to_shard[iterator] = shard_id;
    }

    int list_shards_calls() const { return _list_shards_calls; }
    int get_shard_iterator_calls() const { return _get_shard_iterator_calls; }
    int get_records_calls(const std::string& shard_id) const {
        auto it = _get_records_calls.find(shard_id);
        return it == _get_records_calls.end() ? 0 : it->second;
    }

    Aws::Kinesis::Model::ListShardsOutcome ListShards(
            const Aws::Kinesis::Model::ListShardsRequest& request) const override {
        ++_list_shards_calls;
        size_t page = 0;
        if (request.NextTokenHasBeenSet()) {
            page = std::stoul(request.GetNextToken());
        }
        if (page >= _list_shards_pages.size()) {
            Aws::Kinesis::Model::ListShardsResult result;
            return result;
        }

        Aws::Kinesis::Model::ListShardsResult result;
        for (const auto& shard : _list_shards_pages[page]) {
            Aws::Kinesis::Model::Shard aws_shard;
            aws_shard.SetShardId(shard.id);
            if (!shard.parent.empty()) {
                aws_shard.SetParentShardId(shard.parent);
            }
            if (!shard.adjacent_parent.empty()) {
                aws_shard.SetAdjacentParentShardId(shard.adjacent_parent);
            }
            Aws::Kinesis::Model::SequenceNumberRange range;
            if (shard.closed) {
                range.SetEndingSequenceNumber("end-" + shard.id);
            }
            aws_shard.SetSequenceNumberRange(std::move(range));
            result.AddShards(std::move(aws_shard));
        }
        if (page + 1 < _list_shards_pages.size()) {
            result.SetNextToken(std::to_string(page + 1));
        }
        return result;
    }

    Aws::Kinesis::Model::GetShardIteratorOutcome GetShardIterator(
            const Aws::Kinesis::Model::GetShardIteratorRequest& request) const override {
        ++_get_shard_iterator_calls;
        const std::string iterator = "iterator-" + request.GetShardId();
        _iterator_to_shard[iterator] = request.GetShardId();
        Aws::Kinesis::Model::GetShardIteratorResult result;
        result.SetShardIterator(iterator);
        return result;
    }

    Aws::Kinesis::Model::GetRecordsOutcome GetRecords(
            const Aws::Kinesis::Model::GetRecordsRequest& request) const override {
        const std::string iterator = request.GetShardIterator();
        const auto iterator_it = _iterator_to_shard.find(iterator);
        if (iterator_it == _iterator_to_shard.end()) {
            return Aws::Kinesis::Model::GetRecordsResult();
        }
        const std::string& shard_id = iterator_it->second;
        const size_t page = static_cast<size_t>(_get_records_calls[shard_id]++);
        const auto pages_it = _records_pages.find(shard_id);
        if (pages_it == _records_pages.end() || page >= pages_it->second.size()) {
            return Aws::Kinesis::Model::GetRecordsResult();
        }

        const RecordsPage& fake_page = pages_it->second[page];
        Aws::Kinesis::Model::GetRecordsResult result;
        for (const auto& sequence : fake_page.sequences) {
            Aws::Kinesis::Model::Record record;
            record.SetSequenceNumber(sequence);
            const std::string payload = "{\"sequence\":\"" + sequence + "\"}";
            record.SetData(Aws::Utils::ByteBuffer(
                    reinterpret_cast<const unsigned char*>(payload.data()), payload.size()));
            result.AddRecords(std::move(record));
        }
        result.SetNextShardIterator(fake_page.next_iterator);
        result.SetMillisBehindLatest(fake_page.millis_behind_latest);
        if (!fake_page.next_iterator.empty()) {
            _iterator_to_shard[fake_page.next_iterator] = shard_id;
        }
        for (const auto& [child_id, parents] : fake_page.child_parents) {
            Aws::Kinesis::Model::ChildShard child;
            child.SetShardId(child_id);
            child.SetParentShards(parents);
            result.AddChildShards(std::move(child));
        }
        return result;
    }

private:
    std::vector<std::vector<Shard>> _list_shards_pages;
    std::map<std::string, std::vector<RecordsPage>> _records_pages;
    mutable std::map<std::string, std::string> _iterator_to_shard;
    mutable std::map<std::string, int> _get_records_calls;
    mutable int _list_shards_calls = 0;
    mutable int _get_shard_iterator_calls = 0;
};

} // namespace doris
