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

#include "load/routine_load/kafka_share_consumer.h"

#include <bvar/bvar.h>

#include <algorithm>
#include <utility>

#include "common/metrics/doris_metrics.h"
#include "io/fs/stream_load_pipe.h"
#include "load/routine_load/consumer_group_helpers.h"
#include "util/defer_op.h"
#include "util/stopwatch.hpp"

namespace doris {

namespace {
bvar::Adder<int64_t> share_consumers("routine_load_share_consumers");
bvar::Adder<int64_t> share_polling_consumers("routine_load_share_polling_consumers");
bvar::Adder<int64_t> share_received_records("routine_load_share_received_records");
bvar::Adder<int64_t> share_committed_records("routine_load_share_committed_records");
bvar::Adder<int64_t> share_ack_failures("routine_load_share_ack_failures");

Status share_status(rd_kafka_error_t* error) {
    if (error == nullptr) {
        return Status::OK();
    }
    auto status = Status::InternalError("Kafka share consumer: {}", rd_kafka_error_string(error));
    rd_kafka_error_destroy(error);
    return status;
}
} // namespace

KafkaShareConsumer::~KafkaShareConsumer() {
    if (_consumer != nullptr) {
        if (!_records.empty()) {
            auto status = acknowledge(false);
            if (!status.ok()) {
                LOG(WARNING) << "Failed to release share batch: " << status;
            }
        }
        auto status = share_status(rd_kafka_share_consumer_close(_consumer));
        if (!status.ok()) {
            LOG(WARNING) << "Failed to close share consumer: " << status;
        }
        rd_kafka_share_destroy(_consumer);
        share_consumers << -1;
    }
}

Status KafkaShareConsumer::init(std::shared_ptr<StreamLoadContext> ctx) {
    _brokers = ctx->kafka_info->brokers;
    _topic = ctx->kafka_info->topic;
    _properties = ctx->kafka_info->properties;
    auto* conf = rd_kafka_conf_new();
    Defer destroy_conf {[&] {
        if (conf != nullptr) {
            rd_kafka_conf_destroy(conf);
        }
    }};
    auto set = [&](const std::string& key, const std::string& value) -> Status {
        char error[512];
        if (rd_kafka_conf_set(conf, key.c_str(), value.c_str(), error, sizeof(error)) !=
            RD_KAFKA_CONF_OK) {
            return Status::InvalidArgument("PAUSE: Invalid Kafka share property: {}", key);
        }
        return Status::OK();
    };
    for (const auto& [key, value] : _properties) {
        RETURN_IF_ERROR(set(key, value));
    }
    RETURN_IF_ERROR(set("bootstrap.servers", _brokers));
    RETURN_IF_ERROR(set("share.acknowledgement.mode", "explicit"));
    RETURN_IF_ERROR(set("socket.timeout.ms", "10000"));
    char error[512];
    _consumer = rd_kafka_share_consumer_new(conf, error, sizeof(error));
    if (_consumer == nullptr) {
        return Status::InternalError("PAUSE: Failed to create Kafka share consumer: {}", error);
    }
    share_consumers << 1;
    conf = nullptr;
    auto* topics = rd_kafka_topic_partition_list_new(1);
    Defer destroy_topics {[&] { rd_kafka_topic_partition_list_destroy(topics); }};
    rd_kafka_topic_partition_list_add(topics, _topic.c_str(), RD_KAFKA_PARTITION_UA);
    auto subscribe_error = rd_kafka_share_subscribe(_consumer, topics);
    if (subscribe_error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return Status::InternalError("Kafka share subscribe: {}",
                                     rd_kafka_err2str(subscribe_error));
    }
    _init = true;
    _last_visit_time = time(nullptr);
    return Status::OK();
}

Status KafkaShareConsumer::consume(std::shared_ptr<StreamLoadContext> ctx) {
    DCHECK(_records.empty());
    rd_kafka_messages_t* batch = nullptr;
    Defer destroy_batch {[&] { rd_kafka_messages_destroy(batch); }};
    auto pipe = std::static_pointer_cast<io::StreamLoadPipe>(ctx->body_sink);
    MonotonicStopWatch watch;
    watch.start();
    int64_t remaining_ms = ctx->max_interval_s * 1000;
    share_polling_consumers << 1;
    Defer stop_polling {[&] { share_polling_consumers << -1; }};
    while (batch == nullptr && remaining_ms > 0) {
        RETURN_IF_ERROR(share_status(rd_kafka_share_poll(
                _consumer, static_cast<int>(std::min<int64_t>(remaining_ms, 1000)), &batch)));
        remaining_ms = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000000;
    }
    auto append = FormatAppender::get_append_function<io::StreamLoadPipe>(ctx->format);
    int64_t bytes = 0;
    size_t count = batch == nullptr ? 0 : rd_kafka_messages_count(batch);
    share_received_records << count;
    _records.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        auto* message = rd_kafka_messages_get(batch, i);
        _records.emplace_back(message->partition, message->offset);
    }
    for (size_t i = 0; i < count; ++i) {
        if (std::cmp_greater_equal(_processed, ctx->max_batch_rows) ||
            bytes >= ctx->max_batch_size) {
            break;
        }
        auto* message = rd_kafka_messages_get(batch, i);
        if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            return Status::InternalError("Kafka share record error: {}",
                                         rd_kafka_err2str(message->err));
        }
        if (message->payload != nullptr) {
            RETURN_IF_ERROR((pipe.get()->*append)(static_cast<const char*>(message->payload),
                                                  message->len));
        }
        ++_processed;
        bytes += message->len;
    }
    ctx->receive_bytes = bytes;
    DorisMetrics::instance()->routine_load_consume_rows->increment(_processed);
    DorisMetrics::instance()->routine_load_consume_bytes->increment(bytes);
    return pipe->finish();
}

Status KafkaShareConsumer::acknowledge(bool committed) {
    if (_records.empty()) {
        return Status::OK();
    }
    _reusable = false;
    Defer clear_records {[&] {
        _records.clear();
        _processed = 0;
    }};
    for (size_t i = 0; i < _records.size(); ++i) {
        RETURN_IF_ERROR(acknowledge_record(_records[i].first, _records[i].second,
                                           committed && i < _processed));
    }
    RETURN_IF_ERROR(commit_acknowledgements());
    _reusable = true;
    return Status::OK();
}

Status KafkaShareConsumer::acknowledge_record(int32_t partition, int64_t offset, bool accept) {
    auto type = accept ? RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT
                       : RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE;
    auto error =
            rd_kafka_share_acknowledge_offset(_consumer, _topic.c_str(), partition, offset, type);
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return Status::InternalError("Kafka share acknowledgement: {}", rd_kafka_err2str(error));
    }
    return Status::OK();
}

Status KafkaShareConsumer::commit_acknowledgements() {
    rd_kafka_topic_partition_list_t* results = nullptr;
    Defer destroy_results {[&] {
        if (results != nullptr) {
            rd_kafka_topic_partition_list_destroy(results);
        }
    }};
    RETURN_IF_ERROR(share_status(rd_kafka_share_commit_sync(_consumer, 10000, &results)));
    if (results != nullptr) {
        for (int i = 0; i < results->cnt; ++i) {
            const auto& result = results->elems[i];
            if (result.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                return Status::InternalError("Kafka share acknowledgement partition {}: {}",
                                             result.partition, rd_kafka_err2str(result.err));
            }
        }
    }
    return Status::OK();
}

Status KafkaShareConsumer::acknowledge_committed() {
    share_committed_records << _processed;
    auto status = acknowledge(true);
    if (!status.ok()) {
        share_ack_failures << 1;
    }
    return status;
}

Status KafkaShareConsumer::cancel(std::shared_ptr<StreamLoadContext> ctx) {
    return acknowledge(false);
}

Status KafkaShareConsumer::reset() {
    if (!_records.empty() || !_reusable) {
        return Status::InternalError("Cannot reuse an unresolved Kafka share consumer");
    }
    _last_visit_time = time(nullptr);
    return Status::OK();
}

bool KafkaShareConsumer::match(std::shared_ptr<StreamLoadContext> ctx) {
    return ctx->load_src_type == TLoadSourceType::KAFKA && ctx->kafka_info->use_share_consumer &&
           _reusable && _records.empty() && _brokers == ctx->kafka_info->brokers &&
           _topic == ctx->kafka_info->topic && _properties == ctx->kafka_info->properties;
}

} // namespace doris
