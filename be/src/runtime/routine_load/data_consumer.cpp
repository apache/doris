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

#include "runtime/routine_load/data_consumer.h"

#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <librdkafka/rdkafkacpp.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "runtime/exec_env.h"
#include "runtime/small_file_mgr.h"
#include "service/backend_options.h"
#include "util/blocking_queue.hpp"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/stopwatch.hpp"
#include "util/string_util.h"
#include "util/uid_util.h"

namespace doris {

static const std::string PROP_GROUP_ID = "group.id";
// init kafka consumer will only set common configs such as
// brokers, groupid
Status KafkaDataConsumer::init(std::shared_ptr<StreamLoadContext> ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (_init) {
        // this consumer has already been initialized.
        return Status::OK();
    }

    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    // conf has to be deleted finally
    Defer delete_conf {[conf]() { delete conf; }};

    std::string errstr;
    auto set_conf = [&conf, &errstr](const std::string& conf_key, const std::string& conf_val) {
        RdKafka::Conf::ConfResult res = conf->set(conf_key, conf_val, errstr);
        if (res == RdKafka::Conf::CONF_UNKNOWN) {
            // ignore unknown config
            return Status::OK();
        } else if (errstr.find("not supported") != std::string::npos) {
            // some java-only properties may be passed to here, and librdkafak will return 'xxx' not supported
            // ignore it
            return Status::OK();
        } else if (res != RdKafka::Conf::CONF_OK) {
            std::stringstream ss;
            ss << "PAUSE: failed to set '" << conf_key << "', value: '" << conf_val
               << "', err: " << errstr;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        VLOG_NOTICE << "set " << conf_key << ": " << conf_val;
        return Status::OK();
    };

    RETURN_IF_ERROR(set_conf("metadata.broker.list", ctx->kafka_info->brokers));
    RETURN_IF_ERROR(set_conf("enable.partition.eof", "true"));
    RETURN_IF_ERROR(set_conf("enable.auto.offset.store", "false"));
    // TODO: set it larger than 0 after we set rd_kafka_conf_set_stats_cb()
    RETURN_IF_ERROR(set_conf("statistics.interval.ms", "0"));
    RETURN_IF_ERROR(set_conf("auto.offset.reset", "error"));
    RETURN_IF_ERROR(set_conf("socket.keepalive.enable", "true"));
    RETURN_IF_ERROR(set_conf("reconnect.backoff.ms", "100"));
    RETURN_IF_ERROR(set_conf("reconnect.backoff.max.ms", "10000"));
    RETURN_IF_ERROR(set_conf("api.version.request", config::kafka_api_version_request));
    RETURN_IF_ERROR(set_conf("api.version.fallback.ms", "0"));
    RETURN_IF_ERROR(set_conf("broker.version.fallback", config::kafka_broker_version_fallback));
    RETURN_IF_ERROR(set_conf("broker.address.ttl", "0"));
    if (config::kafka_debug != "disable") {
        RETURN_IF_ERROR(set_conf("debug", config::kafka_debug));
    }

    for (auto& item : ctx->kafka_info->properties) {
        if (starts_with(item.second, "FILE:")) {
            // file property should has format: FILE:file_id:md5
            std::vector<std::string> parts =
                    strings::Split(item.second, ":", strings::SkipWhitespace());
            if (parts.size() != 3) {
                return Status::InternalError("PAUSE: Invalid file property of kafka: " +
                                             item.second);
            }
            int64_t file_id = std::stol(parts[1]);
            std::string file_path;
            Status st = ctx->exec_env()->small_file_mgr()->get_file(file_id, parts[2], &file_path);
            if (!st.ok()) {
                return Status::InternalError("PAUSE: failed to get file for config: {}, error: {}",
                                             item.first, st.to_string());
            }
            RETURN_IF_ERROR(set_conf(item.first, file_path));
        } else {
            RETURN_IF_ERROR(set_conf(item.first, item.second));
        }
        _custom_properties.emplace(item.first, item.second);
    }

    // if not specified group id, generate a random one.
    // ATTN: In the new version, we have set a group.id on the FE side for jobs that have not set a groupid,
    // but in order to ensure compatibility, we still do a check here.
    if (_custom_properties.find(PROP_GROUP_ID) == _custom_properties.end()) {
        std::stringstream ss;
        ss << BackendOptions::get_localhost() << "_";
        std::string group_id = ss.str() + UniqueId::gen_uid().to_string();
        RETURN_IF_ERROR(set_conf(PROP_GROUP_ID, group_id));
        _custom_properties.emplace(PROP_GROUP_ID, group_id);
    }
    LOG(INFO) << "init kafka consumer with group id: " << _custom_properties[PROP_GROUP_ID];

    if (conf->set("event_cb", &_k_event_cb, errstr) != RdKafka::Conf::CONF_OK) {
        std::stringstream ss;
        ss << "PAUSE: failed to set 'event_cb'";
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    // create consumer
    _k_consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    if (!_k_consumer) {
        LOG(WARNING) << "PAUSE: failed to create kafka consumer: " << errstr;
        return Status::InternalError("PAUSE: failed to create kafka consumer: " + errstr);
    }

    VLOG_NOTICE << "finished to init kafka consumer. " << ctx->brief();

    _init = true;
    return Status::OK();
}

Status KafkaDataConsumer::assign_topic_partitions(
        const std::map<int32_t, int64_t>& begin_partition_offset, const std::string& topic,
        std::shared_ptr<StreamLoadContext> ctx) {
    DCHECK(_k_consumer);
    // create TopicPartitions
    std::stringstream ss;
    std::vector<RdKafka::TopicPartition*> topic_partitions;
    for (auto& entry : begin_partition_offset) {
        RdKafka::TopicPartition* tp1 =
                RdKafka::TopicPartition::create(topic, entry.first, entry.second);
        topic_partitions.push_back(tp1);
        _consuming_partition_ids.insert(entry.first);
        ss << "[" << entry.first << ": " << entry.second << "] ";
    }

    LOG(INFO) << "consumer: " << _id << ", grp: " << _grp_id
              << " assign topic partitions: " << topic << ", " << ss.str();

    // delete TopicPartition finally
    Defer delete_tp {[&topic_partitions]() {
        std::for_each(topic_partitions.begin(), topic_partitions.end(),
                      [](RdKafka::TopicPartition* tp1) { delete tp1; });
    }};

    // assign partition
    RdKafka::ErrorCode err = _k_consumer->assign(topic_partitions);
    if (err) {
        LOG(WARNING) << "failed to assign topic partitions: " << ctx->brief(true)
                     << ", err: " << RdKafka::err2str(err);
        _k_consumer->unassign();
        return Status::InternalError("failed to assign topic partitions");
    }

    return Status::OK();
}

Status KafkaDataConsumer::group_consume(BlockingQueue<RdKafka::Message*>* queue,
                                        int64_t max_running_time_ms) {
    static constexpr int MAX_RETRY_TIMES_FOR_TRANSPORT_FAILURE = 3;
    int64_t left_time = max_running_time_ms;
    LOG(INFO) << "start kafka consumer: " << _id << ", grp: " << _grp_id
              << ", max running time(ms): " << left_time;

    int64_t received_rows = 0;
    int64_t put_rows = 0;
    int32_t retry_times = 0;
    Status st = Status::OK();
    MonotonicStopWatch consumer_watch;
    MonotonicStopWatch watch;
    watch.start();
    while (true) {
        {
            std::unique_lock<std::mutex> l(_lock);
            if (_cancelled) {
                break;
            }
        }

        if (left_time <= 0) {
            break;
        }

        bool done = false;
        // consume 1 message at a time
        consumer_watch.start();
        std::unique_ptr<RdKafka::Message> msg(_k_consumer->consume(1000 /* timeout, ms */));
        consumer_watch.stop();
        DBUG_EXECUTE_IF("KafkaDataConsumer.group_consume.out_of_range", {
            done = true;
            std::stringstream ss;
            ss << "Offset out of range"
               << ", consume partition " << msg->partition() << ", consume offset "
               << msg->offset();
            LOG(WARNING) << "kafka consume failed: " << _id << ", msg: " << ss.str();
            st = Status::InternalError<false>(ss.str());
            break;
        });
        switch (msg->err()) {
        case RdKafka::ERR_NO_ERROR:
            if (_consuming_partition_ids.count(msg->partition()) <= 0) {
                _consuming_partition_ids.insert(msg->partition());
            }
            if (msg->len() == 0) {
                // ignore msg with length 0.
                // put empty msg into queue will cause the load process shutting down.
                break;
            } else if (!queue->blocking_put(msg.get())) {
                // queue is shutdown
                done = true;
            } else {
                ++put_rows;
                msg.release(); // release the ownership, msg will be deleted after being processed
            }
            ++received_rows;
            break;
        case RdKafka::ERR__TIMED_OUT:
            // leave the status as OK, because this may happened
            // if there is no data in kafka.
            LOG(INFO) << "kafka consume timeout: " << _id;
            break;
        case RdKafka::ERR__TRANSPORT:
            LOG(INFO) << "kafka consume Disconnected: " << _id
                      << ", retry times: " << retry_times++;
            if (retry_times <= MAX_RETRY_TIMES_FOR_TRANSPORT_FAILURE) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                break;
            }
            [[fallthrough]];
        case RdKafka::ERR__PARTITION_EOF: {
            LOG(INFO) << "consumer meet partition eof: " << _id
                      << " partition offset: " << msg->offset();
            _consuming_partition_ids.erase(msg->partition());
            if (!queue->blocking_put(msg.get())) {
                done = true;
            } else if (_consuming_partition_ids.size() <= 0) {
                msg.release();
                done = true;
            } else {
                msg.release();
            }
            break;
        }
        case RdKafka::ERR_OFFSET_OUT_OF_RANGE: {
            done = true;
            std::stringstream ss;
            ss << msg->errstr() << ", consume partition " << msg->partition() << ", consume offset "
               << msg->offset();
            LOG(WARNING) << "kafka consume failed: " << _id << ", msg: " << ss.str();
            st = Status::InternalError<false>(ss.str());
            break;
        }
        default:
            LOG(WARNING) << "kafka consume failed: " << _id << ", msg: " << msg->errstr();
            done = true;
            st = Status::InternalError<false>(msg->errstr());
            break;
        }

        left_time = max_running_time_ms - watch.elapsed_time() / 1000 / 1000;
        if (done) {
            break;
        }
    }

    LOG(INFO) << "kafka consumer done: " << _id << ", grp: " << _grp_id
              << ". cancelled: " << _cancelled << ", left time(ms): " << left_time
              << ", total cost(ms): " << watch.elapsed_time() / 1000 / 1000
              << ", consume cost(ms): " << consumer_watch.elapsed_time() / 1000 / 1000
              << ", received rows: " << received_rows << ", put rows: " << put_rows;

    return st;
}

Status KafkaDataConsumer::get_partition_meta(std::vector<int32_t>* partition_ids) {
    // create topic conf
    RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    Defer delete_conf {[tconf]() { delete tconf; }};

    // create topic
    std::string errstr;
    RdKafka::Topic* topic = RdKafka::Topic::create(_k_consumer, _topic, tconf, errstr);
    if (topic == nullptr) {
        std::stringstream ss;
        ss << "failed to create topic: " << errstr;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    Defer delete_topic {[topic]() { delete topic; }};

    // get topic metadata
    RdKafka::Metadata* metadata = nullptr;
    RdKafka::ErrorCode err =
            _k_consumer->metadata(false /* for this topic */, topic, &metadata, 5000);
    if (err != RdKafka::ERR_NO_ERROR) {
        std::stringstream ss;
        ss << "failed to get partition meta: " << RdKafka::err2str(err);
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    Defer delete_meta {[metadata]() { delete metadata; }};

    // get partition ids
    RdKafka::Metadata::TopicMetadataIterator it;
    for (it = metadata->topics()->begin(); it != metadata->topics()->end(); ++it) {
        if ((*it)->topic() != _topic) {
            continue;
        }

        if ((*it)->err() != RdKafka::ERR_NO_ERROR) {
            std::stringstream ss;
            ss << "error: " << err2str((*it)->err());
            if ((*it)->err() == RdKafka::ERR_LEADER_NOT_AVAILABLE) {
                ss << ", try again";
            }
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        RdKafka::TopicMetadata::PartitionMetadataIterator ip;
        for (ip = (*it)->partitions()->begin(); ip != (*it)->partitions()->end(); ++ip) {
            partition_ids->push_back((*ip)->id());
        }
    }

    if (partition_ids->empty()) {
        return Status::InternalError("no partition in this topic");
    }

    return Status::OK();
}

// get offsets of each partition for times.
// The input parameter "times" holds <partition, timestamps>
// The output parameter "offsets" returns <partition, offsets>
//
// The returned offset for each partition is the earliest offset whose
// timestamp is greater than or equal to the given timestamp in the
// corresponding partition.
// See librdkafka/rdkafkacpp.h##offsetsForTimes()
Status KafkaDataConsumer::get_offsets_for_times(const std::vector<PIntegerPair>& times,
                                                std::vector<PIntegerPair>* offsets, int timeout) {
    // create topic partition
    std::vector<RdKafka::TopicPartition*> topic_partitions;
    for (const auto& entry : times) {
        RdKafka::TopicPartition* tp1 =
                RdKafka::TopicPartition::create(_topic, entry.key(), entry.val());
        topic_partitions.push_back(tp1);
    }
    // delete TopicPartition finally
    Defer delete_tp {[&topic_partitions]() {
        std::for_each(topic_partitions.begin(), topic_partitions.end(),
                      [](RdKafka::TopicPartition* tp1) { delete tp1; });
    }};

    // get offsets for times
    RdKafka::ErrorCode err = _k_consumer->offsetsForTimes(topic_partitions, timeout);
    if (UNLIKELY(err != RdKafka::ERR_NO_ERROR)) {
        std::stringstream ss;
        ss << "failed to get offsets for times: " << RdKafka::err2str(err);
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    for (const auto& topic_partition : topic_partitions) {
        PIntegerPair pair;
        pair.set_key(topic_partition->partition());
        pair.set_val(topic_partition->offset());
        offsets->push_back(std::move(pair));
    }

    return Status::OK();
}

// get latest offsets for given partitions
Status KafkaDataConsumer::get_latest_offsets_for_partitions(
        const std::vector<int32_t>& partition_ids, std::vector<PIntegerPair>* offsets,
        int timeout) {
    MonotonicStopWatch watch;
    watch.start();
    for (int32_t partition_id : partition_ids) {
        int64_t low = 0;
        int64_t high = 0;
        auto timeout_ms = timeout - static_cast<int>(watch.elapsed_time() / 1000 / 1000);
        if (UNLIKELY(timeout_ms <= 0)) {
            return Status::InternalError("get kafka latest offsets for partitions timeout");
        }

        RdKafka::ErrorCode err =
                _k_consumer->query_watermark_offsets(_topic, partition_id, &low, &high, timeout_ms);
        if (UNLIKELY(err != RdKafka::ERR_NO_ERROR)) {
            std::stringstream ss;
            ss << "failed to get latest offset for partition: " << partition_id
               << ", err: " << RdKafka::err2str(err);
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        PIntegerPair pair;
        pair.set_key(partition_id);
        pair.set_val(high);
        offsets->push_back(std::move(pair));
    }

    return Status::OK();
}

Status KafkaDataConsumer::get_real_offsets_for_partitions(
        const std::vector<PIntegerPair>& offset_flags, std::vector<PIntegerPair>* offsets,
        int timeout) {
    MonotonicStopWatch watch;
    watch.start();
    for (const auto& entry : offset_flags) {
        PIntegerPair pair;
        if (UNLIKELY(entry.val() >= 0)) {
            pair.set_key(entry.key());
            pair.set_val(entry.val());
            offsets->push_back(std::move(pair));
            continue;
        }

        int64_t low = 0;
        int64_t high = 0;
        auto timeout_ms = timeout - static_cast<int>(watch.elapsed_time() / 1000 / 1000);
        if (UNLIKELY(timeout_ms <= 0)) {
            return Status::InternalError("get kafka real offsets for partitions timeout");
        }

        RdKafka::ErrorCode err =
                _k_consumer->query_watermark_offsets(_topic, entry.key(), &low, &high, timeout_ms);
        if (UNLIKELY(err != RdKafka::ERR_NO_ERROR)) {
            std::stringstream ss;
            ss << "failed to get latest offset for partition: " << entry.key()
               << ", err: " << RdKafka::err2str(err);
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        pair.set_key(entry.key());
        if (entry.val() == -1) {
            // OFFSET_END_VAL = -1
            pair.set_val(high);
        } else if (entry.val() == -2) {
            // OFFSET_BEGINNING_VAL = -2
            pair.set_val(low);
        }
        offsets->push_back(std::move(pair));
    }

    return Status::OK();
}

Status KafkaDataConsumer::cancel(std::shared_ptr<StreamLoadContext> ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (!_init) {
        return Status::InternalError("consumer is not initialized");
    }

    _cancelled = true;
    LOG(INFO) << "kafka consumer cancelled. " << _id;
    return Status::OK();
}

Status KafkaDataConsumer::reset() {
    std::unique_lock<std::mutex> l(_lock);
    _cancelled = false;
    _k_consumer->unassign();
    // reset will be called before this consumer being returned to the pool.
    // so update _last_visit_time is reasonable.
    _last_visit_time = time(nullptr);
    return Status::OK();
}

Status KafkaDataConsumer::commit(std::vector<RdKafka::TopicPartition*>& offset) {
    // Use async commit so that it will not block for a long time.
    // Commit failure has no effect on Doris, subsequent tasks will continue to commit the new offset
    RdKafka::ErrorCode err = _k_consumer->commitAsync(offset);
    if (err != RdKafka::ERR_NO_ERROR) {
        return Status::InternalError("failed to commit kafka offset : {}", RdKafka::err2str(err));
    }
    return Status::OK();
}

// if the kafka brokers and topic are same,
// we considered this consumer as matched, thus can be reused.
bool KafkaDataConsumer::match(std::shared_ptr<StreamLoadContext> ctx) {
    if (ctx->load_src_type != TLoadSourceType::KAFKA) {
        return false;
    }
    if (_brokers != ctx->kafka_info->brokers || _topic != ctx->kafka_info->topic) {
        return false;
    }
    // check properties
    if (_custom_properties.size() != ctx->kafka_info->properties.size()) {
        return false;
    }
    for (auto& item : ctx->kafka_info->properties) {
        std::unordered_map<std::string, std::string>::const_iterator itr =
                _custom_properties.find(item.first);
        if (itr == _custom_properties.end()) {
            return false;
        }

        if (itr->second != item.second) {
            return false;
        }
    }
    return true;
}

} // end namespace doris
