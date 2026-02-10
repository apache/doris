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

#include <absl/strings/str_split.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <librdkafka/rdkafkacpp.h>

// AWS Kinesis SDK includes
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/Outcome.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/KinesisErrors.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/ListShardsRequest.h>
#include <aws/kinesis/model/ListShardsResult.h>
#include <aws/kinesis/model/Record.h>
#include <aws/kinesis/model/ShardIteratorType.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "runtime/small_file_mgr.h"
#include "service/backend_options.h"
#include "util/blocking_queue.hpp"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/s3_util.h"
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
                    absl::StrSplit(item.second, ":", absl::SkipWhitespace());
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
        DorisMetrics::instance()->routine_load_get_msg_count->increment(1);
        DorisMetrics::instance()->routine_load_get_msg_latency->increment(
                consumer_watch.elapsed_time() / 1000 / 1000);
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
            DorisMetrics::instance()->routine_load_consume_bytes->increment(msg->len());
            if (msg->len() == 0) {
                // ignore msg with length 0.
                // put empty msg into queue will cause the load process shutting down.
                break;
            } else if (!queue->controlled_blocking_put(msg.get(),
                                                       config::blocking_queue_cv_wait_timeout_ms)) {
                // queue is shutdown
                done = true;
            } else {
                ++put_rows;
                msg.release(); // release the ownership, msg will be deleted after being processed
            }
            ++received_rows;
            DorisMetrics::instance()->routine_load_consume_rows->increment(1);
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
            VLOG_NOTICE << "consumer meet partition eof: " << _id
                        << " partition offset: " << msg->offset();
            _consuming_partition_ids.erase(msg->partition());
            if (!queue->controlled_blocking_put(msg.get(),
                                                config::blocking_queue_cv_wait_timeout_ms)) {
                done = true;
            } else if (_consuming_partition_ids.size() <= 0) {
                LOG(INFO) << "all partitions meet eof: " << _id;
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
    DBUG_EXECUTE_IF("KafkaDataConsumer.get_latest_offsets_for_partitions.timeout", {
        // sleep 60s
        std::this_thread::sleep_for(std::chrono::seconds(60));
    });
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

// ==================== AWS Kinesis Data Consumer Implementation ====================

KinesisDataConsumer::KinesisDataConsumer(std::shared_ptr<StreamLoadContext> ctx)
        : _region(ctx->kinesis_info->region),
          _stream(ctx->kinesis_info->stream),
          _endpoint(ctx->kinesis_info->endpoint) {
    VLOG_NOTICE << "construct Kinesis consumer: stream=" << _stream << ", region=" << _region;
}

KinesisDataConsumer::~KinesisDataConsumer() {
    VLOG_NOTICE << "destruct Kinesis consumer: stream=" << _stream;
    // AWS SDK client managed by shared_ptr, will be automatically cleaned up
}

Status KinesisDataConsumer::init(std::shared_ptr<StreamLoadContext> ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (_init) {
        return Status::OK(); // Already initialized (idempotent)
    }

    // Store custom properties (AWS credentials, etc.)
    _custom_properties = ctx->kinesis_info->properties;

    // Create AWS Kinesis client
    RETURN_IF_ERROR(_create_kinesis_client(ctx));

    VLOG_NOTICE << "finished to init Kinesis consumer. stream=" << _stream
                << ", region=" << _region << ", " << ctx->brief();
    _init = true;
    return Status::OK();
}

Status KinesisDataConsumer::_create_kinesis_client(std::shared_ptr<StreamLoadContext> ctx) {
    // Reuse S3ClientFactory's credential provider logic
    // This supports all AWS authentication methods:
    // - Simple AK/SK
    // - IAM instance profile (EC2)
    // - STS assume role
    // - Session tokens
    // - Environment variables
    // - Default credential chain

    S3ClientConf s3_conf;
    s3_conf.region = _region;
    s3_conf.endpoint = _endpoint;

    // Parse AWS credentials from properties
    auto it_ak = _custom_properties.find("aws.access.key.id");
    auto it_sk = _custom_properties.find("aws.secret.access.key");
    auto it_token = _custom_properties.find("aws.session.token");
    auto it_role_arn = _custom_properties.find("aws.iam.role.arn");
    auto it_external_id = _custom_properties.find("aws.external.id");
    auto it_provider = _custom_properties.find("aws.credentials.provider");

    if (it_ak != _custom_properties.end()) {
        s3_conf.ak = it_ak->second;
    }
    if (it_sk != _custom_properties.end()) {
        s3_conf.sk = it_sk->second;
    }
    if (it_token != _custom_properties.end()) {
        s3_conf.token = it_token->second;
    }
    if (it_role_arn != _custom_properties.end()) {
        s3_conf.role_arn = it_role_arn->second;
    }
    if (it_external_id != _custom_properties.end()) {
        s3_conf.external_id = it_external_id->second;
    }
    if (it_provider != _custom_properties.end()) {
        // Map provider type string to enum
        if (it_provider->second == "instance_profile") {
            s3_conf.cred_provider_type = CredProviderType::InstanceProfile;
        } else if (it_provider->second == "env") {
            s3_conf.cred_provider_type = CredProviderType::Env;
        } else if (it_provider->second == "simple") {
            s3_conf.cred_provider_type = CredProviderType::Simple;
        }
    }

    // Create AWS ClientConfiguration
    Aws::Client::ClientConfiguration aws_config = S3ClientFactory::getClientConfiguration();
    aws_config.region = _region;

    if (!_endpoint.empty()) {
        aws_config.endpointOverride = _endpoint;
    }

    // Set timeouts from properties or use defaults
    auto it_request_timeout = _custom_properties.find("aws.request.timeout.ms");
    if (it_request_timeout != _custom_properties.end()) {
        aws_config.requestTimeoutMs = std::stoi(it_request_timeout->second);
    } else {
        aws_config.requestTimeoutMs = 30000; // 30s default
    }

    auto it_conn_timeout = _custom_properties.find("aws.connection.timeout.ms");
    if (it_conn_timeout != _custom_properties.end()) {
        aws_config.connectTimeoutMs = std::stoi(it_conn_timeout->second);
    }

    // Get credentials provider (reuses S3 infrastructure)
    auto credentials_provider = S3ClientFactory::instance().get_aws_credentials_provider(s3_conf);

    // Create Kinesis client
    _kinesis_client =
            std::make_shared<Aws::Kinesis::KinesisClient>(credentials_provider, aws_config);

    if (!_kinesis_client) {
        return Status::InternalError("Failed to create AWS Kinesis client for stream: {}, region: {}",
                                    _stream, _region);
    }

    LOG(INFO) << "Created Kinesis client for stream: " << _stream << ", region: " << _region;
    return Status::OK();
}

Status KinesisDataConsumer::assign_shards(
        const std::map<std::string, std::string>& shard_sequence_numbers,
        const std::string& stream_name, std::shared_ptr<StreamLoadContext> ctx) {
    DCHECK(_kinesis_client);

    std::stringstream ss;
    ss << "Assigning shards to Kinesis consumer " << _id << ": ";

    for (auto& entry : shard_sequence_numbers) {
        const std::string& shard_id = entry.first;
        const std::string& sequence_number = entry.second;

        // Get shard iterator for this shard
        std::string iterator;
        RETURN_IF_ERROR(_get_shard_iterator(shard_id, sequence_number, &iterator));

        _shard_iterators[shard_id] = iterator;
        _consuming_shard_ids.insert(shard_id);

        ss << "[" << shard_id << ": " << sequence_number << "] ";
    }

    LOG(INFO) << ss.str();
    return Status::OK();
}

Status KinesisDataConsumer::_get_shard_iterator(const std::string& shard_id,
                                               const std::string& sequence_number,
                                               std::string* iterator) {
    Aws::Kinesis::Model::GetShardIteratorRequest request;
    request.SetStreamName(_stream);
    request.SetShardId(shard_id);

    // Determine iterator type based on sequence number
    if (sequence_number.empty() || sequence_number == "TRIM_HORIZON" ||
        sequence_number == "-2") {
        // Start from oldest record in shard
        request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::TRIM_HORIZON);
    } else if (sequence_number == "LATEST" || sequence_number == "-1") {
        // Start from newest record (records arriving after iterator creation)
        request.SetShardIteratorType(Aws::Kinesis::Model::ShardIteratorType::LATEST);
    } else {
        // Resume from specific sequence number
        request.SetShardIteratorType(
                Aws::Kinesis::Model::ShardIteratorType::AFTER_SEQUENCE_NUMBER);
        request.SetStartingSequenceNumber(sequence_number);
    }

    auto outcome = _kinesis_client->GetShardIterator(request);
    if (!outcome.IsSuccess()) {
        auto& error = outcome.GetError();
        return Status::InternalError(
                "Failed to get shard iterator for shard {}: {} ({})", shard_id,
                error.GetMessage(), static_cast<int>(error.GetErrorType()));
    }

    *iterator = outcome.GetResult().GetShardIterator();
    VLOG_NOTICE << "Got shard iterator for shard: " << shard_id;
    return Status::OK();
}

Status KinesisDataConsumer::group_consume(
        BlockingQueue<std::shared_ptr<Aws::Kinesis::Model::Record>>* queue,
        int64_t max_running_time_ms) {
    static constexpr int MAX_RETRY_TIMES_FOR_TRANSPORT_FAILURE = 3;
    static constexpr int RATE_LIMIT_BACKOFF_MS = 1000;             // 1 second
    static constexpr int KINESIS_GET_RECORDS_LIMIT = 1000;         // Max 10000
    static constexpr int INTER_SHARD_SLEEP_MS = 10;                // Small sleep between shards

    int64_t left_time = max_running_time_ms;
    LOG(INFO) << "start Kinesis consumer: " << _id << ", grp: " << _grp_id
              << ", stream: " << _stream << ", max running time(ms): " << left_time;

    int64_t received_rows = 0;
    int64_t put_rows = 0;
    int32_t retry_times = 0;
    Status st = Status::OK();
    bool done = false;

    MonotonicStopWatch consumer_watch;
    MonotonicStopWatch watch;
    watch.start();

    while (true) {
        // Check cancellation flag
        {
            std::unique_lock<std::mutex> l(_lock);
            if (_cancelled) {
                break;
            }
        }

        if (left_time <= 0) {
            break;
        }

        // Round-robin through all active shards
        for (auto it = _consuming_shard_ids.begin(); it != _consuming_shard_ids.end() && !done;) {
            const std::string& shard_id = *it;
            auto iter_it = _shard_iterators.find(shard_id);

            if (iter_it == _shard_iterators.end() || iter_it->second.empty()) {
                // Shard exhausted (closed due to split/merge), remove from active set
                LOG(INFO) << "Shard exhausted: " << shard_id;
                it = _consuming_shard_ids.erase(it);
                continue;
            }

            // Call Kinesis GetRecords API
            consumer_watch.start();

            Aws::Kinesis::Model::GetRecordsRequest request;
            request.SetShardIterator(iter_it->second);
            request.SetLimit(KINESIS_GET_RECORDS_LIMIT);

            auto outcome = _kinesis_client->GetRecords(request);
            consumer_watch.stop();

            // Track metrics (reuse Kafka metrics, they're generic)
            DorisMetrics::instance()->routine_load_get_msg_count->increment(1);
            DorisMetrics::instance()->routine_load_get_msg_latency->increment(
                    consumer_watch.elapsed_time() / 1000 / 1000);

            if (!outcome.IsSuccess()) {
                auto& error = outcome.GetError();

                // Handle throttling (ProvisionedThroughputExceededException)
                if (error.GetErrorType() ==
                    Aws::Kinesis::KinesisErrors::PROVISIONED_THROUGHPUT_EXCEEDED) {
                    LOG(INFO) << "Kinesis rate limit exceeded for shard: " << shard_id
                              << ", backing off " << RATE_LIMIT_BACKOFF_MS << "ms";
                    std::this_thread::sleep_for(std::chrono::milliseconds(RATE_LIMIT_BACKOFF_MS));
                    ++it; // Move to next shard, will retry this one next round
                    continue;
                }

                // Handle retriable errors
                if (_is_retriable_error(error)) {
                    LOG(INFO) << "Kinesis retriable error for shard " << shard_id << ": "
                              << error.GetMessage() << ", retry times: " << retry_times++;
                    if (retry_times <= MAX_RETRY_TIMES_FOR_TRANSPORT_FAILURE) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(200));
                        continue;
                    }
                }

                // Fatal error
                LOG(WARNING) << "Kinesis consume failed for shard " << shard_id << ": "
                            << error.GetMessage() << " (" << static_cast<int>(error.GetErrorType())
                            << ")";
                st = Status::InternalError("Kinesis GetRecords failed for shard {}: {}", shard_id,
                                          error.GetMessage());
                done = true;
                break;
            }

            // Reset retry counter on success
            retry_times = 0;

            // Process records
            auto& result = outcome.GetResult();
            RETURN_IF_ERROR(_process_records(result, queue, &received_rows, &put_rows));

            // Update shard iterator for next call
            std::string next_iterator = result.GetNextShardIterator();
            if (next_iterator.empty()) {
                // Shard is closed (split/merge), remove from active set
                LOG(INFO) << "Shard closed: " << shard_id << " (split/merge detected)";
                _shard_iterators.erase(shard_id);
                it = _consuming_shard_ids.erase(it);
            } else {
                _shard_iterators[shard_id] = next_iterator;
                ++it;
            }

            // Check if all shards are exhausted
            if (_consuming_shard_ids.empty()) {
                LOG(INFO) << "All shards exhausted for consumer: " << _id;
                done = true;
                break;
            }

            // Small sleep to avoid tight loop
            std::this_thread::sleep_for(std::chrono::milliseconds(INTER_SHARD_SLEEP_MS));
        }

        left_time = max_running_time_ms - watch.elapsed_time() / 1000 / 1000;
        if (done) {
            break;
        }
    }

    LOG(INFO) << "Kinesis consumer done: " << _id << ", grp: " << _grp_id
              << ". cancelled: " << _cancelled << ", left time(ms): " << left_time
              << ", total cost(ms): " << watch.elapsed_time() / 1000 / 1000
              << ", consume cost(ms): " << consumer_watch.elapsed_time() / 1000 / 1000
              << ", received rows: " << received_rows << ", put rows: " << put_rows;

    return st;
}

Status KinesisDataConsumer::_process_records(
        const Aws::Kinesis::Model::GetRecordsResult& result,
        BlockingQueue<std::shared_ptr<Aws::Kinesis::Model::Record>>* queue,
        int64_t* received_rows, int64_t* put_rows) {
    auto& records = result.GetRecords();

    for (const auto& record : records) {
        DorisMetrics::instance()->routine_load_consume_bytes->increment(
                record.GetData().GetLength());

        if (record.GetData().GetLength() == 0) {
            // Skip empty records
            continue;
        }

        // Create shared_ptr to record for queue
        auto record_ptr = std::make_shared<Aws::Kinesis::Model::Record>(record);

        if (!queue->controlled_blocking_put(record_ptr.get(),
                                           config::blocking_queue_cv_wait_timeout_ms)) {
            // Queue shutdown
            return Status::InternalError("Queue shutdown during record processing");
        }

        (*put_rows)++;
        (*received_rows)++;
        DorisMetrics::instance()->routine_load_consume_rows->increment(1);
    }

    return Status::OK();
}

bool KinesisDataConsumer::_is_retriable_error(
        const Aws::Client::AWSError<Aws::Kinesis::KinesisErrors>& error) {
    auto error_type = error.GetErrorType();

    return error_type == Aws::Kinesis::KinesisErrors::PROVISIONED_THROUGHPUT_EXCEEDED ||
           error_type == Aws::Kinesis::KinesisErrors::SERVICE_UNAVAILABLE ||
           error_type == Aws::Kinesis::KinesisErrors::INTERNAL_FAILURE ||
           error_type == Aws::Kinesis::KinesisErrors::NETWORK_CONNECTION ||
           error.ShouldRetry();
}

Status KinesisDataConsumer::reset() {
    std::unique_lock<std::mutex> l(_lock);
    _cancelled = false;
    _shard_iterators.clear();
    _consuming_shard_ids.clear();
    _last_visit_time = time(nullptr);
    LOG(INFO) << "Kinesis consumer reset: " << _id;
    return Status::OK();
}

Status KinesisDataConsumer::cancel(std::shared_ptr<StreamLoadContext> ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (!_init) {
        return Status::InternalError("Kinesis consumer is not initialized");
    }
    _cancelled = true;
    LOG(INFO) << "Kinesis consumer cancelled: " << _id << ", " << ctx->brief();
    return Status::OK();
}

bool KinesisDataConsumer::match(std::shared_ptr<StreamLoadContext> ctx) {
    if (ctx->load_src_type != TLoadSourceType::KINESIS) {
        return false;
    }

    if (_region != ctx->kinesis_info->region || _stream != ctx->kinesis_info->stream ||
        _endpoint != ctx->kinesis_info->endpoint) {
        return false;
    }

    // Check that properties match
    if (_custom_properties.size() != ctx->kinesis_info->properties.size()) {
        return false;
    }

    for (auto& item : ctx->kinesis_info->properties) {
        auto itr = _custom_properties.find(item.first);
        if (itr == _custom_properties.end() || itr->second != item.second) {
            return false;
        }
    }

    return true;
}

Status KinesisDataConsumer::get_shard_list(std::vector<std::string>* shard_ids) {
    DCHECK(_kinesis_client);

    Aws::Kinesis::Model::ListShardsRequest request;
    request.SetStreamName(_stream);

    auto outcome = _kinesis_client->ListShards(request);
    if (!outcome.IsSuccess()) {
        auto& error = outcome.GetError();
        return Status::InternalError("Failed to list shards for stream {}: {} ({})", _stream,
                                    error.GetMessage(), static_cast<int>(error.GetErrorType()));
    }

    for (const auto& shard : outcome.GetResult().GetShards()) {
        shard_ids->push_back(shard.GetShardId());
    }

    if (shard_ids->empty()) {
        return Status::InternalError("No shards found in Kinesis stream: {}", _stream);
    }

    LOG(INFO) << "Found " << shard_ids->size() << " shards in stream: " << _stream;
    return Status::OK();
}

} // end namespace doris
