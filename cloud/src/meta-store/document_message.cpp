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

#include "meta-store/document_message.h"

#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <cstdint>
#include <memory>

#include "common/config.h"
#include "common/util.h"
#include "meta-store/codec.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

namespace doris::cloud {
namespace details {

static std::string lexical_next(std::string_view str) {
    return std::string(str) + '\0';
}

static std::string lexical_end(std::string_view str) {
    // Since '\xff' is not used in the key encoding, we can safely use it as a sentinel
    // to indicate the end of a key range.
    return std::string(str) + '\xff';
}

void document_delete_single(Transaction* txn, std::string_view key) {
    txn->remove(key);
}

void document_delete_range(Transaction* txn, std::string_view prefix) {
    txn->remove(prefix, lexical_end(prefix));
}

} // namespace details

// A concept to check if a message has a split schema.
template <typename Message>
concept MessageContainsSplitSchema = IsProtobufMessage<Message> && requires(Message msg) {
    { msg.has___split_schema() } -> std::convertible_to<bool>;
    { msg.__split_schema() } -> std::convertible_to<const SplitSchemaPB&>;
    { msg.mutable___split_schema() } -> std::convertible_to<SplitSchemaPB*>;
};

// A descriptor for messages that can be split into multiple keys.
//
// This descriptor provides methods to check if a message should be split and to get the field IDs
// that are used for splitting the message. If a message does not need to be split, it will return
// false and an empty vector for the field IDs.
template <typename Message>
    requires MessageContainsSplitSchema<Message>
struct SplitMessageDescriptor {
    static bool should_split_message(const Message& msg) { return false; }
    static std::vector<int64_t> get_split_field_ids() { return {}; }
};

template <>
struct SplitMessageDescriptor<RowsetMetaCloudPB> {
    static bool should_split_message(const RowsetMetaCloudPB& msg) {
        return config::enable_split_rowset_meta_pb &&
               config::split_rowset_meta_pb_size < msg.ByteSizeLong();
    }

    static std::vector<int64_t> get_split_field_ids() {
        return {RowsetMetaCloudPB::kSegmentsKeyBoundsFieldNumber};
    }
};

template <>
struct SplitMessageDescriptor<SplitSingleMessagePB> {
    static bool should_split_message(const SplitSingleMessagePB& msg) { return true; }
    static std::vector<int64_t> get_split_field_ids() {
        return {SplitSingleMessagePB::kSegmentKeyBoundsFieldNumber};
    }
};

// Check if the field is a valid splitable field. A splitable field is either a repeated field or a message field.
static bool is_splitable_field(const google::protobuf::FieldDescriptor* field) {
    return field && (field->is_repeated() ||
                     field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE);
}

static bool ensure_fields_are_splitable(const google::protobuf::Message& msg,
                                        const std::vector<int64_t>& split_field_ids) {
    for (int64_t field_id : split_field_ids) {
        const google::protobuf::FieldDescriptor* field =
                msg.GetDescriptor()->FindFieldByNumber(field_id);
        if (!is_splitable_field(field)) {
            LOG(ERROR) << "Field with id " << field_id
                       << " is not a valid message field in message "
                       << msg.GetDescriptor()->full_name();
            return false;
        }
    }
    return true;
}

static bool verify_message_split_fields(const google::protobuf::Message& msg,
                                        const SplitSchemaPB& split_schema) {
    for (int64_t field_id : split_schema.split_field_ids()) {
        const google::protobuf::FieldDescriptor* field =
                msg.GetDescriptor()->FindFieldByNumber(field_id);
        if (!is_splitable_field(field)) {
            LOG(ERROR) << "Field with id " << field_id
                       << " is not a valid message field in message "
                       << msg.GetDescriptor()->full_name();
            return false;
        }
        const google::protobuf::Reflection* reflection = msg.GetReflection();
        if (field->is_repeated()) {
            int field_size = reflection->FieldSize(msg, field);
            if (field_size == 0) {
                LOG(ERROR) << "Repeated field with id " << field_id << " is empty in message "
                           << msg.GetDescriptor()->full_name()
                           << ", but it is expected to be present according to the split schema.";
                return false;
            }
        } else {
            DCHECK(field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE)
                    << "Field with id " << field_id << " is not a message field in message "
                    << msg.GetDescriptor()->full_name() << ", the cpp type is "
                    << field->cpp_type();
            if (!reflection->HasField(msg, field)) {
                LOG(ERROR) << "Field with id " << field_id << " is not set in message "
                           << msg.GetDescriptor()->full_name()
                           << ", but it is expected to be present according to the split schema.";
                return false;
            }
        }
    }
    return true;
}

// Split the fields of a message into multiple keys in the transaction.
// Only supports repeated fields or message fields. The split fields and
// the number of split keys are recorded in the `split_schema` field of the message.
//
// If a field is not set (or empty for repeated fields), it will be skipped.
static bool write_message_split_fields(Transaction* txn, std::string_view key,
                                       google::protobuf::Message* msg,
                                       const std::vector<int64_t>& split_field_ids,
                                       SplitSchemaPB* split_schema) {
    using google::protobuf::FieldDescriptor;
    using google::protobuf::Reflection;
    using google::protobuf::Message;

    DCHECK(ensure_fields_are_splitable(*msg, split_field_ids))
            << "The split fields must be a repeated or message field";

    size_t num_put_keys = txn->num_put_keys();
    for (int64_t field_id : split_field_ids) {
        const FieldDescriptor* field = msg->GetDescriptor()->FindFieldByNumber(field_id);
        const Reflection* reflection = msg->GetReflection();
        if (field->is_repeated()) {
            int field_size = reflection->FieldSize(*msg, field);
            if (field_size == 0) {
                continue; // Skip empty repeated fields
            }
            // Split the repeated message field
            for (int i = 0; i < field_size; ++i) {
                std::string split_key(key);
                encode_int64(field_id, &split_key);
                encode_int64(static_cast<int64_t>(i), &split_key);
                Message* repeated_field = reflection->MutableRepeatedMessage(msg, field, i);
                if (!document_put(txn, split_key, std::move(*repeated_field))) {
                    return false;
                }
            }
        } else {
            DCHECK(field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE);
            if (!reflection->HasField(*msg, field)) {
                // Skip if the field is not set
                continue;
            }

            std::string split_key(key);
            encode_int64(field_id, &split_key);
            Message* message_field = reflection->MutableMessage(msg, field);
            if (!document_put(txn, split_key, std::move(*message_field))) {
                return false;
            }
        }

        // Clear the field after putting it to avoid duplication
        reflection->ClearField(msg, field);
        // Only add the field ID if it was put
        split_schema->mutable_split_field_ids()->Add(field_id);
    }

    split_schema->Clear();
    split_schema->set_num_split_keys(txn->num_put_keys() - num_put_keys);
    return true;
}

template <typename Message>
    requires MessageContainsSplitSchema<Message>
static bool document_put_split_fields(Transaction* txn, std::string_view key_prefix,
                                      google::protobuf::Message* msg) {
    auto* detail = static_cast<Message*>(msg);
    if (!SplitMessageDescriptor<Message>::should_split_message(*detail)) {
        return true; // No need to split, the message is small enough
    }

    auto&& split_field_ids = SplitMessageDescriptor<Message>::get_split_field_ids();
    return write_message_split_fields(txn, key_prefix, detail, split_field_ids,
                                      detail->mutable___split_schema());
}

bool document_put(Transaction* txn, std::string_view key, google::protobuf::Message&& msg) {
    using MessageSplitMethod = bool (*)(Transaction*, std::string_view, google::protobuf::Message*);
    using SplitMethodDescriptor =
            std::tuple<const google::protobuf::Descriptor*, MessageSplitMethod>;

    const SplitMethodDescriptor split_messages[] = {
            {RowsetMetaCloudPB::descriptor(), &document_put_split_fields<RowsetMetaCloudPB>},
            // Add more split messages here as needed ...
            {SplitSingleMessagePB::descriptor(), &document_put_split_fields<SplitSingleMessagePB>},
    };

    const google::protobuf::Descriptor* descriptor = msg.GetDescriptor();
    const std::string& full_name = descriptor->full_name();

    for (const auto& [target_descriptor, split_field_method] : split_messages) {
        if (full_name == target_descriptor->full_name()) {
            if (!split_field_method(txn, key, &msg)) {
                return false;
            }
            break;
        }
    }

    std::string value;
    if (!msg.SerializeToString(&value)) {
        LOG(ERROR) << "Failed to serialize message, key: " << hex(key)
                   << ", message size: " << msg.ByteSizeLong() << ", message type: " << full_name;
        return false;
    }

    txn->put(key, value);
    return true;
}

// Get split fields from the iterator and populate the message.
//
// This function will iterate through the keys that start with the given prefix
// and extract the fields based on the split schema defined in the message.
static TxnErrorCode parse_message_split_fields(FullRangeGetIterator* iter,
                                               std::string_view key_prefix,
                                               const SplitSchemaPB& split_schema,
                                               google::protobuf::Message* msg, bool snapshot) {
    using google::protobuf::FieldDescriptor;
    using google::protobuf::Reflection;
    using google::protobuf::Message;
    using google::protobuf::Descriptor;

    const Reflection* reflection = msg->GetReflection();
    const Descriptor* descriptor = msg->GetDescriptor();
    for (auto&& kv = iter->peek(); kv.has_value(); kv = iter->peek()) {
        auto&& [key, value] = *kv;
        if (!key.starts_with(key_prefix)) {
            break; // Stop if the key does not match the prefix
        }

        std::string_view suffix(key);
        suffix.remove_prefix(key_prefix.size());

        int64_t field_id = 0;
        if (decode_int64(&suffix, &field_id)) {
            LOG(ERROR) << "Failed to decode the field id from key: " << hex(key)
                       << ", key prefix: " << hex(key_prefix);
            return TxnErrorCode::TXN_INVALID_DATA;
        }

        if (field_id < 0 || field_id >= std::numeric_limits<int>::max()) {
            LOG(ERROR) << "Field id " << field_id << " is out of range for message "
                       << descriptor->full_name() << ", key: " << hex(key)
                       << ", key prefix: " << hex(key_prefix);
            return TxnErrorCode::TXN_INVALID_DATA;
        }

        const FieldDescriptor* field = descriptor->FindFieldByNumber(static_cast<int>(field_id));
        if (!field ||
            (!field->is_repeated() && field->cpp_type() != FieldDescriptor::CPPTYPE_MESSAGE)) {
            LOG(ERROR) << "Field with id " << field_id
                       << " is not a valid splitable message field in message "
                       << descriptor->full_name();
            return TxnErrorCode::TXN_INVALID_DATA;
        }

        Message* sub_msg;
        if (field->is_repeated()) {
            sub_msg = reflection->AddMessage(msg, field);
        } else {
            sub_msg = reflection->MutableMessage(msg, field);
        }
        TxnErrorCode code = document_get(iter, sub_msg);
        if (code != TxnErrorCode::TXN_OK) {
            return code;
        }
    }

    return TxnErrorCode::TXN_OK;
}

static TxnErrorCode parse_message_split_fields(Transaction* txn, std::string_view key_prefix,
                                               const SplitSchemaPB& split_schema,
                                               google::protobuf::Message* msg, bool snapshot) {
    std::string begin_key = details::lexical_next(key_prefix);
    std::string end_key = details::lexical_end(key_prefix);

    FullRangeGetOptions options;
    options.batch_limit = 64;
    options.exact_limit = split_schema.num_split_keys();
    options.prefetch = true;
    options.txn = txn;
    std::unique_ptr<FullRangeGetIterator> iter =
            txn->full_range_get(begin_key, end_key, std::move(options));

    TxnErrorCode err =
            parse_message_split_fields(iter.get(), key_prefix, split_schema, msg, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(ERROR) << "Failed to get split fields for key prefix: " << hex(key_prefix)
                   << ", error code: " << err;
        return err;
    }

    if (!iter->is_valid()) {
        LOG(ERROR) << "Document get the split fields failed, iterator is not valid, key prefix: "
                   << hex(key_prefix) << ", error code: " << iter->error_code();
        return iter->error_code();
    }

    // Ensure all split keys are processed
    if (auto&& next = iter->next(); next.has_value()) {
        auto&& [key, value] = *next;
        LOG(ERROR) << "Not all split keys are processed, remaining key found after processing: "
                   << hex(key) << ", key prefix: " << hex(key_prefix);
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    return TxnErrorCode::TXN_OK;
}

template <typename T, typename Message>
    requires MessageContainsSplitSchema<Message>
static TxnErrorCode document_get_split_fields(T* t, std::string_view key_prefix,
                                              google::protobuf::Message* msg, bool snapshot) {
    auto* detail = static_cast<Message*>(msg);
    if (!detail->has___split_schema()) {
        return TxnErrorCode::TXN_OK; // No split schema, nothing to read
    }

    const SplitSchemaPB& split_schema = detail->__split_schema();
    TxnErrorCode code = parse_message_split_fields(t, key_prefix, split_schema, detail, snapshot);
    if (code != TxnErrorCode::TXN_OK) {
        return code;
    }

    DCHECK(verify_message_split_fields(*detail, split_schema))
            << "Split schema verification failed for RowsetMetaCloudPB with key: "
            << hex(key_prefix);
    detail->clear___split_schema(); // Clear split schema after getting fields
    return TxnErrorCode::TXN_OK;
}

template <typename T>
static TxnErrorCode document_get_split_fields_if_exists(T* t, std::string_view key_prefix,
                                                        google::protobuf::Message* msg,
                                                        bool snapshot) {
    using SplitMessageDescriptor =
            std::tuple<const google::protobuf::Descriptor*,
                       TxnErrorCode (*)(T*, std::string_view, google::protobuf::Message*, bool)>;

    const SplitMessageDescriptor split_messages[] = {
            {RowsetMetaCloudPB::descriptor(), &document_get_split_fields<T, RowsetMetaCloudPB>},
            // Add more split messages here as needed ...
            {SplitSingleMessagePB::descriptor(),
             &document_get_split_fields<T, SplitSingleMessagePB>}};

    const google::protobuf::Descriptor* descriptor = msg->GetDescriptor();
    const std::string& full_name = descriptor->full_name();
    for (const auto& [target_descriptor, split_field_method] : split_messages) {
        if (full_name == target_descriptor->full_name()) {
            return split_field_method(t, key_prefix, msg, snapshot);
        }
    }

    return TxnErrorCode::TXN_OK;
}

// Get a document from the iterator and populate the message.
//
// The iterator is expected to be positioned at the key of the document. If the document is
// split into multiple keys, it will reconstruct the message using the split schema defined
// in the message.
//
// The iterator will be advanced to the next key after processing the current key.
TxnErrorCode document_get(FullRangeGetIterator* iter, google::protobuf::Message* msg) {
    auto kv = iter->peek();
    if (!kv.has_value()) {
        return TxnErrorCode::TXN_KEY_NOT_FOUND;
    }

    auto&& [key, value] = *kv;
    if (!msg->ParseFromArray(value.data(), value.size())) {
        LOG(ERROR) << "Failed to parse message, key: " << hex(key)
                   << ", value size: " << value.size()
                   << ", message type: " << msg->GetDescriptor()->full_name();
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    // The snapshot parameter is not used here, so we pass false.
    iter->next(); // Move the iterator to the next key
    TxnErrorCode code = document_get_split_fields_if_exists(iter, key, msg, false);
    if (code != TxnErrorCode::TXN_OK) {
        LOG(ERROR) << "Failed to get split fields for key: " << hex(key)
                   << ", error code: " << code;
        return code;
    }

    if (!iter->is_valid()) {
        LOG(ERROR) << "Document get failed, iterator is not valid, key: " << hex(key)
                   << ", error code: " << iter->error_code();
        return iter->error_code();
    }

    return TxnErrorCode::TXN_OK;
}

TxnErrorCode document_get(Transaction* txn, std::string_view key, google::protobuf::Message* msg,
                          bool snapshot) {
    std::string value;
    TxnErrorCode code = txn->get(key, &value, snapshot);
    if (code != TxnErrorCode::TXN_OK) {
        return code;
    }

    if (!msg->ParseFromString(value)) {
        LOG(ERROR) << "Failed to parse message, key: " << hex(key)
                   << ", value size: " << value.size()
                   << ", message type: " << msg->GetDescriptor()->full_name();
        return TxnErrorCode::TXN_INVALID_DATA;
    }

    return document_get_split_fields_if_exists(txn, key, msg, snapshot);
}

} // namespace doris::cloud
