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

#include "agent/topic_subscriber.h"

#include <pthread.h>

#include <mutex>
#include <utility>

#include "agent/topic_listener.h"

namespace doris {

TopicSubscriber::TopicSubscriber() {}

void TopicSubscriber::register_listener(TTopicInfoType::type topic_type,
                                        std::unique_ptr<TopicListener> topic_listener) {
    // Unique lock here to prevent access to listeners
    std::lock_guard<std::shared_mutex> lock(_listener_mtx);
    this->_registered_listeners.emplace(topic_type, std::move(topic_listener));
}

void TopicSubscriber::handle_topic_info(const TPublishTopicRequest& topic_request) {
    // NOTE(wb): if we found there is bottleneck for handle_topic_info by LOG(INFO)
    // eg, update workload info may delay other listener, then we need add a thread here
    // to handle_topic_info asynchronous
    std::shared_lock lock(_listener_mtx);
    LOG(INFO) << "begin handle topic info";
    for (auto& listener_pair : _registered_listeners) {
        listener_pair.second->handle_topic_info(topic_request);
        LOG(INFO) << "handle topic " << listener_pair.first << " succ";
    }
}
} // namespace doris
