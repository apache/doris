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

#include <condition_variable>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>

#include "common/status.h"
#include "glog/logging.h"

namespace doris {
namespace vectorized {

struct Void {
    bool operator==(const Void&) const { return true; }
    bool operator!=(const Void&) const { return false; }
};

template <typename T>
class SharedListenableFuture;

template <typename T>
class ListenableFuture {
public:
    using Callback = std::function<void(const T&, const doris::Status&)>;

    ListenableFuture(const ListenableFuture&) = delete;
    ListenableFuture& operator=(const ListenableFuture&) = delete;

    ListenableFuture(ListenableFuture&& other) noexcept
            : _ready(other._ready),
              _value(std::move(other._value)),
              _status(std::move(other._status)),
              _callbacks(std::move(other._callbacks)) {
        other._ready = false;
    }

    ListenableFuture& operator=(ListenableFuture&& other) noexcept {
        if (this != &other) {
            std::lock_guard<std::mutex> lock(mutex_);
            std::lock_guard<std::mutex> other_lock(other.mutex_);
            _ready = other._ready;
            _value = std::move(other._value);
            _status = std::move(other._status);
            _callbacks = std::move(other._callbacks);
            other._ready = false;
        }
        return *this;
    }

    ListenableFuture() : _ready(false) {}

    void set_value(const T& value) { _execute(value, doris::Status::OK()); }

    void set_error(const doris::Status& status) { _execute({}, status); }

    void add_callback(Callback cb) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (_ready) {
            cb(_value, _status);
        } else {
            _callbacks.emplace_back(std::move(cb));
        }
    }

    static ListenableFuture<T> create_ready(T value) {
        ListenableFuture<T> future;
        future.set_value(std::move(value));
        return future;
    }

    template <typename U = T>
    static typename std::enable_if<std::is_same<U, Void>::value, ListenableFuture<U>>::type
    create_ready() {
        ListenableFuture<U> future;
        future.set_value(Void {});
        return future;
    }

    bool is_ready() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return _ready;
    }

    bool is_done() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return _ready && _status.ok();
    }

    bool is_error() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return _ready && !_status.ok();
    }

    const doris::Status& get_status() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return _status;
    }

    Result<T> get() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!_ready) {
            cv_.wait(lock);
        }
        if (!_status.ok()) {
            return unexpected(_status);
        }
        return _value;
    }

    SharedListenableFuture<T> share() && { return SharedListenableFuture<T>(std::move(*this)); }

    friend class SharedListenableFuture<T>;

private:
    void _execute(const T& value, const doris::Status& status) {
        std::vector<Callback> tmp_callbacks;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (_ready) {
                return;
            }

            _value = value;
            _status = status;
            _ready = true;
            tmp_callbacks.swap(_callbacks);
        }

        for (auto& cb : tmp_callbacks) {
            cb(_value, _status);
        }
        cv_.notify_all();
    }

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool _ready;
    T _value;
    doris::Status _status = doris::Status::OK();
    std::vector<Callback> _callbacks;
};

template <typename T>
class SharedListenableFuture {
public:
    using Callback = typename ListenableFuture<T>::Callback;

    SharedListenableFuture(const SharedListenableFuture&) = default;
    SharedListenableFuture& operator=(const SharedListenableFuture&) = default;

    SharedListenableFuture(SharedListenableFuture&&) = default;
    SharedListenableFuture& operator=(SharedListenableFuture&&) = default;

    explicit SharedListenableFuture(ListenableFuture<T>&& future)
            : impl_(std::make_shared<ListenableFuture<T>>(std::move(future))) {}

    explicit SharedListenableFuture(std::shared_ptr<ListenableFuture<T>> future_ptr)
            : impl_(std::move(future_ptr)) {}

    void add_callback(Callback cb) { return impl_->add_callback(std::move(cb)); }

    bool is_ready() const { return impl_->is_ready(); }

    bool is_done() const { return impl_->is_done(); }

    bool is_error() const { return impl_->is_error(); }

    const doris::Status& get_status() const { return impl_->get_status(); }

    Result<T> get() { return impl_->get(); }

    static SharedListenableFuture<T> create_ready(T value) {
        return SharedListenableFuture<T>(ListenableFuture<T>::create_ready(std::move(value)));
    }

    SharedListenableFuture() : impl_(std::make_shared<ListenableFuture<T>>()) {}

    void set_value(const T& value) { impl_->set_value(value); }

    void set_error(const doris::Status& status) { impl_->set_error(status); }

    template <typename U = T>
    static typename std::enable_if<std::is_same<U, Void>::value, SharedListenableFuture<U>>::type
    create_ready() {
        return SharedListenableFuture<U>(ListenableFuture<U>::create_ready());
    }

private:
    std::shared_ptr<ListenableFuture<T>> impl_;
};

namespace listenable_future {
inline SharedListenableFuture<Void> null_future =
        SharedListenableFuture<Void>::create_ready(Void {});
} // namespace listenable_future

} // namespace vectorized
} // namespace doris

// 使用示例
//int main() {
//    ListenableFuture<int> future;
//
//    // 添加回调
//    future.add_callback(
//            [](int value) {
//                printf("Got value: %d\n", value);
//            },
//            [](auto e) {
//                try {
//                    if (e) std::rethrow_exception(e);
//                } catch (const std::exception& ex) {
//                    printf("Exception: %s\n", ex.what());
//                }
//            }
//    );
//
//    // 链式调用
//    auto next = future.then([](int x) {
//                          return x * 2;
//                      }).then([](int x) {
//        return std::to_string(x);
//    });
//
//    // 设置结果
//    future.set_value(42);
//
//    return 0;
//}