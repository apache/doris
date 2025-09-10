#pragma once
#include <atomic>
#include <memory>

namespace doris {
#ifndef USE_LIBCPP
template <typename T>
using atomic_shared_ptr = std::atomic<std::shared_ptr<T>>;
#else
// libcpp do not support atomic<std::shared_ptr<T>>
// so we implement a simple version of atomic_shared_ptr here
template <typename T>
class atomic_shared_ptr {
public:
    atomic_shared_ptr() noexcept : _ptr(nullptr) {}
    atomic_shared_ptr(std::shared_ptr<T> desired) noexcept : _ptr(desired) {}
    atomic_shared_ptr(const atomic_shared_ptr&) = delete;
    atomic_shared_ptr& operator=(const atomic_shared_ptr&) = delete;

    void store(std::shared_ptr<T> desired,
               std::memory_order order = std::memory_order_seq_cst) noexcept {
        std::atomic_store_explicit(&_ptr, desired, order);
    }

    std::shared_ptr<T> load(std::memory_order order = std::memory_order_seq_cst) const noexcept {
        return std::atomic_load_explicit(&_ptr, order);
    }
private:
    mutable std::shared_ptr<T> _ptr;
};
#endif
} // namespace doris