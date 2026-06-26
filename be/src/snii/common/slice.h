#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace snii {

// Read-only byte view (does not own memory). Lifetime is managed by the underlying buffer.
class Slice {
public:
    Slice() = default;
    Slice(const uint8_t* d, size_t n) : data_(d), size_(n) {}
    explicit Slice(const std::vector<uint8_t>& v) : data_(v.data()), size_(v.size()) {}
    explicit Slice(std::string_view sv)
            : data_(reinterpret_cast<const uint8_t*>(sv.data())), size_(sv.size()) {}

    const uint8_t* data() const { return data_; }
    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    uint8_t operator[](size_t i) const {
        assert(i < size_);
        return data_[i];
    }

    Slice subslice(size_t off, size_t n) const {
        assert(off + n <= size_);
        return Slice(data_ + off, n);
    }

private:
    const uint8_t* data_ = nullptr;
    size_t size_ = 0;
};

} // namespace snii
