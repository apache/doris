#pragma once
#include <cstdint>
#include <type_traits>

namespace diskann
{
#pragma pack(push, 1)

struct tag_uint128
{
    std::uint64_t _data1 = 0;
    std::uint64_t _data2 = 0;

    bool operator==(const tag_uint128 &other) const
    {
        return _data1 == other._data1 && _data2 == other._data2;
    }

    bool operator==(std::uint64_t other) const
    {
        return _data1 == other && _data2 == 0;
    }

    tag_uint128 &operator=(const tag_uint128 &other)
    {
        _data1 = other._data1;
        _data2 = other._data2;

        return *this;
    }

    tag_uint128 &operator=(std::uint64_t other)
    {
        _data1 = other;
        _data2 = 0;

        return *this;
    }
};

#pragma pack(pop)
} // namespace diskann

namespace std
{
// Hash 128 input bits down to 64 bits of output.
// This is intended to be a reasonably good hash function.
inline std::uint64_t Hash128to64(const std::uint64_t &low, const std::uint64_t &high)
{
    // Murmur-inspired hashing.
    const std::uint64_t kMul = 0x9ddfea08eb382d69ULL;
    std::uint64_t a = (low ^ high) * kMul;
    a ^= (a >> 47);
    std::uint64_t b = (high ^ a) * kMul;
    b ^= (b >> 47);
    b *= kMul;
    return b;
}

template <> struct hash<diskann::tag_uint128>
{
    size_t operator()(const diskann::tag_uint128 &key) const noexcept
    {
        return Hash128to64(key._data1, key._data2); // map -0 to 0
    }
};

} // namespace std