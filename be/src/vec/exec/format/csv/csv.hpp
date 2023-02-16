#pragma once
/*
CSV for C++, version 2.1.3
https://github.com/vincentlaucsb/csv-parser

MIT License

Copyright (c) 2017-2020 Vincent La

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CSV_HPP
#define CSV_HPP

/** @file
 *  @brief Defines functionality needed for basic CSV parsing
 */

#include <algorithm>
#include <deque>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

/* Copyright 2017 https://github.com/mandreyel
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 * CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 * OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef MIO_MMAP_HEADER
#define MIO_MMAP_HEADER

// #include "mio/page.hpp"
/* Copyright 2017 https://github.com/mandreyel
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 * CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 * OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef MIO_PAGE_HEADER
#define MIO_PAGE_HEADER

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace mio {

/**
 * This is used by `basic_mmap` to determine whether to create a read-only or
 * a read-write memory mapping.
 */
enum class access_mode { read, write };

/**
 * Determines the operating system's page allocation granularity.
 *
 * On the first call to this function, it invokes the operating system specific syscall
 * to determine the page size, caches the value, and returns it. Any subsequent call to
 * this function serves the cached value, so no further syscalls are made.
 */
inline size_t page_size() {
    static const size_t page_size = [] {
#ifdef _WIN32
        SYSTEM_INFO SystemInfo;
        GetSystemInfo(&SystemInfo);
        return SystemInfo.dwAllocationGranularity;
#else
        return sysconf(_SC_PAGE_SIZE);
#endif
    }();
    return page_size;
}

/**
 * Alligns `offset` to the operating's system page size such that it subtracts the
 * difference until the nearest page boundary before `offset`, or does nothing if
 * `offset` is already page aligned.
 */
inline size_t make_offset_page_aligned(size_t offset) noexcept {
    const size_t page_size_ = page_size();
    // Use integer division to round down to the nearest page alignment.
    return offset / page_size_ * page_size_;
}

} // namespace mio

#endif // MIO_PAGE_HEADER

#include <cstdint>
#include <iterator>
#include <string>
#include <system_error>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif // WIN32_LEAN_AND_MEAN
#include <windows.h>
#else // ifdef _WIN32
#define INVALID_HANDLE_VALUE -1
#endif // ifdef _WIN32

namespace mio {

// This value may be provided as the `length` parameter to the constructor or
// `map`, in which case a memory mapping of the entire file is created.
enum { map_entire_file = 0 };

#ifdef _WIN32
using file_handle_type = HANDLE;
#else
using file_handle_type = int;
#endif

// This value represents an invalid file handle type. This can be used to
// determine whether `basic_mmap::file_handle` is valid, for example.
const static file_handle_type invalid_handle = INVALID_HANDLE_VALUE;

template <access_mode AccessMode, typename ByteT>
struct basic_mmap {
    using value_type = ByteT;
    using size_type = size_t;
    using reference = value_type&;
    using const_reference = const value_type&;
    using pointer = value_type*;
    using const_pointer = const value_type*;
    using difference_type = std::ptrdiff_t;
    using iterator = pointer;
    using const_iterator = const_pointer;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;
    using iterator_category = std::random_access_iterator_tag;
    using handle_type = file_handle_type;

    static_assert(sizeof(ByteT) == sizeof(char), "ByteT must be the same size as char.");

private:
    // Points to the first requested byte, and not to the actual start of the mapping.
    pointer data_ = nullptr;

    // Length--in bytes--requested by user (which may not be the length of the
    // full mapping) and the length of the full mapping.
    size_type length_ = 0;
    size_type mapped_length_ = 0;

    // Letting user map a file using both an existing file handle and a path
    // introcudes some complexity (see `is_handle_internal_`).
    // On POSIX, we only need a file handle to create a mapping, while on
    // Windows systems the file handle is necessary to retrieve a file mapping
    // handle, but any subsequent operations on the mapped region must be done
    // through the latter.
    handle_type file_handle_ = INVALID_HANDLE_VALUE;
#ifdef _WIN32
    handle_type file_mapping_handle_ = INVALID_HANDLE_VALUE;
#endif

    // Letting user map a file using both an existing file handle and a path
    // introcudes some complexity in that we must not close the file handle if
    // user provided it, but we must close it if we obtained it using the
    // provided path. For this reason, this flag is used to determine when to
    // close `file_handle_`.
    bool is_handle_internal_;

public:
    /**
     * The default constructed mmap object is in a non-mapped state, that is,
     * any operation that attempts to access nonexistent underlying data will
     * result in undefined behaviour/segmentation faults.
     */
    basic_mmap() = default;

#ifdef __cpp_exceptions
    /**
     * The same as invoking the `map` function, except any error that may occur
     * while establishing the mapping is wrapped in a `std::system_error` and is
     * thrown.
     */
    template <typename String>
    basic_mmap(const String& path, const size_type offset = 0,
               const size_type length = map_entire_file) {
        std::error_code error;
        map(path, offset, length, error);
        if (error) {
            throw std::system_error(error);
        }
    }

    /**
     * The same as invoking the `map` function, except any error that may occur
     * while establishing the mapping is wrapped in a `std::system_error` and is
     * thrown.
     */
    basic_mmap(const handle_type handle, const size_type offset = 0,
               const size_type length = map_entire_file) {
        std::error_code error;
        map(handle, offset, length, error);
        if (error) {
            throw std::system_error(error);
        }
    }
#endif // __cpp_exceptions

    /**
     * `basic_mmap` has single-ownership semantics, so transferring ownership
     * may only be accomplished by moving the object.
     */
    basic_mmap(const basic_mmap&) = delete;
    basic_mmap(basic_mmap&&);
    basic_mmap& operator=(const basic_mmap&) = delete;
    basic_mmap& operator=(basic_mmap&&);

    /**
     * If this is a read-write mapping, the destructor invokes sync. Regardless
     * of the access mode, unmap is invoked as a final step.
     */
    ~basic_mmap();

    /**
     * On UNIX systems 'file_handle' and 'mapping_handle' are the same. On Windows,
     * however, a mapped region of a file gets its own handle, which is returned by
     * 'mapping_handle'.
     */
    handle_type file_handle() const noexcept { return file_handle_; }
    handle_type mapping_handle() const noexcept;

    /** Returns whether a valid memory mapping has been created. */
    bool is_open() const noexcept { return file_handle_ != invalid_handle; }

    /**
     * Returns true if no mapping was established, that is, conceptually the
     * same as though the length that was mapped was 0. This function is
     * provided so that this class has Container semantics.
     */
    bool empty() const noexcept { return length() == 0; }

    /** Returns true if a mapping was established. */
    bool is_mapped() const noexcept;

    /**
     * `size` and `length` both return the logical length, i.e. the number of bytes
     * user requested to be mapped, while `mapped_length` returns the actual number of
     * bytes that were mapped which is a multiple of the underlying operating system's
     * page allocation granularity.
     */
    size_type size() const noexcept { return length(); }
    size_type length() const noexcept { return length_; }
    size_type mapped_length() const noexcept { return mapped_length_; }

    /** Returns the offset relative to the start of the mapping. */
    size_type mapping_offset() const noexcept { return mapped_length_ - length_; }

    /**
     * Returns a pointer to the first requested byte, or `nullptr` if no memory mapping
     * exists.
     */
    template <access_mode A = AccessMode,
              typename = typename std::enable_if<A == access_mode::write>::type>
    pointer data() noexcept {
        return data_;
    }
    const_pointer data() const noexcept { return data_; }

    /**
     * Returns an iterator to the first requested byte, if a valid memory mapping
     * exists, otherwise this function call is undefined behaviour.
     */
    template <access_mode A = AccessMode,
              typename = typename std::enable_if<A == access_mode::write>::type>
    iterator begin() noexcept {
        return data();
    }
    const_iterator begin() const noexcept { return data(); }
    const_iterator cbegin() const noexcept { return data(); }

    /**
     * Returns an iterator one past the last requested byte, if a valid memory mapping
     * exists, otherwise this function call is undefined behaviour.
     */
    template <access_mode A = AccessMode,
              typename = typename std::enable_if<A == access_mode::write>::type>
    iterator end() noexcept {
        return data() + length();
    }
    const_iterator end() const noexcept { return data() + length(); }
    const_iterator cend() const noexcept { return data() + length(); }

    /**
     * Returns a reverse iterator to the last memory mapped byte, if a valid
     * memory mapping exists, otherwise this function call is undefined
     * behaviour.
     */
    template <access_mode A = AccessMode,
              typename = typename std::enable_if<A == access_mode::write>::type>
    reverse_iterator rbegin() noexcept {
        return reverse_iterator(end());
    }
    const_reverse_iterator rbegin() const noexcept { return const_reverse_iterator(end()); }
    const_reverse_iterator crbegin() const noexcept { return const_reverse_iterator(end()); }

    /**
     * Returns a reverse iterator past the first mapped byte, if a valid memory
     * mapping exists, otherwise this function call is undefined behaviour.
     */
    template <access_mode A = AccessMode,
              typename = typename std::enable_if<A == access_mode::write>::type>
    reverse_iterator rend() noexcept {
        return reverse_iterator(begin());
    }
    const_reverse_iterator rend() const noexcept { return const_reverse_iterator(begin()); }
    const_reverse_iterator crend() const noexcept { return const_reverse_iterator(begin()); }

    /**
     * Returns a reference to the `i`th byte from the first requested byte (as returned
     * by `data`). If this is invoked when no valid memory mapping has been created
     * prior to this call, undefined behaviour ensues.
     */
    reference operator[](const size_type i) noexcept { return data_[i]; }
    const_reference operator[](const size_type i) const noexcept { return data_[i]; }

    /**
     * Establishes a memory mapping with AccessMode. If the mapping is unsuccesful, the
     * reason is reported via `error` and the object remains in a state as if this
     * function hadn't been called.
     *
     * `path`, which must be a path to an existing file, is used to retrieve a file
     * handle (which is closed when the object destructs or `unmap` is called), which is
     * then used to memory map the requested region. Upon failure, `error` is set to
     * indicate the reason and the object remains in an unmapped state.
     *
     * `offset` is the number of bytes, relative to the start of the file, where the
     * mapping should begin. When specifying it, there is no need to worry about
     * providing a value that is aligned with the operating system's page allocation
     * granularity. This is adjusted by the implementation such that the first requested
     * byte (as returned by `data` or `begin`), so long as `offset` is valid, will be at
     * `offset` from the start of the file.
     *
     * `length` is the number of bytes to map. It may be `map_entire_file`, in which
     * case a mapping of the entire file is created.
     */
    template <typename String>
    void map(const String& path, const size_type offset, const size_type length,
             std::error_code& error);

    /**
     * Establishes a memory mapping with AccessMode. If the mapping is unsuccesful, the
     * reason is reported via `error` and the object remains in a state as if this
     * function hadn't been called.
     *
     * `path`, which must be a path to an existing file, is used to retrieve a file
     * handle (which is closed when the object destructs or `unmap` is called), which is
     * then used to memory map the requested region. Upon failure, `error` is set to
     * indicate the reason and the object remains in an unmapped state.
     * 
     * The entire file is mapped.
     */
    template <typename String>
    void map(const String& path, std::error_code& error) {
        map(path, 0, map_entire_file, error);
    }

    /**
     * Establishes a memory mapping with AccessMode. If the mapping is
     * unsuccesful, the reason is reported via `error` and the object remains in
     * a state as if this function hadn't been called.
     *
     * `handle`, which must be a valid file handle, which is used to memory map the
     * requested region. Upon failure, `error` is set to indicate the reason and the
     * object remains in an unmapped state.
     *
     * `offset` is the number of bytes, relative to the start of the file, where the
     * mapping should begin. When specifying it, there is no need to worry about
     * providing a value that is aligned with the operating system's page allocation
     * granularity. This is adjusted by the implementation such that the first requested
     * byte (as returned by `data` or `begin`), so long as `offset` is valid, will be at
     * `offset` from the start of the file.
     *
     * `length` is the number of bytes to map. It may be `map_entire_file`, in which
     * case a mapping of the entire file is created.
     */
    void map(const handle_type handle, const size_type offset, const size_type length,
             std::error_code& error);

    /**
     * Establishes a memory mapping with AccessMode. If the mapping is
     * unsuccesful, the reason is reported via `error` and the object remains in
     * a state as if this function hadn't been called.
     *
     * `handle`, which must be a valid file handle, which is used to memory map the
     * requested region. Upon failure, `error` is set to indicate the reason and the
     * object remains in an unmapped state.
     * 
     * The entire file is mapped.
     */
    void map(const handle_type handle, std::error_code& error) {
        map(handle, 0, map_entire_file, error);
    }

    /**
     * If a valid memory mapping has been created prior to this call, this call
     * instructs the kernel to unmap the memory region and disassociate this object
     * from the file.
     *
     * The file handle associated with the file that is mapped is only closed if the
     * mapping was created using a file path. If, on the other hand, an existing
     * file handle was used to create the mapping, the file handle is not closed.
     */
    void unmap();

    void swap(basic_mmap& other);

    /** Flushes the memory mapped page to disk. Errors are reported via `error`. */
    template <access_mode A = AccessMode>
    typename std::enable_if<A == access_mode::write, void>::type sync(std::error_code& error);

    /**
     * All operators compare the address of the first byte and size of the two mapped
     * regions.
     */

private:
    template <access_mode A = AccessMode,
              typename = typename std::enable_if<A == access_mode::write>::type>
    pointer get_mapping_start() noexcept {
        return !data() ? nullptr : data() - mapping_offset();
    }

    const_pointer get_mapping_start() const noexcept {
        return !data() ? nullptr : data() - mapping_offset();
    }

    /**
     * The destructor syncs changes to disk if `AccessMode` is `write`, but not
     * if it's `read`, but since the destructor cannot be templated, we need to
     * do SFINAE in a dedicated function, where one syncs and the other is a noop.
     */
    template <access_mode A = AccessMode>
    typename std::enable_if<A == access_mode::write, void>::type conditional_sync();
    template <access_mode A = AccessMode>
    typename std::enable_if<A == access_mode::read, void>::type conditional_sync();
};

template <access_mode AccessMode, typename ByteT>
bool operator==(const basic_mmap<AccessMode, ByteT>& a, const basic_mmap<AccessMode, ByteT>& b);

template <access_mode AccessMode, typename ByteT>
bool operator!=(const basic_mmap<AccessMode, ByteT>& a, const basic_mmap<AccessMode, ByteT>& b);

template <access_mode AccessMode, typename ByteT>
bool operator<(const basic_mmap<AccessMode, ByteT>& a, const basic_mmap<AccessMode, ByteT>& b);

template <access_mode AccessMode, typename ByteT>
bool operator<=(const basic_mmap<AccessMode, ByteT>& a, const basic_mmap<AccessMode, ByteT>& b);

template <access_mode AccessMode, typename ByteT>
bool operator>(const basic_mmap<AccessMode, ByteT>& a, const basic_mmap<AccessMode, ByteT>& b);

template <access_mode AccessMode, typename ByteT>
bool operator>=(const basic_mmap<AccessMode, ByteT>& a, const basic_mmap<AccessMode, ByteT>& b);

/**
 * This is the basis for all read-only mmap objects and should be preferred over
 * directly using `basic_mmap`.
 */
template <typename ByteT>
using basic_mmap_source = basic_mmap<access_mode::read, ByteT>;

/**
 * This is the basis for all read-write mmap objects and should be preferred over
 * directly using `basic_mmap`.
 */
template <typename ByteT>
using basic_mmap_sink = basic_mmap<access_mode::write, ByteT>;

/**
 * These aliases cover the most common use cases, both representing a raw byte stream
 * (either with a char or an unsigned char/uint8_t).
 */
using mmap_source = basic_mmap_source<char>;
using ummap_source = basic_mmap_source<unsigned char>;

using mmap_sink = basic_mmap_sink<char>;
using ummap_sink = basic_mmap_sink<unsigned char>;

/**
 * Convenience factory method that constructs a mapping for any `basic_mmap` or
 * `basic_mmap` type.
 */
template <typename MMap, typename MappingToken>
MMap make_mmap(const MappingToken& token, int64_t offset, int64_t length, std::error_code& error) {
    MMap mmap;
    mmap.map(token, offset, length, error);
    return mmap;
}

/**
 * Convenience factory method.
 *
 * MappingToken may be a String (`std::string`, `std::string_view`, `const char*`,
 * `std::filesystem::path`, `std::vector<char>`, or similar), or a
 * `mmap_source::handle_type`.
 */
template <typename MappingToken>
mmap_source make_mmap_source(const MappingToken& token, mmap_source::size_type offset,
                             mmap_source::size_type length, std::error_code& error) {
    return make_mmap<mmap_source>(token, offset, length, error);
}

template <typename MappingToken>
mmap_source make_mmap_source(const MappingToken& token, std::error_code& error) {
    return make_mmap_source(token, 0, map_entire_file, error);
}

/**
 * Convenience factory method.
 *
 * MappingToken may be a String (`std::string`, `std::string_view`, `const char*`,
 * `std::filesystem::path`, `std::vector<char>`, or similar), or a
 * `mmap_sink::handle_type`.
 */
template <typename MappingToken>
mmap_sink make_mmap_sink(const MappingToken& token, mmap_sink::size_type offset,
                         mmap_sink::size_type length, std::error_code& error) {
    return make_mmap<mmap_sink>(token, offset, length, error);
}

template <typename MappingToken>
mmap_sink make_mmap_sink(const MappingToken& token, std::error_code& error) {
    return make_mmap_sink(token, 0, map_entire_file, error);
}

} // namespace mio

// #include "detail/mmap.ipp"
/* Copyright 2017 https://github.com/mandreyel
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 * CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 * OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef MIO_BASIC_MMAP_IMPL
#define MIO_BASIC_MMAP_IMPL

// #include "mio/mmap.hpp"

// #include "mio/page.hpp"

// #include "mio/detail/string_util.hpp"
/* Copyright 2017 https://github.com/mandreyel
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 * CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 * OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef MIO_STRING_UTIL_HEADER
#define MIO_STRING_UTIL_HEADER

#include <type_traits>

namespace mio {
namespace detail {

template <typename S, typename C = typename std::decay<S>::type,
          typename = decltype(std::declval<C>().data()),
          typename = typename std::enable_if<std::is_same<typename C::value_type, char>::value
#ifdef _WIN32
                                             || std::is_same<typename C::value_type, wchar_t>::value
#endif
                                             >::type>
struct char_type_helper {
    using type = typename C::value_type;
};

template <class T>
struct char_type {
    using type = typename char_type_helper<T>::type;
};

// TODO: can we avoid this brute force approach?
template <>
struct char_type<char*> {
    using type = char;
};

template <>
struct char_type<const char*> {
    using type = char;
};

template <size_t N>
struct char_type<char[N]> {
    using type = char;
};

template <size_t N>
struct char_type<const char[N]> {
    using type = char;
};

#ifdef _WIN32
template <>
struct char_type<wchar_t*> {
    using type = wchar_t;
};

template <>
struct char_type<const wchar_t*> {
    using type = wchar_t;
};

template <size_t N>
struct char_type<wchar_t[N]> {
    using type = wchar_t;
};

template <size_t N>
struct char_type<const wchar_t[N]> {
    using type = wchar_t;
};
#endif // _WIN32

template <typename CharT, typename S>
struct is_c_str_helper {
    static constexpr bool value = std::is_same<
            CharT*,
            // TODO: I'm so sorry for this... Can this be made cleaner?
            typename std::add_pointer<typename std::remove_cv<typename std::remove_pointer<
                    typename std::decay<S>::type>::type>::type>::type>::value;
};

template <typename S>
struct is_c_str {
    static constexpr bool value = is_c_str_helper<char, S>::value;
};

#ifdef _WIN32
template <typename S>
struct is_c_wstr {
    static constexpr bool value = is_c_str_helper<wchar_t, S>::value;
};
#endif // _WIN32

template <typename S>
struct is_c_str_or_c_wstr {
    static constexpr bool value = is_c_str<S>::value
#ifdef _WIN32
                                  || is_c_wstr<S>::value
#endif
            ;
};

template <typename String, typename = decltype(std::declval<String>().data()),
          typename = typename std::enable_if<!is_c_str_or_c_wstr<String>::value>::type>
const typename char_type<String>::type* c_str(const String& path) {
    return path.data();
}

template <typename String, typename = decltype(std::declval<String>().empty()),
          typename = typename std::enable_if<!is_c_str_or_c_wstr<String>::value>::type>
bool empty(const String& path) {
    return path.empty();
}

template <typename String,
          typename = typename std::enable_if<is_c_str_or_c_wstr<String>::value>::type>
const typename char_type<String>::type* c_str(String path) {
    return path;
}

template <typename String,
          typename = typename std::enable_if<is_c_str_or_c_wstr<String>::value>::type>
bool empty(String path) {
    return !path || (*path == 0);
}

} // namespace detail
} // namespace mio

#endif // MIO_STRING_UTIL_HEADER

#include <algorithm>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace mio {
namespace detail {

#ifdef _WIN32
namespace win {

/** Returns the 4 upper bytes of an 8-byte integer. */
inline DWORD int64_high(int64_t n) noexcept {
    return n >> 32;
}

/** Returns the 4 lower bytes of an 8-byte integer. */
inline DWORD int64_low(int64_t n) noexcept {
    return n & 0xffffffff;
}

template <typename String, typename = typename std::enable_if<std::is_same<
                                   typename char_type<String>::type, char>::value>::type>
file_handle_type open_file_helper(const String& path, const access_mode mode) {
    return ::CreateFileA(
            c_str(path), mode == access_mode::read ? GENERIC_READ : GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, 0);
}

template <typename String>
typename std::enable_if<std::is_same<typename char_type<String>::type, wchar_t>::value,
                        file_handle_type>::type
open_file_helper(const String& path, const access_mode mode) {
    return ::CreateFileW(
            c_str(path), mode == access_mode::read ? GENERIC_READ : GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, 0);
}

} // namespace win
#endif // _WIN32

/**
 * Returns the last platform specific system error (errno on POSIX and
 * GetLastError on Win) as a `std::error_code`.
 */
inline std::error_code last_error() noexcept {
    std::error_code error;
#ifdef _WIN32
    error.assign(GetLastError(), std::system_category());
#else
    error.assign(errno, std::system_category());
#endif
    return error;
}

template <typename String>
file_handle_type open_file(const String& path, const access_mode mode, std::error_code& error) {
    error.clear();
    if (detail::empty(path)) {
        error = std::make_error_code(std::errc::invalid_argument);
        return invalid_handle;
    }
#ifdef _WIN32
    const auto handle = win::open_file_helper(path, mode);
#else // POSIX
    const auto handle = ::open(c_str(path), mode == access_mode::read ? O_RDONLY : O_RDWR);
#endif
    if (handle == invalid_handle) {
        error = detail::last_error();
    }
    return handle;
}

inline size_t query_file_size(file_handle_type handle, std::error_code& error) {
    error.clear();
#ifdef _WIN32
    LARGE_INTEGER file_size;
    if (::GetFileSizeEx(handle, &file_size) == 0) {
        error = detail::last_error();
        return 0;
    }
    return static_cast<int64_t>(file_size.QuadPart);
#else // POSIX
    struct stat sbuf;
    if (::fstat(handle, &sbuf) == -1) {
        error = detail::last_error();
        return 0;
    }
    return sbuf.st_size;
#endif
}

struct mmap_context {
    char* data;
    int64_t length;
    int64_t mapped_length;
#ifdef _WIN32
    file_handle_type file_mapping_handle;
#endif
};

inline mmap_context memory_map(const file_handle_type file_handle, const int64_t offset,
                               const int64_t length, const access_mode mode,
                               std::error_code& error) {
    const int64_t aligned_offset = make_offset_page_aligned(offset);
    const int64_t length_to_map = offset - aligned_offset + length;
#ifdef _WIN32
    const int64_t max_file_size = offset + length;
    const auto file_mapping_handle = ::CreateFileMapping(
            file_handle, 0, mode == access_mode::read ? PAGE_READONLY : PAGE_READWRITE,
            win::int64_high(max_file_size), win::int64_low(max_file_size), 0);
    if (file_mapping_handle == invalid_handle) {
        error = detail::last_error();
        return {};
    }
    char* mapping_start = static_cast<char*>(::MapViewOfFile(
            file_mapping_handle, mode == access_mode::read ? FILE_MAP_READ : FILE_MAP_WRITE,
            win::int64_high(aligned_offset), win::int64_low(aligned_offset), length_to_map));
    if (mapping_start == nullptr) {
        // Close file handle if mapping it failed.
        ::CloseHandle(file_mapping_handle);
        error = detail::last_error();
        return {};
    }
#else // POSIX
    char* mapping_start = static_cast<char*>(
            ::mmap(0, // Don't give hint as to where to map.
                   length_to_map, mode == access_mode::read ? PROT_READ : PROT_WRITE, MAP_SHARED,
                   file_handle, aligned_offset));
    if (mapping_start == MAP_FAILED) {
        error = detail::last_error();
        return {};
    }
#endif
    mmap_context ctx;
    ctx.data = mapping_start + offset - aligned_offset;
    ctx.length = length;
    ctx.mapped_length = length_to_map;
#ifdef _WIN32
    ctx.file_mapping_handle = file_mapping_handle;
#endif
    return ctx;
}

} // namespace detail

// -- basic_mmap --

template <access_mode AccessMode, typename ByteT>
basic_mmap<AccessMode, ByteT>::~basic_mmap() {
    conditional_sync();
    unmap();
}

template <access_mode AccessMode, typename ByteT>
basic_mmap<AccessMode, ByteT>::basic_mmap(basic_mmap&& other)
        : data_(std::move(other.data_)),
          length_(std::move(other.length_)),
          mapped_length_(std::move(other.mapped_length_)),
          file_handle_(std::move(other.file_handle_))
#ifdef _WIN32
          ,
          file_mapping_handle_(std::move(other.file_mapping_handle_))
#endif
          ,
          is_handle_internal_(std::move(other.is_handle_internal_)) {
    other.data_ = nullptr;
    other.length_ = other.mapped_length_ = 0;
    other.file_handle_ = invalid_handle;
#ifdef _WIN32
    other.file_mapping_handle_ = invalid_handle;
#endif
}

template <access_mode AccessMode, typename ByteT>
basic_mmap<AccessMode, ByteT>& basic_mmap<AccessMode, ByteT>::operator=(basic_mmap&& other) {
    if (this != &other) {
        // First the existing mapping needs to be removed.
        unmap();
        data_ = std::move(other.data_);
        length_ = std::move(other.length_);
        mapped_length_ = std::move(other.mapped_length_);
        file_handle_ = std::move(other.file_handle_);
#ifdef _WIN32
        file_mapping_handle_ = std::move(other.file_mapping_handle_);
#endif
        is_handle_internal_ = std::move(other.is_handle_internal_);

        // The moved from basic_mmap's fields need to be reset, because
        // otherwise other's destructor will unmap the same mapping that was
        // just moved into this.
        other.data_ = nullptr;
        other.length_ = other.mapped_length_ = 0;
        other.file_handle_ = invalid_handle;
#ifdef _WIN32
        other.file_mapping_handle_ = invalid_handle;
#endif
        other.is_handle_internal_ = false;
    }
    return *this;
}

template <access_mode AccessMode, typename ByteT>
typename basic_mmap<AccessMode, ByteT>::handle_type basic_mmap<AccessMode, ByteT>::mapping_handle()
        const noexcept {
#ifdef _WIN32
    return file_mapping_handle_;
#else
    return file_handle_;
#endif
}

template <access_mode AccessMode, typename ByteT>
template <typename String>
void basic_mmap<AccessMode, ByteT>::map(const String& path, const size_type offset,
                                        const size_type length, std::error_code& error) {
    error.clear();
    if (detail::empty(path)) {
        error = std::make_error_code(std::errc::invalid_argument);
        return;
    }
    const auto handle = detail::open_file(path, AccessMode, error);
    if (error) {
        return;
    }

    map(handle, offset, length, error);
    // This MUST be after the call to map, as that sets this to true.
    if (!error) {
        is_handle_internal_ = true;
    }
}

template <access_mode AccessMode, typename ByteT>
void basic_mmap<AccessMode, ByteT>::map(const handle_type handle, const size_type offset,
                                        const size_type length, std::error_code& error) {
    error.clear();
    if (handle == invalid_handle) {
        error = std::make_error_code(std::errc::bad_file_descriptor);
        return;
    }

    const auto file_size = detail::query_file_size(handle, error);
    if (error) {
        return;
    }

    if (offset + length > file_size) {
        error = std::make_error_code(std::errc::invalid_argument);
        return;
    }

    const auto ctx = detail::memory_map(handle, offset,
                                        length == map_entire_file ? (file_size - offset) : length,
                                        AccessMode, error);
    if (!error) {
        // We must unmap the previous mapping that may have existed prior to this call.
        // Note that this must only be invoked after a new mapping has been created in
        // order to provide the strong guarantee that, should the new mapping fail, the
        // `map` function leaves this instance in a state as though the function had
        // never been invoked.
        unmap();
        file_handle_ = handle;
        is_handle_internal_ = false;
        data_ = reinterpret_cast<pointer>(ctx.data);
        length_ = ctx.length;
        mapped_length_ = ctx.mapped_length;
#ifdef _WIN32
        file_mapping_handle_ = ctx.file_mapping_handle;
#endif
    }
}

template <access_mode AccessMode, typename ByteT>
template <access_mode A>
typename std::enable_if<A == access_mode::write, void>::type basic_mmap<AccessMode, ByteT>::sync(
        std::error_code& error) {
    error.clear();
    if (!is_open()) {
        error = std::make_error_code(std::errc::bad_file_descriptor);
        return;
    }

    if (data()) {
#ifdef _WIN32
        if (::FlushViewOfFile(get_mapping_start(), mapped_length_) == 0 ||
            ::FlushFileBuffers(file_handle_) == 0)
#else // POSIX
        if (::msync(get_mapping_start(), mapped_length_, MS_SYNC) != 0)
#endif
        {
            error = detail::last_error();
            return;
        }
    }
#ifdef _WIN32
    if (::FlushFileBuffers(file_handle_) == 0) {
        error = detail::last_error();
    }
#endif
}

template <access_mode AccessMode, typename ByteT>
void basic_mmap<AccessMode, ByteT>::unmap() {
    if (!is_open()) {
        return;
    }
    // TODO do we care about errors here?
#ifdef _WIN32
    if (is_mapped()) {
        ::UnmapViewOfFile(get_mapping_start());
        ::CloseHandle(file_mapping_handle_);
    }
#else // POSIX
    if (data_) {
        ::munmap(const_cast<pointer>(get_mapping_start()), mapped_length_);
    }
#endif

    // If `file_handle_` was obtained by our opening it (when map is called with
    // a path, rather than an existing file handle), we need to close it,
    // otherwise it must not be closed as it may still be used outside this
    // instance.
    if (is_handle_internal_) {
#ifdef _WIN32
        ::CloseHandle(file_handle_);
#else // POSIX
        ::close(file_handle_);
#endif
    }

    // Reset fields to their default values.
    data_ = nullptr;
    length_ = mapped_length_ = 0;
    file_handle_ = invalid_handle;
#ifdef _WIN32
    file_mapping_handle_ = invalid_handle;
#endif
}

template <access_mode AccessMode, typename ByteT>
bool basic_mmap<AccessMode, ByteT>::is_mapped() const noexcept {
#ifdef _WIN32
    return file_mapping_handle_ != invalid_handle;
#else // POSIX
    return is_open();
#endif
}

template <access_mode AccessMode, typename ByteT>
void basic_mmap<AccessMode, ByteT>::swap(basic_mmap& other) {
    if (this != &other) {
        using std::swap;
        swap(data_, other.data_);
        swap(file_handle_, other.file_handle_);
#ifdef _WIN32
        swap(file_mapping_handle_, other.file_mapping_handle_);
#endif
        swap(length_, other.length_);
        swap(mapped_length_, other.mapped_length_);
        swap(is_handle_internal_, other.is_handle_internal_);
    }
}

template <access_mode AccessMode, typename ByteT>
template <access_mode A>
typename std::enable_if<A == access_mode::write, void>::type
basic_mmap<AccessMode, ByteT>::conditional_sync() {
    // This is invoked from the destructor, so not much we can do about
    // failures here.
    std::error_code ec;
    sync(ec);
}

template <access_mode AccessMode, typename ByteT>
template <access_mode A>
typename std::enable_if<A == access_mode::read, void>::type
basic_mmap<AccessMode, ByteT>::conditional_sync() {
    // noop
}

template <access_mode AccessMode, typename ByteT>
bool operator==(const basic_mmap<AccessMode, ByteT>& a, const basic_mmap<AccessMode, ByteT>& b) {
    return a.data() == b.data() && a.size() == b.size();
}

template <access_mode AccessMode, typename ByteT>
bool operator!=(const basic_mmap<AccessMode, ByteT>& a, const basic_mmap<AccessMode, ByteT>& b) {
    return !(a == b);
}

template <access_mode AccessMode, typename ByteT>
bool operator<(const basic_mmap<AccessMode, ByteT>& a, const basic_mmap<AccessMode, ByteT>& b) {
    if (a.data() == b.data()) {
        return a.size() < b.size();
    }
    return a.data() < b.data();
}

template <access_mode AccessMode, typename ByteT>
bool operator<=(const basic_mmap<AccessMode, ByteT>& a, const basic_mmap<AccessMode, ByteT>& b) {
    return !(a > b);
}

template <access_mode AccessMode, typename ByteT>
bool operator>(const basic_mmap<AccessMode, ByteT>& a, const basic_mmap<AccessMode, ByteT>& b) {
    if (a.data() == b.data()) {
        return a.size() > b.size();
    }
    return a.data() > b.data();
}

template <access_mode AccessMode, typename ByteT>
bool operator>=(const basic_mmap<AccessMode, ByteT>& a, const basic_mmap<AccessMode, ByteT>& b) {
    return !(a < b);
}

} // namespace mio

#endif // MIO_BASIC_MMAP_IMPL

#endif // MIO_MMAP_HEADER
/* Copyright 2017 https://github.com/mandreyel
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 * CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 * OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef MIO_PAGE_HEADER
#define MIO_PAGE_HEADER

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace mio {

/**
 * This is used by `basic_mmap` to determine whether to create a read-only or
 * a read-write memory mapping.
 */
enum class access_mode { read, write };

/**
 * Determines the operating system's page allocation granularity.
 *
 * On the first call to this function, it invokes the operating system specific syscall
 * to determine the page size, caches the value, and returns it. Any subsequent call to
 * this function serves the cached value, so no further syscalls are made.
 */
inline size_t page_size() {
    static const size_t page_size = [] {
#ifdef _WIN32
        SYSTEM_INFO SystemInfo;
        GetSystemInfo(&SystemInfo);
        return SystemInfo.dwAllocationGranularity;
#else
        return sysconf(_SC_PAGE_SIZE);
#endif
    }();
    return page_size;
}

/**
 * Alligns `offset` to the operating's system page size such that it subtracts the
 * difference until the nearest page boundary before `offset`, or does nothing if
 * `offset` is already page aligned.
 */
inline size_t make_offset_page_aligned(size_t offset) noexcept {
    const size_t page_size_ = page_size();
    // Use integer division to round down to the nearest page alignment.
    return offset / page_size_ * page_size_;
}

} // namespace mio

#endif // MIO_PAGE_HEADER
/* Copyright 2017 https://github.com/mandreyel
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
 * CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
 * OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef MIO_SHARED_MMAP_HEADER
#define MIO_SHARED_MMAP_HEADER

// #include "mio/mmap.hpp"

#include <memory>       // std::shared_ptr
#include <system_error> // std::error_code

namespace mio {

/**
 * Exposes (nearly) the same interface as `basic_mmap`, but endowes it with
 * `std::shared_ptr` semantics.
 *
 * This is not the default behaviour of `basic_mmap` to avoid allocating on the heap if
 * shared semantics are not required.
 */
template <access_mode AccessMode, typename ByteT>
class basic_shared_mmap {
    using impl_type = basic_mmap<AccessMode, ByteT>;
    std::shared_ptr<impl_type> pimpl_;

public:
    using value_type = typename impl_type::value_type;
    using size_type = typename impl_type::size_type;
    using reference = typename impl_type::reference;
    using const_reference = typename impl_type::const_reference;
    using pointer = typename impl_type::pointer;
    using const_pointer = typename impl_type::const_pointer;
    using difference_type = typename impl_type::difference_type;
    using iterator = typename impl_type::iterator;
    using const_iterator = typename impl_type::const_iterator;
    using reverse_iterator = typename impl_type::reverse_iterator;
    using const_reverse_iterator = typename impl_type::const_reverse_iterator;
    using iterator_category = typename impl_type::iterator_category;
    using handle_type = typename impl_type::handle_type;
    using mmap_type = impl_type;

    basic_shared_mmap() = default;
    basic_shared_mmap(const basic_shared_mmap&) = default;
    basic_shared_mmap& operator=(const basic_shared_mmap&) = default;
    basic_shared_mmap(basic_shared_mmap&&) = default;
    basic_shared_mmap& operator=(basic_shared_mmap&&) = default;

    /** Takes ownership of an existing mmap object. */
    basic_shared_mmap(mmap_type&& mmap) : pimpl_(std::make_shared<mmap_type>(std::move(mmap))) {}

    /** Takes ownership of an existing mmap object. */
    basic_shared_mmap& operator=(mmap_type&& mmap) {
        pimpl_ = std::make_shared<mmap_type>(std::move(mmap));
        return *this;
    }

    /** Initializes this object with an already established shared mmap. */
    basic_shared_mmap(std::shared_ptr<mmap_type> mmap) : pimpl_(std::move(mmap)) {}

    /** Initializes this object with an already established shared mmap. */
    basic_shared_mmap& operator=(std::shared_ptr<mmap_type> mmap) {
        pimpl_ = std::move(mmap);
        return *this;
    }

#ifdef __cpp_exceptions
    /**
     * The same as invoking the `map` function, except any error that may occur
     * while establishing the mapping is wrapped in a `std::system_error` and is
     * thrown.
     */
    template <typename String>
    basic_shared_mmap(const String& path, const size_type offset = 0,
                      const size_type length = map_entire_file) {
        std::error_code error;
        map(path, offset, length, error);
        if (error) {
            throw std::system_error(error);
        }
    }

    /**
     * The same as invoking the `map` function, except any error that may occur
     * while establishing the mapping is wrapped in a `std::system_error` and is
     * thrown.
     */
    basic_shared_mmap(const handle_type handle, const size_type offset = 0,
                      const size_type length = map_entire_file) {
        std::error_code error;
        map(handle, offset, length, error);
        if (error) {
            throw std::system_error(error);
        }
    }
#endif // __cpp_exceptions

    /**
     * If this is a read-write mapping and the last reference to the mapping,
     * the destructor invokes sync. Regardless of the access mode, unmap is
     * invoked as a final step.
     */
    ~basic_shared_mmap() = default;

    /** Returns the underlying `std::shared_ptr` instance that holds the mmap. */
    std::shared_ptr<mmap_type> get_shared_ptr() { return pimpl_; }

    /**
     * On UNIX systems 'file_handle' and 'mapping_handle' are the same. On Windows,
     * however, a mapped region of a file gets its own handle, which is returned by
     * 'mapping_handle'.
     */
    handle_type file_handle() const noexcept {
        return pimpl_ ? pimpl_->file_handle() : invalid_handle;
    }

    handle_type mapping_handle() const noexcept {
        return pimpl_ ? pimpl_->mapping_handle() : invalid_handle;
    }

    /** Returns whether a valid memory mapping has been created. */
    bool is_open() const noexcept { return pimpl_ && pimpl_->is_open(); }

    /**
     * Returns true if no mapping was established, that is, conceptually the
     * same as though the length that was mapped was 0. This function is
     * provided so that this class has Container semantics.
     */
    bool empty() const noexcept { return !pimpl_ || pimpl_->empty(); }

    /**
     * `size` and `length` both return the logical length, i.e. the number of bytes
     * user requested to be mapped, while `mapped_length` returns the actual number of
     * bytes that were mapped which is a multiple of the underlying operating system's
     * page allocation granularity.
     */
    size_type size() const noexcept { return pimpl_ ? pimpl_->length() : 0; }
    size_type length() const noexcept { return pimpl_ ? pimpl_->length() : 0; }
    size_type mapped_length() const noexcept { return pimpl_ ? pimpl_->mapped_length() : 0; }

    /**
     * Returns a pointer to the first requested byte, or `nullptr` if no memory mapping
     * exists.
     */
    template <access_mode A = AccessMode,
              typename = typename std::enable_if<A == access_mode::write>::type>
    pointer data() noexcept {
        return pimpl_->data();
    }
    const_pointer data() const noexcept { return pimpl_ ? pimpl_->data() : nullptr; }

    /**
     * Returns an iterator to the first requested byte, if a valid memory mapping
     * exists, otherwise this function call is undefined behaviour.
     */
    iterator begin() noexcept { return pimpl_->begin(); }
    const_iterator begin() const noexcept { return pimpl_->begin(); }
    const_iterator cbegin() const noexcept { return pimpl_->cbegin(); }

    /**
     * Returns an iterator one past the last requested byte, if a valid memory mapping
     * exists, otherwise this function call is undefined behaviour.
     */
    template <access_mode A = AccessMode,
              typename = typename std::enable_if<A == access_mode::write>::type>
    iterator end() noexcept {
        return pimpl_->end();
    }
    const_iterator end() const noexcept { return pimpl_->end(); }
    const_iterator cend() const noexcept { return pimpl_->cend(); }

    /**
     * Returns a reverse iterator to the last memory mapped byte, if a valid
     * memory mapping exists, otherwise this function call is undefined
     * behaviour.
     */
    template <access_mode A = AccessMode,
              typename = typename std::enable_if<A == access_mode::write>::type>
    reverse_iterator rbegin() noexcept {
        return pimpl_->rbegin();
    }
    const_reverse_iterator rbegin() const noexcept { return pimpl_->rbegin(); }
    const_reverse_iterator crbegin() const noexcept { return pimpl_->crbegin(); }

    /**
     * Returns a reverse iterator past the first mapped byte, if a valid memory
     * mapping exists, otherwise this function call is undefined behaviour.
     */
    template <access_mode A = AccessMode,
              typename = typename std::enable_if<A == access_mode::write>::type>
    reverse_iterator rend() noexcept {
        return pimpl_->rend();
    }
    const_reverse_iterator rend() const noexcept { return pimpl_->rend(); }
    const_reverse_iterator crend() const noexcept { return pimpl_->crend(); }

    /**
     * Returns a reference to the `i`th byte from the first requested byte (as returned
     * by `data`). If this is invoked when no valid memory mapping has been created
     * prior to this call, undefined behaviour ensues.
     */
    reference operator[](const size_type i) noexcept { return (*pimpl_)[i]; }
    const_reference operator[](const size_type i) const noexcept { return (*pimpl_)[i]; }

    /**
     * Establishes a memory mapping with AccessMode. If the mapping is unsuccesful, the
     * reason is reported via `error` and the object remains in a state as if this
     * function hadn't been called.
     *
     * `path`, which must be a path to an existing file, is used to retrieve a file
     * handle (which is closed when the object destructs or `unmap` is called), which is
     * then used to memory map the requested region. Upon failure, `error` is set to
     * indicate the reason and the object remains in an unmapped state.
     *
     * `offset` is the number of bytes, relative to the start of the file, where the
     * mapping should begin. When specifying it, there is no need to worry about
     * providing a value that is aligned with the operating system's page allocation
     * granularity. This is adjusted by the implementation such that the first requested
     * byte (as returned by `data` or `begin`), so long as `offset` is valid, will be at
     * `offset` from the start of the file.
     *
     * `length` is the number of bytes to map. It may be `map_entire_file`, in which
     * case a mapping of the entire file is created.
     */
    template <typename String>
    void map(const String& path, const size_type offset, const size_type length,
             std::error_code& error) {
        map_impl(path, offset, length, error);
    }

    /**
     * Establishes a memory mapping with AccessMode. If the mapping is unsuccesful, the
     * reason is reported via `error` and the object remains in a state as if this
     * function hadn't been called.
     *
     * `path`, which must be a path to an existing file, is used to retrieve a file
     * handle (which is closed when the object destructs or `unmap` is called), which is
     * then used to memory map the requested region. Upon failure, `error` is set to
     * indicate the reason and the object remains in an unmapped state.
     *
     * The entire file is mapped.
     */
    template <typename String>
    void map(const String& path, std::error_code& error) {
        map_impl(path, 0, map_entire_file, error);
    }

    /**
     * Establishes a memory mapping with AccessMode. If the mapping is unsuccesful, the
     * reason is reported via `error` and the object remains in a state as if this
     * function hadn't been called.
     *
     * `handle`, which must be a valid file handle, which is used to memory map the
     * requested region. Upon failure, `error` is set to indicate the reason and the
     * object remains in an unmapped state.
     *
     * `offset` is the number of bytes, relative to the start of the file, where the
     * mapping should begin. When specifying it, there is no need to worry about
     * providing a value that is aligned with the operating system's page allocation
     * granularity. This is adjusted by the implementation such that the first requested
     * byte (as returned by `data` or `begin`), so long as `offset` is valid, will be at
     * `offset` from the start of the file.
     *
     * `length` is the number of bytes to map. It may be `map_entire_file`, in which
     * case a mapping of the entire file is created.
     */
    void map(const handle_type handle, const size_type offset, const size_type length,
             std::error_code& error) {
        map_impl(handle, offset, length, error);
    }

    /**
     * Establishes a memory mapping with AccessMode. If the mapping is unsuccesful, the
     * reason is reported via `error` and the object remains in a state as if this
     * function hadn't been called.
     *
     * `handle`, which must be a valid file handle, which is used to memory map the
     * requested region. Upon failure, `error` is set to indicate the reason and the
     * object remains in an unmapped state.
     *
     * The entire file is mapped.
     */
    void map(const handle_type handle, std::error_code& error) {
        map_impl(handle, 0, map_entire_file, error);
    }

    /**
     * If a valid memory mapping has been created prior to this call, this call
     * instructs the kernel to unmap the memory region and disassociate this object
     * from the file.
     *
     * The file handle associated with the file that is mapped is only closed if the
     * mapping was created using a file path. If, on the other hand, an existing
     * file handle was used to create the mapping, the file handle is not closed.
     */
    void unmap() {
        if (pimpl_) pimpl_->unmap();
    }

    void swap(basic_shared_mmap& other) { pimpl_.swap(other.pimpl_); }

    /** Flushes the memory mapped page to disk. Errors are reported via `error`. */
    template <access_mode A = AccessMode,
              typename = typename std::enable_if<A == access_mode::write>::type>
    void sync(std::error_code& error) {
        if (pimpl_) pimpl_->sync(error);
    }

    /** All operators compare the underlying `basic_mmap`'s addresses. */

    friend bool operator==(const basic_shared_mmap& a, const basic_shared_mmap& b) {
        return a.pimpl_ == b.pimpl_;
    }

    friend bool operator!=(const basic_shared_mmap& a, const basic_shared_mmap& b) {
        return !(a == b);
    }

    friend bool operator<(const basic_shared_mmap& a, const basic_shared_mmap& b) {
        return a.pimpl_ < b.pimpl_;
    }

    friend bool operator<=(const basic_shared_mmap& a, const basic_shared_mmap& b) {
        return a.pimpl_ <= b.pimpl_;
    }

    friend bool operator>(const basic_shared_mmap& a, const basic_shared_mmap& b) {
        return a.pimpl_ > b.pimpl_;
    }

    friend bool operator>=(const basic_shared_mmap& a, const basic_shared_mmap& b) {
        return a.pimpl_ >= b.pimpl_;
    }

private:
    template <typename MappingToken>
    void map_impl(const MappingToken& token, const size_type offset, const size_type length,
                  std::error_code& error) {
        if (!pimpl_) {
            mmap_type mmap = make_mmap<mmap_type>(token, offset, length, error);
            if (error) {
                return;
            }
            pimpl_ = std::make_shared<mmap_type>(std::move(mmap));
        } else {
            pimpl_->map(token, offset, length, error);
        }
    }
};

/**
 * This is the basis for all read-only mmap objects and should be preferred over
 * directly using basic_shared_mmap.
 */
template <typename ByteT>
using basic_shared_mmap_source = basic_shared_mmap<access_mode::read, ByteT>;

/**
 * This is the basis for all read-write mmap objects and should be preferred over
 * directly using basic_shared_mmap.
 */
template <typename ByteT>
using basic_shared_mmap_sink = basic_shared_mmap<access_mode::write, ByteT>;

/**
 * These aliases cover the most common use cases, both representing a raw byte stream
 * (either with a char or an unsigned char/uint8_t).
 */
using shared_mmap_source = basic_shared_mmap_source<char>;
using shared_ummap_source = basic_shared_mmap_source<unsigned char>;

using shared_mmap_sink = basic_shared_mmap_sink<char>;
using shared_ummap_sink = basic_shared_mmap_sink<unsigned char>;

} // namespace mio

#endif // MIO_SHARED_MMAP_HEADER

/** @file
 *  @brief Contains the main CSV parsing algorithm and various utility functions
 */

#include <algorithm>
#include <array>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/** @file
 *  A standalone header file containing shared code
 */

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdlib>
#include <deque>

#if defined(_WIN32)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>
#undef max
#undef min
#elif defined(__linux__)
#include <unistd.h>
#endif

/** Helper macro which should be #defined as "inline"
  *  in the single header version
  */
#define CSV_INLINE inline

#include <type_traits>

// Copyright 2017-2019 by Martin Moene
//
// string-view lite, a C++17-like string_view for C++98 and later.
// For more information see https://github.com/martinmoene/string-view-lite
//
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef NONSTD_SV_LITE_H_INCLUDED
#define NONSTD_SV_LITE_H_INCLUDED

#define string_view_lite_MAJOR 1
#define string_view_lite_MINOR 1
#define string_view_lite_PATCH 0

#define string_view_lite_VERSION                               \
    nssv_STRINGIFY(string_view_lite_MAJOR) "." nssv_STRINGIFY( \
            string_view_lite_MINOR) "." nssv_STRINGIFY(string_view_lite_PATCH)

#define nssv_STRINGIFY(x) nssv_STRINGIFY_(x)
#define nssv_STRINGIFY_(x) #x

// string-view lite configuration:

#define nssv_STRING_VIEW_DEFAULT 0
#define nssv_STRING_VIEW_NONSTD 1
#define nssv_STRING_VIEW_STD 2

#if !defined(nssv_CONFIG_SELECT_STRING_VIEW)
#define nssv_CONFIG_SELECT_STRING_VIEW \
    (nssv_HAVE_STD_STRING_VIEW ? nssv_STRING_VIEW_STD : nssv_STRING_VIEW_NONSTD)
#endif

#if defined(nssv_CONFIG_SELECT_STD_STRING_VIEW) || defined(nssv_CONFIG_SELECT_NONSTD_STRING_VIEW)
#error nssv_CONFIG_SELECT_STD_STRING_VIEW and nssv_CONFIG_SELECT_NONSTD_STRING_VIEW are deprecated and removed, please use nssv_CONFIG_SELECT_STRING_VIEW=nssv_STRING_VIEW_...
#endif

#ifndef nssv_CONFIG_STD_SV_OPERATOR
#define nssv_CONFIG_STD_SV_OPERATOR 0
#endif

#ifndef nssv_CONFIG_USR_SV_OPERATOR
#define nssv_CONFIG_USR_SV_OPERATOR 1
#endif

#ifdef nssv_CONFIG_CONVERSION_STD_STRING
#define nssv_CONFIG_CONVERSION_STD_STRING_CLASS_METHODS nssv_CONFIG_CONVERSION_STD_STRING
#define nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS nssv_CONFIG_CONVERSION_STD_STRING
#endif

#ifndef nssv_CONFIG_CONVERSION_STD_STRING_CLASS_METHODS
#define nssv_CONFIG_CONVERSION_STD_STRING_CLASS_METHODS 1
#endif

#ifndef nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS
#define nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS 1
#endif

// Control presence of exception handling (try and auto discover):

#ifndef nssv_CONFIG_NO_EXCEPTIONS
#if defined(__cpp_exceptions) || defined(__EXCEPTIONS) || defined(_CPPUNWIND)
#define nssv_CONFIG_NO_EXCEPTIONS 0
#else
#define nssv_CONFIG_NO_EXCEPTIONS 1
#endif
#endif

// C++ language version detection (C++20 is speculative):
// Note: VC14.0/1900 (VS2015) lacks too much from C++14.

#ifndef nssv_CPLUSPLUS
#if defined(_MSVC_LANG) && !defined(__clang__)
#define nssv_CPLUSPLUS (_MSC_VER == 1900 ? 201103L : _MSVC_LANG)
#else
#define nssv_CPLUSPLUS __cplusplus
#endif
#endif

#define nssv_CPP98_OR_GREATER (nssv_CPLUSPLUS >= 199711L)
#define nssv_CPP11_OR_GREATER (nssv_CPLUSPLUS >= 201103L)
#define nssv_CPP11_OR_GREATER_ (nssv_CPLUSPLUS >= 201103L)
#define nssv_CPP14_OR_GREATER (nssv_CPLUSPLUS >= 201402L)
#define nssv_CPP17_OR_GREATER (nssv_CPLUSPLUS >= 201703L)
#define nssv_CPP20_OR_GREATER (nssv_CPLUSPLUS >= 202000L)

// use C++17 std::string_view if available and requested:

#if nssv_CPP17_OR_GREATER && defined(__has_include)
#if __has_include(<string_view> )
#define nssv_HAVE_STD_STRING_VIEW 1
#else
#define nssv_HAVE_STD_STRING_VIEW 0
#endif
#else
#define nssv_HAVE_STD_STRING_VIEW 0
#endif

#define nssv_USES_STD_STRING_VIEW                                \
    ((nssv_CONFIG_SELECT_STRING_VIEW == nssv_STRING_VIEW_STD) || \
     ((nssv_CONFIG_SELECT_STRING_VIEW == nssv_STRING_VIEW_DEFAULT) && nssv_HAVE_STD_STRING_VIEW))

#define nssv_HAVE_STARTS_WITH (nssv_CPP20_OR_GREATER || !nssv_USES_STD_STRING_VIEW)
#define nssv_HAVE_ENDS_WITH nssv_HAVE_STARTS_WITH

//
// Use C++17 std::string_view:
//

#if nssv_USES_STD_STRING_VIEW

#include <string_view>

// Extensions for std::string:

#if nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS

namespace nonstd {

template <class CharT, class Traits, class Allocator = std::allocator<CharT>>
std::basic_string<CharT, Traits, Allocator> to_string(std::basic_string_view<CharT, Traits> v,
                                                      Allocator const& a = Allocator()) {
    return std::basic_string<CharT, Traits, Allocator>(v.begin(), v.end(), a);
}

template <class CharT, class Traits, class Allocator>
std::basic_string_view<CharT, Traits> to_string_view(
        std::basic_string<CharT, Traits, Allocator> const& s) {
    return std::basic_string_view<CharT, Traits>(s.data(), s.size());
}

// Literal operators sv and _sv:

#if nssv_CONFIG_STD_SV_OPERATOR

using namespace std::literals::string_view_literals;

#endif

#if nssv_CONFIG_USR_SV_OPERATOR

inline namespace literals {
inline namespace string_view_literals {

constexpr std::string_view operator"" _sv(const char* str, size_t len) noexcept // (1)
{
    return std::string_view {str, len};
}

constexpr std::u16string_view operator"" _sv(const char16_t* str, size_t len) noexcept // (2)
{
    return std::u16string_view {str, len};
}

constexpr std::u32string_view operator"" _sv(const char32_t* str, size_t len) noexcept // (3)
{
    return std::u32string_view {str, len};
}

constexpr std::wstring_view operator"" _sv(const wchar_t* str, size_t len) noexcept // (4)
{
    return std::wstring_view {str, len};
}

} // namespace string_view_literals
} // namespace literals

#endif // nssv_CONFIG_USR_SV_OPERATOR

} // namespace nonstd

#endif // nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS

namespace nonstd {

using std::string_view;
using std::wstring_view;
using std::u16string_view;
using std::u32string_view;
using std::basic_string_view;

// literal "sv" and "_sv", see above

using std::operator==;
using std::operator!=;
using std::operator<;
using std::operator<=;
using std::operator>;
using std::operator>=;

using std::operator<<;

} // namespace nonstd

#else // nssv_HAVE_STD_STRING_VIEW

//
// Before C++17: use string_view lite:
//

// Compiler versions:
//
// MSVC++ 6.0  _MSC_VER == 1200 (Visual Studio 6.0)
// MSVC++ 7.0  _MSC_VER == 1300 (Visual Studio .NET 2002)
// MSVC++ 7.1  _MSC_VER == 1310 (Visual Studio .NET 2003)
// MSVC++ 8.0  _MSC_VER == 1400 (Visual Studio 2005)
// MSVC++ 9.0  _MSC_VER == 1500 (Visual Studio 2008)
// MSVC++ 10.0 _MSC_VER == 1600 (Visual Studio 2010)
// MSVC++ 11.0 _MSC_VER == 1700 (Visual Studio 2012)
// MSVC++ 12.0 _MSC_VER == 1800 (Visual Studio 2013)
// MSVC++ 14.0 _MSC_VER == 1900 (Visual Studio 2015)
// MSVC++ 14.1 _MSC_VER >= 1910 (Visual Studio 2017)

#if defined(_MSC_VER) && !defined(__clang__)
#define nssv_COMPILER_MSVC_VER (_MSC_VER)
#define nssv_COMPILER_MSVC_VERSION (_MSC_VER / 10 - 10 * (5 + (_MSC_VER < 1900)))
#else
#define nssv_COMPILER_MSVC_VER 0
#define nssv_COMPILER_MSVC_VERSION 0
#endif

#define nssv_COMPILER_VERSION(major, minor, patch) (10 * (10 * major + minor) + patch)

#if defined(__clang__)
#define nssv_COMPILER_CLANG_VERSION \
    nssv_COMPILER_VERSION(__clang_major__, __clang_minor__, __clang_patchlevel__)
#else
#define nssv_COMPILER_CLANG_VERSION 0
#endif

#if defined(__GNUC__) && !defined(__clang__)
#define nssv_COMPILER_GNUC_VERSION \
    nssv_COMPILER_VERSION(__GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__)
#else
#define nssv_COMPILER_GNUC_VERSION 0
#endif

// half-open range [lo..hi):
#define nssv_BETWEEN(v, lo, hi) ((lo) <= (v) && (v) < (hi))

// Presence of language and library features:

#ifdef _HAS_CPP0X
#define nssv_HAS_CPP0X _HAS_CPP0X
#else
#define nssv_HAS_CPP0X 0
#endif

// Unless defined otherwise below, consider VC14 as C++11 for variant-lite:

#if nssv_COMPILER_MSVC_VER >= 1900
#undef nssv_CPP11_OR_GREATER
#define nssv_CPP11_OR_GREATER 1
#endif

#define nssv_CPP11_90 (nssv_CPP11_OR_GREATER_ || nssv_COMPILER_MSVC_VER >= 1500)
#define nssv_CPP11_100 (nssv_CPP11_OR_GREATER_ || nssv_COMPILER_MSVC_VER >= 1600)
#define nssv_CPP11_110 (nssv_CPP11_OR_GREATER_ || nssv_COMPILER_MSVC_VER >= 1700)
#define nssv_CPP11_120 (nssv_CPP11_OR_GREATER_ || nssv_COMPILER_MSVC_VER >= 1800)
#define nssv_CPP11_140 (nssv_CPP11_OR_GREATER_ || nssv_COMPILER_MSVC_VER >= 1900)
#define nssv_CPP11_141 (nssv_CPP11_OR_GREATER_ || nssv_COMPILER_MSVC_VER >= 1910)

#define nssv_CPP14_000 (nssv_CPP14_OR_GREATER)
#define nssv_CPP17_000 (nssv_CPP17_OR_GREATER)

// Presence of C++11 language features:

#define nssv_HAVE_CONSTEXPR_11 nssv_CPP11_140
#define nssv_HAVE_EXPLICIT_CONVERSION nssv_CPP11_140
#define nssv_HAVE_INLINE_NAMESPACE nssv_CPP11_140
#define nssv_HAVE_NOEXCEPT nssv_CPP11_140
#define nssv_HAVE_NULLPTR nssv_CPP11_100
#define nssv_HAVE_REF_QUALIFIER nssv_CPP11_140
#define nssv_HAVE_UNICODE_LITERALS nssv_CPP11_140
#define nssv_HAVE_USER_DEFINED_LITERALS nssv_CPP11_140
#define nssv_HAVE_WCHAR16_T nssv_CPP11_100
#define nssv_HAVE_WCHAR32_T nssv_CPP11_100

#if !((nssv_CPP11 && nssv_COMPILER_CLANG_VERSION) || \
      nssv_BETWEEN(nssv_COMPILER_CLANG_VERSION, 300, 400))
#define nssv_HAVE_STD_DEFINED_LITERALS nssv_CPP11_140
#endif

// Presence of C++14 language features:

#define nssv_HAVE_CONSTEXPR_14 nssv_CPP14_000

// Presence of C++17 language features:

#define nssv_HAVE_NODISCARD nssv_CPP17_000

// Presence of C++ library features:

#define nssv_HAVE_STD_HASH nssv_CPP11_120

// C++ feature usage:

#if nssv_HAVE_CONSTEXPR_11
#define nssv_constexpr constexpr
#else
#define nssv_constexpr /*constexpr*/
#endif

#if nssv_HAVE_CONSTEXPR_14
#define nssv_constexpr14 constexpr
#else
#define nssv_constexpr14 /*constexpr*/
#endif

#if nssv_HAVE_EXPLICIT_CONVERSION
#define nssv_explicit explicit
#else
#define nssv_explicit /*explicit*/
#endif

#if nssv_HAVE_INLINE_NAMESPACE
#define nssv_inline_ns inline
#else
#define nssv_inline_ns /*inline*/
#endif

#if nssv_HAVE_NOEXCEPT
#define nssv_noexcept noexcept
#else
#define nssv_noexcept /*noexcept*/
#endif

//#if nssv_HAVE_REF_QUALIFIER
//# define nssv_ref_qual  &
//# define nssv_refref_qual  &&
//#else
//# define nssv_ref_qual  /*&*/
//# define nssv_refref_qual  /*&&*/
//#endif

#if nssv_HAVE_NULLPTR
#define nssv_nullptr nullptr
#else
#define nssv_nullptr NULL
#endif

#if nssv_HAVE_NODISCARD
#define nssv_nodiscard [[nodiscard]]
#else
#define nssv_nodiscard /*[[nodiscard]]*/
#endif

// Additional includes:

#include <algorithm>
#include <cassert>
#include <iterator>
#include <limits>
#include <ostream>
#include <string> // std::char_traits<>

#if !nssv_CONFIG_NO_EXCEPTIONS
#include <stdexcept>
#endif

#if nssv_CPP11_OR_GREATER
#include <type_traits>
#endif

// Clang, GNUC, MSVC warning suppression macros:

#if defined(__clang__)
#pragma clang diagnostic ignored "-Wreserved-user-defined-literal"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wuser-defined-literals"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wliteral-suffix"
#endif // __clang__

#if nssv_COMPILER_MSVC_VERSION >= 140
#define nssv_SUPPRESS_MSGSL_WARNING(expr) [[gsl::suppress(expr)]]
#define nssv_SUPPRESS_MSVC_WARNING(code, descr) __pragma(warning(suppress : code))
#define nssv_DISABLE_MSVC_WARNINGS(codes) __pragma(warning(push)) __pragma(warning(disable : codes))
#else
#define nssv_SUPPRESS_MSGSL_WARNING(expr)
#define nssv_SUPPRESS_MSVC_WARNING(code, descr)
#define nssv_DISABLE_MSVC_WARNINGS(codes)
#endif

#if defined(__clang__)
#define nssv_RESTORE_WARNINGS() _Pragma("clang diagnostic pop")
#elif defined(__GNUC__)
#define nssv_RESTORE_WARNINGS() _Pragma("GCC diagnostic pop")
#elif nssv_COMPILER_MSVC_VERSION >= 140
#define nssv_RESTORE_WARNINGS() __pragma(warning(pop))
#else
#define nssv_RESTORE_WARNINGS()
#endif

// Suppress the following MSVC (GSL) warnings:
// - C4455, non-gsl   : 'operator ""sv': literal suffix identifiers that do not
//                      start with an underscore are reserved
// - C26472, gsl::t.1 : don't use a static_cast for arithmetic conversions;
//                      use brace initialization, gsl::narrow_cast or gsl::narow
// - C26481: gsl::b.1 : don't use pointer arithmetic. Use span instead

nssv_DISABLE_MSVC_WARNINGS(4455 26481 26472)
        //nssv_DISABLE_CLANG_WARNINGS( "-Wuser-defined-literals" )
        //nssv_DISABLE_GNUC_WARNINGS( -Wliteral-suffix )

        namespace nonstd {
    namespace sv_lite {

    template <class CharT, class Traits = std::char_traits<CharT>>
    class basic_string_view;

    //
    // basic_string_view:
    //

    template <class CharT, class Traits /* = std::char_traits<CharT> */
              >
    class basic_string_view {
    public:
        // Member types:

        typedef Traits traits_type;
        typedef CharT value_type;

        typedef CharT* pointer;
        typedef CharT const* const_pointer;
        typedef CharT& reference;
        typedef CharT const& const_reference;

        typedef const_pointer iterator;
        typedef const_pointer const_iterator;
        typedef std::reverse_iterator<const_iterator> reverse_iterator;
        typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

        typedef std::size_t size_type;
        typedef std::ptrdiff_t difference_type;

        // 24.4.2.1 Construction and assignment:

        nssv_constexpr basic_string_view() nssv_noexcept : data_(nssv_nullptr), size_(0) {}

#if nssv_CPP11_OR_GREATER
        nssv_constexpr basic_string_view(basic_string_view const& other) nssv_noexcept = default;
#else
        nssv_constexpr basic_string_view(basic_string_view const& other) nssv_noexcept
                : data_(other.data_),
                  size_(other.size_) {}
#endif

        nssv_constexpr basic_string_view(CharT const* s, size_type count)
                : data_(s), size_(count) {}

        nssv_constexpr basic_string_view(CharT const* s) : data_(s), size_(Traits::length(s)) {}

        // Assignment:

#if nssv_CPP11_OR_GREATER
        nssv_constexpr14 basic_string_view& operator=(basic_string_view const& other)
                nssv_noexcept = default;
#else
        nssv_constexpr14 basic_string_view& operator=(basic_string_view const& other)
                nssv_noexcept {
            data_ = other.data_;
            size_ = other.size_;
            return *this;
        }
#endif

        // 24.4.2.2 Iterator support:

        nssv_constexpr const_iterator begin() const nssv_noexcept { return data_; }
        nssv_constexpr const_iterator end() const nssv_noexcept { return data_ + size_; }

        nssv_constexpr const_iterator cbegin() const nssv_noexcept { return begin(); }
        nssv_constexpr const_iterator cend() const nssv_noexcept { return end(); }

        nssv_constexpr const_reverse_iterator rbegin() const nssv_noexcept {
            return const_reverse_iterator(end());
        }
        nssv_constexpr const_reverse_iterator rend() const nssv_noexcept {
            return const_reverse_iterator(begin());
        }

        nssv_constexpr const_reverse_iterator crbegin() const nssv_noexcept { return rbegin(); }
        nssv_constexpr const_reverse_iterator crend() const nssv_noexcept { return rend(); }

        // 24.4.2.3 Capacity:

        nssv_constexpr size_type size() const nssv_noexcept { return size_; }
        nssv_constexpr size_type length() const nssv_noexcept { return size_; }
        nssv_constexpr size_type max_size() const nssv_noexcept {
            return (std::numeric_limits<size_type>::max)();
        }

        // since C++20
        nssv_nodiscard nssv_constexpr bool empty() const nssv_noexcept { return 0 == size_; }

        // 24.4.2.4 Element access:

        nssv_constexpr const_reference operator[](size_type pos) const { return data_at(pos); }

        nssv_constexpr14 const_reference at(size_type pos) const {
#if nssv_CONFIG_NO_EXCEPTIONS
            assert(pos < size());
#else
            if (pos >= size()) {
                throw std::out_of_range("nonst::string_view::at()");
            }
#endif
            return data_at(pos);
        }

        nssv_constexpr const_reference front() const { return data_at(0); }
        nssv_constexpr const_reference back() const { return data_at(size() - 1); }

        nssv_constexpr const_pointer data() const nssv_noexcept { return data_; }

        // 24.4.2.5 Modifiers:

        nssv_constexpr14 void remove_prefix(size_type n) {
            assert(n <= size());
            data_ += n;
            size_ -= n;
        }

        nssv_constexpr14 void remove_suffix(size_type n) {
            assert(n <= size());
            size_ -= n;
        }

        nssv_constexpr14 void swap(basic_string_view& other) nssv_noexcept {
            using std::swap;
            swap(data_, other.data_);
            swap(size_, other.size_);
        }

        // 24.4.2.6 String operations:

        size_type copy(CharT* dest, size_type n, size_type pos = 0) const {
#if nssv_CONFIG_NO_EXCEPTIONS
            assert(pos <= size());
#else
            if (pos > size()) {
                throw std::out_of_range("nonst::string_view::copy()");
            }
#endif
            const size_type rlen = (std::min)(n, size() - pos);

            (void)Traits::copy(dest, data() + pos, rlen);

            return rlen;
        }

        nssv_constexpr14 basic_string_view substr(size_type pos = 0, size_type n = npos) const {
#if nssv_CONFIG_NO_EXCEPTIONS
            assert(pos <= size());
#else
            if (pos > size()) {
                throw std::out_of_range("nonst::string_view::substr()");
            }
#endif
            return basic_string_view(data() + pos, (std::min)(n, size() - pos));
        }

        // compare(), 6x:

        nssv_constexpr14 int compare(basic_string_view other) const nssv_noexcept // (1)
        {
            if (const int result =
                        Traits::compare(data(), other.data(), (std::min)(size(), other.size())))
                return result;

            return size() == other.size() ? 0 : size() < other.size() ? -1 : 1;
        }

        nssv_constexpr int compare(size_type pos1, size_type n1,
                                   basic_string_view other) const // (2)
        {
            return substr(pos1, n1).compare(other);
        }

        nssv_constexpr int compare(size_type pos1, size_type n1, basic_string_view other,
                                   size_type pos2, size_type n2) const // (3)
        {
            return substr(pos1, n1).compare(other.substr(pos2, n2));
        }

        nssv_constexpr int compare(CharT const* s) const // (4)
        {
            return compare(basic_string_view(s));
        }

        nssv_constexpr int compare(size_type pos1, size_type n1, CharT const* s) const // (5)
        {
            return substr(pos1, n1).compare(basic_string_view(s));
        }

        nssv_constexpr int compare(size_type pos1, size_type n1, CharT const* s,
                                   size_type n2) const // (6)
        {
            return substr(pos1, n1).compare(basic_string_view(s, n2));
        }

        // 24.4.2.7 Searching:

        // starts_with(), 3x, since C++20:

        nssv_constexpr bool starts_with(basic_string_view v) const nssv_noexcept // (1)
        {
            return size() >= v.size() && compare(0, v.size(), v) == 0;
        }

        nssv_constexpr bool starts_with(CharT c) const nssv_noexcept // (2)
        {
            return starts_with(basic_string_view(&c, 1));
        }

        nssv_constexpr bool starts_with(CharT const* s) const // (3)
        {
            return starts_with(basic_string_view(s));
        }

        // ends_with(), 3x, since C++20:

        nssv_constexpr bool ends_with(basic_string_view v) const nssv_noexcept // (1)
        {
            return size() >= v.size() && compare(size() - v.size(), npos, v) == 0;
        }

        nssv_constexpr bool ends_with(CharT c) const nssv_noexcept // (2)
        {
            return ends_with(basic_string_view(&c, 1));
        }

        nssv_constexpr bool ends_with(CharT const* s) const // (3)
        {
            return ends_with(basic_string_view(s));
        }

        // find(), 4x:

        nssv_constexpr14 size_type find(basic_string_view v,
                                        size_type pos = 0) const nssv_noexcept // (1)
        {
            return assert(v.size() == 0 || v.data() != nssv_nullptr),
                   pos >= size() ? npos
                                 : to_pos(std::search(cbegin() + pos, cend(), v.cbegin(), v.cend(),
                                                      Traits::eq));
        }

        nssv_constexpr14 size_type find(CharT c, size_type pos = 0) const nssv_noexcept // (2)
        {
            return find(basic_string_view(&c, 1), pos);
        }

        nssv_constexpr14 size_type find(CharT const* s, size_type pos, size_type n) const // (3)
        {
            return find(basic_string_view(s, n), pos);
        }

        nssv_constexpr14 size_type find(CharT const* s, size_type pos = 0) const // (4)
        {
            return find(basic_string_view(s), pos);
        }

        // rfind(), 4x:

        nssv_constexpr14 size_type rfind(basic_string_view v,
                                         size_type pos = npos) const nssv_noexcept // (1)
        {
            if (size() < v.size()) return npos;

            if (v.empty()) return (std::min)(size(), pos);

            const_iterator last = cbegin() + (std::min)(size() - v.size(), pos) + v.size();
            const_iterator result = std::find_end(cbegin(), last, v.cbegin(), v.cend(), Traits::eq);

            return result != last ? size_type(result - cbegin()) : npos;
        }

        nssv_constexpr14 size_type rfind(CharT c, size_type pos = npos) const nssv_noexcept // (2)
        {
            return rfind(basic_string_view(&c, 1), pos);
        }

        nssv_constexpr14 size_type rfind(CharT const* s, size_type pos, size_type n) const // (3)
        {
            return rfind(basic_string_view(s, n), pos);
        }

        nssv_constexpr14 size_type rfind(CharT const* s, size_type pos = npos) const // (4)
        {
            return rfind(basic_string_view(s), pos);
        }

        // find_first_of(), 4x:

        nssv_constexpr size_type find_first_of(basic_string_view v,
                                               size_type pos = 0) const nssv_noexcept // (1)
        {
            return pos >= size() ? npos
                                 : to_pos(std::find_first_of(cbegin() + pos, cend(), v.cbegin(),
                                                             v.cend(), Traits::eq));
        }

        nssv_constexpr size_type find_first_of(CharT c,
                                               size_type pos = 0) const nssv_noexcept // (2)
        {
            return find_first_of(basic_string_view(&c, 1), pos);
        }

        nssv_constexpr size_type find_first_of(CharT const* s, size_type pos,
                                               size_type n) const // (3)
        {
            return find_first_of(basic_string_view(s, n), pos);
        }

        nssv_constexpr size_type find_first_of(CharT const* s, size_type pos = 0) const // (4)
        {
            return find_first_of(basic_string_view(s), pos);
        }

        // find_last_of(), 4x:

        nssv_constexpr size_type find_last_of(basic_string_view v,
                                              size_type pos = npos) const nssv_noexcept // (1)
        {
            return empty() ? npos
                   : pos >= size()
                           ? find_last_of(v, size() - 1)
                           : to_pos(std::find_first_of(const_reverse_iterator(cbegin() + pos + 1),
                                                       crend(), v.cbegin(), v.cend(), Traits::eq));
        }

        nssv_constexpr size_type find_last_of(CharT c,
                                              size_type pos = npos) const nssv_noexcept // (2)
        {
            return find_last_of(basic_string_view(&c, 1), pos);
        }

        nssv_constexpr size_type find_last_of(CharT const* s, size_type pos,
                                              size_type count) const // (3)
        {
            return find_last_of(basic_string_view(s, count), pos);
        }

        nssv_constexpr size_type find_last_of(CharT const* s, size_type pos = npos) const // (4)
        {
            return find_last_of(basic_string_view(s), pos);
        }

        // find_first_not_of(), 4x:

        nssv_constexpr size_type find_first_not_of(basic_string_view v,
                                                   size_type pos = 0) const nssv_noexcept // (1)
        {
            return pos >= size() ? npos
                                 : to_pos(std::find_if(cbegin() + pos, cend(), not_in_view(v)));
        }

        nssv_constexpr size_type find_first_not_of(CharT c,
                                                   size_type pos = 0) const nssv_noexcept // (2)
        {
            return find_first_not_of(basic_string_view(&c, 1), pos);
        }

        nssv_constexpr size_type find_first_not_of(CharT const* s, size_type pos,
                                                   size_type count) const // (3)
        {
            return find_first_not_of(basic_string_view(s, count), pos);
        }

        nssv_constexpr size_type find_first_not_of(CharT const* s, size_type pos = 0) const // (4)
        {
            return find_first_not_of(basic_string_view(s), pos);
        }

        // find_last_not_of(), 4x:

        nssv_constexpr size_type find_last_not_of(basic_string_view v,
                                                  size_type pos = npos) const nssv_noexcept // (1)
        {
            return empty()         ? npos
                   : pos >= size() ? find_last_not_of(v, size() - 1)
                                   : to_pos(std::find_if(const_reverse_iterator(cbegin() + pos + 1),
                                                         crend(), not_in_view(v)));
        }

        nssv_constexpr size_type find_last_not_of(CharT c,
                                                  size_type pos = npos) const nssv_noexcept // (2)
        {
            return find_last_not_of(basic_string_view(&c, 1), pos);
        }

        nssv_constexpr size_type find_last_not_of(CharT const* s, size_type pos,
                                                  size_type count) const // (3)
        {
            return find_last_not_of(basic_string_view(s, count), pos);
        }

        nssv_constexpr size_type find_last_not_of(CharT const* s, size_type pos = npos) const // (4)
        {
            return find_last_not_of(basic_string_view(s), pos);
        }

        // Constants:

#if nssv_CPP17_OR_GREATER
        static nssv_constexpr size_type npos = size_type(-1);
#elif nssv_CPP11_OR_GREATER
        enum : size_type { npos = size_type(-1) };
#else
        enum { npos = size_type(-1) };
#endif

    private:
        struct not_in_view {
            const basic_string_view v;

            nssv_constexpr not_in_view(basic_string_view v) : v(v) {}

            nssv_constexpr bool operator()(CharT c) const { return npos == v.find_first_of(c); }
        };

        nssv_constexpr size_type to_pos(const_iterator it) const {
            return it == cend() ? npos : size_type(it - cbegin());
        }

        nssv_constexpr size_type to_pos(const_reverse_iterator it) const {
            return it == crend() ? npos : size_type(crend() - it - 1);
        }

        nssv_constexpr const_reference data_at(size_type pos) const {
#if nssv_BETWEEN(nssv_COMPILER_GNUC_VERSION, 1, 500)
            return data_[pos];
#else
            return assert(pos < size()), data_[pos];
#endif
        }

    private:
        const_pointer data_;
        size_type size_;

    public:
#if nssv_CONFIG_CONVERSION_STD_STRING_CLASS_METHODS

        template <class Allocator>
        basic_string_view(std::basic_string<CharT, Traits, Allocator> const& s) nssv_noexcept
                : data_(s.data()),
                  size_(s.size()) {}

#if nssv_HAVE_EXPLICIT_CONVERSION

        template <class Allocator>
        explicit operator std::basic_string<CharT, Traits, Allocator>() const {
            return to_string(Allocator());
        }

#endif // nssv_HAVE_EXPLICIT_CONVERSION

#if nssv_CPP11_OR_GREATER

        template <class Allocator = std::allocator<CharT>>
        std::basic_string<CharT, Traits, Allocator> to_string(
                Allocator const& a = Allocator()) const {
            return std::basic_string<CharT, Traits, Allocator>(begin(), end(), a);
        }

#else

        std::basic_string<CharT, Traits> to_string() const {
            return std::basic_string<CharT, Traits>(begin(), end());
        }

        template <class Allocator>
        std::basic_string<CharT, Traits, Allocator> to_string(Allocator const& a) const {
            return std::basic_string<CharT, Traits, Allocator>(begin(), end(), a);
        }

#endif // nssv_CPP11_OR_GREATER

#endif // nssv_CONFIG_CONVERSION_STD_STRING_CLASS_METHODS
    };

    //
    // Non-member functions:
    //

    // 24.4.3 Non-member comparison functions:
    // lexicographically compare two string views (function template):

    template <class CharT, class Traits>
    nssv_constexpr bool operator==(basic_string_view<CharT, Traits> lhs,
                                   basic_string_view<CharT, Traits> rhs) nssv_noexcept {
        return lhs.compare(rhs) == 0;
    }

    template <class CharT, class Traits>
    nssv_constexpr bool operator!=(basic_string_view<CharT, Traits> lhs,
                                   basic_string_view<CharT, Traits> rhs) nssv_noexcept {
        return lhs.compare(rhs) != 0;
    }

    template <class CharT, class Traits>
    nssv_constexpr bool operator<(basic_string_view<CharT, Traits> lhs,
                                  basic_string_view<CharT, Traits> rhs) nssv_noexcept {
        return lhs.compare(rhs) < 0;
    }

    template <class CharT, class Traits>
    nssv_constexpr bool operator<=(basic_string_view<CharT, Traits> lhs,
                                   basic_string_view<CharT, Traits> rhs) nssv_noexcept {
        return lhs.compare(rhs) <= 0;
    }

    template <class CharT, class Traits>
    nssv_constexpr bool operator>(basic_string_view<CharT, Traits> lhs,
                                  basic_string_view<CharT, Traits> rhs) nssv_noexcept {
        return lhs.compare(rhs) > 0;
    }

    template <class CharT, class Traits>
    nssv_constexpr bool operator>=(basic_string_view<CharT, Traits> lhs,
                                   basic_string_view<CharT, Traits> rhs) nssv_noexcept {
        return lhs.compare(rhs) >= 0;
    }

    // Let S be basic_string_view<CharT, Traits>, and sv be an instance of S.
    // Implementations shall provide sufficient additional overloads marked
    // constexpr and noexcept so that an object t with an implicit conversion
    // to S can be compared according to Table 67.

#if nssv_CPP11_OR_GREATER && !nssv_BETWEEN(nssv_COMPILER_MSVC_VERSION, 100, 141)

#define nssv_BASIC_STRING_VIEW_I(T, U) typename std::decay<basic_string_view<T, U>>::type

#if nssv_BETWEEN(nssv_COMPILER_MSVC_VERSION, 140, 150)
#define nssv_MSVC_ORDER(x) , int = x
#else
#define nssv_MSVC_ORDER(x) /*, int=x*/
#endif

    // ==

    template <class CharT, class Traits nssv_MSVC_ORDER(1)>
    nssv_constexpr bool operator==(basic_string_view<CharT, Traits> lhs,
                                   nssv_BASIC_STRING_VIEW_I(CharT, Traits) rhs) nssv_noexcept {
        return lhs.compare(rhs) == 0;
    }

    template <class CharT, class Traits nssv_MSVC_ORDER(2)>
    nssv_constexpr bool operator==(nssv_BASIC_STRING_VIEW_I(CharT, Traits) lhs,
                                   basic_string_view<CharT, Traits> rhs) nssv_noexcept {
        return lhs.size() == rhs.size() && lhs.compare(rhs) == 0;
    }

    // !=

    template <class CharT, class Traits nssv_MSVC_ORDER(1)>
    nssv_constexpr bool operator!=(basic_string_view<CharT, Traits> lhs,
                                   nssv_BASIC_STRING_VIEW_I(CharT, Traits) rhs) nssv_noexcept {
        return lhs.size() != rhs.size() || lhs.compare(rhs) != 0;
    }

    template <class CharT, class Traits nssv_MSVC_ORDER(2)>
    nssv_constexpr bool operator!=(nssv_BASIC_STRING_VIEW_I(CharT, Traits) lhs,
                                   basic_string_view<CharT, Traits> rhs) nssv_noexcept {
        return lhs.compare(rhs) != 0;
    }

    // <

    template <class CharT, class Traits nssv_MSVC_ORDER(1)>
    nssv_constexpr bool operator<(basic_string_view<CharT, Traits> lhs,
                                  nssv_BASIC_STRING_VIEW_I(CharT, Traits) rhs) nssv_noexcept {
        return lhs.compare(rhs) < 0;
    }

    template <class CharT, class Traits nssv_MSVC_ORDER(2)>
    nssv_constexpr bool operator<(nssv_BASIC_STRING_VIEW_I(CharT, Traits) lhs,
                                  basic_string_view<CharT, Traits> rhs) nssv_noexcept {
        return lhs.compare(rhs) < 0;
    }

    // <=

    template <class CharT, class Traits nssv_MSVC_ORDER(1)>
    nssv_constexpr bool operator<=(basic_string_view<CharT, Traits> lhs,
                                   nssv_BASIC_STRING_VIEW_I(CharT, Traits) rhs) nssv_noexcept {
        return lhs.compare(rhs) <= 0;
    }

    template <class CharT, class Traits nssv_MSVC_ORDER(2)>
    nssv_constexpr bool operator<=(nssv_BASIC_STRING_VIEW_I(CharT, Traits) lhs,
                                   basic_string_view<CharT, Traits> rhs) nssv_noexcept {
        return lhs.compare(rhs) <= 0;
    }

    // >

    template <class CharT, class Traits nssv_MSVC_ORDER(1)>
    nssv_constexpr bool operator>(basic_string_view<CharT, Traits> lhs,
                                  nssv_BASIC_STRING_VIEW_I(CharT, Traits) rhs) nssv_noexcept {
        return lhs.compare(rhs) > 0;
    }

    template <class CharT, class Traits nssv_MSVC_ORDER(2)>
    nssv_constexpr bool operator>(nssv_BASIC_STRING_VIEW_I(CharT, Traits) lhs,
                                  basic_string_view<CharT, Traits> rhs) nssv_noexcept {
        return lhs.compare(rhs) > 0;
    }

    // >=

    template <class CharT, class Traits nssv_MSVC_ORDER(1)>
    nssv_constexpr bool operator>=(basic_string_view<CharT, Traits> lhs,
                                   nssv_BASIC_STRING_VIEW_I(CharT, Traits) rhs) nssv_noexcept {
        return lhs.compare(rhs) >= 0;
    }

    template <class CharT, class Traits nssv_MSVC_ORDER(2)>
    nssv_constexpr bool operator>=(nssv_BASIC_STRING_VIEW_I(CharT, Traits) lhs,
                                   basic_string_view<CharT, Traits> rhs) nssv_noexcept {
        return lhs.compare(rhs) >= 0;
    }

#undef nssv_MSVC_ORDER
#undef nssv_BASIC_STRING_VIEW_I

#endif // nssv_CPP11_OR_GREATER

    // 24.4.4 Inserters and extractors:

    namespace detail {

    template <class Stream>
    void write_padding(Stream& os, std::streamsize n) {
        for (std::streamsize i = 0; i < n; ++i) os.rdbuf()->sputc(os.fill());
    }

    template <class Stream, class View>
    Stream& write_to_stream(Stream& os, View const& sv) {
        typename Stream::sentry sentry(os);

        if (!os) return os;

        const std::streamsize length = static_cast<std::streamsize>(sv.length());

        // Whether, and how, to pad:
        const bool pad = (length < os.width());
        const bool left_pad =
                pad && (os.flags() & std::ios_base::adjustfield) == std::ios_base::right;

        if (left_pad) write_padding(os, os.width() - length);

        // Write span characters:
        os.rdbuf()->sputn(sv.begin(), length);

        if (pad && !left_pad) write_padding(os, os.width() - length);

        // Reset output stream width:
        os.width(0);

        return os;
    }

    } // namespace detail

    template <class CharT, class Traits>
    std::basic_ostream<CharT, Traits>& operator<<(std::basic_ostream<CharT, Traits>& os,
                                                  basic_string_view<CharT, Traits> sv) {
        return detail::write_to_stream(os, sv);
    }

    // Several typedefs for common character types are provided:

    typedef basic_string_view<char> string_view;
    typedef basic_string_view<wchar_t> wstring_view;
#if nssv_HAVE_WCHAR16_T
    typedef basic_string_view<char16_t> u16string_view;
    typedef basic_string_view<char32_t> u32string_view;
#endif

    } // namespace sv_lite
} // namespace nonstd::sv_lite

//
// 24.4.6 Suffix for basic_string_view literals:
//

#if nssv_HAVE_USER_DEFINED_LITERALS

namespace nonstd {
nssv_inline_ns namespace literals {
    nssv_inline_ns namespace string_view_literals {

#if nssv_CONFIG_STD_SV_OPERATOR && nssv_HAVE_STD_DEFINED_LITERALS

        nssv_constexpr nonstd::sv_lite::string_view operator"" sv(const char* str, size_t len)
                nssv_noexcept // (1)
        {
            return nonstd::sv_lite::string_view {str, len};
        }

        nssv_constexpr nonstd::sv_lite::u16string_view operator"" sv(
                const char16_t* str, size_t len) nssv_noexcept // (2)
        {
            return nonstd::sv_lite::u16string_view {str, len};
        }

        nssv_constexpr nonstd::sv_lite::u32string_view operator"" sv(
                const char32_t* str, size_t len) nssv_noexcept // (3)
        {
            return nonstd::sv_lite::u32string_view {str, len};
        }

        nssv_constexpr nonstd::sv_lite::wstring_view operator"" sv(const wchar_t* str, size_t len)
                nssv_noexcept // (4)
        {
            return nonstd::sv_lite::wstring_view {str, len};
        }

#endif // nssv_CONFIG_STD_SV_OPERATOR && nssv_HAVE_STD_DEFINED_LITERALS

#if nssv_CONFIG_USR_SV_OPERATOR

        nssv_constexpr nonstd::sv_lite::string_view operator"" _sv(const char* str, size_t len)
                nssv_noexcept // (1)
        {
            return nonstd::sv_lite::string_view {str, len};
        }

        nssv_constexpr nonstd::sv_lite::u16string_view operator"" _sv(
                const char16_t* str, size_t len) nssv_noexcept // (2)
        {
            return nonstd::sv_lite::u16string_view {str, len};
        }

        nssv_constexpr nonstd::sv_lite::u32string_view operator"" _sv(
                const char32_t* str, size_t len) nssv_noexcept // (3)
        {
            return nonstd::sv_lite::u32string_view {str, len};
        }

        nssv_constexpr nonstd::sv_lite::wstring_view operator"" _sv(const wchar_t* str, size_t len)
                nssv_noexcept // (4)
        {
            return nonstd::sv_lite::wstring_view {str, len};
        }

#endif // nssv_CONFIG_USR_SV_OPERATOR
    }
}
} // namespace nonstd

#endif

//
// Extensions for std::string:
//

#if nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS

namespace nonstd {
namespace sv_lite {

// Exclude MSVC 14 (19.00): it yields ambiguous to_string():

#if nssv_CPP11_OR_GREATER && nssv_COMPILER_MSVC_VERSION != 140

template <class CharT, class Traits, class Allocator = std::allocator<CharT>>
std::basic_string<CharT, Traits, Allocator> to_string(basic_string_view<CharT, Traits> v,
                                                      Allocator const& a = Allocator()) {
    return std::basic_string<CharT, Traits, Allocator>(v.begin(), v.end(), a);
}

#else

template <class CharT, class Traits>
std::basic_string<CharT, Traits> to_string(basic_string_view<CharT, Traits> v) {
    return std::basic_string<CharT, Traits>(v.begin(), v.end());
}

template <class CharT, class Traits, class Allocator>
std::basic_string<CharT, Traits, Allocator> to_string(basic_string_view<CharT, Traits> v,
                                                      Allocator const& a) {
    return std::basic_string<CharT, Traits, Allocator>(v.begin(), v.end(), a);
}

#endif // nssv_CPP11_OR_GREATER

template <class CharT, class Traits, class Allocator>
basic_string_view<CharT, Traits> to_string_view(
        std::basic_string<CharT, Traits, Allocator> const& s) {
    return basic_string_view<CharT, Traits>(s.data(), s.size());
}

} // namespace sv_lite
} // namespace nonstd

#endif // nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS

//
// make types and algorithms available in namespace nonstd:
//

namespace nonstd {

using sv_lite::basic_string_view;
using sv_lite::string_view;
using sv_lite::wstring_view;

#if nssv_HAVE_WCHAR16_T
using sv_lite::u16string_view;
#endif
#if nssv_HAVE_WCHAR32_T
using sv_lite::u32string_view;
#endif

// literal "sv"

using sv_lite::operator==;
using sv_lite::operator!=;
using sv_lite::operator<;
using sv_lite::operator<=;
using sv_lite::operator>;
using sv_lite::operator>=;

using sv_lite::operator<<;

#if nssv_CONFIG_CONVERSION_STD_STRING_FREE_FUNCTIONS
using sv_lite::to_string;
using sv_lite::to_string_view;
#endif

} // namespace nonstd

// 24.4.5 Hash support (C++11):

// Note: The hash value of a string view object is equal to the hash value of
// the corresponding string object.

#if nssv_HAVE_STD_HASH

#include <functional>

namespace std {

template <>
struct hash<nonstd::string_view> {
public:
    std::size_t operator()(nonstd::string_view v) const nssv_noexcept {
        return std::hash<std::string>()(std::string(v.data(), v.size()));
    }
};

template <>
struct hash<nonstd::wstring_view> {
public:
    std::size_t operator()(nonstd::wstring_view v) const nssv_noexcept {
        return std::hash<std::wstring>()(std::wstring(v.data(), v.size()));
    }
};

template <>
struct hash<nonstd::u16string_view> {
public:
    std::size_t operator()(nonstd::u16string_view v) const nssv_noexcept {
        return std::hash<std::u16string>()(std::u16string(v.data(), v.size()));
    }
};

template <>
struct hash<nonstd::u32string_view> {
public:
    std::size_t operator()(nonstd::u32string_view v) const nssv_noexcept {
        return std::hash<std::u32string>()(std::u32string(v.data(), v.size()));
    }
};

} // namespace std

#endif // nssv_HAVE_STD_HASH

nssv_RESTORE_WARNINGS()

#endif // nssv_HAVE_STD_STRING_VIEW
#endif // NONSTD_SV_LITE_H_INCLUDED

// If there is another version of Hedley, then the newer one
// takes precedence.
// See: https://github.com/nemequ/hedley
/* Hedley - https://nemequ.github.io/hedley
 * Created by Evan Nemerson <evan@nemerson.com>
 *
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to
 * the public domain worldwide. This software is distributed without
 * any warranty.
 *
 * For details, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 * SPDX-License-Identifier: CC0-1.0
 */

#if !defined(HEDLEY_VERSION) || (HEDLEY_VERSION < 9)
#if defined(HEDLEY_VERSION)
#undef HEDLEY_VERSION
#endif
#define HEDLEY_VERSION 9

#if defined(HEDLEY_STRINGIFY_EX)
#undef HEDLEY_STRINGIFY_EX
#endif
#define HEDLEY_STRINGIFY_EX(x) #x

#if defined(HEDLEY_STRINGIFY)
#undef HEDLEY_STRINGIFY
#endif
#define HEDLEY_STRINGIFY(x) HEDLEY_STRINGIFY_EX(x)

#if defined(HEDLEY_CONCAT_EX)
#undef HEDLEY_CONCAT_EX
#endif
#define HEDLEY_CONCAT_EX(a, b) a##b

#if defined(HEDLEY_CONCAT)
#undef HEDLEY_CONCAT
#endif
#define HEDLEY_CONCAT(a, b) HEDLEY_CONCAT_EX(a, b)

#if defined(HEDLEY_VERSION_ENCODE)
#undef HEDLEY_VERSION_ENCODE
#endif
#define HEDLEY_VERSION_ENCODE(major, minor, revision) \
    (((major)*1000000) + ((minor)*1000) + (revision))

#if defined(HEDLEY_VERSION_DECODE_MAJOR)
#undef HEDLEY_VERSION_DECODE_MAJOR
#endif
#define HEDLEY_VERSION_DECODE_MAJOR(version) ((version) / 1000000)

#if defined(HEDLEY_VERSION_DECODE_MINOR)
#undef HEDLEY_VERSION_DECODE_MINOR
#endif
#define HEDLEY_VERSION_DECODE_MINOR(version) (((version) % 1000000) / 1000)

#if defined(HEDLEY_VERSION_DECODE_REVISION)
#undef HEDLEY_VERSION_DECODE_REVISION
#endif
#define HEDLEY_VERSION_DECODE_REVISION(version) ((version) % 1000)

#if defined(HEDLEY_GNUC_VERSION)
#undef HEDLEY_GNUC_VERSION
#endif
#if defined(__GNUC__) && defined(__GNUC_PATCHLEVEL__)
#define HEDLEY_GNUC_VERSION HEDLEY_VERSION_ENCODE(__GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__)
#elif defined(__GNUC__)
#define HEDLEY_GNUC_VERSION HEDLEY_VERSION_ENCODE(__GNUC__, __GNUC_MINOR__, 0)
#endif

#if defined(HEDLEY_GNUC_VERSION_CHECK)
#undef HEDLEY_GNUC_VERSION_CHECK
#endif
#if defined(HEDLEY_GNUC_VERSION)
#define HEDLEY_GNUC_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_GNUC_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_GNUC_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_MSVC_VERSION)
#undef HEDLEY_MSVC_VERSION
#endif
#if defined(_MSC_FULL_VER) && (_MSC_FULL_VER >= 140000000)
#define HEDLEY_MSVC_VERSION                                                              \
    HEDLEY_VERSION_ENCODE(_MSC_FULL_VER / 10000000, (_MSC_FULL_VER % 10000000) / 100000, \
                          (_MSC_FULL_VER % 100000) / 100)
#elif defined(_MSC_FULL_VER)
#define HEDLEY_MSVC_VERSION                                                           \
    HEDLEY_VERSION_ENCODE(_MSC_FULL_VER / 1000000, (_MSC_FULL_VER % 1000000) / 10000, \
                          (_MSC_FULL_VER % 10000) / 10)
#elif defined(_MSC_VER)
#define HEDLEY_MSVC_VERSION HEDLEY_VERSION_ENCODE(_MSC_VER / 100, _MSC_VER % 100, 0)
#endif

#if defined(HEDLEY_MSVC_VERSION_CHECK)
#undef HEDLEY_MSVC_VERSION_CHECK
#endif
#if !defined(_MSC_VER)
#define HEDLEY_MSVC_VERSION_CHECK(major, minor, patch) (0)
#elif defined(_MSC_VER) && (_MSC_VER >= 1400)
#define HEDLEY_MSVC_VERSION_CHECK(major, minor, patch) \
    (_MSC_FULL_VER >= ((major * 10000000) + (minor * 100000) + (patch)))
#elif defined(_MSC_VER) && (_MSC_VER >= 1200)
#define HEDLEY_MSVC_VERSION_CHECK(major, minor, patch) \
    (_MSC_FULL_VER >= ((major * 1000000) + (minor * 10000) + (patch)))
#else
#define HEDLEY_MSVC_VERSION_CHECK(major, minor, patch) (_MSC_VER >= ((major * 100) + (minor)))
#endif

#if defined(HEDLEY_INTEL_VERSION)
#undef HEDLEY_INTEL_VERSION
#endif
#if defined(__INTEL_COMPILER) && defined(__INTEL_COMPILER_UPDATE)
#define HEDLEY_INTEL_VERSION \
    HEDLEY_VERSION_ENCODE(__INTEL_COMPILER / 100, __INTEL_COMPILER % 100, __INTEL_COMPILER_UPDATE)
#elif defined(__INTEL_COMPILER)
#define HEDLEY_INTEL_VERSION \
    HEDLEY_VERSION_ENCODE(__INTEL_COMPILER / 100, __INTEL_COMPILER % 100, 0)
#endif

#if defined(HEDLEY_INTEL_VERSION_CHECK)
#undef HEDLEY_INTEL_VERSION_CHECK
#endif
#if defined(HEDLEY_INTEL_VERSION)
#define HEDLEY_INTEL_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_INTEL_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_INTEL_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_PGI_VERSION)
#undef HEDLEY_PGI_VERSION
#endif
#if defined(__PGI) && defined(__PGIC__) && defined(__PGIC_MINOR__) && defined(__PGIC_PATCHLEVEL__)
#define HEDLEY_PGI_VERSION HEDLEY_VERSION_ENCODE(__PGIC__, __PGIC_MINOR__, __PGIC_PATCHLEVEL__)
#endif

#if defined(HEDLEY_PGI_VERSION_CHECK)
#undef HEDLEY_PGI_VERSION_CHECK
#endif
#if defined(HEDLEY_PGI_VERSION)
#define HEDLEY_PGI_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_PGI_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_PGI_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_SUNPRO_VERSION)
#undef HEDLEY_SUNPRO_VERSION
#endif
#if defined(__SUNPRO_C) && (__SUNPRO_C > 0x1000)
#define HEDLEY_SUNPRO_VERSION                                                             \
    HEDLEY_VERSION_ENCODE((((__SUNPRO_C >> 16) & 0xf) * 10) + ((__SUNPRO_C >> 12) & 0xf), \
                          (((__SUNPRO_C >> 8) & 0xf) * 10) + ((__SUNPRO_C >> 4) & 0xf),   \
                          (__SUNPRO_C & 0xf) * 10)
#elif defined(__SUNPRO_C)
#define HEDLEY_SUNPRO_VERSION \
    HEDLEY_VERSION_ENCODE((__SUNPRO_C >> 8) & 0xf, (__SUNPRO_C >> 4) & 0xf, (__SUNPRO_C)&0xf)
#elif defined(__SUNPRO_CC) && (__SUNPRO_CC > 0x1000)
#define HEDLEY_SUNPRO_VERSION                                                               \
    HEDLEY_VERSION_ENCODE((((__SUNPRO_CC >> 16) & 0xf) * 10) + ((__SUNPRO_CC >> 12) & 0xf), \
                          (((__SUNPRO_CC >> 8) & 0xf) * 10) + ((__SUNPRO_CC >> 4) & 0xf),   \
                          (__SUNPRO_CC & 0xf) * 10)
#elif defined(__SUNPRO_CC)
#define HEDLEY_SUNPRO_VERSION \
    HEDLEY_VERSION_ENCODE((__SUNPRO_CC >> 8) & 0xf, (__SUNPRO_CC >> 4) & 0xf, (__SUNPRO_CC)&0xf)
#endif

#if defined(HEDLEY_SUNPRO_VERSION_CHECK)
#undef HEDLEY_SUNPRO_VERSION_CHECK
#endif
#if defined(HEDLEY_SUNPRO_VERSION)
#define HEDLEY_SUNPRO_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_SUNPRO_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_SUNPRO_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_EMSCRIPTEN_VERSION)
#undef HEDLEY_EMSCRIPTEN_VERSION
#endif
#if defined(__EMSCRIPTEN__)
#define HEDLEY_EMSCRIPTEN_VERSION \
    HEDLEY_VERSION_ENCODE(__EMSCRIPTEN_major__, __EMSCRIPTEN_minor__, __EMSCRIPTEN_tiny__)
#endif

#if defined(HEDLEY_EMSCRIPTEN_VERSION_CHECK)
#undef HEDLEY_EMSCRIPTEN_VERSION_CHECK
#endif
#if defined(HEDLEY_EMSCRIPTEN_VERSION)
#define HEDLEY_EMSCRIPTEN_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_EMSCRIPTEN_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_EMSCRIPTEN_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_ARM_VERSION)
#undef HEDLEY_ARM_VERSION
#endif
#if defined(__CC_ARM) && defined(__ARMCOMPILER_VERSION)
#define HEDLEY_ARM_VERSION                                           \
    HEDLEY_VERSION_ENCODE(__ARMCOMPILER_VERSION / 1000000,           \
                          (__ARMCOMPILER_VERSION % 1000000) / 10000, \
                          (__ARMCOMPILER_VERSION % 10000) / 100)
#elif defined(__CC_ARM) && defined(__ARMCC_VERSION)
#define HEDLEY_ARM_VERSION                                                                \
    HEDLEY_VERSION_ENCODE(__ARMCC_VERSION / 1000000, (__ARMCC_VERSION % 1000000) / 10000, \
                          (__ARMCC_VERSION % 10000) / 100)
#endif

#if defined(HEDLEY_ARM_VERSION_CHECK)
#undef HEDLEY_ARM_VERSION_CHECK
#endif
#if defined(HEDLEY_ARM_VERSION)
#define HEDLEY_ARM_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_ARM_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_ARM_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_IBM_VERSION)
#undef HEDLEY_IBM_VERSION
#endif
#if defined(__ibmxl__)
#define HEDLEY_IBM_VERSION \
    HEDLEY_VERSION_ENCODE(__ibmxl_version__, __ibmxl_release__, __ibmxl_modification__)
#elif defined(__xlC__) && defined(__xlC_ver__)
#define HEDLEY_IBM_VERSION \
    HEDLEY_VERSION_ENCODE(__xlC__ >> 8, __xlC__ & 0xff, (__xlC_ver__ >> 8) & 0xff)
#elif defined(__xlC__)
#define HEDLEY_IBM_VERSION HEDLEY_VERSION_ENCODE(__xlC__ >> 8, __xlC__ & 0xff, 0)
#endif

#if defined(HEDLEY_IBM_VERSION_CHECK)
#undef HEDLEY_IBM_VERSION_CHECK
#endif
#if defined(HEDLEY_IBM_VERSION)
#define HEDLEY_IBM_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_IBM_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_IBM_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_TI_VERSION)
#undef HEDLEY_TI_VERSION
#endif
#if defined(__TI_COMPILER_VERSION__)
#define HEDLEY_TI_VERSION                                             \
    HEDLEY_VERSION_ENCODE(__TI_COMPILER_VERSION__ / 1000000,          \
                          (__TI_COMPILER_VERSION__ % 1000000) / 1000, \
                          (__TI_COMPILER_VERSION__ % 1000))
#endif

#if defined(HEDLEY_TI_VERSION_CHECK)
#undef HEDLEY_TI_VERSION_CHECK
#endif
#if defined(HEDLEY_TI_VERSION)
#define HEDLEY_TI_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_TI_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_TI_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_CRAY_VERSION)
#undef HEDLEY_CRAY_VERSION
#endif
#if defined(_CRAYC)
#if defined(_RELEASE_PATCHLEVEL)
#define HEDLEY_CRAY_VERSION \
    HEDLEY_VERSION_ENCODE(_RELEASE_MAJOR, _RELEASE_MINOR, _RELEASE_PATCHLEVEL)
#else
#define HEDLEY_CRAY_VERSION HEDLEY_VERSION_ENCODE(_RELEASE_MAJOR, _RELEASE_MINOR, 0)
#endif
#endif

#if defined(HEDLEY_CRAY_VERSION_CHECK)
#undef HEDLEY_CRAY_VERSION_CHECK
#endif
#if defined(HEDLEY_CRAY_VERSION)
#define HEDLEY_CRAY_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_CRAY_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_CRAY_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_IAR_VERSION)
#undef HEDLEY_IAR_VERSION
#endif
#if defined(__IAR_SYSTEMS_ICC__)
#if __VER__ > 1000
#define HEDLEY_IAR_VERSION \
    HEDLEY_VERSION_ENCODE((__VER__ / 1000000), ((__VER__ / 1000) % 1000), (__VER__ % 1000))
#else
#define HEDLEY_IAR_VERSION HEDLEY_VERSION_ENCODE(VER / 100, __VER__ % 100, 0)
#endif
#endif

#if defined(HEDLEY_IAR_VERSION_CHECK)
#undef HEDLEY_IAR_VERSION_CHECK
#endif
#if defined(HEDLEY_IAR_VERSION)
#define HEDLEY_IAR_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_IAR_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_IAR_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_TINYC_VERSION)
#undef HEDLEY_TINYC_VERSION
#endif
#if defined(__TINYC__)
#define HEDLEY_TINYC_VERSION \
    HEDLEY_VERSION_ENCODE(__TINYC__ / 1000, (__TINYC__ / 100) % 10, __TINYC__ % 100)
#endif

#if defined(HEDLEY_TINYC_VERSION_CHECK)
#undef HEDLEY_TINYC_VERSION_CHECK
#endif
#if defined(HEDLEY_TINYC_VERSION)
#define HEDLEY_TINYC_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_TINYC_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_TINYC_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_DMC_VERSION)
#undef HEDLEY_DMC_VERSION
#endif
#if defined(__DMC__)
#define HEDLEY_DMC_VERSION HEDLEY_VERSION_ENCODE(__DMC__ >> 8, (__DMC__ >> 4) & 0xf, __DMC__ & 0xf)
#endif

#if defined(HEDLEY_DMC_VERSION_CHECK)
#undef HEDLEY_DMC_VERSION_CHECK
#endif
#if defined(HEDLEY_DMC_VERSION)
#define HEDLEY_DMC_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_DMC_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_DMC_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_COMPCERT_VERSION)
#undef HEDLEY_COMPCERT_VERSION
#endif
#if defined(__COMPCERT_VERSION__)
#define HEDLEY_COMPCERT_VERSION                                                             \
    HEDLEY_VERSION_ENCODE(__COMPCERT_VERSION__ / 10000, (__COMPCERT_VERSION__ / 100) % 100, \
                          __COMPCERT_VERSION__ % 100)
#endif

#if defined(HEDLEY_COMPCERT_VERSION_CHECK)
#undef HEDLEY_COMPCERT_VERSION_CHECK
#endif
#if defined(HEDLEY_COMPCERT_VERSION)
#define HEDLEY_COMPCERT_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_COMPCERT_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_COMPCERT_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_PELLES_VERSION)
#undef HEDLEY_PELLES_VERSION
#endif
#if defined(__POCC__)
#define HEDLEY_PELLES_VERSION HEDLEY_VERSION_ENCODE(__POCC__ / 100, __POCC__ % 100, 0)
#endif

#if defined(HEDLEY_PELLES_VERSION_CHECK)
#undef HEDLEY_PELLES_VERSION_CHECK
#endif
#if defined(HEDLEY_PELLES_VERSION)
#define HEDLEY_PELLES_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_PELLES_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_PELLES_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_GCC_VERSION)
#undef HEDLEY_GCC_VERSION
#endif
#if defined(HEDLEY_GNUC_VERSION) && !defined(__clang__) && !defined(HEDLEY_INTEL_VERSION) && \
        !defined(HEDLEY_PGI_VERSION) && !defined(HEDLEY_ARM_VERSION) &&                      \
        !defined(HEDLEY_TI_VERSION) && !defined(__COMPCERT__)
#define HEDLEY_GCC_VERSION HEDLEY_GNUC_VERSION
#endif

#if defined(HEDLEY_GCC_VERSION_CHECK)
#undef HEDLEY_GCC_VERSION_CHECK
#endif
#if defined(HEDLEY_GCC_VERSION)
#define HEDLEY_GCC_VERSION_CHECK(major, minor, patch) \
    (HEDLEY_GCC_VERSION >= HEDLEY_VERSION_ENCODE(major, minor, patch))
#else
#define HEDLEY_GCC_VERSION_CHECK(major, minor, patch) (0)
#endif

#if defined(HEDLEY_HAS_ATTRIBUTE)
#undef HEDLEY_HAS_ATTRIBUTE
#endif
#if defined(__has_attribute)
#define HEDLEY_HAS_ATTRIBUTE(attribute) __has_attribute(attribute)
#else
#define HEDLEY_HAS_ATTRIBUTE(attribute) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_ATTRIBUTE)
#undef HEDLEY_GNUC_HAS_ATTRIBUTE
#endif
#if defined(__has_attribute)
#define HEDLEY_GNUC_HAS_ATTRIBUTE(attribute, major, minor, patch) __has_attribute(attribute)
#else
#define HEDLEY_GNUC_HAS_ATTRIBUTE(attribute, major, minor, patch) \
    HEDLEY_GNUC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_GCC_HAS_ATTRIBUTE)
#undef HEDLEY_GCC_HAS_ATTRIBUTE
#endif
#if defined(__has_attribute)
#define HEDLEY_GCC_HAS_ATTRIBUTE(attribute, major, minor, patch) __has_attribute(attribute)
#else
#define HEDLEY_GCC_HAS_ATTRIBUTE(attribute, major, minor, patch) \
    HEDLEY_GCC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_HAS_CPP_ATTRIBUTE)
#undef HEDLEY_HAS_CPP_ATTRIBUTE
#endif
#if defined(__has_cpp_attribute) && defined(__cplusplus)
#define HEDLEY_HAS_CPP_ATTRIBUTE(attribute) __has_cpp_attribute(attribute)
#else
#define HEDLEY_HAS_CPP_ATTRIBUTE(attribute) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_CPP_ATTRIBUTE)
#undef HEDLEY_GNUC_HAS_CPP_ATTRIBUTE
#endif
#if defined(__has_cpp_attribute) && defined(__cplusplus)
#define HEDLEY_GNUC_HAS_CPP_ATTRIBUTE(attribute, major, minor, patch) __has_cpp_attribute(attribute)
#else
#define HEDLEY_GNUC_HAS_CPP_ATTRIBUTE(attribute, major, minor, patch) \
    HEDLEY_GNUC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_GCC_HAS_CPP_ATTRIBUTE)
#undef HEDLEY_GCC_HAS_CPP_ATTRIBUTE
#endif
#if defined(__has_cpp_attribute) && defined(__cplusplus)
#define HEDLEY_GCC_HAS_CPP_ATTRIBUTE(attribute, major, minor, patch) __has_cpp_attribute(attribute)
#else
#define HEDLEY_GCC_HAS_CPP_ATTRIBUTE(attribute, major, minor, patch) \
    HEDLEY_GCC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_HAS_BUILTIN)
#undef HEDLEY_HAS_BUILTIN
#endif
#if defined(__has_builtin)
#define HEDLEY_HAS_BUILTIN(builtin) __has_builtin(builtin)
#else
#define HEDLEY_HAS_BUILTIN(builtin) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_BUILTIN)
#undef HEDLEY_GNUC_HAS_BUILTIN
#endif
#if defined(__has_builtin)
#define HEDLEY_GNUC_HAS_BUILTIN(builtin, major, minor, patch) __has_builtin(builtin)
#else
#define HEDLEY_GNUC_HAS_BUILTIN(builtin, major, minor, patch) \
    HEDLEY_GNUC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_GCC_HAS_BUILTIN)
#undef HEDLEY_GCC_HAS_BUILTIN
#endif
#if defined(__has_builtin)
#define HEDLEY_GCC_HAS_BUILTIN(builtin, major, minor, patch) __has_builtin(builtin)
#else
#define HEDLEY_GCC_HAS_BUILTIN(builtin, major, minor, patch) \
    HEDLEY_GCC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_HAS_FEATURE)
#undef HEDLEY_HAS_FEATURE
#endif
#if defined(__has_feature)
#define HEDLEY_HAS_FEATURE(feature) __has_feature(feature)
#else
#define HEDLEY_HAS_FEATURE(feature) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_FEATURE)
#undef HEDLEY_GNUC_HAS_FEATURE
#endif
#if defined(__has_feature)
#define HEDLEY_GNUC_HAS_FEATURE(feature, major, minor, patch) __has_feature(feature)
#else
#define HEDLEY_GNUC_HAS_FEATURE(feature, major, minor, patch) \
    HEDLEY_GNUC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_GCC_HAS_FEATURE)
#undef HEDLEY_GCC_HAS_FEATURE
#endif
#if defined(__has_feature)
#define HEDLEY_GCC_HAS_FEATURE(feature, major, minor, patch) __has_feature(feature)
#else
#define HEDLEY_GCC_HAS_FEATURE(feature, major, minor, patch) \
    HEDLEY_GCC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_HAS_EXTENSION)
#undef HEDLEY_HAS_EXTENSION
#endif
#if defined(__has_extension)
#define HEDLEY_HAS_EXTENSION(extension) __has_extension(extension)
#else
#define HEDLEY_HAS_EXTENSION(extension) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_EXTENSION)
#undef HEDLEY_GNUC_HAS_EXTENSION
#endif
#if defined(__has_extension)
#define HEDLEY_GNUC_HAS_EXTENSION(extension, major, minor, patch) __has_extension(extension)
#else
#define HEDLEY_GNUC_HAS_EXTENSION(extension, major, minor, patch) \
    HEDLEY_GNUC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_GCC_HAS_EXTENSION)
#undef HEDLEY_GCC_HAS_EXTENSION
#endif
#if defined(__has_extension)
#define HEDLEY_GCC_HAS_EXTENSION(extension, major, minor, patch) __has_extension(extension)
#else
#define HEDLEY_GCC_HAS_EXTENSION(extension, major, minor, patch) \
    HEDLEY_GCC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_HAS_DECLSPEC_ATTRIBUTE)
#undef HEDLEY_HAS_DECLSPEC_ATTRIBUTE
#endif
#if defined(__has_declspec_attribute)
#define HEDLEY_HAS_DECLSPEC_ATTRIBUTE(attribute) __has_declspec_attribute(attribute)
#else
#define HEDLEY_HAS_DECLSPEC_ATTRIBUTE(attribute) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_DECLSPEC_ATTRIBUTE)
#undef HEDLEY_GNUC_HAS_DECLSPEC_ATTRIBUTE
#endif
#if defined(__has_declspec_attribute)
#define HEDLEY_GNUC_HAS_DECLSPEC_ATTRIBUTE(attribute, major, minor, patch) \
    __has_declspec_attribute(attribute)
#else
#define HEDLEY_GNUC_HAS_DECLSPEC_ATTRIBUTE(attribute, major, minor, patch) \
    HEDLEY_GNUC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_GCC_HAS_DECLSPEC_ATTRIBUTE)
#undef HEDLEY_GCC_HAS_DECLSPEC_ATTRIBUTE
#endif
#if defined(__has_declspec_attribute)
#define HEDLEY_GCC_HAS_DECLSPEC_ATTRIBUTE(attribute, major, minor, patch) \
    __has_declspec_attribute(attribute)
#else
#define HEDLEY_GCC_HAS_DECLSPEC_ATTRIBUTE(attribute, major, minor, patch) \
    HEDLEY_GCC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_HAS_WARNING)
#undef HEDLEY_HAS_WARNING
#endif
#if defined(__has_warning)
#define HEDLEY_HAS_WARNING(warning) __has_warning(warning)
#else
#define HEDLEY_HAS_WARNING(warning) (0)
#endif

#if defined(HEDLEY_GNUC_HAS_WARNING)
#undef HEDLEY_GNUC_HAS_WARNING
#endif
#if defined(__has_warning)
#define HEDLEY_GNUC_HAS_WARNING(warning, major, minor, patch) __has_warning(warning)
#else
#define HEDLEY_GNUC_HAS_WARNING(warning, major, minor, patch) \
    HEDLEY_GNUC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_GCC_HAS_WARNING)
#undef HEDLEY_GCC_HAS_WARNING
#endif
#if defined(__has_warning)
#define HEDLEY_GCC_HAS_WARNING(warning, major, minor, patch) __has_warning(warning)
#else
#define HEDLEY_GCC_HAS_WARNING(warning, major, minor, patch) \
    HEDLEY_GCC_VERSION_CHECK(major, minor, patch)
#endif

#if (defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)) || defined(__clang__) || \
        HEDLEY_GCC_VERSION_CHECK(3, 0, 0) || HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) ||      \
        HEDLEY_IAR_VERSION_CHECK(8, 0, 0) || HEDLEY_PGI_VERSION_CHECK(18, 4, 0) ||        \
        HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || HEDLEY_TI_VERSION_CHECK(6, 0, 0) ||          \
        HEDLEY_CRAY_VERSION_CHECK(5, 0, 0) || HEDLEY_TINYC_VERSION_CHECK(0, 9, 17) ||     \
        HEDLEY_SUNPRO_VERSION_CHECK(8, 0, 0) ||                                           \
        (HEDLEY_IBM_VERSION_CHECK(10, 1, 0) && defined(__C99_PRAGMA_OPERATOR))
#define HEDLEY_PRAGMA(value) _Pragma(#value)
#elif HEDLEY_MSVC_VERSION_CHECK(15, 0, 0)
#define HEDLEY_PRAGMA(value) __pragma(value)
#else
#define HEDLEY_PRAGMA(value)
#endif

#if defined(HEDLEY_DIAGNOSTIC_PUSH)
#undef HEDLEY_DIAGNOSTIC_PUSH
#endif
#if defined(HEDLEY_DIAGNOSTIC_POP)
#undef HEDLEY_DIAGNOSTIC_POP
#endif
#if defined(__clang__)
#define HEDLEY_DIAGNOSTIC_PUSH _Pragma("clang diagnostic push")
#define HEDLEY_DIAGNOSTIC_POP _Pragma("clang diagnostic pop")
#elif HEDLEY_INTEL_VERSION_CHECK(13, 0, 0)
#define HEDLEY_DIAGNOSTIC_PUSH _Pragma("warning(push)")
#define HEDLEY_DIAGNOSTIC_POP _Pragma("warning(pop)")
#elif HEDLEY_GCC_VERSION_CHECK(4, 6, 0)
#define HEDLEY_DIAGNOSTIC_PUSH _Pragma("GCC diagnostic push")
#define HEDLEY_DIAGNOSTIC_POP _Pragma("GCC diagnostic pop")
#elif HEDLEY_MSVC_VERSION_CHECK(15, 0, 0)
#define HEDLEY_DIAGNOSTIC_PUSH __pragma(warning(push))
#define HEDLEY_DIAGNOSTIC_POP __pragma(warning(pop))
#elif HEDLEY_ARM_VERSION_CHECK(5, 6, 0)
#define HEDLEY_DIAGNOSTIC_PUSH _Pragma("push")
#define HEDLEY_DIAGNOSTIC_POP _Pragma("pop")
#elif HEDLEY_TI_VERSION_CHECK(8, 1, 0)
#define HEDLEY_DIAGNOSTIC_PUSH _Pragma("diag_push")
#define HEDLEY_DIAGNOSTIC_POP _Pragma("diag_pop")
#elif HEDLEY_PELLES_VERSION_CHECK(2, 90, 0)
#define HEDLEY_DIAGNOSTIC_PUSH _Pragma("warning(push)")
#define HEDLEY_DIAGNOSTIC_POP _Pragma("warning(pop)")
#else
#define HEDLEY_DIAGNOSTIC_PUSH
#define HEDLEY_DIAGNOSTIC_POP
#endif

#if defined(HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED)
#undef HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED
#endif
#if HEDLEY_HAS_WARNING("-Wdeprecated-declarations")
#define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED \
    _Pragma("clang diagnostic ignored \"-Wdeprecated-declarations\"")
#elif HEDLEY_INTEL_VERSION_CHECK(13, 0, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("warning(disable:1478 1786)")
#elif HEDLEY_PGI_VERSION_CHECK(17, 10, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("diag_suppress 1215,1444")
#elif HEDLEY_GCC_VERSION_CHECK(4, 3, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED \
    _Pragma("GCC diagnostic ignored \"-Wdeprecated-declarations\"")
#elif HEDLEY_MSVC_VERSION_CHECK(15, 0, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED __pragma(warning(disable : 4996))
#elif HEDLEY_TI_VERSION_CHECK(8, 0, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("diag_suppress 1291,1718")
#elif HEDLEY_SUNPRO_VERSION_CHECK(5, 13, 0) && !defined(__cplusplus)
#define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED \
    _Pragma("error_messages(off,E_DEPRECATED_ATT,E_DEPRECATED_ATT_MESS)")
#elif HEDLEY_SUNPRO_VERSION_CHECK(5, 13, 0) && defined(__cplusplus)
#define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED \
    _Pragma("error_messages(off,symdeprecated,symdeprecated2)")
#elif HEDLEY_IAR_VERSION_CHECK(8, 0, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("diag_suppress=Pe1444,Pe1215")
#elif HEDLEY_PELLES_VERSION_CHECK(2, 90, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED _Pragma("warn(disable:2241)")
#else
#define HEDLEY_DIAGNOSTIC_DISABLE_DEPRECATED
#endif

#if defined(HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS)
#undef HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS
#endif
#if HEDLEY_HAS_WARNING("-Wunknown-pragmas")
#define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS \
    _Pragma("clang diagnostic ignored \"-Wunknown-pragmas\"")
#elif HEDLEY_INTEL_VERSION_CHECK(13, 0, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS _Pragma("warning(disable:161)")
#elif HEDLEY_PGI_VERSION_CHECK(17, 10, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS _Pragma("diag_suppress 1675")
#elif HEDLEY_GCC_VERSION_CHECK(4, 3, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS \
    _Pragma("GCC diagnostic ignored \"-Wunknown-pragmas\"")
#elif HEDLEY_MSVC_VERSION_CHECK(15, 0, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS __pragma(warning(disable : 4068))
#elif HEDLEY_TI_VERSION_CHECK(8, 0, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS _Pragma("diag_suppress 163")
#elif HEDLEY_IAR_VERSION_CHECK(8, 0, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS _Pragma("diag_suppress=Pe161")
#else
#define HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS
#endif

#if defined(HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL)
#undef HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL
#endif
#if HEDLEY_HAS_WARNING("-Wcast-qual")
#define HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL _Pragma("clang diagnostic ignored \"-Wcast-qual\"")
#elif HEDLEY_INTEL_VERSION_CHECK(13, 0, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL _Pragma("warning(disable:2203 2331)")
#elif HEDLEY_GCC_VERSION_CHECK(3, 0, 0)
#define HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL _Pragma("GCC diagnostic ignored \"-Wcast-qual\"")
#else
#define HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL
#endif

#if defined(HEDLEY_DEPRECATED)
#undef HEDLEY_DEPRECATED
#endif
#if defined(HEDLEY_DEPRECATED_FOR)
#undef HEDLEY_DEPRECATED_FOR
#endif
#if defined(__cplusplus) && (__cplusplus >= 201402L)
#define HEDLEY_DEPRECATED(since) [[deprecated("Since " #since)]]
#define HEDLEY_DEPRECATED_FOR(since, replacement) \
    [[deprecated("Since " #since "; use " #replacement)]]
#elif HEDLEY_HAS_EXTENSION(attribute_deprecated_with_message) ||                      \
        HEDLEY_GCC_VERSION_CHECK(4, 5, 0) || HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) ||  \
        HEDLEY_ARM_VERSION_CHECK(5, 6, 0) || HEDLEY_SUNPRO_VERSION_CHECK(5, 13, 0) || \
        HEDLEY_PGI_VERSION_CHECK(17, 10, 0) || HEDLEY_TI_VERSION_CHECK(8, 3, 0)
#define HEDLEY_DEPRECATED(since) __attribute__((__deprecated__("Since " #since)))
#define HEDLEY_DEPRECATED_FOR(since, replacement) \
    __attribute__((__deprecated__("Since " #since "; use " #replacement)))
#elif HEDLEY_HAS_ATTRIBUTE(deprecated) || HEDLEY_GCC_VERSION_CHECK(3, 1, 0) ||   \
        HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || HEDLEY_TI_VERSION_CHECK(8, 0, 0) || \
        (HEDLEY_TI_VERSION_CHECK(7, 3, 0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#define HEDLEY_DEPRECATED(since) __attribute__((__deprecated__))
#define HEDLEY_DEPRECATED_FOR(since, replacement) __attribute__((__deprecated__))
#elif HEDLEY_MSVC_VERSION_CHECK(14, 0, 0)
#define HEDLEY_DEPRECATED(since) __declspec(deprecated("Since " #since))
#define HEDLEY_DEPRECATED_FOR(since, replacement) \
    __declspec(deprecated("Since " #since "; use " #replacement))
#elif HEDLEY_MSVC_VERSION_CHECK(13, 10, 0) || HEDLEY_PELLES_VERSION_CHECK(6, 50, 0)
#define HEDLEY_DEPRECATED(since) _declspec(deprecated)
#define HEDLEY_DEPRECATED_FOR(since, replacement) __declspec(deprecated)
#elif HEDLEY_IAR_VERSION_CHECK(8, 0, 0)
#define HEDLEY_DEPRECATED(since) _Pragma("deprecated")
#define HEDLEY_DEPRECATED_FOR(since, replacement) _Pragma("deprecated")
#else
#define HEDLEY_DEPRECATED(since)
#define HEDLEY_DEPRECATED_FOR(since, replacement)
#endif

#if defined(HEDLEY_UNAVAILABLE)
#undef HEDLEY_UNAVAILABLE
#endif
#if HEDLEY_HAS_ATTRIBUTE(warning) || HEDLEY_GCC_VERSION_CHECK(4, 3, 0) || \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0)
#define HEDLEY_UNAVAILABLE(available_since) \
    __attribute__((__warning__("Not available until " #available_since)))
#else
#define HEDLEY_UNAVAILABLE(available_since)
#endif

#if defined(HEDLEY_WARN_UNUSED_RESULT)
#undef HEDLEY_WARN_UNUSED_RESULT
#endif
#if defined(__cplusplus) && (__cplusplus >= 201703L)
#define HEDLEY_WARN_UNUSED_RESULT [[nodiscard]]
#elif HEDLEY_HAS_ATTRIBUTE(warn_unused_result) || HEDLEY_GCC_VERSION_CHECK(3, 4, 0) || \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || HEDLEY_TI_VERSION_CHECK(8, 0, 0) ||    \
        (HEDLEY_TI_VERSION_CHECK(7, 3, 0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__)) || \
        (HEDLEY_SUNPRO_VERSION_CHECK(5, 15, 0) && defined(__cplusplus)) ||             \
        HEDLEY_PGI_VERSION_CHECK(17, 10, 0)
#define HEDLEY_WARN_UNUSED_RESULT __attribute__((__warn_unused_result__))
#elif defined(_Check_return_) /* SAL */
#define HEDLEY_WARN_UNUSED_RESULT _Check_return_
#else
#define HEDLEY_WARN_UNUSED_RESULT
#endif

#if defined(HEDLEY_SENTINEL)
#undef HEDLEY_SENTINEL
#endif
#if HEDLEY_HAS_ATTRIBUTE(sentinel) || HEDLEY_GCC_VERSION_CHECK(4, 0, 0) || \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || HEDLEY_ARM_VERSION_CHECK(5, 4, 0)
#define HEDLEY_SENTINEL(position) __attribute__((__sentinel__(position)))
#else
#define HEDLEY_SENTINEL(position)
#endif

#if defined(HEDLEY_NO_RETURN)
#undef HEDLEY_NO_RETURN
#endif
#if HEDLEY_IAR_VERSION_CHECK(8, 0, 0)
#define HEDLEY_NO_RETURN __noreturn
#elif HEDLEY_INTEL_VERSION_CHECK(13, 0, 0)
#define HEDLEY_NO_RETURN __attribute__((__noreturn__))
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
#define HEDLEY_NO_RETURN _Noreturn
#elif defined(__cplusplus) && (__cplusplus >= 201103L)
#define HEDLEY_NO_RETURN [[noreturn]]
#elif HEDLEY_HAS_ATTRIBUTE(noreturn) || HEDLEY_GCC_VERSION_CHECK(3, 2, 0) ||          \
        HEDLEY_SUNPRO_VERSION_CHECK(5, 11, 0) || HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || \
        HEDLEY_IBM_VERSION_CHECK(10, 1, 0) || HEDLEY_TI_VERSION_CHECK(18, 0, 0) ||    \
        (HEDLEY_TI_VERSION_CHECK(17, 3, 0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#define HEDLEY_NO_RETURN __attribute__((__noreturn__))
#elif HEDLEY_MSVC_VERSION_CHECK(13, 10, 0)
#define HEDLEY_NO_RETURN __declspec(noreturn)
#elif HEDLEY_TI_VERSION_CHECK(6, 0, 0) && defined(__cplusplus)
#define HEDLEY_NO_RETURN _Pragma("FUNC_NEVER_RETURNS;")
#elif HEDLEY_COMPCERT_VERSION_CHECK(3, 2, 0)
#define HEDLEY_NO_RETURN __attribute((noreturn))
#elif HEDLEY_PELLES_VERSION_CHECK(9, 0, 0)
#define HEDLEY_NO_RETURN __declspec(noreturn)
#else
#define HEDLEY_NO_RETURN
#endif

#if defined(HEDLEY_UNREACHABLE)
#undef HEDLEY_UNREACHABLE
#endif
#if defined(HEDLEY_UNREACHABLE_RETURN)
#undef HEDLEY_UNREACHABLE_RETURN
#endif
#if (HEDLEY_HAS_BUILTIN(__builtin_unreachable) && (!defined(HEDLEY_ARM_VERSION))) || \
        HEDLEY_GCC_VERSION_CHECK(4, 5, 0) || HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || \
        HEDLEY_IBM_VERSION_CHECK(13, 1, 5)
#define HEDLEY_UNREACHABLE() __builtin_unreachable()
#elif HEDLEY_MSVC_VERSION_CHECK(13, 10, 0)
#define HEDLEY_UNREACHABLE() __assume(0)
#elif HEDLEY_TI_VERSION_CHECK(6, 0, 0)
#if defined(__cplusplus)
#define HEDLEY_UNREACHABLE() std::_nassert(0)
#else
#define HEDLEY_UNREACHABLE() _nassert(0)
#endif
#define HEDLEY_UNREACHABLE_RETURN(value) return value
#elif defined(EXIT_FAILURE)
#define HEDLEY_UNREACHABLE() abort()
#else
#define HEDLEY_UNREACHABLE()
#define HEDLEY_UNREACHABLE_RETURN(value) return value
#endif
#if !defined(HEDLEY_UNREACHABLE_RETURN)
#define HEDLEY_UNREACHABLE_RETURN(value) HEDLEY_UNREACHABLE()
#endif

#if defined(HEDLEY_ASSUME)
#undef HEDLEY_ASSUME
#endif
#if HEDLEY_MSVC_VERSION_CHECK(13, 10, 0) || HEDLEY_INTEL_VERSION_CHECK(13, 0, 0)
#define HEDLEY_ASSUME(expr) __assume(expr)
#elif HEDLEY_HAS_BUILTIN(__builtin_assume)
#define HEDLEY_ASSUME(expr) __builtin_assume(expr)
#elif HEDLEY_TI_VERSION_CHECK(6, 0, 0)
#if defined(__cplusplus)
#define HEDLEY_ASSUME(expr) std::_nassert(expr)
#else
#define HEDLEY_ASSUME(expr) _nassert(expr)
#endif
#elif (HEDLEY_HAS_BUILTIN(__builtin_unreachable) && !defined(HEDLEY_ARM_VERSION)) || \
        HEDLEY_GCC_VERSION_CHECK(4, 5, 0) || HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || \
        HEDLEY_IBM_VERSION_CHECK(13, 1, 5)
#define HEDLEY_ASSUME(expr) ((void)((expr) ? 1 : (__builtin_unreachable(), 1)))
#else
#define HEDLEY_ASSUME(expr) ((void)(expr))
#endif

HEDLEY_DIAGNOSTIC_PUSH
#if HEDLEY_HAS_WARNING("-Wvariadic-macros") || HEDLEY_GCC_VERSION_CHECK(4, 0, 0)
#if defined(__clang__)
#pragma clang diagnostic ignored "-Wvariadic-macros"
#elif defined(HEDLEY_GCC_VERSION)
#pragma GCC diagnostic ignored "-Wvariadic-macros"
#endif
#endif
#if defined(HEDLEY_NON_NULL)
#undef HEDLEY_NON_NULL
#endif
#if HEDLEY_HAS_ATTRIBUTE(nonnull) || HEDLEY_GCC_VERSION_CHECK(3, 3, 0) || \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || HEDLEY_ARM_VERSION_CHECK(4, 1, 0)
#define HEDLEY_NON_NULL(...) __attribute__((__nonnull__(__VA_ARGS__)))
#else
#define HEDLEY_NON_NULL(...)
#endif
HEDLEY_DIAGNOSTIC_POP

#if defined(HEDLEY_PRINTF_FORMAT)
#undef HEDLEY_PRINTF_FORMAT
#endif
#if defined(__MINGW32__) && HEDLEY_GCC_HAS_ATTRIBUTE(format, 4, 4, 0) && \
        !defined(__USE_MINGW_ANSI_STDIO)
#define HEDLEY_PRINTF_FORMAT(string_idx, first_to_check) \
    __attribute__((__format__(ms_printf, string_idx, first_to_check)))
#elif defined(__MINGW32__) && HEDLEY_GCC_HAS_ATTRIBUTE(format, 4, 4, 0) && \
        defined(__USE_MINGW_ANSI_STDIO)
#define HEDLEY_PRINTF_FORMAT(string_idx, first_to_check) \
    __attribute__((__format__(gnu_printf, string_idx, first_to_check)))
#elif HEDLEY_HAS_ATTRIBUTE(format) || HEDLEY_GCC_VERSION_CHECK(3, 1, 0) ||           \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || HEDLEY_ARM_VERSION_CHECK(5, 6, 0) || \
        HEDLEY_IBM_VERSION_CHECK(10, 1, 0) || HEDLEY_TI_VERSION_CHECK(8, 0, 0) ||    \
        (HEDLEY_TI_VERSION_CHECK(7, 3, 0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#define HEDLEY_PRINTF_FORMAT(string_idx, first_to_check) \
    __attribute__((__format__(__printf__, string_idx, first_to_check)))
#elif HEDLEY_PELLES_VERSION_CHECK(6, 0, 0)
#define HEDLEY_PRINTF_FORMAT(string_idx, first_to_check) \
    __declspec(vaformat(printf, string_idx, first_to_check))
#else
#define HEDLEY_PRINTF_FORMAT(string_idx, first_to_check)
#endif

#if defined(HEDLEY_CONSTEXPR)
#undef HEDLEY_CONSTEXPR
#endif
#if defined(__cplusplus)
#if __cplusplus >= 201103L
#define HEDLEY_CONSTEXPR constexpr
#endif
#endif
#if !defined(HEDLEY_CONSTEXPR)
#define HEDLEY_CONSTEXPR
#endif

#if defined(HEDLEY_PREDICT)
#undef HEDLEY_PREDICT
#endif
#if defined(HEDLEY_LIKELY)
#undef HEDLEY_LIKELY
#endif
#if defined(HEDLEY_UNLIKELY)
#undef HEDLEY_UNLIKELY
#endif
#if defined(HEDLEY_UNPREDICTABLE)
#undef HEDLEY_UNPREDICTABLE
#endif
#if HEDLEY_HAS_BUILTIN(__builtin_unpredictable)
#define HEDLEY_UNPREDICTABLE(expr) __builtin_unpredictable(!!(expr))
#endif
#if HEDLEY_HAS_BUILTIN(__builtin_expect_with_probability) || HEDLEY_GCC_VERSION_CHECK(9, 0, 0)
#define HEDLEY_PREDICT(expr, value, probability) \
    __builtin_expect_with_probability(expr, value, probability)
#define HEDLEY_PREDICT_TRUE(expr, probability) \
    __builtin_expect_with_probability(!!(expr), 1, probability)
#define HEDLEY_PREDICT_FALSE(expr, probability) \
    __builtin_expect_with_probability(!!(expr), 0, probability)
#define HEDLEY_LIKELY(expr) __builtin_expect(!!(expr), 1)
#define HEDLEY_UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#if !defined(HEDLEY_BUILTIN_UNPREDICTABLE)
#define HEDLEY_BUILTIN_UNPREDICTABLE(expr) __builtin_expect_with_probability(!!(expr), 1, 0.5)
#endif
#elif HEDLEY_HAS_BUILTIN(__builtin_expect) || HEDLEY_GCC_VERSION_CHECK(3, 0, 0) || \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) ||                                    \
        (HEDLEY_SUNPRO_VERSION_CHECK(5, 15, 0) && defined(__cplusplus)) ||         \
        HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || HEDLEY_IBM_VERSION_CHECK(10, 1, 0) || \
        HEDLEY_TI_VERSION_CHECK(6, 1, 0) || HEDLEY_TINYC_VERSION_CHECK(0, 9, 27)
#define HEDLEY_PREDICT(expr, expected, probability)                  \
    (((probability) >= 0.9) ? __builtin_expect(!!(expr), (expected)) \
                            : (((void)(expected)), !!(expr)))
#define HEDLEY_PREDICT_TRUE(expr, probability)                                                 \
    (__extension__({                                                                           \
        HEDLEY_CONSTEXPR double hedley_probability_ = (probability);                           \
        ((hedley_probability_ >= 0.9)                                                          \
                 ? __builtin_expect(!!(expr), 1)                                               \
                 : ((hedley_probability_ <= 0.1) ? __builtin_expect(!!(expr), 0) : !!(expr))); \
    }))
#define HEDLEY_PREDICT_FALSE(expr, probability)                                                \
    (__extension__({                                                                           \
        HEDLEY_CONSTEXPR double hedley_probability_ = (probability);                           \
        ((hedley_probability_ >= 0.9)                                                          \
                 ? __builtin_expect(!!(expr), 0)                                               \
                 : ((hedley_probability_ <= 0.1) ? __builtin_expect(!!(expr), 1) : !!(expr))); \
    }))
#define HEDLEY_LIKELY(expr) __builtin_expect(!!(expr), 1)
#define HEDLEY_UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#else
#define HEDLEY_PREDICT(expr, expected, probability) (((void)(expected)), !!(expr))
#define HEDLEY_PREDICT_TRUE(expr, probability) (!!(expr))
#define HEDLEY_PREDICT_FALSE(expr, probability) (!!(expr))
#define HEDLEY_LIKELY(expr) (!!(expr))
#define HEDLEY_UNLIKELY(expr) (!!(expr))
#endif
#if !defined(HEDLEY_UNPREDICTABLE)
#define HEDLEY_UNPREDICTABLE(expr) HEDLEY_PREDICT(expr, 1, 0.5)
#endif

#if defined(HEDLEY_MALLOC)
#undef HEDLEY_MALLOC
#endif
#if HEDLEY_HAS_ATTRIBUTE(malloc) || HEDLEY_GCC_VERSION_CHECK(3, 1, 0) ||                 \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || HEDLEY_SUNPRO_VERSION_CHECK(5, 11, 0) || \
        HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || HEDLEY_IBM_VERSION_CHECK(12, 1, 0) ||       \
        HEDLEY_TI_VERSION_CHECK(8, 0, 0) ||                                              \
        (HEDLEY_TI_VERSION_CHECK(7, 3, 0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#define HEDLEY_MALLOC __attribute__((__malloc__))
#elif HEDLEY_MSVC_VERSION_CHECK(14, 0, 0)
#define HEDLEY_MALLOC __declspec(restrict)
#else
#define HEDLEY_MALLOC
#endif

#if defined(HEDLEY_PURE)
#undef HEDLEY_PURE
#endif
#if HEDLEY_HAS_ATTRIBUTE(pure) || HEDLEY_GCC_VERSION_CHECK(2, 96, 0) ||                  \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || HEDLEY_SUNPRO_VERSION_CHECK(5, 11, 0) || \
        HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || HEDLEY_IBM_VERSION_CHECK(10, 1, 0) ||       \
        HEDLEY_TI_VERSION_CHECK(8, 0, 0) ||                                              \
        (HEDLEY_TI_VERSION_CHECK(7, 3, 0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__)) ||   \
        HEDLEY_PGI_VERSION_CHECK(17, 10, 0)
#define HEDLEY_PURE __attribute__((__pure__))
#elif HEDLEY_TI_VERSION_CHECK(6, 0, 0) && defined(__cplusplus)
#define HEDLEY_PURE _Pragma("FUNC_IS_PURE;")
#else
#define HEDLEY_PURE
#endif

#if defined(HEDLEY_CONST)
#undef HEDLEY_CONST
#endif
#if HEDLEY_HAS_ATTRIBUTE(const) || HEDLEY_GCC_VERSION_CHECK(2, 5, 0) ||                  \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || HEDLEY_SUNPRO_VERSION_CHECK(5, 11, 0) || \
        HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || HEDLEY_IBM_VERSION_CHECK(10, 1, 0) ||       \
        HEDLEY_TI_VERSION_CHECK(8, 0, 0) ||                                              \
        (HEDLEY_TI_VERSION_CHECK(7, 3, 0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__)) ||   \
        HEDLEY_PGI_VERSION_CHECK(17, 10, 0)
#define HEDLEY_CONST __attribute__((__const__))
#else
#define HEDLEY_CONST HEDLEY_PURE
#endif

#if defined(HEDLEY_RESTRICT)
#undef HEDLEY_RESTRICT
#endif
#if defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) && !defined(__cplusplus)
#define HEDLEY_RESTRICT restrict
#elif HEDLEY_GCC_VERSION_CHECK(3, 1, 0) || HEDLEY_MSVC_VERSION_CHECK(14, 0, 0) ||    \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || \
        HEDLEY_IBM_VERSION_CHECK(10, 1, 0) || HEDLEY_PGI_VERSION_CHECK(17, 10, 0) || \
        HEDLEY_TI_VERSION_CHECK(8, 0, 0) ||                                          \
        (HEDLEY_SUNPRO_VERSION_CHECK(5, 14, 0) && defined(__cplusplus)) ||           \
        HEDLEY_IAR_VERSION_CHECK(8, 0, 0) || defined(__clang__)
#define HEDLEY_RESTRICT __restrict
#elif HEDLEY_SUNPRO_VERSION_CHECK(5, 3, 0) && !defined(__cplusplus)
#define HEDLEY_RESTRICT _Restrict
#else
#define HEDLEY_RESTRICT
#endif

#if defined(HEDLEY_INLINE)
#undef HEDLEY_INLINE
#endif
#if (defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)) || \
        (defined(__cplusplus) && (__cplusplus >= 199711L))
#define HEDLEY_INLINE inline
#elif defined(HEDLEY_GCC_VERSION) || HEDLEY_ARM_VERSION_CHECK(6, 2, 0)
#define HEDLEY_INLINE __inline__
#elif HEDLEY_MSVC_VERSION_CHECK(12, 0, 0) || HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || \
        HEDLEY_TI_VERSION_CHECK(8, 0, 0)
#define HEDLEY_INLINE __inline
#else
#define HEDLEY_INLINE
#endif

#if defined(HEDLEY_ALWAYS_INLINE)
#undef HEDLEY_ALWAYS_INLINE
#endif
#if HEDLEY_HAS_ATTRIBUTE(always_inline) || HEDLEY_GCC_VERSION_CHECK(4, 0, 0) ||          \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || HEDLEY_SUNPRO_VERSION_CHECK(5, 11, 0) || \
        HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || HEDLEY_IBM_VERSION_CHECK(10, 1, 0) ||       \
        HEDLEY_TI_VERSION_CHECK(8, 0, 0) ||                                              \
        (HEDLEY_TI_VERSION_CHECK(7, 3, 0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#define HEDLEY_ALWAYS_INLINE __attribute__((__always_inline__)) HEDLEY_INLINE
#elif HEDLEY_MSVC_VERSION_CHECK(12, 0, 0)
#define HEDLEY_ALWAYS_INLINE __forceinline
#elif HEDLEY_TI_VERSION_CHECK(7, 0, 0) && defined(__cplusplus)
#define HEDLEY_ALWAYS_INLINE _Pragma("FUNC_ALWAYS_INLINE;")
#elif HEDLEY_IAR_VERSION_CHECK(8, 0, 0)
#define HEDLEY_ALWAYS_INLINE _Pragma("inline=forced")
#else
#define HEDLEY_ALWAYS_INLINE HEDLEY_INLINE
#endif

#if defined(HEDLEY_NEVER_INLINE)
#undef HEDLEY_NEVER_INLINE
#endif
#if HEDLEY_HAS_ATTRIBUTE(noinline) || HEDLEY_GCC_VERSION_CHECK(4, 0, 0) ||               \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || HEDLEY_SUNPRO_VERSION_CHECK(5, 11, 0) || \
        HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || HEDLEY_IBM_VERSION_CHECK(10, 1, 0) ||       \
        HEDLEY_TI_VERSION_CHECK(8, 0, 0) ||                                              \
        (HEDLEY_TI_VERSION_CHECK(7, 3, 0) && defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#define HEDLEY_NEVER_INLINE __attribute__((__noinline__))
#elif HEDLEY_MSVC_VERSION_CHECK(13, 10, 0)
#define HEDLEY_NEVER_INLINE __declspec(noinline)
#elif HEDLEY_PGI_VERSION_CHECK(10, 2, 0)
#define HEDLEY_NEVER_INLINE _Pragma("noinline")
#elif HEDLEY_TI_VERSION_CHECK(6, 0, 0) && defined(__cplusplus)
#define HEDLEY_NEVER_INLINE _Pragma("FUNC_CANNOT_INLINE;")
#elif HEDLEY_IAR_VERSION_CHECK(8, 0, 0)
#define HEDLEY_NEVER_INLINE _Pragma("inline=never")
#elif HEDLEY_COMPCERT_VERSION_CHECK(3, 2, 0)
#define HEDLEY_NEVER_INLINE __attribute((noinline))
#elif HEDLEY_PELLES_VERSION_CHECK(9, 0, 0)
#define HEDLEY_NEVER_INLINE __declspec(noinline)
#else
#define HEDLEY_NEVER_INLINE
#endif

#if defined(HEDLEY_PRIVATE)
#undef HEDLEY_PRIVATE
#endif
#if defined(HEDLEY_PUBLIC)
#undef HEDLEY_PUBLIC
#endif
#if defined(HEDLEY_IMPORT)
#undef HEDLEY_IMPORT
#endif
#if defined(_WIN32) || defined(__CYGWIN__)
#define HEDLEY_PRIVATE
#define HEDLEY_PUBLIC __declspec(dllexport)
#define HEDLEY_IMPORT __declspec(dllimport)
#else
#if HEDLEY_HAS_ATTRIBUTE(visibility) || HEDLEY_GCC_VERSION_CHECK(3, 3, 0) ||             \
        HEDLEY_SUNPRO_VERSION_CHECK(5, 11, 0) || HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || \
        HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || HEDLEY_IBM_VERSION_CHECK(13, 1, 0) ||       \
        HEDLEY_TI_VERSION_CHECK(8, 0, 0) ||                                              \
        (HEDLEY_TI_VERSION_CHECK(7, 3, 0) && defined(__TI_EABI__) &&                     \
         defined(__TI_GNU_ATTRIBUTE_SUPPORT__))
#define HEDLEY_PRIVATE __attribute__((__visibility__("hidden")))
#define HEDLEY_PUBLIC __attribute__((__visibility__("default")))
#else
#define HEDLEY_PRIVATE
#define HEDLEY_PUBLIC
#endif
#define HEDLEY_IMPORT extern
#endif

#if defined(HEDLEY_NO_THROW)
#undef HEDLEY_NO_THROW
#endif
#if HEDLEY_HAS_ATTRIBUTE(nothrow) || HEDLEY_GCC_VERSION_CHECK(3, 3, 0) || \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0)
#define HEDLEY_NO_THROW __attribute__((__nothrow__))
#elif HEDLEY_MSVC_VERSION_CHECK(13, 1, 0) || HEDLEY_ARM_VERSION_CHECK(4, 1, 0)
#define HEDLEY_NO_THROW __declspec(nothrow)
#else
#define HEDLEY_NO_THROW
#endif

#if defined(HEDLEY_FALL_THROUGH)
#undef HEDLEY_FALL_THROUGH
#endif
#if defined(__cplusplus) &&                                                           \
        (!defined(HEDLEY_SUNPRO_VERSION) || HEDLEY_SUNPRO_VERSION_CHECK(5, 15, 0)) && \
        !defined(HEDLEY_PGI_VERSION)
#if (__cplusplus >= 201703L) || ((__cplusplus >= 201103L) && HEDLEY_HAS_CPP_ATTRIBUTE(fallthrough))
#define HEDLEY_FALL_THROUGH [[fallthrough]]
#elif (__cplusplus >= 201103L) && HEDLEY_HAS_CPP_ATTRIBUTE(clang::fallthrough)
#define HEDLEY_FALL_THROUGH [[clang::fallthrough]]
#elif (__cplusplus >= 201103L) && HEDLEY_GCC_VERSION_CHECK(7, 0, 0)
#define HEDLEY_FALL_THROUGH [[gnu::fallthrough]]
#endif
#endif
#if !defined(HEDLEY_FALL_THROUGH)
#if HEDLEY_GNUC_HAS_ATTRIBUTE(fallthrough, 7, 0, 0) && !defined(HEDLEY_PGI_VERSION)
#define HEDLEY_FALL_THROUGH __attribute__((__fallthrough__))
#elif defined(__fallthrough) /* SAL */
#define HEDLEY_FALL_THROUGH __fallthrough
#else
#define HEDLEY_FALL_THROUGH
#endif
#endif

#if defined(HEDLEY_RETURNS_NON_NULL)
#undef HEDLEY_RETURNS_NON_NULL
#endif
#if HEDLEY_HAS_ATTRIBUTE(returns_nonnull) || HEDLEY_GCC_VERSION_CHECK(4, 9, 0)
#define HEDLEY_RETURNS_NON_NULL __attribute__((__returns_nonnull__))
#elif defined(_Ret_notnull_) /* SAL */
#define HEDLEY_RETURNS_NON_NULL _Ret_notnull_
#else
#define HEDLEY_RETURNS_NON_NULL
#endif

#if defined(HEDLEY_ARRAY_PARAM)
#undef HEDLEY_ARRAY_PARAM
#endif
#if defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) && !defined(__STDC_NO_VLA__) && \
        !defined(__cplusplus) && !defined(HEDLEY_PGI_VERSION) && !defined(HEDLEY_TINYC_VERSION)
#define HEDLEY_ARRAY_PARAM(name) (name)
#else
#define HEDLEY_ARRAY_PARAM(name)
#endif

#if defined(HEDLEY_IS_CONSTANT)
#undef HEDLEY_IS_CONSTANT
#endif
#if defined(HEDLEY_REQUIRE_CONSTEXPR)
#undef HEDLEY_REQUIRE_CONSTEXPR
#endif
/* Note the double-underscore. For internal use only; no API
 * guarantees! */
#if defined(HEDLEY__IS_CONSTEXPR)
#undef HEDLEY__IS_CONSTEXPR
#endif

#if HEDLEY_HAS_BUILTIN(__builtin_constant_p) || HEDLEY_GCC_VERSION_CHECK(3, 4, 0) ||    \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || HEDLEY_TINYC_VERSION_CHECK(0, 9, 19) || \
        HEDLEY_ARM_VERSION_CHECK(4, 1, 0) || HEDLEY_IBM_VERSION_CHECK(13, 1, 0) ||      \
        HEDLEY_TI_VERSION_CHECK(6, 1, 0) || HEDLEY_SUNPRO_VERSION_CHECK(5, 10, 0) ||    \
        HEDLEY_CRAY_VERSION_CHECK(8, 1, 0)
#define HEDLEY_IS_CONSTANT(expr) __builtin_constant_p(expr)
#endif
#if !defined(__cplusplus)
#if HEDLEY_HAS_BUILTIN(__builtin_types_compatible_p) || HEDLEY_GCC_VERSION_CHECK(3, 4, 0) || \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || HEDLEY_IBM_VERSION_CHECK(13, 1, 0) ||        \
        HEDLEY_CRAY_VERSION_CHECK(8, 1, 0) || HEDLEY_ARM_VERSION_CHECK(5, 4, 0) ||           \
        HEDLEY_TINYC_VERSION_CHECK(0, 9, 24)
#if defined(__INTPTR_TYPE__)
#define HEDLEY__IS_CONSTEXPR(expr)                                                                 \
    __builtin_types_compatible_p(__typeof__((1 ? (void*)((__INTPTR_TYPE__)((expr)*0)) : (int*)0)), \
                                 int*)
#else
#include <stdint.h>
#define HEDLEY__IS_CONSTEXPR(expr) \
    __builtin_types_compatible_p(__typeof__((1 ? (void*)((intptr_t)((expr)*0)) : (int*)0)), int*)
#endif
#elif (defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 201112L) &&                       \
       !defined(HEDLEY_SUNPRO_VERSION) && !defined(HEDLEY_PGI_VERSION)) ||                 \
        HEDLEY_HAS_EXTENSION(c_generic_selections) || HEDLEY_GCC_VERSION_CHECK(4, 9, 0) || \
        HEDLEY_INTEL_VERSION_CHECK(17, 0, 0) || HEDLEY_IBM_VERSION_CHECK(12, 1, 0) ||      \
        HEDLEY_ARM_VERSION_CHECK(5, 3, 0)
#if defined(__INTPTR_TYPE__)
#define HEDLEY__IS_CONSTEXPR(expr) \
    _Generic((1 ? (void*)((__INTPTR_TYPE__)((expr)*0)) : (int*)0), int* : 1, void* : 0)
#else
#include <stdint.h>
#define HEDLEY__IS_CONSTEXPR(expr) \
    _Generic((1 ? (void*)((intptr_t)*0) : (int*)0), int* : 1, void* : 0)
#endif
#elif defined(HEDLEY_GCC_VERSION) || defined(HEDLEY_INTEL_VERSION) || \
        defined(HEDLEY_TINYC_VERSION) || defined(HEDLEY_TI_VERSION) || defined(__clang__)
#define HEDLEY__IS_CONSTEXPR(expr) \
    (sizeof(void) !=               \
     sizeof(*(1 ? ((void*)((expr)*0L)) : ((struct { char v[sizeof(void) * 2]; }*)1))))
#endif
#endif
#if defined(HEDLEY__IS_CONSTEXPR)
#if !defined(HEDLEY_IS_CONSTANT)
#define HEDLEY_IS_CONSTANT(expr) HEDLEY__IS_CONSTEXPR(expr)
#endif
#define HEDLEY_REQUIRE_CONSTEXPR(expr) (HEDLEY__IS_CONSTEXPR(expr) ? (expr) : (-1))
#else
#if !defined(HEDLEY_IS_CONSTANT)
#define HEDLEY_IS_CONSTANT(expr) (0)
#endif
#define HEDLEY_REQUIRE_CONSTEXPR(expr) (expr)
#endif

#if defined(HEDLEY_BEGIN_C_DECLS)
#undef HEDLEY_BEGIN_C_DECLS
#endif
#if defined(HEDLEY_END_C_DECLS)
#undef HEDLEY_END_C_DECLS
#endif
#if defined(HEDLEY_C_DECL)
#undef HEDLEY_C_DECL
#endif
#if defined(__cplusplus)
#define HEDLEY_BEGIN_C_DECLS extern "C" {
#define HEDLEY_END_C_DECLS }
#define HEDLEY_C_DECL extern "C"
#else
#define HEDLEY_BEGIN_C_DECLS
#define HEDLEY_END_C_DECLS
#define HEDLEY_C_DECL
#endif

#if defined(HEDLEY_STATIC_ASSERT)
#undef HEDLEY_STATIC_ASSERT
#endif
#if !defined(__cplusplus) &&                                                         \
        ((defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 201112L)) ||             \
         HEDLEY_HAS_FEATURE(c_static_assert) || HEDLEY_GCC_VERSION_CHECK(6, 0, 0) || \
         HEDLEY_INTEL_VERSION_CHECK(13, 0, 0) || defined(_Static_assert))
#define HEDLEY_STATIC_ASSERT(expr, message) _Static_assert(expr, message)
#elif (defined(__cplusplus) && (__cplusplus >= 201703L)) || HEDLEY_MSVC_VERSION_CHECK(16, 0, 0) || \
        (defined(__cplusplus) && HEDLEY_TI_VERSION_CHECK(8, 3, 0))
#define HEDLEY_STATIC_ASSERT(expr, message) static_assert(expr, message)
#elif defined(__cplusplus) && (__cplusplus >= 201103L)
#define HEDLEY_STATIC_ASSERT(expr, message) static_assert(expr)
#else
#define HEDLEY_STATIC_ASSERT(expr, message)
#endif

#if defined(HEDLEY_CONST_CAST)
#undef HEDLEY_CONST_CAST
#endif
#if defined(__cplusplus)
#define HEDLEY_CONST_CAST(T, expr) (const_cast<T>(expr))
#elif HEDLEY_HAS_WARNING("-Wcast-qual") || HEDLEY_GCC_VERSION_CHECK(4, 6, 0) || \
        HEDLEY_INTEL_VERSION_CHECK(13, 0, 0)
#define HEDLEY_CONST_CAST(T, expr)                      \
    (__extension__({                                    \
        HEDLEY_DIAGNOSTIC_PUSH                          \
        HEDLEY_DIAGNOSTIC_DISABLE_CAST_QUAL((T)(expr)); \
        HEDLEY_DIAGNOSTIC_POP                           \
    }))
#else
#define HEDLEY_CONST_CAST(T, expr) ((T)(expr))
#endif

#if defined(HEDLEY_REINTERPRET_CAST)
#undef HEDLEY_REINTERPRET_CAST
#endif
#if defined(__cplusplus)
#define HEDLEY_REINTERPRET_CAST(T, expr) (reinterpret_cast<T>(expr))
#else
#define HEDLEY_REINTERPRET_CAST(T, expr) (*((T*)&(expr)))
#endif

#if defined(HEDLEY_STATIC_CAST)
#undef HEDLEY_STATIC_CAST
#endif
#if defined(__cplusplus)
#define HEDLEY_STATIC_CAST(T, expr) (static_cast<T>(expr))
#else
#define HEDLEY_STATIC_CAST(T, expr) ((T)(expr))
#endif

#if defined(HEDLEY_CPP_CAST)
#undef HEDLEY_CPP_CAST
#endif
#if defined(__cplusplus)
#define HEDLEY_CPP_CAST(T, expr) static_cast<T>(expr)
#else
#define HEDLEY_CPP_CAST(T, expr) (expr)
#endif

#if defined(HEDLEY_MESSAGE)
#undef HEDLEY_MESSAGE
#endif
#if HEDLEY_HAS_WARNING("-Wunknown-pragmas")
#define HEDLEY_MESSAGE(msg)                   \
    HEDLEY_DIAGNOSTIC_PUSH                    \
    HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS \
    HEDLEY_PRAGMA(message msg)                \
    HEDLEY_DIAGNOSTIC_POP
#elif HEDLEY_GCC_VERSION_CHECK(4, 4, 0) || HEDLEY_INTEL_VERSION_CHECK(13, 0, 0)
#define HEDLEY_MESSAGE(msg) HEDLEY_PRAGMA(message msg)
#elif HEDLEY_CRAY_VERSION_CHECK(5, 0, 0)
#define HEDLEY_MESSAGE(msg) HEDLEY_PRAGMA(_CRI message msg)
#elif HEDLEY_IAR_VERSION_CHECK(8, 0, 0)
#define HEDLEY_MESSAGE(msg) HEDLEY_PRAGMA(message(msg))
#elif HEDLEY_PELLES_VERSION_CHECK(2, 0, 0)
#define HEDLEY_MESSAGE(msg) HEDLEY_PRAGMA(message(msg))
#else
#define HEDLEY_MESSAGE(msg)
#endif

#if defined(HEDLEY_WARNING)
#undef HEDLEY_WARNING
#endif
#if HEDLEY_HAS_WARNING("-Wunknown-pragmas")
#define HEDLEY_WARNING(msg)                   \
    HEDLEY_DIAGNOSTIC_PUSH                    \
    HEDLEY_DIAGNOSTIC_DISABLE_UNKNOWN_PRAGMAS \
    HEDLEY_PRAGMA(clang warning msg)          \
    HEDLEY_DIAGNOSTIC_POP
#elif HEDLEY_GCC_VERSION_CHECK(4, 8, 0) || HEDLEY_PGI_VERSION_CHECK(18, 4, 0)
#define HEDLEY_WARNING(msg) HEDLEY_PRAGMA(GCC warning msg)
#elif HEDLEY_MSVC_VERSION_CHECK(15, 0, 0)
#define HEDLEY_WARNING(msg) HEDLEY_PRAGMA(message(msg))
#else
#define HEDLEY_WARNING(msg) HEDLEY_MESSAGE(msg)
#endif

#if defined(HEDLEY_REQUIRE_MSG)
#undef HEDLEY_REQUIRE_MSG
#endif
#if HEDLEY_HAS_ATTRIBUTE(diagnose_if)
#if HEDLEY_HAS_WARNING("-Wgcc-compat")
#define HEDLEY_REQUIRE_MSG(expr, msg)                    \
    HEDLEY_DIAGNOSTIC_PUSH                               \
    _Pragma("clang diagnostic ignored \"-Wgcc-compat\"") \
            __attribute__((__diagnose_if__(!(expr), msg, "error"))) HEDLEY_DIAGNOSTIC_POP
#else
#define HEDLEY_REQUIRE_MSG(expr, msg) __attribute__((__diagnose_if__(!(expr), msg, "error")))
#endif
#else
#define HEDLEY_REQUIRE_MSG(expr, msg)
#endif

#if defined(HEDLEY_REQUIRE)
#undef HEDLEY_REQUIRE
#endif
#define HEDLEY_REQUIRE(expr) HEDLEY_REQUIRE_MSG(expr, #expr)

#if defined(HEDLEY_FLAGS)
#undef HEDLEY_FLAGS
#endif
#if HEDLEY_HAS_ATTRIBUTE(flag_enum)
#define HEDLEY_FLAGS __attribute__((__flag_enum__))
#endif

#if defined(HEDLEY_FLAGS_CAST)
#undef HEDLEY_FLAGS_CAST
#endif
#if HEDLEY_INTEL_VERSION_CHECK(19, 0, 0)
#define HEDLEY_FLAGS_CAST(T, expr)                  \
    (__extension__({                                \
        HEDLEY_DIAGNOSTIC_PUSH                      \
        _Pragma("warning(disable:188)")((T)(expr)); \
        HEDLEY_DIAGNOSTIC_POP                       \
    }))
#else
#define HEDLEY_FLAGS_CAST(T, expr) HEDLEY_STATIC_CAST(T, expr)
#endif

/* Remaining macros are deprecated. */

#if defined(HEDLEY_GCC_NOT_CLANG_VERSION_CHECK)
#undef HEDLEY_GCC_NOT_CLANG_VERSION_CHECK
#endif
#if defined(__clang__)
#define HEDLEY_GCC_NOT_CLANG_VERSION_CHECK(major, minor, patch) (0)
#else
#define HEDLEY_GCC_NOT_CLANG_VERSION_CHECK(major, minor, patch) \
    HEDLEY_GCC_VERSION_CHECK(major, minor, patch)
#endif

#if defined(HEDLEY_CLANG_HAS_ATTRIBUTE)
#undef HEDLEY_CLANG_HAS_ATTRIBUTE
#endif
#define HEDLEY_CLANG_HAS_ATTRIBUTE(attribute) HEDLEY_HAS_ATTRIBUTE(attribute)

#if defined(HEDLEY_CLANG_HAS_CPP_ATTRIBUTE)
#undef HEDLEY_CLANG_HAS_CPP_ATTRIBUTE
#endif
#define HEDLEY_CLANG_HAS_CPP_ATTRIBUTE(attribute) HEDLEY_HAS_CPP_ATTRIBUTE(attribute)

#if defined(HEDLEY_CLANG_HAS_BUILTIN)
#undef HEDLEY_CLANG_HAS_BUILTIN
#endif
#define HEDLEY_CLANG_HAS_BUILTIN(builtin) HEDLEY_HAS_BUILTIN(builtin)

#if defined(HEDLEY_CLANG_HAS_FEATURE)
#undef HEDLEY_CLANG_HAS_FEATURE
#endif
#define HEDLEY_CLANG_HAS_FEATURE(feature) HEDLEY_HAS_FEATURE(feature)

#if defined(HEDLEY_CLANG_HAS_EXTENSION)
#undef HEDLEY_CLANG_HAS_EXTENSION
#endif
#define HEDLEY_CLANG_HAS_EXTENSION(extension) HEDLEY_HAS_EXTENSION(extension)

#if defined(HEDLEY_CLANG_HAS_DECLSPEC_DECLSPEC_ATTRIBUTE)
#undef HEDLEY_CLANG_HAS_DECLSPEC_DECLSPEC_ATTRIBUTE
#endif
#define HEDLEY_CLANG_HAS_DECLSPEC_ATTRIBUTE(attribute) HEDLEY_HAS_DECLSPEC_ATTRIBUTE(attribute)

#if defined(HEDLEY_CLANG_HAS_WARNING)
#undef HEDLEY_CLANG_HAS_WARNING
#endif
#define HEDLEY_CLANG_HAS_WARNING(warning) HEDLEY_HAS_WARNING(warning)

#endif /* !defined(HEDLEY_VERSION) || (HEDLEY_VERSION < X) */

namespace csv {
#ifdef _MSC_VER
#pragma region Compatibility Macros
#endif
/**
     *  @def IF_CONSTEXPR
     *  Expands to `if constexpr` in C++17 and `if` otherwise
     *
     *  @def CONSTEXPR_VALUE
     *  Expands to `constexpr` in C++17 and `const` otherwise.
     *  Mainly used for global variables.
     *
     *  @def CONSTEXPR
     *  Expands to `constexpr` in decent compilers and `inline` otherwise.
     *  Intended for functions and methods.
     */

#define STATIC_ASSERT(x) static_assert(x, "Assertion failed")

#if CMAKE_CXX_STANDARD == 17 || __cplusplus >= 201703L
#define CSV_HAS_CXX17
#endif

#if CMAKE_CXX_STANDARD >= 14 || __cplusplus >= 201402L
#define CSV_HAS_CXX14
#endif

#ifdef CSV_HAS_CXX17
#include <string_view>
/** @typedef string_view
      *  The string_view class used by this library.
      */
using string_view = std::string_view;
#else
/** @typedef string_view
      *  The string_view class used by this library.
      */
using string_view = nonstd::string_view;
#endif

#ifdef CSV_HAS_CXX17
#define IF_CONSTEXPR if constexpr
#define CONSTEXPR_VALUE constexpr

#define CONSTEXPR_17 constexpr
#else
#define IF_CONSTEXPR if
#define CONSTEXPR_VALUE const

#define CONSTEXPR_17 inline
#endif

#ifdef CSV_HAS_CXX14
template <bool B, class T = void>
using enable_if_t = std::enable_if_t<B, T>;

#define CONSTEXPR_14 constexpr
#define CONSTEXPR_VALUE_14 constexpr
#else
template <bool B, class T = void>
using enable_if_t = typename std::enable_if<B, T>::type;

#define CONSTEXPR_14 inline
#define CONSTEXPR_VALUE_14 const
#endif

// Resolves g++ bug with regard to constexpr methods
// See: https://stackoverflow.com/questions/36489369/constexpr-non-static-member-function-with-non-constexpr-constructor-gcc-clang-d
#if defined __GNUC__ && !defined __clang__
#if (__GNUC__ >= 7 && __GNUC_MINOR__ >= 2) || (__GNUC__ >= 8)
#define CONSTEXPR constexpr
#endif
#else
#ifdef CSV_HAS_CXX17
#define CONSTEXPR constexpr
#endif
#endif

#ifndef CONSTEXPR
#define CONSTEXPR inline
#endif

#ifdef _MSC_VER
#pragma endregion
#endif

namespace internals {
// PAGE_SIZE macro could be already defined by the host system.
#if defined(PAGE_SIZE)
#undef PAGE_SIZE
#endif

// Get operating system specific details
#if defined(_WIN32)
inline int getpagesize() {
    _SYSTEM_INFO sys_info = {};
    GetSystemInfo(&sys_info);
    return std::max(sys_info.dwPageSize, sys_info.dwAllocationGranularity);
}

const int PAGE_SIZE = getpagesize();
#elif defined(__linux__)
const int PAGE_SIZE = getpagesize();
#else
/** Size of a memory page in bytes. Used by
         *  csv::internals::CSVFieldArray when allocating blocks.
         */
const int PAGE_SIZE = 4096;
#endif

/** For functions that lazy load a large CSV, this determines how
         *  many bytes are read at a time
         */
constexpr size_t ITERATION_CHUNK_SIZE = 10000000; // 10MB

template <typename T>
inline bool is_equal(T a, T b, T epsilon = 0.001) {
    /** Returns true if two floating point values are about the same */
    static_assert(std::is_floating_point<T>::value, "T must be a floating point type.");
    return std::abs(a - b) < epsilon;
}

/**  @typedef ParseFlags
         *   An enum used for describing the significance of each character
         *   with respect to CSV parsing
         *
         *   @see quote_escape_flag
         */
enum class ParseFlags {
    QUOTE_ESCAPE_QUOTE = 0, /**< A quote inside or terminating a quote_escaped field */
    QUOTE = 2 | 1,          /**< Characters which may signify a quote escape */
    NOT_SPECIAL = 4,   /**< Characters with no special meaning or escaped delimiters and newlines */
    DELIMITER = 4 | 2, /**< Characters which signify a new field */
    NEWLINE = 4 | 2 | 1 /**< Characters which signify a new row */
};

/** Transform the ParseFlags given the context of whether or not the current
         *  field is quote escaped */
constexpr ParseFlags quote_escape_flag(ParseFlags flag, bool quote_escape) noexcept {
    return (ParseFlags)((int)flag & ~((int)ParseFlags::QUOTE * quote_escape));
}

// Assumed to be true by parsing functions: allows for testing
// if an item is DELIMITER or NEWLINE with a >= statement
STATIC_ASSERT(ParseFlags::DELIMITER < ParseFlags::NEWLINE);

/** Optimizations for reducing branching in parsing loop
         *
         *  Idea: The meaning of all non-quote characters changes depending
         *  on whether or not the parser is in a quote-escaped mode (0 or 1)
         */
STATIC_ASSERT(quote_escape_flag(ParseFlags::NOT_SPECIAL, false) == ParseFlags::NOT_SPECIAL);
STATIC_ASSERT(quote_escape_flag(ParseFlags::QUOTE, false) == ParseFlags::QUOTE);
STATIC_ASSERT(quote_escape_flag(ParseFlags::DELIMITER, false) == ParseFlags::DELIMITER);
STATIC_ASSERT(quote_escape_flag(ParseFlags::NEWLINE, false) == ParseFlags::NEWLINE);

STATIC_ASSERT(quote_escape_flag(ParseFlags::NOT_SPECIAL, true) == ParseFlags::NOT_SPECIAL);
STATIC_ASSERT(quote_escape_flag(ParseFlags::QUOTE, true) == ParseFlags::QUOTE_ESCAPE_QUOTE);
STATIC_ASSERT(quote_escape_flag(ParseFlags::DELIMITER, true) == ParseFlags::NOT_SPECIAL);
STATIC_ASSERT(quote_escape_flag(ParseFlags::NEWLINE, true) == ParseFlags::NOT_SPECIAL);

/** An array which maps ASCII chars to a parsing flag */
using ParseFlagMap = std::array<ParseFlags, 256>;

/** An array which maps ASCII chars to a flag indicating if it is whitespace */
using WhitespaceMap = std::array<bool, 256>;
} // namespace internals

/** Integer indicating a requested column wasn't found. */
constexpr int CSV_NOT_FOUND = -1;
} // namespace csv

namespace csv {
namespace internals {
struct ColNames;
using ColNamesPtr = std::shared_ptr<ColNames>;

/** @struct ColNames
             *  A data structure for handling column name information.
             *
             *  These are created by CSVReader and passed (via smart pointer)
             *  to CSVRow objects it creates, thus
             *  allowing for indexing by column name.
             */
struct ColNames {
public:
    ColNames() = default;
    ColNames(const std::vector<std::string>& names) { set_col_names(names); }

    std::vector<std::string> get_col_names() const;
    void set_col_names(const std::vector<std::string>&);
    int index_of(csv::string_view) const;

    bool empty() const noexcept { return this->col_names.empty(); }
    size_t size() const noexcept;

private:
    std::vector<std::string> col_names;
    std::unordered_map<std::string, size_t> col_pos;
};
} // namespace internals
} // namespace csv
/** @file
 *  Defines an object used to store CSV format settings
 */

#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

namespace csv {
namespace internals {
class IBasicCSVParser;
}

class CSVReader;

/** Determines how to handle rows that are shorter or longer than the majority */
enum class VariableColumnPolicy { THROW = -1, IGNORE_ROW = 0, KEEP = 1 };

/** Stores the inferred format of a CSV file. */
struct CSVGuessResult {
    char delim;
    int header_row;
};

/** Stores information about how to parse a CSV file.
     *  Can be used to construct a csv::CSVReader. 
     */
class CSVFormat {
public:
    /** Settings for parsing a RFC 4180 CSV file */
    CSVFormat() = default;

    /** Sets the delimiter of the CSV file
         *
         *  @throws `std::runtime_error` thrown if trim, quote, or possible delimiting characters overlap
         */
    CSVFormat& delimiter(char delim);

    /** Sets a list of potential delimiters
         *  
         *  @throws `std::runtime_error` thrown if trim, quote, or possible delimiting characters overlap
         *  @param[in] delim An array of possible delimiters to try parsing the CSV with
         */
    CSVFormat& delimiter(const std::vector<char>& delim);

    /** Sets the whitespace characters to be trimmed
         *
         *  @throws `std::runtime_error` thrown if trim, quote, or possible delimiting characters overlap
         *  @param[in] ws An array of whitespace characters that should be trimmed
         */
    CSVFormat& trim(const std::vector<char>& ws);

    /** Sets the quote character
         *
         *  @throws `std::runtime_error` thrown if trim, quote, or possible delimiting characters overlap
         */
    CSVFormat& quote(char quote);

    /** Sets the column names.
         *
         *  @note Unsets any values set by header_row()
         */
    CSVFormat& column_names(const std::vector<std::string>& names);

    /** Sets the header row
         *
         *  @note Unsets any values set by column_names()
         */
    CSVFormat& header_row(int row);

    /** Tells the parser that this CSV has no header row
         *
         *  @note Equivalent to `header_row(-1)`
         *
         */
    CSVFormat& no_header() {
        this->header_row(-1);
        return *this;
    }

    /** Turn quoting on or off */
    CSVFormat& quote(bool use_quote) {
        this->no_quote = !use_quote;
        return *this;
    }

    /** Tells the parser how to handle columns of a different length than the others */
    CONSTEXPR_14 CSVFormat& variable_columns(
            VariableColumnPolicy policy = VariableColumnPolicy::IGNORE_ROW) {
        this->variable_column_policy = policy;
        return *this;
    }

    /** Tells the parser how to handle columns of a different length than the others */
    CONSTEXPR_14 CSVFormat& variable_columns(bool policy) {
        this->variable_column_policy = (VariableColumnPolicy)policy;
        return *this;
    }

#ifndef DOXYGEN_SHOULD_SKIP_THIS
    char get_delim() const {
        // This error should never be received by end users.
        if (this->possible_delimiters.size() > 1) {
            throw std::runtime_error("There is more than one possible delimiter.");
        }

        return this->possible_delimiters.at(0);
    }

    CONSTEXPR bool is_quoting_enabled() const { return !this->no_quote; }
    CONSTEXPR char get_quote_char() const { return this->quote_char; }
    CONSTEXPR int get_header() const { return this->header; }
    std::vector<char> get_possible_delims() const { return this->possible_delimiters; }
    std::vector<char> get_trim_chars() const { return this->trim_chars; }
    CONSTEXPR VariableColumnPolicy get_variable_column_policy() const {
        return this->variable_column_policy;
    }
#endif

    /** CSVFormat for guessing the delimiter */
    CSV_INLINE static CSVFormat guess_csv() {
        CSVFormat format;
        format.delimiter({',', '|', '\t', ';', '^'}).quote('"').header_row(0);

        return format;
    }

    bool guess_delim() { return this->possible_delimiters.size() > 1; }

    friend CSVReader;
    friend internals::IBasicCSVParser;

private:
    /**< Throws an error if delimiters and trim characters overlap */
    void assert_no_char_overlap();

    /**< Set of possible delimiters */
    std::vector<char> possible_delimiters = {','};

    /**< Set of whitespace characters to trim */
    std::vector<char> trim_chars = {};

    /**< Row number with columns (ignored if col_names is non-empty) */
    int header = 0;

    /**< Whether or not to use quoting */
    bool no_quote = false;

    /**< Quote character */
    char quote_char = '"';

    /**< Should be left empty unless file doesn't include header */
    std::vector<std::string> col_names = {};

    /**< Allow variable length columns? */
    VariableColumnPolicy variable_column_policy = VariableColumnPolicy::IGNORE_ROW;
};
} // namespace csv
/** @file
 *  Defines the data type used for storing information about a CSV row
 */

#include <cmath>
#include <iterator>
#include <limits> // For CSVField
#include <memory> // For CSVField
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/** @file
 *  @brief Implements data type parsing functionality
 */

#include <cassert>
#include <cctype>
#include <cmath>
#include <string>

namespace csv {
/** Enumerates the different CSV field types that are
     *  recognized by this library
     *
     *  @note Overflowing integers will be stored and classified as doubles.
     *  @note Unlike previous releases, integer enums here are platform agnostic.
     */
enum class DataType {
    UNKNOWN = -1,
    CSV_NULL,   /**< Empty string */
    CSV_STRING, /**< Non-numeric string */
    CSV_INT8,   /**< 8-bit integer */
    CSV_INT16,  /**< 16-bit integer (short on MSVC/GCC) */
    CSV_INT32,  /**< 32-bit integer (int on MSVC/GCC) */
    CSV_INT64,  /**< 64-bit integer (long long on MSVC/GCC) */
    CSV_DOUBLE  /**< Floating point value */
};

static_assert(DataType::CSV_STRING < DataType::CSV_INT8,
              "String type should come before numeric types.");
static_assert(DataType::CSV_INT8 < DataType::CSV_INT64,
              "Smaller integer types should come before larger integer types.");
static_assert(DataType::CSV_INT64 < DataType::CSV_DOUBLE,
              "Integer types should come before floating point value types.");

namespace internals {
/** Compute 10 to the power of n */
template <typename T>
HEDLEY_CONST CONSTEXPR_14 long double pow10(const T& n) noexcept {
    long double multiplicand = n > 0 ? 10 : 0.1, ret = 1;

    // Make all numbers positive
    T iterations = n > 0 ? n : -n;

    for (T i = 0; i < iterations; i++) {
        ret *= multiplicand;
    }

    return ret;
}

/** Compute 10 to the power of n */
template <>
HEDLEY_CONST CONSTEXPR_14 long double pow10(const unsigned& n) noexcept {
    long double multiplicand = n > 0 ? 10 : 0.1, ret = 1;

    for (unsigned i = 0; i < n; i++) {
        ret *= multiplicand;
    }

    return ret;
}

#ifndef DOXYGEN_SHOULD_SKIP_THIS
/** Private site-indexed array mapping byte sizes to an integer size enum */
constexpr DataType int_type_arr[8] = {
        DataType::CSV_INT8,  // 1
        DataType::CSV_INT16, // 2
        DataType::UNKNOWN,
        DataType::CSV_INT32, // 4
        DataType::UNKNOWN,   DataType::UNKNOWN, DataType::UNKNOWN,
        DataType::CSV_INT64 // 8
};

template <typename T>
inline DataType type_num() {
    static_assert(std::is_integral<T>::value, "T should be an integral type.");
    static_assert(sizeof(T) <= 8, "Byte size must be no greater than 8.");
    return int_type_arr[sizeof(T) - 1];
}

template <>
inline DataType type_num<float>() {
    return DataType::CSV_DOUBLE;
}
template <>
inline DataType type_num<double>() {
    return DataType::CSV_DOUBLE;
}
template <>
inline DataType type_num<long double>() {
    return DataType::CSV_DOUBLE;
}
template <>
inline DataType type_num<std::nullptr_t>() {
    return DataType::CSV_NULL;
}
template <>
inline DataType type_num<std::string>() {
    return DataType::CSV_STRING;
}

CONSTEXPR_14 DataType data_type(csv::string_view in, long double* const out = nullptr);
#endif

/** Given a byte size, return the largest number than can be stored in
         *  an integer of that size
         *
         *  Note: Provides a platform-agnostic way of mapping names like "long int" to
         *  byte sizes
         */
template <size_t Bytes>
CONSTEXPR_14 long double get_int_max() {
    static_assert(Bytes == 1 || Bytes == 2 || Bytes == 4 || Bytes == 8,
                  "Bytes must be a power of 2 below 8.");

    IF_CONSTEXPR(sizeof(signed char) == Bytes) {
        return (long double)std::numeric_limits<signed char>::max();
    }

    IF_CONSTEXPR(sizeof(short) == Bytes) {
        return (long double)std::numeric_limits<short>::max();
    }

    IF_CONSTEXPR(sizeof(int) == Bytes) {
        return (long double)std::numeric_limits<int>::max();
    }

    IF_CONSTEXPR(sizeof(long int) == Bytes) {
        return (long double)std::numeric_limits<long int>::max();
    }

    IF_CONSTEXPR(sizeof(long long int) == Bytes) {
        return (long double)std::numeric_limits<long long int>::max();
    }

    HEDLEY_UNREACHABLE();
}

/** Given a byte size, return the largest number than can be stored in
         *  an unsigned integer of that size
         */
template <size_t Bytes>
CONSTEXPR_14 long double get_uint_max() {
    static_assert(Bytes == 1 || Bytes == 2 || Bytes == 4 || Bytes == 8,
                  "Bytes must be a power of 2 below 8.");

    IF_CONSTEXPR(sizeof(unsigned char) == Bytes) {
        return (long double)std::numeric_limits<unsigned char>::max();
    }

    IF_CONSTEXPR(sizeof(unsigned short) == Bytes) {
        return (long double)std::numeric_limits<unsigned short>::max();
    }

    IF_CONSTEXPR(sizeof(unsigned int) == Bytes) {
        return (long double)std::numeric_limits<unsigned int>::max();
    }

    IF_CONSTEXPR(sizeof(unsigned long int) == Bytes) {
        return (long double)std::numeric_limits<unsigned long int>::max();
    }

    IF_CONSTEXPR(sizeof(unsigned long long int) == Bytes) {
        return (long double)std::numeric_limits<unsigned long long int>::max();
    }

    HEDLEY_UNREACHABLE();
}

/** Largest number that can be stored in a 8-bit integer */
CONSTEXPR_VALUE_14 long double CSV_INT8_MAX = get_int_max<1>();

/** Largest number that can be stored in a 16-bit integer */
CONSTEXPR_VALUE_14 long double CSV_INT16_MAX = get_int_max<2>();

/** Largest number that can be stored in a 32-bit integer */
CONSTEXPR_VALUE_14 long double CSV_INT32_MAX = get_int_max<4>();

/** Largest number that can be stored in a 64-bit integer */
CONSTEXPR_VALUE_14 long double CSV_INT64_MAX = get_int_max<8>();

/** Largest number that can be stored in a 8-bit ungisned integer */
CONSTEXPR_VALUE_14 long double CSV_UINT8_MAX = get_uint_max<1>();

/** Largest number that can be stored in a 16-bit unsigned integer */
CONSTEXPR_VALUE_14 long double CSV_UINT16_MAX = get_uint_max<2>();

/** Largest number that can be stored in a 32-bit unsigned integer */
CONSTEXPR_VALUE_14 long double CSV_UINT32_MAX = get_uint_max<4>();

/** Largest number that can be stored in a 64-bit unsigned integer */
CONSTEXPR_VALUE_14 long double CSV_UINT64_MAX = get_uint_max<8>();

/** Given a pointer to the start of what is start of
         *  the exponential part of a number written (possibly) in scientific notation
         *  parse the exponent
         */
HEDLEY_PRIVATE CONSTEXPR_14 DataType _process_potential_exponential(
        csv::string_view exponential_part, const long double& coeff, long double* const out) {
    long double exponent = 0;
    auto result = data_type(exponential_part, &exponent);

    // Exponents in scientific notation should not be decimal numbers
    if (result >= DataType::CSV_INT8 && result < DataType::CSV_DOUBLE) {
        if (out) *out = coeff * pow10(exponent);
        return DataType::CSV_DOUBLE;
    }

    return DataType::CSV_STRING;
}

/** Given the absolute value of an integer, determine what numeric type
         *  it fits in
         */
HEDLEY_PRIVATE HEDLEY_PURE CONSTEXPR_14 DataType
_determine_integral_type(const long double& number) noexcept {
    // We can assume number is always non-negative
    assert(number >= 0);

    if (number <= internals::CSV_INT8_MAX)
        return DataType::CSV_INT8;
    else if (number <= internals::CSV_INT16_MAX)
        return DataType::CSV_INT16;
    else if (number <= internals::CSV_INT32_MAX)
        return DataType::CSV_INT32;
    else if (number <= internals::CSV_INT64_MAX)
        return DataType::CSV_INT64;
    else // Conversion to long long will cause an overflow
        return DataType::CSV_DOUBLE;
}

/** Distinguishes numeric from other text values. Used by various
         *  type casting functions, like csv_parser::CSVReader::read_row()
         *
         *  #### Rules
         *   - Leading and trailing whitespace ("padding") ignored
         *   - A string of just whitespace is NULL
         *
         *  @param[in]  in  String value to be examined
         *  @param[out] out Pointer to long double where results of numeric parsing
         *                  get stored
         */
CONSTEXPR_14
DataType data_type(csv::string_view in, long double* const out) {
    // Empty string --> NULL
    if (in.size() == 0) return DataType::CSV_NULL;

    bool ws_allowed = true, neg_allowed = true, dot_allowed = true, digit_allowed = true,
         has_digit = false, prob_float = false;

    unsigned places_after_decimal = 0;
    long double integral_part = 0, decimal_part = 0;

    for (size_t i = 0, ilen = in.size(); i < ilen; i++) {
        const char& current = in[i];

        switch (current) {
        case ' ':
            if (!ws_allowed) {
                if (isdigit(in[i - 1])) {
                    digit_allowed = false;
                    ws_allowed = true;
                } else {
                    // Ex: '510 123 4567'
                    return DataType::CSV_STRING;
                }
            }
            break;
        case '-':
            if (!neg_allowed) {
                // Ex: '510-123-4567'
                return DataType::CSV_STRING;
            }

            neg_allowed = false;
            break;
        case '.':
            if (!dot_allowed) {
                return DataType::CSV_STRING;
            }

            dot_allowed = false;
            prob_float = true;
            break;
        case 'e':
        case 'E':
            // Process scientific notation
            if (prob_float || (i && i + 1 < ilen && isdigit(in[i - 1]))) {
                size_t exponent_start_idx = i + 1;
                prob_float = true;

                // Strip out plus sign
                if (in[i + 1] == '+') {
                    exponent_start_idx++;
                }

                return _process_potential_exponential(in.substr(exponent_start_idx),
                                                      neg_allowed ? integral_part + decimal_part
                                                                  : -(integral_part + decimal_part),
                                                      out);
            }

            return DataType::CSV_STRING;
            break;
        default:
            short digit = static_cast<short>(current - '0');
            if (digit >= 0 && digit <= 9) {
                // Process digit
                has_digit = true;

                if (!digit_allowed)
                    return DataType::CSV_STRING;
                else if (ws_allowed) // Ex: '510 456'
                    ws_allowed = false;

                // Build current number
                if (prob_float)
                    decimal_part += digit / pow10(++places_after_decimal);
                else
                    integral_part = (integral_part * 10) + digit;
            } else {
                return DataType::CSV_STRING;
            }
        }
    }

    // No non-numeric/non-whitespace characters found
    if (has_digit) {
        long double number = integral_part + decimal_part;
        if (out) {
            *out = neg_allowed ? number : -number;
        }

        return prob_float ? DataType::CSV_DOUBLE : _determine_integral_type(number);
    }

    // Just whitespace
    return DataType::CSV_NULL;
}
} // namespace internals
} // namespace csv

namespace csv {
namespace internals {
class IBasicCSVParser;

static const std::string ERROR_NAN = "Not a number.";
static const std::string ERROR_OVERFLOW = "Overflow error.";
static const std::string ERROR_FLOAT_TO_INT =
        "Attempted to convert a floating point value to an integral type.";
static const std::string ERROR_NEG_TO_UNSIGNED =
        "Negative numbers cannot be converted to unsigned types.";

std::string json_escape_string(csv::string_view s) noexcept;

/** A barebones class used for describing CSV fields */
struct RawCSVField {
    RawCSVField() = default;
    RawCSVField(size_t _start, size_t _length, bool _double_quote = false) {
        start = _start;
        length = _length;
        has_double_quote = _double_quote;
    }

    /** The start of the field, relative to the beginning of the row */
    size_t start;

    /** The length of the row, ignoring quote escape characters */
    size_t length;

    /** Whether or not the field contains an escaped quote */
    bool has_double_quote;
};

/** A class used for efficiently storing RawCSVField objects and expanding as necessary
         *
         *  @par Implementation
         *  This data structure stores RawCSVField in continguous blocks. When more capacity
         *  is needed, a new block is allocated, but previous data stays put.
         *
         *  @par Thread Safety
         *  This class may be safely read from multiple threads and written to from one,
         *  as long as the writing thread does not actively touch fields which are being
         *  read.
         */
class CSVFieldList {
public:
    /** Construct a CSVFieldList which allocates blocks of a certain size */
    CSVFieldList(size_t single_buffer_capacity = (size_t)(internals::PAGE_SIZE /
                                                          sizeof(RawCSVField)))
            : _single_buffer_capacity(single_buffer_capacity) {
        this->allocate();
    }

    // No copy constructor
    CSVFieldList(const CSVFieldList& other) = delete;

    // CSVFieldArrays may be moved
    CSVFieldList(CSVFieldList&& other) : _single_buffer_capacity(other._single_buffer_capacity) {
        buffers = std::move(other.buffers);
        _current_buffer_size = other._current_buffer_size;
        _back = other._back;
    }

    ~CSVFieldList() {
        for (auto& buffer : buffers) delete[] buffer;
    }

    template <class... Args>
    void emplace_back(Args&&... args) {
        if (this->_current_buffer_size == this->_single_buffer_capacity) {
            this->allocate();
        }

        *(_back++) = RawCSVField(std::forward<Args>(args)...);
        _current_buffer_size++;
    }

    size_t size() const noexcept {
        return this->_current_buffer_size +
               ((this->buffers.size() - 1) * this->_single_buffer_capacity);
    }

    RawCSVField& operator[](size_t n) const;

private:
    const size_t _single_buffer_capacity;

    std::vector<RawCSVField*> buffers = {};

    /** Number of items in the current buffer */
    size_t _current_buffer_size = 0;

    /** Pointer to the current empty field */
    RawCSVField* _back = nullptr;

    /** Allocate a new page of memory */
    void allocate();
};

/** A class for storing raw CSV data and associated metadata */
struct RawCSVData {
    std::shared_ptr<void> _data = nullptr;
    csv::string_view data = "";

    internals::CSVFieldList fields;

    std::unordered_set<size_t> has_double_quotes = {};

    // TODO: Consider replacing with a more thread-safe structure
    std::unordered_map<size_t, std::string> double_quote_fields = {};

    internals::ColNamesPtr col_names = nullptr;
    internals::ParseFlagMap parse_flags;
    internals::WhitespaceMap ws_flags;
};

using RawCSVDataPtr = std::shared_ptr<RawCSVData>;
} // namespace internals

/**
    * @class CSVField
    * @brief Data type representing individual CSV values.
    *        CSVFields can be obtained by using CSVRow::operator[]
    */
class CSVField {
public:
    /** Constructs a CSVField from a string_view */
    constexpr explicit CSVField(csv::string_view _sv) noexcept : sv(_sv) {};

    operator std::string() const { return std::string("<CSVField> ") + std::string(this->sv); }

    /** Returns the value casted to the requested type, performing type checking before.
        *
        *  \par Valid options for T
        *   - std::string or csv::string_view
        *   - signed integral types (signed char, short, int, long int, long long int)
        *   - floating point types (float, double, long double)
        *   - unsigned integers are not supported at this time, but may be in a later release
        *
        *  \par Invalid conversions
        *   - Converting non-numeric values to any numeric type
        *   - Converting floating point values to integers
        *   - Converting a large integer to a smaller type that will not hold it
        *
        *  @note    This method is capable of parsing scientific E-notation.
        *           See [this page](md_docs_source_scientific_notation.html)
        *           for more details.
        *
        *  @throws  std::runtime_error Thrown if an invalid conversion is performed.
        *
        *  @warning Currently, conversions to floating point types are not
        *           checked for loss of precision
        *
        *  @warning Any string_views returned are only guaranteed to be valid
        *           if the parent CSVRow is still alive. If you are concerned
        *           about object lifetimes, then grab a std::string or a
        *           numeric value.
        *
        */
    template <typename T = std::string>
    T get() {
        IF_CONSTEXPR(std::is_arithmetic<T>::value) {
            // Note: this->type() also converts the CSV value to float
            if (this->type() <= DataType::CSV_STRING) {
                throw std::runtime_error(internals::ERROR_NAN);
            }
        }

        IF_CONSTEXPR(std::is_integral<T>::value) {
            // Note: this->is_float() also converts the CSV value to float
            if (this->is_float()) {
                throw std::runtime_error(internals::ERROR_FLOAT_TO_INT);
            }

            IF_CONSTEXPR(std::is_unsigned<T>::value) {
                if (this->value < 0) {
                    throw std::runtime_error(internals::ERROR_NEG_TO_UNSIGNED);
                }
            }
        }

        // Allow fallthrough from previous if branch
        IF_CONSTEXPR(!std::is_floating_point<T>::value) {
            IF_CONSTEXPR(std::is_unsigned<T>::value) {
                // Quick hack to perform correct unsigned integer boundary checks
                if (this->value > internals::get_uint_max<sizeof(T)>()) {
                    throw std::runtime_error(internals::ERROR_OVERFLOW);
                }
            }
            else if (internals::type_num<T>() < this->_type) {
                throw std::runtime_error(internals::ERROR_OVERFLOW);
            }
        }

        return static_cast<T>(this->value);
    }

    /** Parse a hexadecimal value, returning false if the value is not hex. */
    bool try_parse_hex(int& parsedValue);

    /** Compares the contents of this field to a numeric value. If this
         *  field does not contain a numeric value, then all comparisons return
         *  false.
         *
         *  @note    Floating point values are considered equal if they are within
         *           `0.000001` of each other.
         *
         *  @warning Multiple numeric comparisons involving the same field can
         *           be done more efficiently by calling the CSVField::get<>() method.
         *
         *  @sa      csv::CSVField::operator==(const char * other)
         *  @sa      csv::CSVField::operator==(csv::string_view other)
         */
    template <typename T>
    CONSTEXPR_14 bool operator==(T other) const noexcept {
        static_assert(std::is_arithmetic<T>::value, "T should be a numeric value.");

        if (this->_type != DataType::UNKNOWN) {
            if (this->_type == DataType::CSV_STRING) {
                return false;
            }

            return internals::is_equal(value, static_cast<long double>(other), 0.000001L);
        }

        long double out = 0;
        if (internals::data_type(this->sv, &out) == DataType::CSV_STRING) {
            return false;
        }

        return internals::is_equal(out, static_cast<long double>(other), 0.000001L);
    }

    /** Return a string view over the field's contents */
    CONSTEXPR csv::string_view get_sv() const noexcept { return this->sv; }

    /** Returns true if field is an empty string or string of whitespace characters */
    CONSTEXPR_14 bool is_null() noexcept { return type() == DataType::CSV_NULL; }

    /** Returns true if field is a non-numeric, non-empty string */
    CONSTEXPR_14 bool is_str() noexcept { return type() == DataType::CSV_STRING; }

    /** Returns true if field is an integer or float */
    CONSTEXPR_14 bool is_num() noexcept { return type() >= DataType::CSV_INT8; }

    /** Returns true if field is an integer */
    CONSTEXPR_14 bool is_int() noexcept {
        return (type() >= DataType::CSV_INT8) && (type() <= DataType::CSV_INT64);
    }

    /** Returns true if field is a floating point value */
    CONSTEXPR_14 bool is_float() noexcept { return type() == DataType::CSV_DOUBLE; };

    /** Return the type of the underlying CSV data */
    CONSTEXPR_14 DataType type() noexcept {
        this->get_value();
        return _type;
    }

private:
    long double value = 0;              /**< Cached numeric value */
    csv::string_view sv = "";           /**< A pointer to this field's text */
    DataType _type = DataType::UNKNOWN; /**< Cached data type value */
    CONSTEXPR_14 void get_value() noexcept {
        /* Check to see if value has been cached previously, if not
             * evaluate it
             */
        if ((int)_type < 0) {
            this->_type = internals::data_type(this->sv, &this->value);
        }
    }
};

/** Data structure for representing CSV rows */
class CSVRow {
public:
    friend internals::IBasicCSVParser;

    CSVRow() = default;

    /** Construct a CSVRow from a RawCSVDataPtr */
    CSVRow(internals::RawCSVDataPtr _data) : data(_data) {}
    CSVRow(internals::RawCSVDataPtr _data, size_t _data_start, size_t _field_bounds)
            : data(_data), data_start(_data_start), fields_start(_field_bounds) {}

    /** Indicates whether row is empty or not */
    CONSTEXPR bool empty() const noexcept { return this->size() == 0; }

    /** Return the number of fields in this row */
    CONSTEXPR size_t size() const noexcept { return row_length; }

    /** @name Value Retrieval */
    ///@{
    CSVField operator[](size_t n) const;
    CSVField operator[](const std::string&) const;
    std::string to_json(const std::vector<std::string>& subset = {}) const;
    std::string to_json_array(const std::vector<std::string>& subset = {}) const;

    /** Retrieve this row's associated column names */
    std::vector<std::string> get_col_names() const {
        return this->data->col_names->get_col_names();
    }

    /** Convert this CSVRow into a vector of strings.
         *  **Note**: This is a less efficient method of
         *  accessing data than using the [] operator.
         */
    operator std::vector<std::string>() const;
    ///@}

    /** A random access iterator over the contents of a CSV row.
         *  Each iterator points to a CSVField.
         */
    class iterator {
    public:
#ifndef DOXYGEN_SHOULD_SKIP_THIS
        using value_type = CSVField;
        using difference_type = int;

        // Using CSVField * as pointer type causes segfaults in MSVC debug builds
        // but using shared_ptr as pointer type won't compile in g++
#ifdef _MSC_BUILD
        using pointer = std::shared_ptr<CSVField>;
#else
        using pointer = CSVField*;
#endif

        using reference = CSVField&;
        using iterator_category = std::random_access_iterator_tag;
#endif
        iterator(const CSVRow*, int i);

        reference operator*() const;
        pointer operator->() const;

        iterator operator++(int);
        iterator& operator++();
        iterator operator--(int);
        iterator& operator--();
        iterator operator+(difference_type n) const;
        iterator operator-(difference_type n) const;

        /** Two iterators are equal if they point to the same field */
        CONSTEXPR bool operator==(const iterator& other) const noexcept {
            return this->i == other.i;
        };

        CONSTEXPR bool operator!=(const iterator& other) const noexcept {
            return !operator==(other);
        }

#ifndef NDEBUG
        friend CSVRow;
#endif

    private:
        const CSVRow* daddy = nullptr;             // Pointer to parent
        std::shared_ptr<CSVField> field = nullptr; // Current field pointed at
        int i = 0;                                 // Index of current field
    };

    /** A reverse iterator over the contents of a CSVRow. */
    using reverse_iterator = std::reverse_iterator<iterator>;

    /** @name Iterators
         *  @brief Each iterator points to a CSVField object.
         */
    ///@{
    iterator begin() const;
    iterator end() const noexcept;
    reverse_iterator rbegin() const noexcept;
    reverse_iterator rend() const;
    ///@}

private:
    /** Retrieve a string view corresponding to the specified index */
    csv::string_view get_field(size_t index) const;

    internals::RawCSVDataPtr data;

    /** Where in RawCSVData.data we start */
    size_t data_start = 0;

    /** Where in the RawCSVDataPtr.fields array we start */
    size_t fields_start = 0;

    /** How many columns this row spans */
    size_t row_length = 0;
};

#ifdef _MSC_VER
#pragma region CSVField::get Specializations
#endif
/** Retrieve this field's original string */
template <>
inline std::string CSVField::get<std::string>() {
    return std::string(this->sv);
}

/** Retrieve a view over this field's string
     *
     *  @warning This string_view is only guaranteed to be valid as long as this
     *           CSVRow is still alive.
     */
template <>
CONSTEXPR_14 csv::string_view CSVField::get<csv::string_view>() {
    return this->sv;
}

/** Retrieve this field's value as a long double */
template <>
CONSTEXPR_14 long double CSVField::get<long double>() {
    if (!is_num()) throw std::runtime_error(internals::ERROR_NAN);

    return this->value;
}
#ifdef _MSC_VER
#pragma endregion CSVField::get Specializations
#endif

/** Compares the contents of this field to a string */
template <>
CONSTEXPR bool CSVField::operator==(const char* other) const noexcept {
    return this->sv == other;
}

/** Compares the contents of this field to a string */
template <>
CONSTEXPR bool CSVField::operator==(csv::string_view other) const noexcept {
    return this->sv == other;
}
} // namespace csv

inline std::ostream& operator<<(std::ostream& os, csv::CSVField const& value) {
    os << std::string(value);
    return os;
}

namespace csv {
namespace internals {
/** Create a vector v where each index i corresponds to the
         *  ASCII number for a character and, v[i + 128] labels it according to
         *  the CSVReader::ParseFlags enum
         */
HEDLEY_CONST CONSTEXPR_17 ParseFlagMap make_parse_flags(char delimiter) {
    std::array<ParseFlags, 256> ret = {};
    for (int i = -128; i < 128; i++) {
        const int arr_idx = i + 128;
        char ch = char(i);

        if (ch == delimiter)
            ret[arr_idx] = ParseFlags::DELIMITER;
        else if (ch == '\r' || ch == '\n')
            ret[arr_idx] = ParseFlags::NEWLINE;
        else
            ret[arr_idx] = ParseFlags::NOT_SPECIAL;
    }

    return ret;
}

/** Create a vector v where each index i corresponds to the
         *  ASCII number for a character and, v[i + 128] labels it according to
         *  the CSVReader::ParseFlags enum
         */
HEDLEY_CONST CONSTEXPR_17 ParseFlagMap make_parse_flags(char delimiter, char quote_char) {
    std::array<ParseFlags, 256> ret = make_parse_flags(delimiter);
    ret[(size_t)quote_char + 128] = ParseFlags::QUOTE;
    return ret;
}

/** Create a vector v where each index i corresponds to the
         *  ASCII number for a character c and, v[i + 128] is true if
         *  c is a whitespace character
         */
HEDLEY_CONST CONSTEXPR_17 WhitespaceMap make_ws_flags(const char* ws_chars, size_t n_chars) {
    std::array<bool, 256> ret = {};
    for (int i = -128; i < 128; i++) {
        const int arr_idx = i + 128;
        char ch = char(i);
        ret[arr_idx] = false;

        for (size_t j = 0; j < n_chars; j++) {
            if (ws_chars[j] == ch) {
                ret[arr_idx] = true;
            }
        }
    }

    return ret;
}

inline WhitespaceMap make_ws_flags(const std::vector<char>& flags) {
    return make_ws_flags(flags.data(), flags.size());
}

CSV_INLINE size_t get_file_size(csv::string_view filename);

CSV_INLINE std::string get_csv_head(csv::string_view filename);

/** Read the first 500KB of a CSV file */
CSV_INLINE std::string get_csv_head(csv::string_view filename, size_t file_size);

/** A std::deque wrapper which allows multiple read and write threads to concurrently
         *  access it along with providing read threads the ability to wait for the deque
         *  to become populated
         */
template <typename T>
class ThreadSafeDeque {
public:
    ThreadSafeDeque(size_t notify_size = 100) : _notify_size(notify_size) {};
    ThreadSafeDeque(const ThreadSafeDeque& other) {
        this->data = other.data;
        this->_notify_size = other._notify_size;
    }

    ThreadSafeDeque(const std::deque<T>& source) : ThreadSafeDeque() { this->data = source; }

    void clear() noexcept { this->data.clear(); }

    bool empty() const noexcept { return this->data.empty(); }

    T& front() noexcept { return this->data.front(); }

    T& operator[](size_t n) { return this->data[n]; }

    void push_back(T&& item) {
        std::lock_guard<std::mutex> lock {this->_lock};
        this->data.push_back(std::move(item));

        if (this->size() >= _notify_size) {
            this->_cond.notify_all();
        }
    }

    T pop_front() noexcept {
        std::lock_guard<std::mutex> lock {this->_lock};
        T item = std::move(data.front());
        data.pop_front();
        return item;
    }

    size_t size() const noexcept { return this->data.size(); }

    /** Returns true if a thread is actively pushing items to this deque */
    constexpr bool is_waitable() const noexcept { return this->_is_waitable; }

    /** Wait for an item to become available */
    void wait() {
        if (!is_waitable()) {
            return;
        }

        std::unique_lock<std::mutex> lock {this->_lock};
        this->_cond.wait(lock,
                         [this] { return this->size() >= _notify_size || !this->is_waitable(); });
        lock.unlock();
    }

    typename std::deque<T>::iterator begin() noexcept { return this->data.begin(); }

    typename std::deque<T>::iterator end() noexcept { return this->data.end(); }

    /** Tell listeners that this deque is actively being pushed to */
    void notify_all() {
        std::unique_lock<std::mutex> lock {this->_lock};
        this->_is_waitable = true;
        this->_cond.notify_all();
    }

    /** Tell all listeners to stop */
    void kill_all() {
        std::unique_lock<std::mutex> lock {this->_lock};
        this->_is_waitable = false;
        this->_cond.notify_all();
    }

private:
    bool _is_waitable = false;
    size_t _notify_size;
    std::mutex _lock;
    std::condition_variable _cond;
    std::deque<T> data;
};

constexpr const int UNINITIALIZED_FIELD = -1;
} // namespace internals

/** Standard type for storing collection of rows */
using RowCollection = internals::ThreadSafeDeque<CSVRow>;

namespace internals {
/** Abstract base class which provides CSV parsing logic.
         *
         *  Concrete implementations may customize this logic across
         *  different input sources, such as memory mapped files, stringstreams,
         *  etc...
         */
class IBasicCSVParser {
public:
    IBasicCSVParser() = default;
    IBasicCSVParser(const CSVFormat&, const ColNamesPtr&);
    IBasicCSVParser(const ParseFlagMap& parse_flags, const WhitespaceMap& ws_flags)
            : _parse_flags(parse_flags), _ws_flags(ws_flags) {};

    virtual ~IBasicCSVParser() {}

    /** Whether or not we have reached the end of source */
    bool eof() { return this->_eof; }

    /** Parse the next block of data */
    virtual void next(size_t bytes) = 0;

    /** Indicate the last block of data has been parsed */
    void end_feed();

    CONSTEXPR_17 ParseFlags parse_flag(const char ch) const noexcept {
        return _parse_flags.data()[ch + 128];
    }

    CONSTEXPR_17 ParseFlags compound_parse_flag(const char ch) const noexcept {
        return quote_escape_flag(parse_flag(ch), this->quote_escape);
    }

    /** Whether or not this CSV has a UTF-8 byte order mark */
    CONSTEXPR bool utf8_bom() const { return this->_utf8_bom; }

    void set_output(RowCollection& rows) { this->_records = &rows; }

protected:
    /** @name Current Parser State */
    ///@{
    CSVRow current_row;
    RawCSVDataPtr data_ptr = nullptr;
    ColNamesPtr _col_names = nullptr;
    CSVFieldList* fields = nullptr;
    int field_start = UNINITIALIZED_FIELD;
    size_t field_length = 0;

    /** An array where the (i + 128)th slot gives the ParseFlags for ASCII character i */
    ParseFlagMap _parse_flags;
    ///@}

    /** @name Current Stream/File State */
    ///@{
    bool _eof = false;

    /** The size of the incoming CSV */
    size_t source_size = 0;
    ///@}

    /** Whether or not source needs to be read in chunks */
    CONSTEXPR bool no_chunk() const { return this->source_size < ITERATION_CHUNK_SIZE; }

    /** Parse the current chunk of data *
             *
             *  @returns How many character were read that are part of complete rows
             */
    size_t parse();

    /** Create a new RawCSVDataPtr for a new chunk of data */
    void reset_data_ptr();

private:
    /** An array where the (i + 128)th slot determines whether ASCII character i should
             *  be trimmed
             */
    WhitespaceMap _ws_flags;
    bool quote_escape = false;
    bool field_has_double_quote = false;

    /** Where we are in the current data block */
    size_t data_pos = 0;

    /** Whether or not an attempt to find Unicode BOM has been made */
    bool unicode_bom_scan = false;
    bool _utf8_bom = false;

    /** Where complete rows should be pushed to */
    RowCollection* _records = nullptr;

    CONSTEXPR_17 bool ws_flag(const char ch) const noexcept { return _ws_flags.data()[ch + 128]; }

    size_t& current_row_start() { return this->current_row.data_start; }

    void parse_field() noexcept;

    /** Finish parsing the current field */
    void push_field();

    /** Finish parsing the current row */
    void push_row();

    /** Handle possible Unicode byte order mark */
    void trim_utf8_bom();
};

/** A class for parsing CSV data from a `std::stringstream`
         *  or an `std::ifstream`
         */
template <typename TStream>
class StreamParser : public IBasicCSVParser {
    using RowCollection = ThreadSafeDeque<CSVRow>;

public:
    StreamParser(TStream& source, const CSVFormat& format, const ColNamesPtr& col_names = nullptr)
            : IBasicCSVParser(format, col_names), _source(std::move(source)) {};

    StreamParser(TStream& source, internals::ParseFlagMap parse_flags,
                 internals::WhitespaceMap ws_flags)
            : IBasicCSVParser(parse_flags, ws_flags), _source(std::move(source)) {};

    ~StreamParser() {}

    void next(size_t bytes = ITERATION_CHUNK_SIZE) override {
        if (this->eof()) return;

        this->reset_data_ptr();
        this->data_ptr->_data = std::make_shared<std::string>();

        if (source_size == 0) {
            const auto start = _source.tellg();
            _source.seekg(0, std::ios::end);
            const auto end = _source.tellg();
            _source.seekg(0, std::ios::beg);

            source_size = end - start;
        }

        // Read data into buffer
        size_t length = std::min(source_size - stream_pos, bytes);
        std::unique_ptr<char[]> buff(new char[length]);
        _source.seekg(stream_pos, std::ios::beg);
        _source.read(buff.get(), length);
        stream_pos = _source.tellg();
        ((std::string*)(this->data_ptr->_data.get()))->assign(buff.get(), length);

        // Create string_view
        this->data_ptr->data = *((std::string*)this->data_ptr->_data.get());

        // Parse
        this->current_row = CSVRow(this->data_ptr);
        size_t remainder = this->parse();

        if (stream_pos == source_size || no_chunk()) {
            this->_eof = true;
            this->end_feed();
        } else {
            this->stream_pos -= (length - remainder);
        }
    }

private:
    TStream _source;
    size_t stream_pos = 0;
};

/** Parser for memory-mapped files
         *
         *  @par Implementation
         *  This class constructs moving windows over a file to avoid
         *  creating massive memory maps which may require more RAM
         *  than the user has available. It contains logic to automatically
         *  re-align each memory map to the beginning of a CSV row.
         *
         */
class MmapParser : public IBasicCSVParser {
public:
    MmapParser(csv::string_view filename, const CSVFormat& format,
               const ColNamesPtr& col_names = nullptr)
            : IBasicCSVParser(format, col_names) {
        this->_filename = filename.data();
        this->source_size = get_file_size(filename);
    };

    ~MmapParser() {}

    void next(size_t bytes) override;

private:
    std::string _filename;
    size_t mmap_pos = 0;
};
} // namespace internals
} // namespace csv

/** The all encompassing namespace */
namespace csv {
/** Stuff that is generally not of interest to end-users */
namespace internals {
std::string format_row(const std::vector<std::string>& row, csv::string_view delim = ", ");

std::vector<std::string> _get_col_names(csv::string_view head,
                                        const CSVFormat format = CSVFormat::guess_csv());

struct GuessScore {
    double score;
    size_t header;
};

CSV_INLINE GuessScore calculate_score(csv::string_view head, CSVFormat format);

CSVGuessResult _guess_format(csv::string_view head,
                             const std::vector<char>& delims = {',', '|', '\t', ';', '^', '~'});
} // namespace internals

std::vector<std::string> get_col_names(csv::string_view filename,
                                       const CSVFormat format = CSVFormat::guess_csv());

/** Guess the delimiter used by a delimiter-separated values file */
CSVGuessResult guess_format(csv::string_view filename,
                            const std::vector<char>& delims = {',', '|', '\t', ';', '^', '~'});

/** @class CSVReader
     *  @brief Main class for parsing CSVs from files and in-memory sources
     *
     *  All rows are compared to the column names for length consistency
     *  - By default, rows that are too short or too long are dropped
     *  - Custom behavior can be defined by overriding bad_row_handler in a subclass
     */
class CSVReader {
public:
    /**
         * An input iterator capable of handling large files.
         * @note Created by CSVReader::begin() and CSVReader::end().
         *
         * @par Iterating over a file
         * @snippet tests/test_csv_iterator.cpp CSVReader Iterator 1
         *
         * @par Using with `<algorithm>` library
         * @snippet tests/test_csv_iterator.cpp CSVReader Iterator 2
         */
    class iterator {
    public:
#ifndef DOXYGEN_SHOULD_SKIP_THIS
        using value_type = CSVRow;
        using difference_type = std::ptrdiff_t;
        using pointer = CSVRow*;
        using reference = CSVRow&;
        using iterator_category = std::input_iterator_tag;
#endif

        iterator() = default;
        iterator(CSVReader* reader) : daddy(reader) {};
        iterator(CSVReader*, CSVRow&&);

        /** Access the CSVRow held by the iterator */
        CONSTEXPR_14 reference operator*() { return this->row; }

        /** Return a pointer to the CSVRow the iterator has stopped at */
        CONSTEXPR_14 pointer operator->() { return &(this->row); }

        iterator& operator++();   /**< Pre-increment iterator */
        iterator operator++(int); /**< Post-increment ierator */
        iterator& operator--();

        /** Returns true if iterators were constructed from the same CSVReader
             *  and point to the same row
             */
        CONSTEXPR bool operator==(const iterator& other) const noexcept {
            return (this->daddy == other.daddy) && (this->i == other.i);
        }

        CONSTEXPR bool operator!=(const iterator& other) const noexcept {
            return !operator==(other);
        }

    private:
        CSVReader* daddy = nullptr; // Pointer to parent
        CSVRow row;                 // Current row
        size_t i = 0;               // Index of current row
    };

    /** @name Constructors
         *  Constructors for iterating over large files and parsing in-memory sources.
         */
    ///@{
    CSVReader(csv::string_view filename, CSVFormat format = CSVFormat::guess_csv());

    /** Allows parsing stream sources such as `std::stringstream` or `std::ifstream`
         *
         *  @tparam TStream An input stream deriving from `std::istream`
         *  @note   Currently this constructor requires special CSV dialects to be manually
         *          specified.
         */
    template <typename TStream,
              csv::enable_if_t<std::is_base_of<std::istream, TStream>::value, int> = 0>
    CSVReader(TStream& source, CSVFormat format = CSVFormat()) : _format(format) {
        using Parser = internals::StreamParser<TStream>;

        if (!format.col_names.empty()) this->set_col_names(format.col_names);

        this->parser = std::unique_ptr<Parser>(new Parser(source, format, col_names)); // For C++11
        this->initial_read();
    }
    ///@}

    CSVReader(const CSVReader&) = delete;            // No copy constructor
    CSVReader(CSVReader&&) = default;                // Move constructor
    CSVReader& operator=(const CSVReader&) = delete; // No copy assignment
    CSVReader& operator=(CSVReader&& other) = default;
    ~CSVReader() {
        if (this->read_csv_worker.joinable()) {
            this->read_csv_worker.join();
        }
    }

    /** @name Retrieving CSV Rows */
    ///@{
    bool read_row(CSVRow& row);
    iterator begin();
    HEDLEY_CONST iterator end() const noexcept;

    /** Returns true if we have reached end of file */
    bool eof() const noexcept { return this->parser->eof(); };
    ///@}

    /** @name CSV Metadata */
    ///@{
    CSVFormat get_format() const;
    std::vector<std::string> get_col_names() const;
    int index_of(csv::string_view col_name) const;
    ///@}

    /** @name CSV Metadata: Attributes */
    ///@{
    /** Whether or not the file or stream contains valid CSV rows,
         *  not including the header.
         *
         *  @note Gives an accurate answer regardless of when it is called.
         *
         */
    CONSTEXPR bool empty() const noexcept { return this->n_rows() == 0; }

    /** Retrieves the number of rows that have been read so far */
    CONSTEXPR size_t n_rows() const noexcept { return this->_n_rows; }

    /** Whether or not CSV was prefixed with a UTF-8 bom */
    bool utf8_bom() const noexcept { return this->parser->utf8_bom(); }
    ///@}

protected:
    /**
         * \defgroup csv_internal CSV Parser Internals
         * @brief Internals of CSVReader. Only maintainers and those looking to
         *        extend the parser should read this.
         * @{
         */

    /** Sets this reader's column names and associated data */
    void set_col_names(const std::vector<std::string>&);

    /** @name CSV Settings **/
    ///@{
    CSVFormat _format;
    ///@}

    /** @name Parser State */
    ///@{
    /** Pointer to a object containing column information */
    internals::ColNamesPtr col_names = std::make_shared<internals::ColNames>();

    /** Helper class which actually does the parsing */
    std::unique_ptr<internals::IBasicCSVParser> parser = nullptr;

    /** Queue of parsed CSV rows */
    std::unique_ptr<RowCollection> records {new RowCollection(100)};

    size_t n_cols = 0;  /**< The number of columns in this CSV */
    size_t _n_rows = 0; /**< How many rows (minus header) have been read so far */

    /** @name Multi-Threaded File Reading Functions */
    ///@{
    bool read_csv(size_t bytes = internals::ITERATION_CHUNK_SIZE);
    ///@}

    /**@}*/

private:
    /** Whether or not rows before header were trimmed */
    bool header_trimmed = false;

    /** @name Multi-Threaded File Reading: Flags and State */
    ///@{
    std::thread read_csv_worker; /**< Worker thread for read_csv() */
    ///@}

    /** Read initial chunk to get metadata */
    void initial_read() {
        this->read_csv_worker =
                std::thread(&CSVReader::read_csv, this, internals::ITERATION_CHUNK_SIZE);
        this->read_csv_worker.join();
    }

    void trim_header();
};
} // namespace csv

/** @file
 *  Calculates statistics from CSV files
 */

#include <sstream>
#include <unordered_map>
#include <vector>

namespace csv {
/** Class for calculating statistics from CSV files and in-memory sources
     *
     *  **Example**
     *  \include programs/csv_stats.cpp
     *
     */
class CSVStat {
public:
    using FreqCount = std::unordered_map<std::string, size_t>;
    using TypeCount = std::unordered_map<DataType, size_t>;

    std::vector<long double> get_mean() const;
    std::vector<long double> get_variance() const;
    std::vector<long double> get_mins() const;
    std::vector<long double> get_maxes() const;
    std::vector<FreqCount> get_counts() const;
    std::vector<TypeCount> get_dtypes() const;

    std::vector<std::string> get_col_names() const { return this->reader.get_col_names(); }

    CSVStat(csv::string_view filename, CSVFormat format = CSVFormat::guess_csv());
    CSVStat(std::stringstream& source, CSVFormat format = CSVFormat());

private:
    // An array of rolling averages
    // Each index corresponds to the rolling mean for the column at said index
    std::vector<long double> rolling_means;
    std::vector<long double> rolling_vars;
    std::vector<long double> mins;
    std::vector<long double> maxes;
    std::vector<FreqCount> counts;
    std::vector<TypeCount> dtypes;
    std::vector<long double> n;

    // Statistic calculators
    void variance(const long double&, const size_t&);
    void count(CSVField&, const size_t&);
    void min_max(const long double&, const size_t&);
    void dtype(CSVField&, const size_t&);

    void calc();
    void calc_chunk();
    void calc_worker(const size_t&);

    CSVReader reader;
    std::deque<CSVRow> records = {};
};
} // namespace csv

#include <string>
#include <type_traits>
#include <unordered_map>

namespace csv {
/** Returned by get_file_info() */
struct CSVFileInfo {
    std::string filename;               /**< Filename */
    std::vector<std::string> col_names; /**< CSV column names */
    char delim;                         /**< Delimiting character */
    size_t n_rows;                      /**< Number of rows in a file */
    size_t n_cols;                      /**< Number of columns in a CSV */
};

/** @name Shorthand Parsing Functions
     *  @brief Convienience functions for parsing small strings
     */
///@{
CSVReader operator""_csv(const char*, size_t);
CSVReader operator""_csv_no_header(const char*, size_t);
CSVReader parse(csv::string_view in, CSVFormat format = CSVFormat());
CSVReader parse_no_header(csv::string_view in);
///@}

/** @name Utility Functions */
///@{
std::unordered_map<std::string, DataType> csv_data_types(const std::string&);
CSVFileInfo get_file_info(const std::string& filename);
int get_col_pos(csv::string_view filename, csv::string_view col_name,
                const CSVFormat& format = CSVFormat::guess_csv());
///@}
} // namespace csv
/** @file
  *  A standalone header file for writing delimiter-separated files
  */

#include <fstream>
#include <iostream>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

namespace csv {
namespace internals {
static int DECIMAL_PLACES = 5;

/** to_string() for unsigned integers */
template <typename T, csv::enable_if_t<std::is_unsigned<T>::value, int> = 0>
inline std::string to_string(T value) {
    std::string digits_reverse = "";

    if (value == 0) return "0";

    while (value > 0) {
        digits_reverse += (char)('0' + (value % 10));
        value /= 10;
    }

    return std::string(digits_reverse.rbegin(), digits_reverse.rend());
}

/** to_string() for signed integers */
template <typename T,
          csv::enable_if_t<std::is_integral<T>::value && std::is_signed<T>::value, int> = 0>
inline std::string to_string(T value) {
    if (value >= 0) return to_string((size_t)value);

    return "-" + to_string((size_t)(value * -1));
}

/** to_string() for floating point numbers */
template <typename T, csv::enable_if_t<std::is_floating_point<T>::value, int> = 0>
inline std::string to_string(T value) {
    std::string result;

    T integral_part;
    T fractional_part = std::abs(std::modf(value, &integral_part));
    integral_part = std::abs(integral_part);

    // Integral part
    if (value < 0) result = "-";

    if (integral_part == 0) {
        result = "0";
    } else {
        for (int n_digits = (int)(std::log(integral_part) / std::log(10)); n_digits + 1 > 0;
             n_digits--) {
            int digit = (int)(std::fmod(integral_part, pow10(n_digits + 1)) / pow10(n_digits));
            result += (char)('0' + digit);
        }
    }

    // Decimal part
    result += ".";

    if (fractional_part > 0) {
        fractional_part *= (T)(pow10(DECIMAL_PLACES));
        for (int n_digits = DECIMAL_PLACES; n_digits > 0; n_digits--) {
            int digit = (int)(std::fmod(fractional_part, pow10(n_digits)) / pow10(n_digits - 1));
            result += (char)('0' + digit);
        }
    } else {
        result += "0";
    }

    return result;
}
} // namespace internals

/** Sets how many places after the decimal will be written for floating point numbers
     *
     *  @param  precision   Number of decimal places
     */
inline static void set_decimal_places(int precision) {
    internals::DECIMAL_PLACES = precision;
}

/** @name CSV Writing */
///@{
/** 
     *  Class for writing delimiter separated values files
     *
     *  To write formatted strings, one should
     *   -# Initialize a DelimWriter with respect to some output stream 
     *   -# Call write_row() on std::vector<std::string>s of unformatted text
     *
     *  @tparam OutputStream The output stream, e.g. `std::ofstream`, `std::stringstream`
     *  @tparam Delim        The delimiter character
     *  @tparam Quote        The quote character
     *  @tparam Flush        True: flush after every writing function,
     *                       false: you need to flush explicitly if needed.
     *                       In both cases the destructor will flush.
     *
     *  @par Hint
     *  Use the aliases csv::CSVWriter<OutputStream> to write CSV
     *  formatted strings and csv::TSVWriter<OutputStream>
     *  to write tab separated strings
     *
     *  @par Example w/ std::vector, std::deque, std::list
     *  @snippet test_write_csv.cpp CSV Writer Example
     *
     *  @par Example w/ std::tuple
     *  @snippet test_write_csv.cpp CSV Writer Tuple Example
     */
template <class OutputStream, char Delim, char Quote, bool Flush>
class DelimWriter {
public:
    /** Construct a DelimWriter over the specified output stream
         *
         *  @param  _out           Stream to write to
         *  @param  _quote_minimal Limit field quoting to only when necessary
        */

    DelimWriter(OutputStream& _out, bool _quote_minimal = true)
            : out(_out), quote_minimal(_quote_minimal) {};

    /** Construct a DelimWriter over the file
         *
         *  @param[out] filename  File to write to
         */
    DelimWriter(const std::string& filename) : DelimWriter(std::ifstream(filename)) {};

    /** Destructor will flush remaining data
         *
         */
    ~DelimWriter() { out.flush(); }

    /** Format a sequence of strings and write to CSV according to RFC 4180
         *
         *  @warning This does not check to make sure row lengths are consistent
         *
         *  @param[in]  record          Sequence of strings to be formatted
         *
         *  @return  The current DelimWriter instance (allowing for operator chaining)
         */
    template <typename T, size_t Size>
    DelimWriter& operator<<(const std::array<T, Size>& record) {
        for (size_t i = 0; i < Size; i++) {
            out << csv_escape(record[i]);
            if (i + 1 != Size) out << Delim;
        }

        end_out();
        return *this;
    }

    /** @copydoc operator<< */
    template <typename... T>
    DelimWriter& operator<<(const std::tuple<T...>& record) {
        this->write_tuple<0, T...>(record);
        return *this;
    }

    /**
         * @tparam T A container such as std::vector, std::deque, or std::list
         * 
         * @copydoc operator<<
         */
    template <typename T, typename Alloc, template <typename, typename> class Container,

              // Avoid conflicting with tuples with two elements
              csv::enable_if_t<std::is_class<Alloc>::value, int> = 0>
    DelimWriter& operator<<(const Container<T, Alloc>& record) {
        const size_t ilen = record.size();
        size_t i = 0;
        for (const auto& field : record) {
            out << csv_escape(field);
            if (i + 1 != ilen) out << Delim;
            i++;
        }

        end_out();
        return *this;
    }

    /** Flushes the written data
         *
         */
    void flush() { out.flush(); }

private:
    template <typename T, csv::enable_if_t<!std::is_convertible<T, std::string>::value &&
                                                   !std::is_convertible<T, csv::string_view>::value,
                                           int> = 0>
    std::string csv_escape(T in) {
        return internals::to_string(in);
    }

    template <typename T, csv::enable_if_t<std::is_convertible<T, std::string>::value ||
                                                   std::is_convertible<T, csv::string_view>::value,
                                           int> = 0>
    std::string csv_escape(T in) {
        IF_CONSTEXPR(std::is_convertible<T, csv::string_view>::value) {
            return _csv_escape(in);
        }

        return _csv_escape(std::string(in));
    }

    std::string _csv_escape(csv::string_view in) {
        /** Format a string to be RFC 4180-compliant
             *  @param[in]  in              String to be CSV-formatted
             *  @param[out] quote_minimal   Only quote fields if necessary.
             *                              If False, everything is quoted.
             */

        // Do we need a quote escape
        bool quote_escape = false;

        for (auto ch : in) {
            if (ch == Quote || ch == Delim || ch == '\r' || ch == '\n') {
                quote_escape = true;
                break;
            }
        }

        if (!quote_escape) {
            if (quote_minimal)
                return std::string(in);
            else {
                std::string ret(1, Quote);
                ret += in.data();
                ret += Quote;
                return ret;
            }
        }

        // Start initial quote escape sequence
        std::string ret(1, Quote);
        for (auto ch : in) {
            if (ch == Quote)
                ret += std::string(2, Quote);
            else
                ret += ch;
        }

        // Finish off quote escape
        ret += Quote;
        return ret;
    }

    /** Recurisve template for writing std::tuples */
    template <size_t Index = 0, typename... T>
            typename std::enable_if <
            Index<sizeof...(T), void>::type write_tuple(const std::tuple<T...>& record) {
        out << csv_escape(std::get<Index>(record));

        IF_CONSTEXPR(Index + 1 < sizeof...(T)) out << Delim;

        this->write_tuple<Index + 1>(record);
    }

    /** Base case for writing std::tuples */
    template <size_t Index = 0, typename... T>
    typename std::enable_if<Index == sizeof...(T), void>::type write_tuple(
            const std::tuple<T...>& record) {
        (void)record;
        end_out();
    }

    /** Ends a line in 'out' and flushes, if Flush is true.*/
    void end_out() {
        out << '\n';
        IF_CONSTEXPR(Flush) out.flush();
    }

    OutputStream& out;
    bool quote_minimal;
};

/** An alias for csv::DelimWriter for writing standard CSV files
     *
     *  @sa csv::DelimWriter::operator<<()
     *
     *  @note Use `csv::make_csv_writer()` to in instatiate this class over
     *        an actual output stream.
     */
template <class OutputStream, bool Flush = true>
using CSVWriter = DelimWriter<OutputStream, ',', '"', Flush>;

/** Class for writing tab-separated values files
    *
     *  @sa csv::DelimWriter::write_row()
     *  @sa csv::DelimWriter::operator<<()
     *
     *  @note Use `csv::make_tsv_writer()` to in instatiate this class over
     *        an actual output stream.
     */
template <class OutputStream, bool Flush = true>
using TSVWriter = DelimWriter<OutputStream, '\t', '"', Flush>;

/** Return a csv::CSVWriter over the output stream */
template <class OutputStream>
inline CSVWriter<OutputStream> make_csv_writer(OutputStream& out, bool quote_minimal = true) {
    return CSVWriter<OutputStream>(out, quote_minimal);
}

/** Return a buffered csv::CSVWriter over the output stream (does not auto flush) */
template <class OutputStream>
inline CSVWriter<OutputStream, false> make_csv_writer_buffered(OutputStream& out,
                                                               bool quote_minimal = true) {
    return CSVWriter<OutputStream, false>(out, quote_minimal);
}

/** Return a csv::TSVWriter over the output stream */
template <class OutputStream>
inline TSVWriter<OutputStream> make_tsv_writer(OutputStream& out, bool quote_minimal = true) {
    return TSVWriter<OutputStream>(out, quote_minimal);
}

/** Return a buffered csv::TSVWriter over the output stream (does not auto flush) */
template <class OutputStream>
inline TSVWriter<OutputStream, false> make_tsv_writer_buffered(OutputStream& out,
                                                               bool quote_minimal = true) {
    return TSVWriter<OutputStream, false>(out, quote_minimal);
}
///@}
} // namespace csv

namespace csv {
namespace internals {
CSV_INLINE size_t get_file_size(csv::string_view filename) {
    std::ifstream infile(std::string(filename), std::ios::binary);
    const auto start = infile.tellg();
    infile.seekg(0, std::ios::end);
    const auto end = infile.tellg();

    return end - start;
}

CSV_INLINE std::string get_csv_head(csv::string_view filename) {
    return get_csv_head(filename, get_file_size(filename));
}

CSV_INLINE std::string get_csv_head(csv::string_view filename, size_t file_size) {
    const size_t bytes = 500000;

    std::error_code error;
    size_t length = std::min((size_t)file_size, bytes);
    auto mmap = mio::make_mmap_source(std::string(filename), 0, length, error);

    if (error) {
        throw std::runtime_error("Cannot open file " + std::string(filename));
    }

    return std::string(mmap.begin(), mmap.end());
}

#ifdef _MSC_VER
#pragma region IBasicCVParser
#endif
CSV_INLINE IBasicCSVParser::IBasicCSVParser(const CSVFormat& format, const ColNamesPtr& col_names)
        : _col_names(col_names) {
    if (format.no_quote) {
        _parse_flags = internals::make_parse_flags(format.get_delim());
    } else {
        _parse_flags = internals::make_parse_flags(format.get_delim(), format.quote_char);
    }

    _ws_flags = internals::make_ws_flags(format.trim_chars.data(), format.trim_chars.size());
}

CSV_INLINE void IBasicCSVParser::end_feed() {
    using internals::ParseFlags;

    bool empty_last_field = this->data_ptr && this->data_ptr->_data &&
                            !this->data_ptr->data.empty() &&
                            parse_flag(this->data_ptr->data.back()) == ParseFlags::DELIMITER;

    // Push field
    if (this->field_length > 0 || empty_last_field) {
        this->push_field();
    }

    // Push row
    if (this->current_row.size() > 0) this->push_row();
}

CSV_INLINE void IBasicCSVParser::parse_field() noexcept {
    using internals::ParseFlags;
    auto& in = this->data_ptr->data;

    // Trim off leading whitespace
    while (data_pos < in.size() && ws_flag(in[data_pos])) data_pos++;

    if (field_start == UNINITIALIZED_FIELD) field_start = (int)(data_pos - current_row_start());

    // Optimization: Since NOT_SPECIAL characters tend to occur in contiguous
    // sequences, use the loop below to avoid having to go through the outer
    // switch statement as much as possible
    while (data_pos < in.size() && compound_parse_flag(in[data_pos]) == ParseFlags::NOT_SPECIAL)
        data_pos++;

    field_length = data_pos - (field_start + current_row_start());

    // Trim off trailing whitespace, this->field_length constraint matters
    // when field is entirely whitespace
    for (size_t j = data_pos - 1; ws_flag(in[j]) && this->field_length > 0; j--)
        this->field_length--;
}

CSV_INLINE void IBasicCSVParser::push_field() {
    // Update
    if (field_has_double_quote) {
        fields->emplace_back(field_start == UNINITIALIZED_FIELD ? 0 : (unsigned int)field_start,
                             field_length, true);
        field_has_double_quote = false;

    } else {
        fields->emplace_back(field_start == UNINITIALIZED_FIELD ? 0 : (unsigned int)field_start,
                             field_length);
    }

    current_row.row_length++;

    // Reset field state
    field_start = UNINITIALIZED_FIELD;
    field_length = 0;
}

/** @return The number of characters parsed that belong to complete rows */
CSV_INLINE size_t IBasicCSVParser::parse() {
    using internals::ParseFlags;

    this->quote_escape = false;
    this->data_pos = 0;
    this->current_row_start() = 0;
    this->trim_utf8_bom();

    auto& in = this->data_ptr->data;
    while (this->data_pos < in.size()) {
        switch (compound_parse_flag(in[this->data_pos])) {
        case ParseFlags::DELIMITER:
            this->push_field();
            this->data_pos++;
            break;

        case ParseFlags::NEWLINE:
            this->data_pos++;

            // Catches CRLF (or LFLF)
            if (this->data_pos < in.size() && parse_flag(in[this->data_pos]) == ParseFlags::NEWLINE)
                this->data_pos++;

            // End of record -> Write record
            this->push_field();
            this->push_row();

            // Reset
            this->current_row = CSVRow(data_ptr, this->data_pos, fields->size());
            break;

        case ParseFlags::NOT_SPECIAL:
            this->parse_field();
            break;

        case ParseFlags::QUOTE_ESCAPE_QUOTE:
            if (data_pos + 1 == in.size())
                return this->current_row_start();
            else if (data_pos + 1 < in.size()) {
                auto next_ch = parse_flag(in[data_pos + 1]);
                if (next_ch >= ParseFlags::DELIMITER) {
                    quote_escape = false;
                    data_pos++;
                    break;
                } else if (next_ch == ParseFlags::QUOTE) {
                    // Case: Escaped quote
                    data_pos += 2;
                    this->field_length += 2;
                    this->field_has_double_quote = true;
                    break;
                }
            }

            // Case: Unescaped single quote => not strictly valid but we'll keep it
            this->field_length++;
            data_pos++;

            break;

        default: // Quote (currently not quote escaped)
            if (this->field_length == 0) {
                quote_escape = true;
                data_pos++;
                if (field_start == UNINITIALIZED_FIELD && data_pos < in.size() &&
                    !ws_flag(in[data_pos]))
                    field_start = (int)(data_pos - current_row_start());
                break;
            }

            // Case: Unescaped quote
            this->field_length++;
            data_pos++;

            break;
        }
    }

    return this->current_row_start();
}

CSV_INLINE void IBasicCSVParser::push_row() {
    current_row.row_length = fields->size() - current_row.fields_start;
    this->_records->push_back(std::move(current_row));
}

CSV_INLINE void IBasicCSVParser::reset_data_ptr() {
    this->data_ptr = std::make_shared<RawCSVData>();
    this->data_ptr->parse_flags = this->_parse_flags;
    this->data_ptr->col_names = this->_col_names;
    this->fields = &(this->data_ptr->fields);
}

CSV_INLINE void IBasicCSVParser::trim_utf8_bom() {
    auto& data = this->data_ptr->data;

    if (!this->unicode_bom_scan && data.size() >= 3) {
        if (data[0] == '\xEF' && data[1] == '\xBB' && data[2] == '\xBF') {
            this->data_pos += 3; // Remove BOM from input string
            this->_utf8_bom = true;
        }

        this->unicode_bom_scan = true;
    }
}
#ifdef _MSC_VER
#pragma endregion
#endif

#ifdef _MSC_VER
#pragma region Specializations
#endif
CSV_INLINE void MmapParser::next(size_t bytes = ITERATION_CHUNK_SIZE) {
    // Reset parser state
    this->field_start = UNINITIALIZED_FIELD;
    this->field_length = 0;
    this->reset_data_ptr();

    // Create memory map
    size_t length = std::min(this->source_size - this->mmap_pos, bytes);
    std::error_code error;
    this->data_ptr->_data = std::make_shared<mio::basic_mmap_source<char>>(
            mio::make_mmap_source(this->_filename, this->mmap_pos, length, error));
    this->mmap_pos += length;
    if (error) throw error;

    auto mmap_ptr = (mio::basic_mmap_source<char>*)(this->data_ptr->_data.get());

    // Create string view
    this->data_ptr->data = csv::string_view(mmap_ptr->data(), mmap_ptr->length());

    // Parse
    this->current_row = CSVRow(this->data_ptr);
    size_t remainder = this->parse();

    if (this->mmap_pos == this->source_size || no_chunk()) {
        this->_eof = true;
        this->end_feed();
    }

    this->mmap_pos -= (length - remainder);
}
#ifdef _MSC_VER
#pragma endregion
#endif
} // namespace internals
} // namespace csv

namespace csv {
namespace internals {
CSV_INLINE std::vector<std::string> ColNames::get_col_names() const {
    return this->col_names;
}

CSV_INLINE void ColNames::set_col_names(const std::vector<std::string>& cnames) {
    this->col_names = cnames;

    for (size_t i = 0; i < cnames.size(); i++) {
        this->col_pos[cnames[i]] = i;
    }
}

CSV_INLINE int ColNames::index_of(csv::string_view col_name) const {
    auto pos = this->col_pos.find(col_name.data());
    if (pos != this->col_pos.end()) return (int)pos->second;

    return CSV_NOT_FOUND;
}

CSV_INLINE size_t ColNames::size() const noexcept {
    return this->col_names.size();
}

} // namespace internals
} // namespace csv
/** @file
 *  Defines an object used to store CSV format settings
 */

#include <algorithm>
#include <set>

namespace csv {
CSV_INLINE CSVFormat& CSVFormat::delimiter(char delim) {
    this->possible_delimiters = {delim};
    this->assert_no_char_overlap();
    return *this;
}

CSV_INLINE CSVFormat& CSVFormat::delimiter(const std::vector<char>& delim) {
    this->possible_delimiters = delim;
    this->assert_no_char_overlap();
    return *this;
}

CSV_INLINE CSVFormat& CSVFormat::quote(char quote) {
    this->no_quote = false;
    this->quote_char = quote;
    this->assert_no_char_overlap();
    return *this;
}

CSV_INLINE CSVFormat& CSVFormat::trim(const std::vector<char>& chars) {
    this->trim_chars = chars;
    this->assert_no_char_overlap();
    return *this;
}

CSV_INLINE CSVFormat& CSVFormat::column_names(const std::vector<std::string>& names) {
    this->col_names = names;
    this->header = -1;
    return *this;
}

CSV_INLINE CSVFormat& CSVFormat::header_row(int row) {
    if (row < 0) this->variable_column_policy = VariableColumnPolicy::KEEP;

    this->header = row;
    this->col_names = {};
    return *this;
}

CSV_INLINE void CSVFormat::assert_no_char_overlap() {
    auto delims =
                 std::set<char>(this->possible_delimiters.begin(), this->possible_delimiters.end()),
         trims = std::set<char>(this->trim_chars.begin(), this->trim_chars.end());

    // Stores intersection of possible delimiters and trim characters
    std::vector<char> intersection = {};

    // Find which characters overlap, if any
    std::set_intersection(delims.begin(), delims.end(), trims.begin(), trims.end(),
                          std::back_inserter(intersection));

    // Make sure quote character is not contained in possible delimiters
    // or whitespace characters
    if (delims.find(this->quote_char) != delims.end() ||
        trims.find(this->quote_char) != trims.end()) {
        intersection.push_back(this->quote_char);
    }

    if (!intersection.empty()) {
        std::string err_msg =
                "There should be no overlap between the quote character, "
                "the set of possible delimiters "
                "and the set of whitespace characters. Offending characters: ";

        // Create a pretty error message with the list of overlapping
        // characters
        for (size_t i = 0; i < intersection.size(); i++) {
            err_msg += "'";
            err_msg += intersection[i];
            err_msg += "'";

            if (i + 1 < intersection.size()) err_msg += ", ";
        }

        throw std::runtime_error(err_msg + '.');
    }
}
} // namespace csv
/** @file
 *  @brief Defines functionality needed for basic CSV parsing
 */

namespace csv {
namespace internals {
CSV_INLINE std::string format_row(const std::vector<std::string>& row, csv::string_view delim) {
    /** Print a CSV row */
    std::stringstream ret;
    for (size_t i = 0; i < row.size(); i++) {
        ret << row[i];
        if (i + 1 < row.size())
            ret << delim;
        else
            ret << '\n';
    }
    ret.flush();

    return ret.str();
}

/** Return a CSV's column names
         *
         *  @param[in] filename  Path to CSV file
         *  @param[in] format    Format of the CSV file
         *
         */
CSV_INLINE std::vector<std::string> _get_col_names(csv::string_view head, CSVFormat format) {
    // Parse the CSV
    auto trim_chars = format.get_trim_chars();
    std::stringstream source(head.data());
    RowCollection rows;

    StreamParser<std::stringstream> parser(source, format);
    parser.set_output(rows);
    parser.next();

    return CSVRow(std::move(rows[format.get_header()]));
}

CSV_INLINE GuessScore calculate_score(csv::string_view head, CSVFormat format) {
    // Frequency counter of row length
    std::unordered_map<size_t, size_t> row_tally = {{0, 0}};

    // Map row lengths to row num where they first occurred
    std::unordered_map<size_t, size_t> row_when = {{0, 0}};

    // Parse the CSV
    std::stringstream source(head.data());
    RowCollection rows;

    StreamParser<std::stringstream> parser(source, format);
    parser.set_output(rows);
    parser.next();

    for (size_t i = 0; i < rows.size(); i++) {
        auto& row = rows[i];

        // Ignore zero-length rows
        if (row.size() > 0) {
            if (row_tally.find(row.size()) != row_tally.end()) {
                row_tally[row.size()]++;
            } else {
                row_tally[row.size()] = 1;
                row_when[row.size()] = i;
            }
        }
    }

    double final_score = 0;
    size_t header_row = 0;

    // Final score is equal to the largest
    // row size times rows of that size
    for (auto& pair : row_tally) {
        auto row_size = pair.first;
        auto row_count = pair.second;
        double score = (double)(row_size * row_count);
        if (score > final_score) {
            final_score = score;
            header_row = row_when[row_size];
        }
    }

    return {final_score, header_row};
}

/** Guess the delimiter used by a delimiter-separated values file */
CSV_INLINE CSVGuessResult _guess_format(csv::string_view head, const std::vector<char>& delims) {
    /** For each delimiter, find out which row length was most common.
             *  The delimiter with the longest mode row length wins.
             *  Then, the line number of the header row is the first row with
             *  the mode row length.
             */

    CSVFormat format;
    size_t max_score = 0, header = 0;
    char current_delim = delims[0];

    for (char cand_delim : delims) {
        auto result = calculate_score(head, format.delimiter(cand_delim));

        if ((size_t)result.score > max_score) {
            max_score = (size_t)result.score;
            current_delim = cand_delim;
            header = result.header;
        }
    }

    return {current_delim, (int)header};
}
} // namespace internals

/** Return a CSV's column names
     *
     *  @param[in] filename  Path to CSV file
     *  @param[in] format    Format of the CSV file
     *
     */
CSV_INLINE std::vector<std::string> get_col_names(csv::string_view filename, CSVFormat format) {
    auto head = internals::get_csv_head(filename);

    /** Guess delimiter and header row */
    if (format.guess_delim()) {
        auto guess_result = guess_format(filename, format.get_possible_delims());
        format.delimiter(guess_result.delim).header_row(guess_result.header_row);
    }

    return internals::_get_col_names(head, format);
}

/** Guess the delimiter used by a delimiter-separated values file */
CSV_INLINE CSVGuessResult guess_format(csv::string_view filename, const std::vector<char>& delims) {
    auto head = internals::get_csv_head(filename);
    return internals::_guess_format(head, delims);
}

/** Reads an arbitrarily large CSV file using memory-mapped IO.
     *
     *  **Details:** Reads the first block of a CSV file synchronously to get information
     *               such as column names and delimiting character.
     *
     *  @param[in] filename  Path to CSV file
     *  @param[in] format    Format of the CSV file
     *
     *  \snippet tests/test_read_csv.cpp CSVField Example
     *
     */
CSV_INLINE CSVReader::CSVReader(csv::string_view filename, CSVFormat format) : _format(format) {
    auto head = internals::get_csv_head(filename);
    using Parser = internals::MmapParser;

    /** Guess delimiter and header row */
    if (format.guess_delim()) {
        auto guess_result = internals::_guess_format(head, format.possible_delimiters);
        format.delimiter(guess_result.delim);
        format.header = guess_result.header_row;
        this->_format = format;
    }

    if (!format.col_names.empty()) this->set_col_names(format.col_names);

    this->parser =
            std::unique_ptr<Parser>(new Parser(filename, format, this->col_names)); // For C++11
    this->initial_read();
}

/** Return the format of the original raw CSV */
CSV_INLINE CSVFormat CSVReader::get_format() const {
    CSVFormat new_format = this->_format;

    // Since users are normally not allowed to set
    // column names and header row simulatenously,
    // we will set the backing variables directly here
    new_format.col_names = this->col_names->get_col_names();
    new_format.header = this->_format.header;

    return new_format;
}

/** Return the CSV's column names as a vector of strings. */
CSV_INLINE std::vector<std::string> CSVReader::get_col_names() const {
    if (this->col_names) {
        return this->col_names->get_col_names();
    }

    return std::vector<std::string>();
}

/** Return the index of the column name if found or
     *         csv::CSV_NOT_FOUND otherwise.
     */
CSV_INLINE int CSVReader::index_of(csv::string_view col_name) const {
    auto _col_names = this->get_col_names();
    for (size_t i = 0; i < _col_names.size(); i++)
        if (_col_names[i] == col_name) return (int)i;

    return CSV_NOT_FOUND;
}

CSV_INLINE void CSVReader::trim_header() {
    if (!this->header_trimmed) {
        for (int i = 0; i <= this->_format.header && !this->records->empty(); i++) {
            if (i == this->_format.header && this->col_names->empty()) {
                this->set_col_names(this->records->pop_front());
            } else {
                this->records->pop_front();
            }
        }

        this->header_trimmed = true;
    }
}

/**
     *  @param[in] names Column names
     */
CSV_INLINE void CSVReader::set_col_names(const std::vector<std::string>& names) {
    this->col_names->set_col_names(names);
    this->n_cols = names.size();
}

/**
     * Read a chunk of CSV data.
     *
     * @note This method is meant to be run on its own thread. Only one `read_csv()` thread
     *       should be active at a time.
     *
     * @param[in] bytes Number of bytes to read.
     *
     * @see CSVReader::read_csv_worker
     * @see CSVReader::read_row()
     */
CSV_INLINE bool CSVReader::read_csv(size_t bytes) {
    // Tell read_row() to listen for CSV rows
    this->records->notify_all();

    this->parser->set_output(*this->records);
    this->parser->next(bytes);

    if (!this->header_trimmed) {
        this->trim_header();
    }

    // Tell read_row() to stop waiting
    this->records->kill_all();

    return true;
}

/**
     * Retrieve rows as CSVRow objects, returning true if more rows are available.
     *
     * @par Performance Notes
     *  - Reads chunks of data that are csv::internals::ITERATION_CHUNK_SIZE bytes large at a time
     *  - For performance details, read the documentation for CSVRow and CSVField.
     *
     * @param[out] row The variable where the parsed row will be stored
     * @see CSVRow, CSVField
     *
     * **Example:**
     * \snippet tests/test_read_csv.cpp CSVField Example
     *
     */
CSV_INLINE bool CSVReader::read_row(CSVRow& row) {
    while (true) {
        if (this->records->empty()) {
            if (this->records->is_waitable())
                // Reading thread is currently active => wait for it to populate records
                this->records->wait();
            else if (this->parser->eof())
                // End of file and no more records
                return false;
            else {
                // Reading thread is not active => start another one
                if (this->read_csv_worker.joinable()) this->read_csv_worker.join();

                this->read_csv_worker =
                        std::thread(&CSVReader::read_csv, this, internals::ITERATION_CHUNK_SIZE);
            }
        } else if (this->records->front().size() != this->n_cols &&
                   this->_format.variable_column_policy != VariableColumnPolicy::KEEP) {
            auto errored_row = this->records->pop_front();

            if (this->_format.variable_column_policy == VariableColumnPolicy::THROW) {
                if (errored_row.size() < this->n_cols)
                    throw std::runtime_error("Line too short " +
                                             internals::format_row(errored_row));

                throw std::runtime_error("Line too long " + internals::format_row(errored_row));
            }
        } else {
            row = this->records->pop_front();
            this->_n_rows++;
            return true;
        }
    }

    return false;
}
} // namespace csv

/** @file
 *  Defines an input iterator for csv::CSVReader
 */

namespace csv {
/** Return an iterator to the first row in the reader */
CSV_INLINE CSVReader::iterator CSVReader::begin() {
    if (this->records->empty()) {
        this->read_csv_worker =
                std::thread(&CSVReader::read_csv, this, internals::ITERATION_CHUNK_SIZE);
        this->read_csv_worker.join();

        // Still empty => return end iterator
        if (this->records->empty()) return this->end();
    }

    CSVReader::iterator ret(this, this->records->pop_front());
    return ret;
}

/** A placeholder for the imaginary past the end row in a CSV.
     *  Attempting to deference this will lead to bad things.
     */
CSV_INLINE HEDLEY_CONST CSVReader::iterator CSVReader::end() const noexcept {
    return CSVReader::iterator();
}

/////////////////////////
// CSVReader::iterator //
/////////////////////////

CSV_INLINE CSVReader::iterator::iterator(CSVReader* _daddy, CSVRow&& _row) : daddy(_daddy) {
    row = std::move(_row);
}

/** Advance the iterator by one row. If this CSVReader has an
     *  associated file, then the iterator will lazily pull more data from
     *  that file until the end of file is reached.
     *
     *  @note This iterator does **not** block the thread responsible for parsing CSV.
     *
     */
CSV_INLINE CSVReader::iterator& CSVReader::iterator::operator++() {
    if (!daddy->read_row(this->row)) {
        this->daddy = nullptr; // this == end()
    }

    return *this;
}

/** Post-increment iterator */
CSV_INLINE CSVReader::iterator CSVReader::iterator::operator++(int) {
    auto temp = *this;
    if (!daddy->read_row(this->row)) {
        this->daddy = nullptr; // this == end()
    }

    return temp;
}
} // namespace csv

/** @file
 *  Defines the data type used for storing information about a CSV row
 */

#include <cassert>
#include <functional>

namespace csv {
namespace internals {
CSV_INLINE RawCSVField& CSVFieldList::operator[](size_t n) const {
    const size_t page_no = n / _single_buffer_capacity;
    const size_t buffer_idx = (page_no < 1) ? n : n % _single_buffer_capacity;
    return this->buffers[page_no][buffer_idx];
}

CSV_INLINE void CSVFieldList::allocate() {
    RawCSVField* buffer = new RawCSVField[_single_buffer_capacity];
    buffers.push_back(buffer);
    _current_buffer_size = 0;
    _back = &(buffers.back()[0]);
}
} // namespace internals

/** Return a CSVField object corrsponding to the nth value in the row.
     *
     *  @note This method performs bounds checking, and will throw an
     *        `std::runtime_error` if n is invalid.
     *
     *  @complexity
     *  Constant, by calling csv::CSVRow::get_csv::string_view()
     *
     */
CSV_INLINE CSVField CSVRow::operator[](size_t n) const {
    return CSVField(this->get_field(n));
}

/** Retrieve a value by its associated column name. If the column
     *  specified can't be round, a runtime error is thrown.
     *
     *  @complexity
     *  Constant. This calls the other CSVRow::operator[]() after
     *  converting column names into indices using a hash table.
     *
     *  @param[in] col_name The column to look for
     */
CSV_INLINE CSVField CSVRow::operator[](const std::string& col_name) const {
    auto& col_names = this->data->col_names;
    auto col_pos = col_names->index_of(col_name);
    if (col_pos > -1) {
        return this->operator[](col_pos);
    }

    throw std::runtime_error("Can't find a column named " + col_name);
}

CSV_INLINE CSVRow::operator std::vector<std::string>() const {
    std::vector<std::string> ret;
    for (size_t i = 0; i < size(); i++) ret.push_back(std::string(this->get_field(i)));

    return ret;
}

CSV_INLINE csv::string_view CSVRow::get_field(size_t index) const {
    using internals::ParseFlags;

    if (index >= this->size()) throw std::runtime_error("Index out of bounds.");

    const size_t field_index = this->fields_start + index;
    auto& field = this->data->fields[field_index];
    auto field_str = csv::string_view(this->data->data).substr(this->data_start + field.start);

    if (field.has_double_quote) {
        auto& value = this->data->double_quote_fields[field_index];
        if (value.empty()) {
            bool prev_ch_quote = false;
            for (size_t i = 0; i < field.length; i++) {
                if (this->data->parse_flags[field_str[i] + 128] == ParseFlags::QUOTE) {
                    if (prev_ch_quote) {
                        prev_ch_quote = false;
                        continue;
                    } else {
                        prev_ch_quote = true;
                    }
                }

                value += field_str[i];
            }
        }

        return csv::string_view(value);
    }

    return field_str.substr(0, field.length);
}

CSV_INLINE bool CSVField::try_parse_hex(int& parsedValue) {
    size_t start = 0, end = 0;

    // Trim out whitespace chars
    for (; start < this->sv.size() && this->sv[start] == ' '; start++)
        ;
    for (end = start; end < this->sv.size() && this->sv[end] != ' '; end++)
        ;

    unsigned long long int value = 0;

    size_t digits = (end - start);
    size_t base16_exponent = digits - 1;

    if (digits == 0) return false;

    for (const auto& ch : this->sv.substr(start, digits)) {
        int digit = 0;

        switch (ch) {
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            digit = static_cast<int>(ch - '0');
            break;
        case 'a':
        case 'A':
            digit = 10;
            break;
        case 'b':
        case 'B':
            digit = 11;
            break;
        case 'c':
        case 'C':
            digit = 12;
            break;
        case 'd':
        case 'D':
            digit = 13;
            break;
        case 'e':
        case 'E':
            digit = 14;
            break;
        case 'f':
        case 'F':
            digit = 15;
            break;
        default:
            return false;
        }

        value += digit * pow(16, base16_exponent);
        base16_exponent--;
    }

    parsedValue = value;
    return true;
}

#ifdef _MSC_VER
#pragma region CSVRow Iterator
#endif
/** Return an iterator pointing to the first field. */
CSV_INLINE CSVRow::iterator CSVRow::begin() const {
    return CSVRow::iterator(this, 0);
}

/** Return an iterator pointing to just after the end of the CSVRow.
     *
     *  @warning Attempting to dereference the end iterator results
     *           in dereferencing a null pointer.
     */
CSV_INLINE CSVRow::iterator CSVRow::end() const noexcept {
    return CSVRow::iterator(this, (int)this->size());
}

CSV_INLINE CSVRow::reverse_iterator CSVRow::rbegin() const noexcept {
    return std::reverse_iterator<CSVRow::iterator>(this->end());
}

CSV_INLINE CSVRow::reverse_iterator CSVRow::rend() const {
    return std::reverse_iterator<CSVRow::iterator>(this->begin());
}

CSV_INLINE HEDLEY_NON_NULL(2) CSVRow::iterator::iterator(const CSVRow* _reader, int _i)
        : daddy(_reader), i(_i) {
    if (_i < (int)this->daddy->size())
        this->field = std::make_shared<CSVField>(this->daddy->operator[](_i));
    else
        this->field = nullptr;
}

CSV_INLINE CSVRow::iterator::reference CSVRow::iterator::operator*() const {
    return *(this->field.get());
}

CSV_INLINE CSVRow::iterator::pointer CSVRow::iterator::operator->() const {
// Using CSVField * as pointer type causes segfaults in MSVC debug builds
#ifdef _MSC_BUILD
    return this->field;
#else
    return this->field.get();
#endif
}

CSV_INLINE CSVRow::iterator& CSVRow::iterator::operator++() {
    // Pre-increment operator
    this->i++;
    if (this->i < (int)this->daddy->size())
        this->field = std::make_shared<CSVField>(this->daddy->operator[](i));
    else // Reached the end of row
        this->field = nullptr;
    return *this;
}

CSV_INLINE CSVRow::iterator CSVRow::iterator::operator++(int) {
    // Post-increment operator
    auto temp = *this;
    this->operator++();
    return temp;
}

CSV_INLINE CSVRow::iterator& CSVRow::iterator::operator--() {
    // Pre-decrement operator
    this->i--;
    this->field = std::make_shared<CSVField>(this->daddy->operator[](this->i));
    return *this;
}

CSV_INLINE CSVRow::iterator CSVRow::iterator::operator--(int) {
    // Post-decrement operator
    auto temp = *this;
    this->operator--();
    return temp;
}

CSV_INLINE CSVRow::iterator CSVRow::iterator::operator+(difference_type n) const {
    // Allows for iterator arithmetic
    return CSVRow::iterator(this->daddy, i + (int)n);
}

CSV_INLINE CSVRow::iterator CSVRow::iterator::operator-(difference_type n) const {
    // Allows for iterator arithmetic
    return CSVRow::iterator::operator+(-n);
}
#ifdef _MSC_VER
#pragma endregion CSVRow Iterator
#endif
} // namespace csv

/** @file
 *  Implements JSON serialization abilities
 */

namespace csv {
/*
    The implementations for json_extra_space() and json_escape_string()
    were modified from source code for JSON for Modern C++.

    The respective license is below:

    The code is licensed under the [MIT
    License](http://opensource.org/licenses/MIT):
    
    Copyright &copy; 2013-2015 Niels Lohmann.
    
    Permission is hereby granted, free of charge, to any person
    obtaining a copy of this software and associated documentation files
    (the "Software"), to deal in the Software without restriction,
    including without limitation the rights to use, copy, modify, merge,
    publish, distribute, sublicense, and/or sell copies of the Software,
    and to permit persons to whom the Software is furnished to do so,
    subject to the following conditions:
    
    The above copyright notice and this permission notice shall be
    included in all copies or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
    BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
    ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
    CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
    */

namespace internals {
/*!
         @brief calculates the extra space to escape a JSON string

         @param[in] s  the string to escape
         @return the number of characters required to escape string @a s

         @complexity Linear in the length of string @a s.
        */
static std::size_t json_extra_space(csv::string_view& s) noexcept {
    std::size_t result = 0;

    for (const auto& c : s) {
        switch (c) {
        case '"':
        case '\\':
        case '\b':
        case '\f':
        case '\n':
        case '\r':
        case '\t': {
            // from c (1 byte) to \x (2 bytes)
            result += 1;
            break;
        }

        default: {
            if (c >= 0x00 && c <= 0x1f) {
                // from c (1 byte) to \uxxxx (6 bytes)
                result += 5;
            }
            break;
        }
        }
    }

    return result;
}

CSV_INLINE std::string json_escape_string(csv::string_view s) noexcept {
    const auto space = json_extra_space(s);
    if (space == 0) {
        return std::string(s);
    }

    // create a result string of necessary size
    std::string result(s.size() + space, '\\');
    std::size_t pos = 0;

    for (const auto& c : s) {
        switch (c) {
        // quotation mark (0x22)
        case '"': {
            result[pos + 1] = '"';
            pos += 2;
            break;
        }

        // reverse solidus (0x5c)
        case '\\': {
            // nothing to change
            pos += 2;
            break;
        }

        // backspace (0x08)
        case '\b': {
            result[pos + 1] = 'b';
            pos += 2;
            break;
        }

        // formfeed (0x0c)
        case '\f': {
            result[pos + 1] = 'f';
            pos += 2;
            break;
        }

        // newline (0x0a)
        case '\n': {
            result[pos + 1] = 'n';
            pos += 2;
            break;
        }

        // carriage return (0x0d)
        case '\r': {
            result[pos + 1] = 'r';
            pos += 2;
            break;
        }

        // horizontal tab (0x09)
        case '\t': {
            result[pos + 1] = 't';
            pos += 2;
            break;
        }

        default: {
            if (c >= 0x00 && c <= 0x1f) {
                // print character c as \uxxxx
                sprintf(&result[pos + 1], "u%04x", int(c));
                pos += 6;
                // overwrite trailing null character
                result[pos] = '\\';
            } else {
                // all other characters are added as-is
                result[pos++] = c;
            }
            break;
        }
        }
    }

    return result;
}
} // namespace internals

/** Convert a CSV row to a JSON object, i.e.
     *  `{"col1":"value1","col2":"value2"}`
     *
     *  @note All strings are properly escaped. Numeric values are not quoted.
     *  @param[in] subset A subset of columns to contain in the JSON.
     *                    Leave empty for original columns.
     */
CSV_INLINE std::string CSVRow::to_json(const std::vector<std::string>& subset) const {
    std::vector<std::string> col_names = subset;
    if (subset.empty()) {
        col_names = this->data ? this->get_col_names() : std::vector<std::string>({});
    }

    const size_t _n_cols = col_names.size();
    std::string ret = "{";

    for (size_t i = 0; i < _n_cols; i++) {
        auto& col = col_names[i];
        auto field = this->operator[](col);

        // TODO: Possible performance enhancements by caching escaped column names
        ret += '"' + internals::json_escape_string(col) + "\":";

        // Add quotes around strings but not numbers
        if (field.is_num())
            ret += internals::json_escape_string(field.get<csv::string_view>());
        else
            ret += '"' + internals::json_escape_string(field.get<csv::string_view>()) + '"';

        // Do not add comma after last string
        if (i + 1 < _n_cols) ret += ',';
    }

    ret += '}';
    return ret;
}

/** Convert a CSV row to a JSON array, i.e.
     *  `["value1","value2",...]`
     *
     *  @note All strings are properly escaped. Numeric values are not quoted.
     *  @param[in] subset A subset of columns to contain in the JSON.
     *                    Leave empty for all columns.
     */
CSV_INLINE std::string CSVRow::to_json_array(const std::vector<std::string>& subset) const {
    std::vector<std::string> col_names = subset;
    if (subset.empty())
        col_names = this->data ? this->get_col_names() : std::vector<std::string>({});

    const size_t _n_cols = col_names.size();
    std::string ret = "[";

    for (size_t i = 0; i < _n_cols; i++) {
        auto field = this->operator[](col_names[i]);

        // Add quotes around strings but not numbers
        if (field.is_num())
            ret += internals::json_escape_string(field.get<csv::string_view>());
        else
            ret += '"' + internals::json_escape_string(field.get<csv::string_view>()) + '"';

        // Do not add comma after last string
        if (i + 1 < _n_cols) ret += ',';
    }

    ret += ']';
    return ret;
}
} // namespace csv
/** @file
 *  Calculates statistics from CSV files
 */

#include <string>

namespace csv {
/** Calculate statistics for an arbitrarily large file. When this constructor
     *  is called, CSVStat will process the entire file iteratively. Once finished,
     *  methods like get_mean(), get_counts(), etc... can be used to retrieve statistics.
     */
CSV_INLINE CSVStat::CSVStat(csv::string_view filename, CSVFormat format)
        : reader(filename, format) {
    this->calc();
}

/** Calculate statistics for a CSV stored in a std::stringstream */
CSV_INLINE CSVStat::CSVStat(std::stringstream& stream, CSVFormat format) : reader(stream, format) {
    this->calc();
}

/** Return current means */
CSV_INLINE std::vector<long double> CSVStat::get_mean() const {
    std::vector<long double> ret;
    for (size_t i = 0; i < this->get_col_names().size(); i++) {
        ret.push_back(this->rolling_means[i]);
    }
    return ret;
}

/** Return current variances */
CSV_INLINE std::vector<long double> CSVStat::get_variance() const {
    std::vector<long double> ret;
    for (size_t i = 0; i < this->get_col_names().size(); i++) {
        ret.push_back(this->rolling_vars[i] / (this->n[i] - 1));
    }
    return ret;
}

/** Return current mins */
CSV_INLINE std::vector<long double> CSVStat::get_mins() const {
    std::vector<long double> ret;
    for (size_t i = 0; i < this->get_col_names().size(); i++) {
        ret.push_back(this->mins[i]);
    }
    return ret;
}

/** Return current maxes */
CSV_INLINE std::vector<long double> CSVStat::get_maxes() const {
    std::vector<long double> ret;
    for (size_t i = 0; i < this->get_col_names().size(); i++) {
        ret.push_back(this->maxes[i]);
    }
    return ret;
}

/** Get counts for each column */
CSV_INLINE std::vector<CSVStat::FreqCount> CSVStat::get_counts() const {
    std::vector<FreqCount> ret;
    for (size_t i = 0; i < this->get_col_names().size(); i++) {
        ret.push_back(this->counts[i]);
    }
    return ret;
}

/** Get data type counts for each column */
CSV_INLINE std::vector<CSVStat::TypeCount> CSVStat::get_dtypes() const {
    std::vector<TypeCount> ret;
    for (size_t i = 0; i < this->get_col_names().size(); i++) {
        ret.push_back(this->dtypes[i]);
    }
    return ret;
}

CSV_INLINE void CSVStat::calc_chunk() {
    /** Only create stats counters the first time **/
    if (dtypes.empty()) {
        /** Go through all records and calculate specified statistics */
        for (size_t i = 0; i < this->get_col_names().size(); i++) {
            dtypes.push_back({});
            counts.push_back({});
            rolling_means.push_back(0);
            rolling_vars.push_back(0);
            mins.push_back(NAN);
            maxes.push_back(NAN);
            n.push_back(0);
        }
    }

    // Start threads
    std::vector<std::thread> pool;
    for (size_t i = 0; i < this->get_col_names().size(); i++)
        pool.push_back(std::thread(&CSVStat::calc_worker, this, i));

    // Block until done
    for (auto& th : pool) th.join();

    this->records.clear();
}

CSV_INLINE void CSVStat::calc() {
    constexpr size_t CALC_CHUNK_SIZE = 5000;

    for (auto& row : reader) {
        this->records.push_back(std::move(row));

        /** Chunk rows */
        if (this->records.size() == CALC_CHUNK_SIZE) {
            calc_chunk();
        }
    }

    if (!this->records.empty()) {
        calc_chunk();
    }
}

CSV_INLINE void CSVStat::calc_worker(const size_t& i) {
    /** Worker thread for CSVStat::calc() which calculates statistics for one column.
         * 
         *  @param[in] i Column index
         */

    auto current_record = this->records.begin();

    for (size_t processed = 0; current_record != this->records.end(); processed++) {
        if (current_record->size() == this->get_col_names().size()) {
            auto current_field = (*current_record)[i];

            // Optimization: Don't count() if there's too many distinct values in the first 1000 rows
            if (processed < 1000 || this->counts[i].size() <= 500) this->count(current_field, i);

            this->dtype(current_field, i);

            // Numeric Stuff
            if (current_field.is_num()) {
                long double x_n = current_field.get<long double>();

                // This actually calculates mean AND variance
                this->variance(x_n, i);
                this->min_max(x_n, i);
            }
        } else if (this->reader.get_format().get_variable_column_policy() ==
                   VariableColumnPolicy::THROW) {
            throw std::runtime_error("Line has different length than the others " +
                                     internals::format_row(*current_record));
        }

        ++current_record;
    }
}

CSV_INLINE void CSVStat::dtype(CSVField& data, const size_t& i) {
    /** Given a record update the type counter
         *  @param[in]  record Data observation
         *  @param[out] i      The column index that should be updated
         */

    auto type = data.type();
    if (this->dtypes[i].find(type) != this->dtypes[i].end()) {
        // Increment count
        this->dtypes[i][type]++;
    } else {
        // Initialize count
        this->dtypes[i].insert(std::make_pair(type, 1));
    }
}

CSV_INLINE void CSVStat::count(CSVField& data, const size_t& i) {
    /** Given a record update the frequency counter
         *  @param[in]  record Data observation
         *  @param[out] i      The column index that should be updated
         */

    auto item = data.get<std::string>();

    if (this->counts[i].find(item) != this->counts[i].end()) {
        // Increment count
        this->counts[i][item]++;
    } else {
        // Initialize count
        this->counts[i].insert(std::make_pair(item, 1));
    }
}

CSV_INLINE void CSVStat::min_max(const long double& x_n, const size_t& i) {
    /** Update current minimum and maximum
         *  @param[in]  x_n Data observation
         *  @param[out] i   The column index that should be updated
         */
    if (std::isnan(this->mins[i])) this->mins[i] = x_n;
    if (std::isnan(this->maxes[i])) this->maxes[i] = x_n;

    if (x_n < this->mins[i])
        this->mins[i] = x_n;
    else if (x_n > this->maxes[i])
        this->maxes[i] = x_n;
}

CSV_INLINE void CSVStat::variance(const long double& x_n, const size_t& i) {
    /** Given a record update rolling mean and variance for all columns
         *  using Welford's Algorithm
         *  @param[in]  x_n Data observation
         *  @param[out] i   The column index that should be updated
         */
    long double& current_rolling_mean = this->rolling_means[i];
    long double& current_rolling_var = this->rolling_vars[i];
    long double& current_n = this->n[i];
    long double delta;
    long double delta2;

    current_n++;

    if (current_n == 1) {
        current_rolling_mean = x_n;
    } else {
        delta = x_n - current_rolling_mean;
        current_rolling_mean += delta / current_n;
        delta2 = x_n - current_rolling_mean;
        current_rolling_var += delta * delta2;
    }
}

/** Useful for uploading CSV files to SQL databases.
     *
     *  Return a data type for each column such that every value in a column can be
     *  converted to the corresponding data type without data loss.
     *  @param[in]  filename The CSV file
     *
     *  \return A mapping of column names to csv::DataType enums
     */
CSV_INLINE std::unordered_map<std::string, DataType> csv_data_types(const std::string& filename) {
    CSVStat stat(filename);
    std::unordered_map<std::string, DataType> csv_dtypes;

    auto col_names = stat.get_col_names();
    auto temp = stat.get_dtypes();

    for (size_t i = 0; i < stat.get_col_names().size(); i++) {
        auto& col = temp[i];
        auto& col_name = col_names[i];

        if (col[DataType::CSV_STRING])
            csv_dtypes[col_name] = DataType::CSV_STRING;
        else if (col[DataType::CSV_INT64])
            csv_dtypes[col_name] = DataType::CSV_INT64;
        else if (col[DataType::CSV_INT32])
            csv_dtypes[col_name] = DataType::CSV_INT32;
        else if (col[DataType::CSV_INT16])
            csv_dtypes[col_name] = DataType::CSV_INT16;
        else if (col[DataType::CSV_INT8])
            csv_dtypes[col_name] = DataType::CSV_INT8;
        else
            csv_dtypes[col_name] = DataType::CSV_DOUBLE;
    }

    return csv_dtypes;
}
} // namespace csv
#include <sstream>
#include <vector>

namespace csv {
/** Shorthand function for parsing an in-memory CSV string
     *
     *  @return A collection of CSVRow objects
     *
     *  @par Example
     *  @snippet tests/test_read_csv.cpp Parse Example
     */
CSV_INLINE CSVReader parse(csv::string_view in, CSVFormat format) {
    std::stringstream stream(in.data());
    return CSVReader(stream, format);
}

/** Parses a CSV string with no headers
     *
     *  @return A collection of CSVRow objects
     */
CSV_INLINE CSVReader parse_no_header(csv::string_view in) {
    CSVFormat format;
    format.header_row(-1);

    return parse(in, format);
}

/** Parse a RFC 4180 CSV string, returning a collection
     *  of CSVRow objects
     *
     *  @par Example
     *  @snippet tests/test_read_csv.cpp Escaped Comma
     *
     */
CSV_INLINE CSVReader operator""_csv(const char* in, size_t n) {
    return parse(csv::string_view(in, n));
}

/** A shorthand for csv::parse_no_header() */
CSV_INLINE CSVReader operator""_csv_no_header(const char* in, size_t n) {
    return parse_no_header(csv::string_view(in, n));
}

/**
     *  Find the position of a column in a CSV file or CSV_NOT_FOUND otherwise
     *
     *  @param[in] filename  Path to CSV file
     *  @param[in] col_name  Column whose position we should resolve
     *  @param[in] format    Format of the CSV file
     */
CSV_INLINE int get_col_pos(csv::string_view filename, csv::string_view col_name,
                           const CSVFormat& format) {
    CSVReader reader(filename, format);
    return reader.index_of(col_name);
}

/** Get basic information about a CSV file
     *  @include programs/csv_info.cpp
     */
CSV_INLINE CSVFileInfo get_file_info(const std::string& filename) {
    CSVReader reader(filename);
    CSVFormat format = reader.get_format();
    for (auto it = reader.begin(); it != reader.end(); ++it)
        ;

    CSVFileInfo info = {filename, reader.get_col_names(), format.get_delim(), reader.n_rows(),
                        reader.get_col_names().size()};

    return info;
}
} // namespace csv

#endif
