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

#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>

#include <algorithm>
#include <cstring>
#include <streambuf>
#include <vector>

namespace doris {

// A non-copying iostream.
// See https://stackoverflow.com/questions/35322033/aws-c-sdk-uploadpart-times-out
// https://stackoverflow.com/questions/13059091/creating-an-input-stream-from-constant-memory
class StringViewStream : Aws::Utils::Stream::PreallocatedStreamBuf, public std::iostream {
public:
    StringViewStream(const void* buf, int64_t nbytes)
            : Aws::Utils::Stream::PreallocatedStreamBuf(
                      reinterpret_cast<unsigned char*>(const_cast<void*>(buf)),
                      static_cast<size_t>(nbytes)),
              std::iostream(this) {}
};

// The AWS SDK writes the body of every response into the stream built by the response
// stream factory of the request, whatever the status of that response is. Reading an
// object range straight into the buffer of the caller therefore breaks as soon as the
// server answers with an error: the XML body of a `429 SlowDown` is a few hundred bytes
// and does not fit into the buffer of a small range read. `PreallocatedStreamBuf` does not
// implement `overflow()`, so the stream turns bad, curl aborts the transfer with
// `CURLE_WRITE_ERROR`, and the SDK reports an `INTERNAL_FAILURE` named "Failed to flush
// response stream" while never recording the status code of the response. Both the retry
// strategy of the SDK and the retry of `S3FileReader` key on that status code, so an error
// the server asked us to retry ends up cancelling the query instead.
//
// This stream buffer writes into the buffer of the caller as long as the body fits, which
// is the case for every successful ranged read, and spills the rest into a buffer of its
// own. The stream never turns bad, so the SDK reports the real status code and can parse
// the error out of the body.
class S3ResponseStreamBuf final : public std::streambuf {
public:
    // Bodies beyond this size are truncated. Only error documents are expected to overflow
    // and one is a few hundred bytes, so this leaves them two orders of magnitude of room
    // while bounding what a single failing request can hold. Kept small on purpose: this
    // buffer is allocated on the transport thread of the SDK, out of the reach of the memory
    // tracker of the query, and every concurrent read that fails holds one of its own.
    static constexpr size_t MAX_SPILL_SIZE = 64 * 1024;

    S3ResponseStreamBuf(void* buf, size_t nbytes) : _buf(static_cast<char*>(buf)) {
        setp(_buf, _buf + nbytes);
        setg(_buf, _buf, _buf);
    }

protected:
    std::streamsize xsputn(const char* s, std::streamsize n) override {
        if (!_spilled) {
            if (n <= epptr() - pptr()) {
                std::memcpy(pptr(), s, n);
                pbump(static_cast<int>(n));
                return n;
            }
            _spill_over();
        }
        // Saturating on its own: the spill is clamped when it is filled from the buffer of
        // the caller, and this must not underflow into an unbounded write if it ever is not.
        auto room = _spill.size() < MAX_SPILL_SIZE ? MAX_SPILL_SIZE - _spill.size() : 0;
        auto writable = std::min(static_cast<size_t>(n), room);
        _spill.insert(_spill.end(), s, s + writable);
        // Always report the whole write as consumed. A short write is what makes curl
        // abort the transfer and lose the status code of the response.
        return n;
    }

    int_type overflow(int_type ch) override {
        if (traits_type::eq_int_type(ch, traits_type::eof())) {
            return traits_type::not_eof(ch);
        }
        auto c = traits_type::to_char_type(ch);
        xsputn(&c, 1);
        return ch;
    }

    int_type underflow() override {
        _reset_get_area(_read_pos());
        if (gptr() == egptr()) {
            return traits_type::eof();
        }
        return traits_type::to_int_type(*gptr());
    }

    pos_type seekoff(off_type off, std::ios_base::seekdir dir,
                     std::ios_base::openmode which) override {
        auto size = static_cast<off_type>(_written());
        if ((which & std::ios_base::out) && !(which & std::ios_base::in)) {
            // The SDK only asks for the write position, to tell an empty body apart from a
            // body it has to parse. Moving the write pointer is not supported.
            return dir == std::ios_base::cur && off == 0 ? pos_type(size) : pos_type(off_type(-1));
        }
        // A seek asking for both areas at once, which is what the default argument of
        // `pubseekoff()` and `pubseekpos()` does, is served as a seek of the read area. The
        // write area is append only, so there is nothing to move there.
        off_type pos = off;
        if (dir == std::ios_base::cur) {
            pos += static_cast<off_type>(_read_pos());
        } else if (dir == std::ios_base::end) {
            pos += size;
        }
        if (pos < 0 || pos > size) {
            return pos_type(off_type(-1));
        }
        _reset_get_area(static_cast<size_t>(pos));
        return pos_type(pos);
    }

    pos_type seekpos(pos_type pos, std::ios_base::openmode which) override {
        return seekoff(pos, std::ios_base::beg, which);
    }

private:
    // Moves what has been written so far into the spill buffer, so that the body stays
    // contiguous and the SDK can parse the error out of it. Truncated right here: the buffer
    // of the caller is the size of the range that was asked for, `remote_storage_read_buffer_mb`
    // of it for a prefetched read and the whole file for a download, so it can be far larger
    // than the bound of the spill. Starting the spill beyond its own bound would leave no room
    // for the truncation to ever apply and let a server answering a ranged read with the whole
    // object be buffered in full.
    void _spill_over() {
        auto kept = std::min(static_cast<size_t>(pptr() - _buf), MAX_SPILL_SIZE);
        _spill.assign(_buf, _buf + kept);
        setp(nullptr, nullptr);
        _spilled = true;
    }

    // Bytes of the body held by this buffer, truncation excluded.
    size_t _written() const { return _spilled ? _spill.size() : pptr() - _buf; }

    // Both areas start at the same logical offset, so the read position survives a spill.
    size_t _read_pos() const { return gptr() - eback(); }

    void _reset_get_area(size_t pos) {
        char* begin = _spilled ? _spill.data() : _buf;
        auto size = _written();
        pos = std::min(pos, size);
        setg(begin, begin + pos, begin + size);
    }

    char* _buf;
    std::vector<char> _spill;
    bool _spilled = false;
};

class S3ResponseStream final : public std::iostream {
public:
    S3ResponseStream(void* buf, size_t nbytes) : std::iostream(&_buf), _buf(buf, nbytes) {}

private:
    S3ResponseStreamBuf _buf;
};

// By default, the AWS SDK reads object data into an auto-growing StringStream.
// To avoid copies, read the body directly into our preallocated buffer instead, and keep
// only what does not fit, which is an error document, in a buffer of the stream itself.
// See https://github.com/aws/aws-sdk-cpp/issues/64 for an alternative but
// functionally similar recipe.
inline Aws::IOStreamFactory AwsWriteableStreamFactory(void* buf, int64_t nbytes) {
    return [=]() { return Aws::New<S3ResponseStream>("", buf, static_cast<size_t>(nbytes)); };
}

} // namespace doris
