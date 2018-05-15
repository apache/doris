// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_RPC_COMM_BUF_H
#define BDG_PALO_BE_SRC_RPC_COMM_BUF_H

#include "comm_header.h"
#include "byte_string.h"
#include "inet_addr.h"
#include "common/logging.h"
#include "serialization.h"
#include "static_buffer.h"

#include <boost/shared_array.hpp>
#include <memory>
#include <string>

namespace palo {

/** Message buffer for holding data to be transmitted over a network.
 * The CommBuf class contains a primary buffer and an extended buffer along
 * with buffer pointers to keep track of how much data has been written into
 * the buffers. These pointers are managed by the IOHandler while the buffer
 * is being transmitted. The following example illustrates how to build a
 * request message using the CommBuf.  
 *
 * <pre>
 *   CommHeader header(COMMAND_FETCH_SCANBLOCK);
 *   header.gid = scanner_id;
 *   CommBuf *cbuf = new CommBuf(header, 4);
 *   cbuf->append_i32(scanner_id);
 * </pre>
 *
 * The following is a real world example of a CommBuf being used
 * to send back a response from a read request.
 *
 * <pre>
 *   CommHeader header;
 *   header.initialize_from_request_header(m_event->header);
 *   CommBufPtr cbp(new CommBuf( header, 10, ext));
 *   cbp->append_i32(error::OK);
 *   cbp->append_i16(moreflag);
 *   cbp->append_i32(id);
 *   error = m_comm->send_response(m_event->addr, cbp);
 * </pre>
 *
 */
class CommBuf {
public:
    /** Constructor.  This constructor initializes the CommBuf object by
     * allocating a primary buffer of length len and writing the header into it.
     * The internal pointer into the primary buffer is positioned to
     * just after the header.
     * @param hdr Comm header
     * @param len Length of the primary buffer to allocate
     */
    CommBuf(CommHeader &hdr, uint32_t len = 0) : header(hdr), ext_ptr(0) {
        len += header.encoded_length();
        data.set(new uint8_t[len], len, true);
        data_ptr = data.base + header.encoded_length();
        header.set_total_length(len);
    }

    /** Constructor. This constructor initializes the CommBuf object by
     * allocating a primary buffer of length len and writing the header into it.
     * It also sets the extended buffer to ext and takes ownership of it.
     * The total length written into the header is len plus ext.size.  The
     * internal pointer into the primary buffer is positioned to just after
     * the header.
     * @param hdr Comm header
     * @param len Length of the primary buffer to allocate
     * @param buffer Extended buffer
     */
    CommBuf(CommHeader &hdr, uint32_t len, StaticBuffer &buffer)
        : ext(buffer), header(hdr) {
            len += header.encoded_length();
            data.set(new uint8_t[len], len, true);
            data_ptr = data.base + header.encoded_length();
            header.set_total_length(len+buffer.size);
            ext_ptr = ext.base;
        }

    /** Constructor. This constructor initializes the CommBuf object by
     * allocating a primary buffer of length len and writing the header into it.
     * It also sets the extended buffer to the buffer pointed to by ext_buffer.
     * The total length written into the header is len plus ext_len.  The
     * internal pointer into the primary buffer is positioned to just after
     * the header.
     * @param hdr Comm header
     * @param len Length of the primary buffer to allocate
     * @param ext_buffer Shared array pointer to extended buffer
     * @param ext_len Length of valid data in ext_buffer
     */
    CommBuf(CommHeader &hdr, uint32_t len,
            boost::shared_array<uint8_t> &ext_buffer, uint32_t ext_len) :
        header(hdr), ext_shared_array(ext_buffer) {
            len += header.encoded_length();
            data.set(new uint8_t[len], len, true);
            data_ptr = data.base + header.encoded_length();
            ext.base = ext_shared_array.get();
            ext.size = ext_len;
            ext.own = false;
            header.set_total_length(len+ext_len);
            ext_ptr = ext.base;
        }

    ~CommBuf() { }

    /** Encodes the header at the beginning of the primary buffer.
     * This method resets the primary and extended data pointers to point to the
     * beginning of their respective buffers.  The AsyncComm layer
     * uses these pointers to track how much data has been sent and
     * what is remaining to be sent.
     */
    void write_header_and_reset() {
        uint8_t *buf = data.base;
        assert((data_ptr-data.base) == (int)data.size || data_ptr == data.base);
        header.encode(&buf);
        data_ptr = data.base;
        ext_ptr = ext.base;
    }

    /** Returns the primary buffer internal data pointer
    */
    void* get_data_ptr() { 
        return data_ptr; 
    }

    // get user data ptr and size
    void get_user_data(const uint8_t** ptr, uint32_t* size) {
        *ptr = (const uint8_t*)data.base + header.encoded_length();
        *size = header.total_len - header.encoded_length() - ext.size;
    }

    /** Returns address of the primary buffer internal data pointer
    */
    uint8_t** get_data_ptr_address() { 
        return &data_ptr; 
    }

    /** Advance the primary buffer internal data pointer by <code>len</code>
     * bytes
     * @param len the number of bytes to advance the pointer by
     * @return returns the advanced internal data pointer
     */
    void* advance_data_ptr(size_t len) { 
        data_ptr += len; 
        return data_ptr; 
    }

    /** Appends a boolean value to the primary buffer.  After appending, this
     * method advances the primary buffer internal data pointer by 1
     * @param bval Boolean value to append to primary buffer
     */
    void append_bool(bool bval) { 
        serialization::encode_bool(&data_ptr, bval);
    }

    /** Appends a byte of data to the primary buffer.  After appending, this
     * method advances the primary buffer internal data pointer by 1
     * @param bval byte value to append into buffer
     */
    void append_byte(uint8_t bval) { 
        *data_ptr++ = bval; 
    }

    /** Appends a sequence of bytes to the primary buffer.  After appending,
     * this method advances the primary buffer internal data pointer by the
     * number of bytes appended
     * @param bytes Starting address of byte sequence
     * @param len Number of bytes in sequence
     */
    void append_bytes(const uint8_t *bytes, uint32_t len) {
        memcpy(data_ptr, bytes, len);
        data_ptr += len;
    }

    /** Appends a c-style string to the primary buffer.  A string is encoded
     * as a 16-bit length, followed by the characters, followed by
     * a terminating '\\0'.
     *
     * @param str c-style string to append
     * @see serialization::encode_str16
     */
    void append_str16(const char *str) {
        serialization::encode_str16(&data_ptr, str);
    }

    /**
     * Appends a std::string to the primary buffer.  A string is encoded as
     * a 16-bit length, followed by the characters, followed by
     * a terminating '\\0'.
     *
     * @param str std string to append
     * @see serialization::encode_str16
     */
    void append_str16(const std::string &str) {
        serialization::encode_str16(&data_ptr, str);
    }

    /**
     * Appends a 16-bit integer to the primary buffer.  The integer is encoded
     * in little endian order and the primary buffer internal data pointer is
     * advanced to the position immediately following the encoded integer.
     * @param sval Two-byte short integer to append into buffer
     */
    void append_i16(uint16_t sval) {
        serialization::encode_i16(&data_ptr, sval);
    }

    /** Appends a 32-bit integer to the primary buffer.  The integer is encoded
     * in little endian order and the primary buffer internal data pointer is
     * advanced to the position immediately following the encoded integer.
     * @param ival Four-byte integer value to append into buffer
     */
    void append_i32(uint32_t ival) {
        serialization::encode_i32(&data_ptr, ival);
    }

    /** Appends a 64-bit integer to the primary buffer.  The integer is encoded
     * in little endian order and the primary buffer pointer is advanced
     * to the position immediately following the encoded integer.
     * @param lval Eight-byte long integer value to append into buffer
     */
    void append_i64(uint64_t lval) {
        serialization::encode_i64(&data_ptr, lval);
    }

    /** Appends a c-style string to the primary buffer.  A string is encoded
     * as a vint64 length, followed by the characters, followed by
     * a terminating '\\0'.
     *
     * @param str C-style string to append
     * @see serialization::encode_vstr
     */
    void append_vstr(const char *str) {
        serialization::encode_vstr(&data_ptr, str);
    }

    /** Appends a std::string to the primary buffer.  A string is encoded as
     * a vint64 length, followed by the characters, followed by
     * a terminating '\\0'.
     * @param str C++ string to append
     * @see serialization::encode_vstr
     */
    void append_vstr(const std::string &str) {
        serialization::encode_vstr(&data_ptr, str);
    }

    /** Appends a variable sized string to the primary buffer.  The
     * string is encoded as a vint length, followed by the bytes
     * (followed by a terminating '\\0').
     *
     * @param str C-style string to encode
     * @param len Length of string
     * @see serialization::encode_vstr
     */
    void append_vstr(const void *str, uint32_t len) {
        serialization::encode_vstr(&data_ptr, str, len);
    }

    /** Appends an InetAddr structure to the primary buffer.
     *
     * @param addr address structure
     * @see serialization::encode_inet_addr
     */
    void append_inet_addr(const InetAddr &addr) {
        serialization::encode_inet_addr(&data_ptr, addr);
    }

    friend class IOHandlerData;
    friend class IOHandlerDatagram;
    StaticBuffer data; //!< Primary data buffer
    StaticBuffer ext;  //!< Extended buffer
    CommHeader header; //!< Comm header

protected:
    /// Write pointer into #data buffer
    uint8_t* data_ptr;
    /// Write pointer into #ext buffer
    const uint8_t* ext_ptr;
    /// Smart pointer to extended buffer memory
    boost::shared_array<uint8_t> ext_shared_array;
};

/// Smart pointer to CommBuf
typedef std::shared_ptr<CommBuf> CommBufPtr;

} // namespace palo

#endif //BDG_PALO_BE_SRC_RPC_COMM_BUF_H
