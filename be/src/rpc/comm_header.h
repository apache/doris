// Copyright (C) 2007-2016 Hypertable, Inc.
//
// This file is part of Hypertable.
// 
// Hypertable is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 3
// of the License, or any later version.
//
// Hypertable is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301, USA.

#ifndef BDG_PALO_BE_SRC_RPC_COMM_HEADER_H
#define BDG_PALO_BE_SRC_RPC_COMM_HEADER_H

namespace palo {
/** Header for messages transmitted via AsyncComm.
 */
class CommHeader {
public:
    static const size_t FIXED_LENGTH = 38;
    
    /** Enumeration constants for bits in flags field
    */
    enum Flags {
        FLAGS_BIT_REQUEST          = 0x0001, //!< Request message
        FLAGS_BIT_IGNORE_RESPONSE  = 0x0002, //!< Response should be ignored
        FLAGS_BIT_URGENT           = 0x0004, //!< Request is urgent
        FLAGS_BIT_PROFILE          = 0x0008, //!< Request should be profiled
        FLAGS_BIT_PROXY_MAP_UPDATE = 0x4000, //!< ProxyMap update message
        FLAGS_BIT_PAYLOAD_CHECKSUM = 0x8000  //!< Payload checksumming is enabled
    };

    /** Enumeration constants for flags field bitmaks
    */
    enum FlagMask {
        FLAGS_MASK_REQUEST          = 0xFFFE, //!< Request message bit
        FLAGS_MASK_IGNORE_RESPONSE  = 0xFFFD, //!< Response should be ignored bit
        FLAGS_MASK_URGENT           = 0xFFFB, //!< Request is urgent bit
        FLAGS_MASK_PROFILE          = 0xFFF7, //!< Request should be profiled
        FLAGS_MASK_PROXY_MAP_UPDATE = 0xBFFF, //!< ProxyMap update message bit
        FLAGS_MASK_PAYLOAD_CHECKSUM = 0x7FFF  //!< Payload checksumming is enabled bit
    };

    /** Default constructor.
    */
    CommHeader()
        : version(1), header_len(FIXED_LENGTH), alignment(0), flags(0),
        header_checksum(0), id(0), gid(0), total_len(0),
        timeout_ms(0), payload_checksum(0), command(0) {  }

    /** Constructor taking command number and optional timeout.
     * @param cmd Command number
     * @param timeout Request timeout
     */
    CommHeader(uint64_t cmd, uint32_t timeout = 0)
        : version(1), header_len(FIXED_LENGTH), alignment(0), flags(0),
        header_checksum(0), id(0), gid(0), total_len(0),
        timeout_ms(timeout), payload_checksum(0),
        command(cmd) {  }

    /** Returns fixed length of header.
     * @return Fixed length of header
     */
    size_t fixed_length() const { return FIXED_LENGTH; }

    /** Returns encoded length of header.
     * @return Encoded length of header
     */
    size_t encoded_length() const { return FIXED_LENGTH; }

    /** Encode header to memory pointed to by <code>*bufp</code>.
     * The <code>bufp</code> pointer is advanced to address immediately
     * following the encoded header.
     * @param bufp Address of memory pointer to where header is to be encoded.
     */
    void encode(uint8_t **bufp);

    /** Decode serialized header at <code>*bufp</code>
     * The <code>bufp</code> pointer is advanced to the address immediately
     * following the decoded header and <code>remainp</code> is decremented
     * by the length of the serialized header.
     * @param bufp Address of memory pointer to where header is to be encoded.
     * @param remainp Pointer to valid bytes remaining in buffer (decremented
     *                by call)
     * @throws error::COMM_BAD_HEADER If fixed header size is less than
     * <code>*remainp</code>.
     * @throws error::COMM_HEADER_CHECKSUM_MISMATCH If computed checksum does
     * not match checksum field
     */
    void decode(const uint8_t **bufp, size_t *remainp);

    /** Set total length of message (header + payload).
     * @param len Total length of message (header + payload)
     */
    void set_total_length(uint32_t len) { total_len = len; }

    /** Initializes header from <code>req_header</code>.
     * This method is typically used to initialize a response header
     * from a corresponding request header.
     * @param req_header Request header from which to initialize
     */
    void initialize_from_request_header(CommHeader &req_header) {
        flags = req_header.flags;
        id = req_header.id;
        gid = req_header.gid;
        command = req_header.command;
        total_len = 0;
    }

    uint8_t version;     //!< Protocol version
    uint8_t header_len;  //!< Length of header
    uint16_t alignment;  //!< Align payload to this byte offset
    uint16_t flags;      //!< Flags
    uint32_t header_checksum; //!< Header checksum (computed with this member 0)
    uint32_t id;         //!< Request ID
    uint32_t gid;        //!< Group ID (see ApplicationQueue)
    uint32_t total_len;  //!< Total length of message including header
    uint32_t timeout_ms; //!< Request timeout
    uint32_t payload_checksum; //!< Payload checksum (currently unused)
    uint64_t command;    //!< Request command number
};

} //namespace palo

#endif //BDG_PALO_BE_SRC_RPC_COMM_HEADER_H
