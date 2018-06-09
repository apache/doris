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

#ifndef BDG_PALO_BE_SRC_RPC_INET_ADDR_H
#define BDG_PALO_BE_SRC_RPC_INET_ADDR_H

#include "util.h"
#include <cstring>

extern "C" {
#include <netinet/in.h>
}

namespace palo {
/**
 * High-level entry point to a service; wraps a host:port pair.
 */
struct Endpoint {
    Endpoint(const std::string &host, uint16_t port)
        : host(host), port(port) {
        }
    Endpoint()
        : port(0) {
        }
    std::string host;
    uint16_t port;
};

/** Stream operator to print an Endpoint structure */
std::ostream &operator<<(std::ostream &, const Endpoint &);

/**
 * Encapsulate an internet address.
 *
 * Note, deriving from sockaddr_in is just a path of least resistance
 * and should ,e changed to aggregation if we decide to support ipv6
 */
class InetAddr : public sockaddr_in {
public:

    /** Constructor creates an empty internet address */
    InetAddr();

    /** Constructor creates an internet address from an endpoint; resolves
     * the hostname to an IP address
     *
     * @param host The hostname
     * @param port The port of the remote service
     * @throws error::BAD_DOMAIN_NAME if the host cannot be resolved
     */

    InetAddr(const std::string &host, uint16_t port);
    /** Constructor creates an internet address from an endpoint encoded
     * as "hostname:port"
     *
     * @param endpoint The remote addres ("hostname:port")
     * @throws error::BAD_DOMAIN_NAME if the host cannot be resolved
     */

    InetAddr(const std::string &endpoint);
    /** Constructor creates an internet address from a 32bit IP address
     *
     * @param ip32 The 32bit IP address
     * @param port The port of the remote service
     * @throws error::BAD_DOMAIN_NAME if the host cannot be resolved
     */

    InetAddr(uint32_t ip32, uint16_t port);
    /** Constructor creates an internet address from a unix sockaddr_in
     * structure
     *
     * @param addr The unix sockaddr_in structure
     * @throws error::BAD_DOMAIN_NAME if the host cannot be resolved
     */
    InetAddr(const sockaddr_in &addr) { operator=(addr); }

    /** Assigns a unix sockaddr_in address structure to this object */
    InetAddr &operator=(const sockaddr_in &addr) {
        if (this != &addr) {
            memcpy(this, &addr, sizeof(sockaddr_in));
        }
        return *this;
    }

    bool operator==(const InetAddr &other) const {
        return (bool)!memcmp(this, &other, sizeof(InetAddr));
    }

    bool operator!=(const InetAddr &other) const {
        return !(*this == other);
    }

    bool operator<(const InetAddr &other) const {
        if (sin_family != other.sin_family) {
            return sin_family < other.sin_family;
        }
        if (sin_addr.s_addr != other.sin_addr.s_addr) {
            return sin_addr.s_addr < other.sin_addr.s_addr;
        }
        return sin_port < other.sin_port;
    }

    /** Returns a string with a dotted notation ("127.0.0.1:8080") including
     * the port */
    std::string format(int sep = ':') const { return InetAddr::format(*this, sep); }

    /** Returns a string with a dotted notation ("127.0.0.1") without!
     * the port */
    std::string format_ipaddress() { return InetAddr::format_ipaddress(*this); }

    /** Returns a compact std::string representation ("0x2387646:80") including
     * the port */
    std::string hex(int sep = ':') { return InetAddr::hex(*this, sep); }

    // convenient/legacy static methods
    /** Initialize a sockaddr_in structure from host:port
     *
     * @param addr The sockaddr_in structure (or a pointer to an InetAddr)
     * @param host The hostname of the remote service
     * @param port The port of the remote service
     * @return true on success, otherwise false
     */
    static bool initialize(sockaddr_in *addr, const char *host, uint16_t port);

    /** Initialize addr from an endpoint string (host:port)
     *
     * @param addr The sockaddr_in structure (or a pointer to an InetAddr)
     * @param addr_str A string with the remote address
     * @return true on success, otherwise false
     */
    static bool initialize(sockaddr_in *addr, const char *addr_str);

    /** Initializes addr from an integer ip address and port
     *
     * @param addr The sockaddr_in structure (or a pointer to an InetAddr)
     * @param haddr The remote host's address (IPv4)
     * @param port The remote host's port
     * @return true on success, otherwise false
     */
    static bool initialize(sockaddr_in *addr, uint32_t haddr, uint16_t port);

    /**
     * Parse an endpoint string in (host:port) format
     *
     * @param endpoint Hostname or hostname:port of the endpoint
     * @param defport Default port if the hostname does not include a port
     * @return Endpoint tuple
     */
    static Endpoint parse_endpoint(const char *endpoint, int defport = 0);

    /**
     * Parse an endpoint string in (host:port) format
     *
     * @param endpoint Hostname or hostname:port of the endpoint
     * @param defport Default port if the hostname does not include a port
     * @return Endpoint tuple
     */
    static Endpoint parse_endpoint(const std::string &endpoint, int defport = 0) {
        return parse_endpoint(endpoint.c_str(), defport);
    }

    /** Parses an ipv4 address string in dotted notiation ("n.n.n.n") or as a
     * number and initializes a sockaddr_in structure with the address
     *
     * @param ip The ipv4 string in dotted notiation or the number
     * @param port The port of the address
     * @param addr Returns the parsed address
     * @param base Base of the integer representation (default: 0, handle 0x)
     * @return true on success, otherwise false
     */
    static bool parse_ipv4(const char *ip, uint16_t port, sockaddr_in &addr,
            int base = 0);

    /** Tests whether the input string in n.n.n.n format (base 10)
     *
     * @param ip - ipv4 string
     * @return true if it's an address in dotted notation, otherwise false
     */
    static bool is_ipv4(const char *ip);

    /** Formats a socket address
     *
     * @param addr The address structure
     * @param sep The separator character between hostname and port
     * @return A string with the formatted "hostname:port" tuple
     */
    static std::string format(const sockaddr_in &addr, int sep = ':');

    /** Formats a socket address without the port
     *
     * @param addr The address structure
     * @return A string with the formatted "hostname:port" tuple
     */
    static std::string format_ipaddress(const sockaddr_in &addr);

    /** Returns a compact std::string representation ("0x2387646:80") including
     * the port
     *
     * @param addr The address structure
     * @param sep The separator character between hostname and port
     * @return A string with the formatted tuple
     */
    static std::string hex(const sockaddr_in &addr, int sep = ':');

    /// Returns serialized object length.
    /// Returns the serialized length of the object as encoded by encode().
    /// @see encode() for encoding format
    /// @return Overall serialized object length
    size_t encoded_length() const;

    /// Writes serialized representation of object to a buffer.
    /// This function encodes a serialized representation of the object,
    /// starting with a header that includes the encoding version and the
    /// serialized length of the object.  After the header, the object per-se is
    /// encoded with encode_internal().
    /// @param bufp Address of destination buffer pointer (advanced by call)
    void encode(uint8_t **bufp) const;

    /// Reads serialized representation of object from a buffer.
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of remaining buffer
    /// @see encode() for encoding format
    /// @throws Exception with code error::PROTOCOL_ERROR if version being
    /// decoded is greater than that returned by encoding_version().
    void decode(const uint8_t **bufp, size_t *remainp);

    /// Deserializes object from legacy serialization format.
    void legacy_decode(const uint8_t **bufp, size_t *remainp);

private:

    /// Returns encoding version.
    /// @return Encoding version
    uint8_t encoding_version() const;

    /// Returns internal serialized length.
    /// This function is to be overridden by derived classes and should return
    /// the length of the the serialized object per-se.
    /// @return Internal serialized length
    /// @see encode_internal() for encoding format
    size_t encoded_length_internal() const;

    /// Writes serialized representation of object to a buffer.
    /// This function is to be overridden by derived classes and should encode
    /// the object per-se.
    /// @param bufp Address of destination buffer pointer (advanced by call)
    void encode_internal(uint8_t **bufp) const;

    /// Reads serialized representation of object from a buffer.
    /// This function is to be overridden by derived classes and should decode
    /// the object per-se as encoded with encode_internal().
    /// @param version Encoding version
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of serialized encoding remaining
    /// @see encode_internal() for encoding format
    void decode_internal(uint8_t version, const uint8_t **bufp, size_t *remainp);
};

/** Helper operator for streaming a sockaddr_in structure (or an InetAddr
 * object) */
std::ostream &operator<<(std::ostream &, const sockaddr_in &);

} //namespace palo
#endif //BDG_PALO_BE_SRC_RPC_INET_ADDR_H
