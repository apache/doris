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

suite("test_pythonudf_ip") {
    def tableName = "test_pythonudf_ip_table"
    def runtime_version = "3.8.10"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            ip_v4 IPv4,
            ip_v6 IPv6,
            cidr STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """ INSERT INTO ${tableName} VALUES 
        (1, '192.168.1.1', '2001:db8::1', '192.168.1.0/24'),
        (2, '10.0.0.1', '::1', '10.0.0.0/8'),
        (3, '172.16.0.1', 'fe80::1', '172.16.0.0/12'),
        (4, '8.8.8.8', '2001:4860:4860::8888', '8.8.8.0/24'),
        (5, null, null, null)
    """

    try {
        // Test 1: Convert IPv4 to string (with automatic conversion)
        sql """ DROP FUNCTION IF EXISTS py_ipv4_to_string(IPv4); """
        sql """
        CREATE FUNCTION py_ipv4_to_string(IPv4) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(ip):
    # ip is automatically converted to ipaddress.IPv4Address
    if ip is None:
        return None
    return str(ip)
\$\$;
        """
        
        qt_select_ipv4_to_string """ 
            SELECT id, ip_v4, py_ipv4_to_string(ip_v4) AS ip_str 
            FROM ${tableName} 
            ORDER BY id 
        """

        // Test 2: Check if IPv4 is private (with automatic conversion)
        sql """ DROP FUNCTION IF EXISTS py_is_private_ipv4(IPv4); """
        sql """
        CREATE FUNCTION py_is_private_ipv4(IPv4) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(ip):
    # ip is automatically converted to ipaddress.IPv4Address
    if ip is None:
        return None
    return ip.is_private
\$\$;
        """
        
        qt_select_is_private """ 
            SELECT id, ip_v4, py_is_private_ipv4(ip_v4) AS is_private 
            FROM ${tableName} 
            WHERE ip_v4 IS NOT NULL
            ORDER BY id 
        """

        // Test 3: Check if IPv4 is in CIDR range (with automatic conversion)
        sql """ DROP FUNCTION IF EXISTS py_ipv4_in_cidr(IPv4, STRING); """
        sql """
        CREATE FUNCTION py_ipv4_in_cidr(IPv4, STRING) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
import ipaddress

def evaluate(ip, cidr_str):
    # ip is automatically converted to ipaddress.IPv4Address
    if ip is None or cidr_str is None:
        return None
    try:
        network = ipaddress.ip_network(cidr_str, strict=False)
        return ip in network
    except (ValueError, TypeError):
        return None
\$\$;
        """
        
        qt_select_ip_in_cidr """ 
            SELECT id, ip_v4, cidr, py_ipv4_in_cidr(ip_v4, cidr) AS in_range 
            FROM ${tableName} 
            WHERE ip_v4 IS NOT NULL AND cidr IS NOT NULL
            ORDER BY id 
        """

        // Test 4: Get network address from CIDR
        sql """ DROP FUNCTION IF EXISTS py_get_network_address(STRING); """
        sql """
        CREATE FUNCTION py_get_network_address(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
import ipaddress

def evaluate(cidr_str):
    if cidr_str is None:
        return None
    try:
        network = ipaddress.ip_network(cidr_str, strict=False)
        return str(network.network_address)
    except ValueError:
        return None
\$\$;
        """
        
        qt_select_network_address """ 
            SELECT id, cidr, py_get_network_address(cidr) AS network_addr 
            FROM ${tableName} 
            WHERE cidr IS NOT NULL
            ORDER BY id 
        """

        // Test 5: Get broadcast address from CIDR
        sql """ DROP FUNCTION IF EXISTS py_get_broadcast_address(STRING); """
        sql """
        CREATE FUNCTION py_get_broadcast_address(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
import ipaddress

def evaluate(cidr_str):
    if cidr_str is None:
        return None
    try:
        network = ipaddress.ip_network(cidr_str, strict=False)
        return str(network.broadcast_address)
    except ValueError:
        return None
\$\$;
        """
        
        qt_select_broadcast_address """ 
            SELECT id, cidr, py_get_broadcast_address(cidr) AS broadcast_addr 
            FROM ${tableName} 
            WHERE cidr IS NOT NULL
            ORDER BY id 
        """

        // Test 6: Convert IPv4 to integer (returns the same value, just for demonstration)
        sql """ DROP FUNCTION IF EXISTS py_ipv4_to_int(IPv4); """
        sql """
        CREATE FUNCTION py_ipv4_to_int(IPv4) 
        RETURNS BIGINT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(ip):
    # ip is automatically converted to ipaddress.IPv4Address
    if ip is None:
        return None
    # Convert IPv4Address to integer (uint32 range: 0 to 4294967295)
    return int(ip)
\$\$;
        """
        
        qt_select_ipv4_to_int """ 
            SELECT id, ip_v4, py_ipv4_to_int(ip_v4) AS ip_as_int 
            FROM ${tableName} 
            WHERE ip_v4 IS NOT NULL
            ORDER BY id 
        """

        // Test 7: Convert integer to IPv4
        sql """ DROP FUNCTION IF EXISTS py_int_to_ipv4(BIGINT); """
        sql """
        CREATE FUNCTION py_int_to_ipv4(BIGINT) 
        RETURNS IPv4 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
import ipaddress

def evaluate(ip_int):
    if ip_int is None:
        return None
    # Convert integer to IPv4Address object (required for IPv4 return type)
    # Make sure it's in valid range [0, 2^32-1]
    if 0 <= ip_int <= 4294967295:
        return ipaddress.IPv4Address(ip_int)
    return None
\$\$;
        """
        
        qt_select_int_to_ipv4 """ 
            SELECT py_int_to_ipv4(3232235777) AS ip
        """

        // Test 8: Convert IPv6 to string
        sql """ DROP FUNCTION IF EXISTS py_ipv6_to_string(IPv6); """
        sql """
        CREATE FUNCTION py_ipv6_to_string(IPv6) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(ip):
    # ip is automatically converted to ipaddress.IPv6Address
    if ip is None:
        return None
    return str(ip)
\$\$;
        """
        
        qt_select_ipv6_to_string """ 
            SELECT id, ip_v6, py_ipv6_to_string(ip_v6) AS ip_str 
            FROM ${tableName} 
            ORDER BY id 
        """

        // Test 9: Check if IPv6 is loopback
        sql """ DROP FUNCTION IF EXISTS py_is_loopback_ipv6(IPv6); """
        sql """
        CREATE FUNCTION py_is_loopback_ipv6(IPv6) 
        RETURNS BOOLEAN 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(ip):
    # ip is automatically converted to ipaddress.IPv6Address
    if ip is None:
        return None
    return ip.is_loopback
\$\$;
        """
        
        qt_select_is_loopback """ 
            SELECT id, ip_v6, py_is_loopback_ipv6(ip_v6) AS is_loopback 
            FROM ${tableName} 
            WHERE ip_v6 IS NOT NULL
            ORDER BY id 
        """

        // Test 10: Compare two IPv4 addresses
        sql """ DROP FUNCTION IF EXISTS py_compare_ipv4(IPv4, IPv4); """
        sql """
        CREATE FUNCTION py_compare_ipv4(IPv4, IPv4) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(ip1, ip2):
    # ip1 and ip2 are automatically converted to ipaddress.IPv4Address
    if ip1 is None or ip2 is None:
        return None
    # IPv4Address objects support comparison operators
    if ip1 < ip2:
        return -1
    elif ip1 > ip2:
        return 1
    else:
        return 0
\$\$;
        """
        
        qt_select_compare_ipv4 """ 
            SELECT 
                py_compare_ipv4('192.168.1.1', '192.168.1.2') AS compare1,
                py_compare_ipv4('10.0.0.1', '10.0.0.1') AS compare2,
                py_compare_ipv4('172.16.0.1', '10.0.0.1') AS compare3
        """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_ipv4_to_string(IPv4);")
        try_sql("DROP FUNCTION IF EXISTS py_is_private_ipv4(IPv4);")
        try_sql("DROP FUNCTION IF EXISTS py_ipv4_in_cidr(IPv4, STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_get_network_address(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_get_broadcast_address(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_ipv4_to_int(IPv4);")
        try_sql("DROP FUNCTION IF EXISTS py_int_to_ipv4(BIGINT);")
        try_sql("DROP FUNCTION IF EXISTS py_ipv6_to_string(IPv6);")
        try_sql("DROP FUNCTION IF EXISTS py_is_loopback_ipv6(IPv6);")
        try_sql("DROP FUNCTION IF EXISTS py_compare_ipv4(IPv4, IPv4);")
        try_sql("DROP TABLE IF EXISTS ${tableName};")
    }
}
