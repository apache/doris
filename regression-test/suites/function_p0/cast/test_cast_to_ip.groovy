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


suite("test_cast_to_ip") {
    sql "set debug_skip_fold_constant=true;"
    
    // Test casting to IPv4 from valid string formats
    sql "set enable_strict_cast=false;"
    qt_cast_str_to_ipv4_valid """
    SELECT 
        CAST('192.168.0.1' AS IPv4) AS ipv4_standard,
        CAST('127.0.0.1' AS IPv4) AS ipv4_localhost,
        CAST('0.0.0.0' AS IPv4) AS ipv4_zero,
        CAST('255.255.255.255' AS IPv4) AS ipv4_broadcast,
        CAST(' 10.0.0.1 ' AS IPv4) AS ipv4_with_spaces,
        CAST('010.000.000.001' AS IPv4) AS ipv4_with_leading_zeros;
    """
    
    // Test casting to IPv4 from invalid string formats (non-strict mode)
    qt_cast_str_to_ipv4_invalid """
    SELECT 
        CAST('not_an_ip' AS IPv4) AS ipv4_invalid_str,
        CAST('192.168.0' AS IPv4) AS ipv4_incomplete,
        CAST('192.168.0.256' AS IPv4) AS ipv4_out_of_range,
        CAST('192.168.0.1.5' AS IPv4) AS ipv4_too_many_parts,
        CAST('2001:db8::1' AS IPv4) AS ipv4_from_ipv6,
        CAST('192.168.0' AS IPv4) AS ipv4_too_few_parts,
        CAST('192.168.0.a' AS IPv4) AS ipv4_non_numeric;
    """

    qt_cast_str_to_ipv4 """
    SELECT 
        CAST('192.168.1.1' AS IPv4) AS standard_valid_ipv4,           
        CAST('0.0.0.0' AS IPv4) AS min_boundary_ipv4,                
        CAST('255.255.255.255' AS IPv4) AS max_boundary_ipv4,        
        CAST('10.20.30.40' AS IPv4) AS regular_ipv4,                 
        CAST(' 192.168.1.1 ' AS IPv4) AS whitespace_ipv4,          
        CAST('192.168.01.1' AS IPv4) AS leading_zero_ipv4,         
        CAST('1.2.3' AS IPv4) AS missing_segment_ipv4,              
        CAST('1.2.3.4.5' AS IPv4) AS extra_segment_ipv4,            
        CAST('256.0.0.1' AS IPv4) AS first_out_of_range_ipv4,        
        CAST('1.300.2.3' AS IPv4) AS second_out_of_range_ipv4,      
        CAST('1.2.3.' AS IPv4) AS last_segment_missing_ipv4,         
        CAST('.1.2.3' AS IPv4) AS first_segment_missing_ipv4,       
        CAST('1..2.3' AS IPv4) AS empty_segment_ipv4,               
        CAST('a.b.c.d' AS IPv4) AS non_numeric_ipv4,               
        CAST('1.2.+3.4' AS IPv4) AS invalid_symbol_ipv4;           
    """

   qt_cast_str_to_ipv6 """
    SELECT 
    CAST('2001:db8:85a3:0000:0000:8a2e:0370:7334' AS IPv6) AS standard_valid_ipv6,  
    CAST('::' AS IPv6) AS zero_ipv6,                                                
    CAST('2001:db8::' AS IPv6) AS compressed_ipv6,                                  
    CAST('::ffff:192.168.1.1' AS IPv6) AS ipv4_mapped_ipv6,                        
    CAST(' 2001:db8::1 ' AS IPv6) AS whitespace_ipv6,                               
    CAST('2001:db8::1::2' AS IPv6) AS double_compression_ipv6,                    
    CAST('2001:db8:85a3:0000:0000:8a2e:0370:7334:1234' AS IPv6) AS too_many_groups_ipv6, 
    CAST('2001:db8:85a3:0000:8a2e:0370' AS IPv6) AS too_few_groups_ipv6,            
    CAST('2001:db8:85g3:0000:0000:8a2e:0370:7334' AS IPv6) AS invalid_hex_char_ipv6,
    CAST('2001:db8::ffff:192.168.1.260' AS IPv6) AS ipv4_part_out_of_range_ipv6,    
    CAST('2001:db8::ffff:192.168..1' AS IPv6) AS ipv4_part_format_error_ipv6,       
    CAST('2001:0db8:85a3:::8a2e:0370:7334' AS IPv6) AS triple_colon_ipv6,         
    CAST('20001:db8::1' AS IPv6) AS first_group_too_long_ipv6;                  
   """


    // Test casting to IPv6 from valid string formats
    qt_cast_str_to_ipv6_valid """
    SELECT 
        CAST('2001:db8::1' AS IPv6) AS ipv6_standard,
        CAST('::1' AS IPv6) AS ipv6_localhost,
        CAST('::' AS IPv6) AS ipv6_zero,
        CAST('2001:db8:0:0:0:0:0:1' AS IPv6) AS ipv6_full,
        CAST('2001:db8::0:1' AS IPv6) AS ipv6_compressed,
        CAST(' 2001:db8::1 ' AS IPv6) AS ipv6_with_spaces,
        CAST('2001:DB8::1' AS IPv6) AS ipv6_uppercase,
        CAST('2001:db8:85a3:0000:0000:8a2e:0370:7334' AS IPv6) AS ipv6_full_standard,
        CAST('2001:db8::' AS IPv6) AS ipv6_compressed_end;
    """
    
    // Test IPv6 mapped IPv4 addresses
    qt_cast_str_to_ipv6_mapped_ipv4 """
    SELECT 
        CAST('::ffff:192.168.0.1' AS IPv6) AS ipv6_mapped_ipv4,
        CAST('::ffff:c0a8:1' AS IPv6) AS ipv6_mapped_ipv4_hex,
        CAST('::192.168.0.1' AS IPv6) AS ipv6_compat_ipv4,
        CAST('::ffff:192.168.1.1' AS IPv6) AS ipv6_mapped_ipv4_new;
    """
    
    // Test casting to IPv6 from invalid string formats (non-strict mode)
    qt_cast_str_to_ipv6_invalid """
    SELECT 
        CAST('not_an_ip' AS IPv6) AS ipv6_invalid_str,
        CAST('2001:db8::gggg' AS IPv6) AS ipv6_invalid_hex,
        CAST('2001:db8:::1' AS IPv6) AS ipv6_invalid_format,
        CAST('2001:db8::1::2' AS IPv6) AS ipv6_too_many_compressions,
        CAST(' 1:1:::1' AS IPv6) AS ipv6_with_double_compression,
        CAST(' 1:1:1::1:1:1:1:1 ' AS IPv6) AS ipv6_with_not_use_compression,
        CAST('2001:db8:0:0:0:0:0:0:1' AS IPv6) AS ipv6_too_many_parts;
    """

    // Test casting between IPv4 and IPv6
    qt_cast_between_ip_types """
    SELECT 
        CAST(CAST('192.168.0.1' AS IPv4) AS VARCHAR) AS ipv4_to_string,
        CAST(CAST('2001:db8::1' AS IPv6) AS VARCHAR) AS ipv6_to_string,
        CAST(CAST('192.168.0.1' AS IPv4) AS IPv6) AS ipv4_to_ipv6;
    """
    
    // Enable strict mode for the same tests
    sql "set enable_strict_cast=true;"
    
    // Test casting to IPv4 from valid string formats (strict mode)
    qt_cast_str_to_ipv4_valid_strict """
    SELECT 
        CAST('192.168.0.1' AS IPv4) AS ipv4_standard,
        CAST('127.0.0.1' AS IPv4) AS ipv4_localhost,
        CAST('0.0.0.0' AS IPv4) AS ipv4_zero,
        CAST('255.255.255.255' AS IPv4) AS ipv4_broadcast,
        CAST(' 10.0.0.1 ' AS IPv4) AS ipv4_with_spaces,
        CAST('010.000.000.001' AS IPv4) AS ipv4_with_leading_zeros;
    """
    
    // Test casting to IPv6 from valid string formats (strict mode)
    qt_cast_str_to_ipv6_valid_strict """
    SELECT 
        CAST('2001:db8::1' AS IPv6) AS ipv6_standard,
        CAST('::1' AS IPv6) AS ipv6_localhost,
        CAST('::' AS IPv6) AS ipv6_zero,
        CAST('2001:db8:0:0:0:0:0:1' AS IPv6) AS ipv6_full,
        CAST('2001:db8::0:1' AS IPv6) AS ipv6_compressed,
        CAST(' 2001:db8::1 ' AS IPv6) AS ipv6_with_spaces,
        CAST('2001:DB8::1' AS IPv6) AS ipv6_uppercase;
    """



    // IPv4 map to IPv6 addresses in strict mode
    qt_cast_str_to_ipv6_mapped_ipv4_strict """
    SELECT 
        CAST('192.168.0.0' as IPv4) AS ipv4_address,
        CAST( CAST('192.168.0.0' as IPv4) as IPv6) AS ipv6_mapped_ipv4,
        CAST('0.0.0.0' as IPv4) AS ipv4_address2,
        CAST( CAST('0.0.0.0' as IPv4) as IPv6) AS ipv6_mapped_ipv4_zero;
    """
    // Test invalid IP formats in strict mode (should throw error)
    test {
        sql "SELECT CAST('not_an_ip' AS IPv4) AS invalid_ipv4;"
        exception "parse ipv4 fail"
    }
    
    test {
        sql "SELECT CAST('192.168.0.256' AS IPv4) AS out_of_range_ipv4;"
        exception "parse ipv4 fail"
    }
    
    test {
        sql "SELECT CAST('not_an_ip' AS IPv6) AS invalid_ipv6;"
        exception "parse ipv6 fail"
    }
    
    test {
        sql "SELECT CAST('2001:db8::gggg' AS IPv6) AS invalid_hex_ipv6;"
       exception "parse ipv6 fail"
    }  

    test {
        sql "SELECT CAST('2001:db8:::1' AS IPv6) AS invalid_format_ipv6;"
        exception "parse ipv6 fail"
    }

    test {
        sql "SELECT CAST('1:1:::1' AS IPv6) AS too_many_compressions_ipv6;"
        exception "parse ipv6 fail"
    }

    test {
        sql "SELECT CAST(' 1:1:1::1:1:1:1:1' AS IPv6) AS too_many_compressions_ipv6;"
        exception "parse ipv6 fail"
    }

}