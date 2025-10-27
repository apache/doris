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

suite("test_error_msg_truncate","nonConcurrent") {
    def tableName = "test_error_msg_truncate_table"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT,
            value VARCHAR(10)
        ) PROPERTIES (
            "replication_num" = "1"
        );
        """
    
    GetDebugPoint().clearDebugPointsForAllFEs()
    
    try {
        // case1: Test with debug point - error message should be truncated to 512 bytes
        GetDebugPoint().enableDebugPointForAllFEs("TestErrorMsgTruncate")
        
        def hasException = false
        try {
            sql """
                INSERT INTO ${tableName} VALUES (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
            """
        } catch (Exception e) {
            hasException = true
            String errorMsg = e.getMessage()
            logger.info("Case1 - Error message with debug point: " + errorMsg)
            logger.info("Case1 - Error message length: " + errorMsg.length() + " bytes")
            
            // Check if error message contains "Test" (added by debug point)
            assertTrue(errorMsg.contains("Test"), 
                "Error message should contain 'Test' from debug point injection")
            
            // Check if error message contains url
            assertTrue(errorMsg.contains("url:"), 
                "Error message should contain 'url:'")

            // Check if error message contains url
            assertTrue(errorMsg.contains("please use `show load` for detail msg"), 
                "Error message should contain 'please use `show load` for detail msg'")
            
            // Check if error message total length is within limit (512 bytes)
            assertTrue(errorMsg.length() <= 512, 
                "Error message length should not exceed 512 bytes, actual: " + errorMsg.length())
            
            logger.info("Case1 - Truncation test passed")
        }
        
        assertTrue(hasException, "Should throw exception for inserting oversized value")
        
    } catch (Exception e) {
        logger.info("Test failed with exception: " + e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
    }
    
    // case2: Test without debug point - verify first_error_msg and url content
    try {
        sql """
            INSERT INTO ${tableName} VALUES (2, 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');
        """
        fail("Should throw exception")
    } catch (Exception e) {
        String errorMsg = e.getMessage()
        logger.info("Case2 - Normal error message: " + errorMsg)
        logger.info("Case2 - Error message length: " + errorMsg.length() + " bytes")
        
        // Should NOT contain "Test" (no debug point)
        assertFalse(errorMsg.contains("Test"), 
            "Error message should NOT contain 'Test' without debug point")
        
        // Check if error message contains first_error_msg
        assertTrue(errorMsg.contains("first_error_msg:"), 
            "Error message should contain 'first_error_msg:'")
        
        // Extract and verify first_error_msg content
        def firstErrorMsgPattern = ~/first_error_msg:\s*([^.]+(?:\.[^u])*)/
        def matcher = errorMsg =~ firstErrorMsgPattern
        if (matcher.find()) {
            String firstErrorMsg = matcher.group(1).trim()
            logger.info("Case2 - first_error_msg content: " + firstErrorMsg)
            
            // Verify it contains expected error information
            assertTrue(firstErrorMsg.contains("column_name[value]"), 
                "first_error_msg should mention the column name")
            assertTrue(firstErrorMsg.contains("length"), 
                "first_error_msg should mention length issue")
            assertTrue(firstErrorMsg.contains("schema"), 
                "first_error_msg should mention schema")
            
            logger.info("Case2 - first_error_msg validation passed")
        } else {
            fail("Could not extract first_error_msg from error message")
        }
        
        // Check if error message contains url
        assertTrue(errorMsg.contains("url:"), 
            "Error message should contain 'url:'")
        
        // Extract and verify URL
        def urlPattern = ~/url:\s*(http:\/\/[^\s]+)/
        matcher = errorMsg =~ urlPattern
        if (matcher.find()) {
            String url = matcher.group(1).trim()
            logger.info("Case2 - URL: " + url)
            
            // Verify URL format
            assertTrue(url.startsWith("http://"), 
                "URL should start with http://")
            assertTrue(url.contains("_load_error_log"), 
                "URL should be a load error log URL")
            assertTrue(url.contains("file="), 
                "URL should contain file parameter")
            
            // Check URL contains necessary components
            def urlComponents = url.split("\\?")
            assertTrue(urlComponents.length == 2, 
                "URL should have query parameters")
            
            def queryParams = urlComponents[1]
            assertTrue(queryParams.contains("file="), 
                "URL should have file parameter")
            
            logger.info("Case2 - URL format validation passed")
            logger.info("Case2 - URL is complete and well-formed")
        } else {
            fail("Could not extract URL from error message")
        }
    }
    
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
