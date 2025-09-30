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

suite("test_tvf_fast_fail", "p0,external,tvf") {
    String enabled = context.config.otherConfigs.get("enableS3Test")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable S3 test.")
        return;
    }

    def tableName = "tvf_fast_fail_test"
    
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            r_reason_sk bigint,
            r_reason_id char(16),
            r_reason_desc char(100)
        )
        DUPLICATE KEY(r_reason_sk)
        DISTRIBUTED BY HASH(r_reason_sk) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // Test 1: Invalid endpoint should fail fast (within 15 seconds)
    def testInvalidEndpoint = {
        def startTime = System.currentTimeMillis()
        def errorOccurred = false
        def errorMessage = ""
        
        try {
            sql """
                INSERT INTO ${tableName}
                SELECT * FROM S3(
                    'uri' = 's3://test-bucket/test.csv',
                    'access_key' = 'test_access_key',
                    'secret_key' = 'test_secret_key',
                    's3.endpoint' = 'invalid-endpoint.com',
                    'region' = 'us-east-1',
                    'format' = 'csv',
                    'column_separator' = '|'
                );
            """
        } catch (Exception e) {
            errorOccurred = true
            errorMessage = e.getMessage()
            logger.info("Expected error occurred: ${errorMessage}")
        }
        
        def endTime = System.currentTimeMillis()
        def duration = (endTime - startTime) / 1000.0
        
        logger.info("Test invalid endpoint - Duration: ${duration} seconds, Error: ${errorOccurred}")
        
        // Assertions
        assertTrue(errorOccurred, "Expected error for invalid endpoint")
        assertTrue(duration < 15.0, "Fast fail should complete within 15 seconds, but took ${duration} seconds")
        assertTrue(errorMessage.contains("Failed to access object storage") || 
                  errorMessage.contains("invalid-endpoint.com"), 
                  "Error message should indicate connection failure: ${errorMessage}")
    }

    // Test 2: Non-existent endpoint should fail fast
    def testNonExistentEndpoint = {
        def startTime = System.currentTimeMillis()
        def errorOccurred = false
        def errorMessage = ""
        
        try {
            sql """
                INSERT INTO ${tableName}
                SELECT * FROM S3(
                    'uri' = 's3://test-bucket/test.csv',
                    'access_key' = 'test_access_key',
                    'secret_key' = 'test_secret_key',
                    's3.endpoint' = 'non-existent-s3-endpoint-12345.com',
                    'region' = 'us-east-1',
                    'format' = 'csv',
                    'column_separator' = '|'
                );
            """
        } catch (Exception e) {
            errorOccurred = true
            errorMessage = e.getMessage()
            logger.info("Expected error occurred: ${errorMessage}")
        }
        
        def endTime = System.currentTimeMillis()
        def duration = (endTime - startTime) / 1000.0
        
        logger.info("Test non-existent endpoint - Duration: ${duration} seconds, Error: ${errorOccurred}")
        
        // Assertions
        assertTrue(errorOccurred, "Expected error for non-existent endpoint")
        assertTrue(duration < 15.0, "Fast fail should complete within 15 seconds, but took ${duration} seconds")
        assertTrue(errorMessage.contains("Failed to access object storage") || 
                  errorMessage.contains("non-existent-s3-endpoint-12345.com"), 
                  "Error message should indicate connection failure: ${errorMessage}")
    }

    // Test 3: Malformed endpoint should fail fast
    def testMalformedEndpoint = {
        def startTime = System.currentTimeMillis()
        def errorOccurred = false
        def errorMessage = ""
        
        try {
            sql """
                INSERT INTO ${tableName}
                SELECT * FROM S3(
                    'uri' = 's3://test-bucket/test.csv',
                    'access_key' = 'test_access_key',
                    'secret_key' = 'test_secret_key',
                    's3.endpoint' = 'malformed-url-without-domain',
                    'region' = 'us-east-1',
                    'format' = 'csv',
                    'column_separator' = '|'
                );
            """
        } catch (Exception e) {
            errorOccurred = true
            errorMessage = e.getMessage()
            logger.info("Expected error occurred: ${errorMessage}")
        }
        
        def endTime = System.currentTimeMillis()
        def duration = (endTime - startTime) / 1000.0
        
        logger.info("Test malformed endpoint - Duration: ${duration} seconds, Error: ${errorOccurred}")
        
        // Assertions
        assertTrue(errorOccurred, "Expected error for malformed endpoint")
        assertTrue(duration < 15.0, "Fast fail should complete within 15 seconds, but took ${duration} seconds")
        assertTrue(errorMessage.contains("Failed to access object storage") || 
                  errorMessage.contains("malformed-url-without-domain"), 
                  "Error message should indicate connection failure: ${errorMessage}")
    }

    // Test 4: Compare with valid endpoint (if available) - this should work or fail for other reasons
    def testValidEndpointComparison = {
        if (context.config.otherConfigs.get("s3Endpoint") != null) {
            def validEndpoint = context.config.otherConfigs.get("s3Endpoint")
            def startTime = System.currentTimeMillis()
            def errorOccurred = false
            def errorMessage = ""
            
            try {
                sql """
                    INSERT INTO ${tableName}
                    SELECT * FROM S3(
                        'uri' = 's3://non-existent-bucket-12345/test.csv',
                        'access_key' = 'test_access_key',
                        'secret_key' = 'test_secret_key',
                        's3.endpoint' = '${validEndpoint}',
                        'region' = 'us-east-1',
                        'format' = 'csv',
                        'column_separator' = '|'
                    );
                """
            } catch (Exception e) {
                errorOccurred = true
                errorMessage = e.getMessage()
                logger.info("Error with valid endpoint (expected due to invalid credentials): ${errorMessage}")
            }
            
            def endTime = System.currentTimeMillis()
            def duration = (endTime - startTime) / 1000.0
            
            logger.info("Test valid endpoint - Duration: ${duration} seconds, Error: ${errorOccurred}")
            
            // With valid endpoint, the error should be about credentials/bucket, not connection
            if (errorOccurred) {
                assertFalse(errorMessage.contains("Failed to access object storage") && 
                           errorMessage.contains(validEndpoint), 
                           "Valid endpoint should not have connection failure: ${errorMessage}")
            }
        } else {
            logger.info("Skipping valid endpoint test - no valid endpoint configured")
        }
    }

    // Run all tests
    logger.info("=== Starting TVF Fast Fail Tests ===")
    
    logger.info("Test 1: Invalid endpoint fast fail")
    testInvalidEndpoint()
    
    logger.info("Test 2: Non-existent endpoint fast fail")  
    testNonExistentEndpoint()
    
    logger.info("Test 3: Malformed endpoint fast fail")
    testMalformedEndpoint()
    
    logger.info("Test 4: Valid endpoint comparison")
    testValidEndpointComparison()
    
    logger.info("=== TVF Fast Fail Tests Completed Successfully ===")

    // Cleanup
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
