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

suite("test_no_backslash_escape") {
    sql """
    SET sql_mode = 'NO_BACKSLASH_ESCAPES';
    """
	
    try {
        sql "SELECT 'It\'s a test'"
    } catch (Exception e) {
        assert e.message.contains("invalid escape character when sql_mode='NO_BACKSLASH_ESCAPES'(line 1, pos 3)")
    }
	
    try {
        sql "SELECT '\'It\'s a test'"
    } catch (Exception e) {
        assert e.message.contains("invalid escape character when sql_mode='NO_BACKSLASH_ESCAPES'(line 1, pos 1)")
    }
	
	try {
        sql "SELECT 'It\\\'s a test'"
    } catch (Exception e) {
        assert e.message.contains("invalid escape character when sql_mode='NO_BACKSLASH_ESCAPES'(line 1, pos 3)")
    }
	
	try {
        sql "SELECT 'It\\\\\'s a test'"
    } catch (Exception e) {
        assert e.message.contains("invalid escape character when sql_mode='NO_BACKSLASH_ESCAPES'(line 1, pos 3)")
    }
	
	try {
        sql "SELECT 'It\\\\\\\'s a test'"
    } catch (Exception e) {
        assert e.message.contains("invalid escape character when sql_mode='NO_BACKSLASH_ESCAPES'(line 1, pos 3)")
    }
	
    sql """
    SET sql_mode = 'DEFAULT';
    """

	def res1 = sql "SELECT 'It\'s a test'"
	assert res1 == "It's a test"
	
	def res2 = sql "SELECT '\'It\'s a test'"
	assert res2 == "'It's a test"
	
	def res3 = sql "SELECT 'It\\\'s a test'"
	assert res3 == "It\'s a test"
	
	def res4 = sql "SELECT 'It\\\\\'s a test'"
	assert res4 == "It\\'s a test"
	
	def res5 = sql "SELECT 'It\\\\\\\'s a test'"
	assert res5 == "It\\\'s a test"
}
