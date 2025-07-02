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

suite("test_like_escapes") {
    qt_test """
    select "%a" like "a%_" ESCAPE "a";
    """
    qt_test """
    select "%_" like "a%_" ESCAPE "a";
    """
    qt_test """
    select "a" like "a" ESCAPE "a";
    """
    qt_test """
    select "a" like "aa" ESCAPE "a";
    """
    qt_test """
    select "%a" like "a%a" ESCAPE "a";
    """
    qt_test """
    select "%_" like "a%a" ESCAPE "a";
    """
    qt_test """
    select "%a" like "a%a_" ESCAPE "a";
    """
    qt_test """
    select "%_" like "a%a_" ESCAPE "a";
    """

    test {
        sql """select "啊啊" like "啊啊" ESCAPE "啊";"""
        exception "like escape character must be a single ascii character"
    }
    test {
        sql """select "a" like "aa" ESCAPE "aa";"""
        exception "like escape character must be a single ascii character"
    }
    test {
        sql """select "a" like "aa" ESCAPE 1;"""
        exception "like escape character must be a string literal"
    }
    qt_test """
    select "啊%a" like "啊a%_" ESCAPE "a";
    """
    qt_test """
    select "%a" like "a%_" ESCAPE "A";
    """
    qt_test """
    select "\\\\" like "\\\\%" ESCAPE "A";
    """
    qt_test """
    select "\\\\" like "\\\\A%" ESCAPE "A";
    """
    qt_test """
    select "\\\\%" like "\\\\A%" ESCAPE "A";
    """
}