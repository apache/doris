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

suite("doc_bitmap_functions_test") {
    qt_bitmap_to_base64_null '''
        SELECT bitmap_to_base64(NULL);
    '''
    testFoldConst('''
        SELECT bitmap_to_base64(NULL);
    ''')

    qt_bitmap_to_base64_empty '''
        SELECT bitmap_to_base64(bitmap_empty());
    '''
    testFoldConst('''
        SELECT bitmap_to_base64(bitmap_empty());
    ''')

    def bitmapToBase64Single = sql '''
        SELECT bitmap_to_base64(to_bitmap(1));
    '''
    def bitmapToBase64SingleExpected = ["BQEBAAAAAAAAAA==", "AQEAAAA="]
    assertTrue(bitmapToBase64SingleExpected.contains(bitmapToBase64Single[0][0].toString()),
            "Unexpected bitmap_to_base64 single result: ${bitmapToBase64Single}")
    testFoldConst('''
        SELECT bitmap_to_base64(to_bitmap(1));
    ''')

    def bitmapToBase64Multi = sql '''
        SELECT bitmap_to_base64(bitmap_from_string("1,9999999"));
    '''
    def bitmapToBase64MultiExpected = ["BQIBAAAAAAAAAH+WmAAAAAAA",
            "AjowAAACAAAAAAAAAJgAAAAYAAAAGgAAAAEAf5Y="]
    assertTrue(bitmapToBase64MultiExpected.contains(bitmapToBase64Multi[0][0].toString()),
            "Unexpected bitmap_to_base64 multi result: ${bitmapToBase64Multi}")
    testFoldConst('''
        SELECT bitmap_to_base64(bitmap_from_string("1,9999999"));
    ''')
}
