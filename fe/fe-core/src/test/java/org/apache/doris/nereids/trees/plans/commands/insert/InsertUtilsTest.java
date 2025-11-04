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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for InsertUtils.getFinalErrorMsg()
 */
public class InsertUtilsTest {

    private static final int MAX_TOTAL_BYTES = 512;

    private String generateString(int length) {
        return generateString(length, "X");
    }

    private String generateString(int length, String prefix) {
        StringBuilder sb = new StringBuilder(length);
        sb.append(prefix);
        for (int i = prefix.length(); i < length; i++) {
            sb.append((char) ('A' + (i % 26)));
        }
        return sb.toString();
    }

    /**
     * case1: normal
     */
    @Test
    public void testNormalCase() {
        String msg = "Insert failed";
        String firstErrorMsg = "Row format error";
        String url = "http://example.com/error_log";

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(msg));
        Assertions.assertTrue(result.contains(firstErrorMsg));
        Assertions.assertTrue(result.contains(url));
        Assertions.assertTrue(result.contains("first_error_msg:"));
        Assertions.assertTrue(result.contains("url:"));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
    }

    /**
     * case2: Msg is too long
     */
    @Test
    public void testLongMsg() {
        String msg = generateString(600);
        String firstErrorMsg = "Short error";
        String url = "http://example.com";

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(firstErrorMsg));
        Assertions.assertTrue(result.contains(url));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
        Assertions.assertTrue(result.indexOf(msg) == -1 || result.length() <= MAX_TOTAL_BYTES);
    }

    /**
     * case3: firstErrorMsg is too long
     */
    @Test
    public void testLongFirstErrorMsg() {
        String msg = "Insert failed";
        String firstErrorMsg = generateString(600);
        String url = "http://example.com";

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(msg));
        Assertions.assertTrue(result.contains("please use `show load` for detail msg"));
        Assertions.assertTrue(result.contains(url));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
        Assertions.assertFalse(result.contains(firstErrorMsg));
    }

    /**
     * case4: url is too long
     */
    @Test
    public void testLongUrl() {
        String msg = "Insert failed";
        String firstErrorMsg = "Row format error";
        String url = "http://example.com/" + generateString(600);

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(msg));
        Assertions.assertTrue(result.contains(firstErrorMsg));
        Assertions.assertTrue(result.contains("please use `show load` for detail msg"));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
        Assertions.assertFalse(result.contains(url));
    }

    /**
     * case5：firstErrorMsg and url are too long
     */
    @Test
    public void testBothFirstErrorMsgAndUrlTooLong() {
        String msg = "Insert failed";
        String firstErrorMsg = generateString(600);
        String url = "http://example.com/" + generateString(600);

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(msg));
        Assertions.assertTrue(result.contains("please use `show load` for detail msg"));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
        Assertions.assertFalse(result.contains(firstErrorMsg));
        Assertions.assertFalse(result.contains(url));
    }

    /**
     * case6: firstErrorMsg , msg and url are too long
     */
    @Test
    public void testAllParametersTooLong() {
        String msg = generateString(600);
        String firstErrorMsg = generateString(600);
        String url = "http://example.com/" + generateString(600);

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains("please use `show load` for detail msg"));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
        Assertions.assertFalse(result.contains(msg));
        Assertions.assertFalse(result.contains(firstErrorMsg));
        Assertions.assertFalse(result.contains(url));
    }

    /**
     * case7 :  msg length == 512
     */
    @Test
    public void testMsgExactly512() {
        String msg = generateString(512);
        String firstErrorMsg = "";
        String url = "";

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
    }

    /**
     * case8: urlPartLen + firstErrorMsgPartLen > 512, but tempLen + firstErrorMsgPartLen still > 512
     * Should only keep firstErrorMsg and drop url
     */
    @Test
    public void testUrlAndFirstErrorMsgSumTooLong_DropUrl() {
        String msg = "Insert failed";
        // ". first_error_msg: ".length() = 19
        // We need firstErrorMsgPartLen such that:
        // ". url: please use `show load` for detail msg".length() + firstErrorMsgPartLen > 512
        // ". url: please use `show load` for detail msg".length() = 47
        // So firstErrorMsgPartLen should be > 465
        // firstErrorMsgPartLen = 19 + firstErrorMsg.length() > 465
        // firstErrorMsg.length() should be > 446
        String firstErrorMsg = generateString(470, "ERROR_MSG_");
        String url = generateString(100, "URL_");

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(msg));
        Assertions.assertTrue(result.contains(firstErrorMsg));
        Assertions.assertFalse(result.contains(url));
        Assertions.assertFalse(result.contains("please use `show load` for detail msg"));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
    }

    /**
     * case9: urlPartLen + firstErrorMsgPartLen > 512, but tempLen + firstErrorMsgPartLen <= 512
     * Should use url placeholder and keep firstErrorMsg
     */
    @Test
    public void testUrlAndFirstErrorMsgSumTooLong_UseUrlPlaceholder() {
        String msg = "Insert failed";
        // ". first_error_msg: ".length() = 19
        // ". url: ".length() = 7
        // ". url: please use `show load` for detail msg".length() = 47
        // We need: urlPartLen + firstErrorMsgPartLen > 512
        // AND: 47 + firstErrorMsgPartLen <= 512
        // So firstErrorMsgPartLen should be in range (512-urlPartLen, 465]
        // Let's make firstErrorMsg.length() = 400, so firstErrorMsgPartLen = 419
        // And url.length() = 100, so urlPartLen = 107
        // Then urlPartLen + firstErrorMsgPartLen = 526 > 512 ✓
        // And 47 + 419 = 466 <= 512 ✓
        String firstErrorMsg = generateString(400, "FIRST_ERROR_");
        String url = generateString(100, "URL_CONTENT_");

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(msg));
        Assertions.assertTrue(result.contains(firstErrorMsg));
        Assertions.assertTrue(result.contains("please use `show load` for detail msg"));
        Assertions.assertFalse(result.contains(url));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
    }
}

