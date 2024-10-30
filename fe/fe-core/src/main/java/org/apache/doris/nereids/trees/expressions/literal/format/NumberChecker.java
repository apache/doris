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

package org.apache.doris.nereids.trees.expressions.literal.format;

/** NumberChecker */
public class NumberChecker extends FormatChecker {
    private int minCount;
    private int maxRead;

    public NumberChecker(StringInspect stringInspect, int minCount, int maxRead) {
        super(stringInspect);
        this.minCount = minCount;
        this.maxRead = maxRead;
    }

    @Override
    protected boolean doCheck() {
        int numberCount = 0;
        boolean checkRead = maxRead >= 0;
        while (!stringInspect.eos() && (!checkRead || numberCount < maxRead)) {
            char c = stringInspect.lookAt();
            if (!('0' <= c && c <= '9')) {
                break;
            }
            stringInspect.step();
            numberCount++;
        }
        return numberCount >= minCount;
    }

    public static void main(String[] args) {
        NumberChecker numberChecker = new NumberChecker(new StringInspect("123"), 1, 3);
        System.out.println(numberChecker.check());
        System.out.println(numberChecker.getCheckContent());
    }
}
