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

package org.apache.doris.backup;

import org.apache.doris.analysis.PartitionName;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class PartitionNameTest {

    @Test
    public void testCompletion() throws AnalysisException {
        PartitionName p1 = new PartitionName("t1", null, null, null);
        PartitionName p2 = new PartitionName("t1", "t2", null, null);
        PartitionName p3 = new PartitionName("t1", null, "p1", null);
        PartitionName p4 = new PartitionName("t1", "t2", "p1", "p1");

        try {
            p1.analyze(null);
            Assert.assertEquals("`t1` AS `t1`", p1.toSql());
            p2.analyze(null);
            Assert.assertEquals("`t1` AS `t2`", p2.toSql());
            p3.analyze(null);
            Assert.assertEquals("`t1`.`p1` AS `t1`.`p1`", p3.toSql());
            p4.analyze(null);
            Assert.assertEquals("`t1`.`p1` AS `t2`.`p1`", p4.toSql());
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test
    public void testCompletionErr() throws AnalysisException {
        PartitionName p1 = new PartitionName("t1", "t1", null, "p1");
        PartitionName p2 = new PartitionName("t1", "t1", "p1", null);
        PartitionName p3 = new PartitionName("t1", "t1", "p1", "p2");

        try {
            p1.analyze(null);
            Assert.fail();
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }

        try {
            p2.analyze(null);
            Assert.fail();
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }

        try {
            p3.analyze(null);
            Assert.fail();
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test(expected = AnalysisException.class)
    public void testIntersect1() throws AnalysisException {
        PartitionName p1 = new PartitionName("t1", null, "p1", null);
        PartitionName p2 = new PartitionName("t1", null, "p1", null);

        try {
            p1.analyze(null);
            p2.analyze(null);
            PartitionName.checkIntersect(Lists.newArrayList(p1, p2));
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test(expected = AnalysisException.class)
    public void testIntersect2() throws AnalysisException {
        PartitionName p1 = new PartitionName("t1", null, "p1", null);
        PartitionName p2 = new PartitionName("t1", null, null, null);

        try {
            p1.analyze(null);
            p2.analyze(null);

            PartitionName.checkIntersect(Lists.newArrayList(p1, p2));
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test(expected = AnalysisException.class)
    public void testIntersect3() throws AnalysisException {
        PartitionName p1 = new PartitionName("t1", null, null, null);
        PartitionName p2 = new PartitionName("t1", null, null, null);

        try {

            p1.analyze(null);
            p2.analyze(null);

            PartitionName.checkIntersect(Lists.newArrayList(p1, p2));
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test(expected = AnalysisException.class)
    public void testIntersect4() throws AnalysisException {
        PartitionName p1 = new PartitionName("t1", null, null, null);
        PartitionName p2 = new PartitionName("t1", null, "p2", null);

        try {
            p1.analyze(null);
            p2.analyze(null);

            PartitionName.checkIntersect(Lists.newArrayList(p1, p2));
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test(expected = AnalysisException.class)
    public void testIntersect5() throws AnalysisException {
        PartitionName p1 = new PartitionName("t1", null, "p1", null);
        PartitionName p2 = new PartitionName("t1", null, "p2", null);
        PartitionName p3 = new PartitionName("t2", null, "p1", null);
        PartitionName p4 = new PartitionName("t1", null, null, null);
        PartitionName p5 = new PartitionName("t1", null, "p3", null);

        try {
            p1.analyze(null);
            p2.analyze(null);
            p3.analyze(null);
            p4.analyze(null);
            p5.analyze(null);

            PartitionName.checkIntersect(Lists.newArrayList(p1, p2, p3, p4, p5));
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test(expected = AnalysisException.class)
    public void testIntersect6() throws AnalysisException {
        PartitionName p1 = new PartitionName("t1", "t2", null, null);
        PartitionName p2 = new PartitionName("t2", null, null, null);

        try {
            p1.analyze(null);
            p2.analyze(null);

            PartitionName.checkIntersect(Lists.newArrayList(p1, p2));
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test(expected = AnalysisException.class)
    public void testIntersect7() throws AnalysisException {
        PartitionName p1 = new PartitionName("t1", null, "p1", null);
        PartitionName p2 = new PartitionName("t1", null, "p2", null);
        PartitionName p3 = new PartitionName("t2", null, "p1", null);
        PartitionName p4 = new PartitionName("t1", null, null, null);
        PartitionName p5 = new PartitionName("t1", null, "p3", null);

        try {
            p1.analyze(null);
            p2.analyze(null);
            p3.analyze(null);
            p4.analyze(null);
            p5.analyze(null);

            PartitionName.checkIntersect(Lists.newArrayList(p1, p2, p3, p4, p5));
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test(expected = AnalysisException.class)
    public void testIntersect8() throws AnalysisException {
        PartitionName p1 = new PartitionName("t1", null, "p1", null);
        PartitionName p2 = new PartitionName("t1", "t2", "p2", "p2");

        try {
            p1.analyze(null);
            p2.analyze(null);

            PartitionName.checkIntersect(Lists.newArrayList(p1, p2));
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test
    public void testIntersect9() throws AnalysisException {
        PartitionName p1 = new PartitionName("t1", null, "p1", null);
        PartitionName p2 = new PartitionName("t1", null, "p2", null);
        PartitionName p3 = new PartitionName("t2", "t3", "p1", "p1");
        PartitionName p4 = new PartitionName("t3", "t2", null, null);

        try {
            p1.analyze(null);
            p2.analyze(null);
            p3.analyze(null);
            p4.analyze(null);

            PartitionName.checkIntersect(Lists.newArrayList(p1, p2, p3, p4));
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test(expected = AnalysisException.class)
    public void testRename10() throws AnalysisException {
        PartitionName p1 = new PartitionName("t1", "p2", "p1", null);

        try {
            p1.analyze(null);
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

}
