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

package org.apache.doris.regression

import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.*

class RegressionSuiteExclusionTest {
    @Test
    void qualifiedExclusionsPreserveSameNamedExportSuites() {
        ['test_outfile', 'test_outfile_expr', 'test_outfile_parquet'].each { name ->
            def config = new Config()
            config.excludeSuiteWildcard.add("query_p0/outfile/${name}.groovy#${name}".toString())
            assertFalse(RegressionTest.filterSuites(config, name, "query_p0/outfile/${name}.groovy"))
            assertTrue(RegressionTest.filterSuites(config, name, "export_p0/${name}.groovy"))
            assertTrue(RegressionTest.filterSuites(config, name, "export_p0/outfile/parquet/${name}.groovy"))
            assertTrue(RegressionTest.filterSuites(config, 'another_suite', "query_p0/outfile/${name}.groovy"))
            assertTrue(RegressionTest.filterSuites(config, name))
        }
    }

    @Test
    void shortNamesAndWildcardsKeepTheirOriginalBehavior() {
        def config = new Config()
        config.excludeSuiteWildcard.add('test_outfile*')
        assertFalse(RegressionTest.filterSuites(config, 'test_outfile'))
        assertFalse(RegressionTest.filterSuites(config, 'test_outfile_expr', 'export_p0/test_outfile_expr.groovy'))
        assertTrue(RegressionTest.filterSuites(config, 'another_suite', 'query_p0/outfile/test_outfile.groovy'))
        config.suiteWildcard.add('wanted*')
        assertFalse(RegressionTest.filterSuites(config, 'another_suite'))
        assertTrue(RegressionTest.filterSuites(config, 'wanted_suite'))
    }

    @Test
    void qualifiedWildcardAndPathSeparatorsAreSupported() {
        def config = new Config()
        config.excludeSuiteWildcard.add('query_p0/outfile/*.groovy#test_outfile*')
        assertFalse(RegressionTest.filterSuites(config, 'test_outfile', 'query_p0\\outfile\\test_outfile.groovy'))
        assertTrue(RegressionTest.filterSuites(config, 'test_outfile', 'export_p0/test_outfile.groovy'))
        assertTrue(RegressionTest.filterSuites(config, 'test_outfile', 'query_p0/outfile/test_outfile.sql'))
    }

    @Test
    void sourcePathsAreNormalizedRelativeToTheSuiteRoot() {
        ['/checkout-one/suites', '/another-checkout/suites'].each { root ->
            def config = new Config(suitePath: root + '/./')
            assertEquals('query_p0/outfile/test_outfile.groovy', RegressionTest.suiteSourcePath(config,
                    new File(root, 'query_p0/unused/../outfile/test_outfile.groovy')))
            assertEquals('query_p0/example.sql', RegressionTest.suiteSourcePath(config,
                    new File(root, 'query_p0/example.sql')))
        }
    }

    @Test
    void groupAndExecutionTypeFiltersStillApply() {
        def config = new Config()
        config.groups.add('p0')
        config.excludeGroupSet.add('p2')
        config.excludeSuiteWildcard.add('query_p0/outfile/test_outfile.groovy#test_outfile')
        assertFalse(RegressionTest.canRun(config, 'test_outfile', 'p0',
                RegressionTest.GroupExecType.NORMAL, 'query_p0/outfile/test_outfile.groovy'))
        assertTrue(RegressionTest.canRun(config, 'test_outfile', 'p0',
                RegressionTest.GroupExecType.NORMAL, 'export_p0/test_outfile.groovy'))
        assertFalse(RegressionTest.canRun(config, 'test_outfile', 'p2',
                RegressionTest.GroupExecType.NORMAL, 'export_p0/test_outfile.groovy'))
        assertFalse(RegressionTest.canRun(config, 'test_outfile', 'p0,nonConcurrent',
                RegressionTest.GroupExecType.NORMAL, 'export_p0/test_outfile.groovy'))
        assertTrue(RegressionTest.canRun(config, 'test_outfile', 'p0', RegressionTest.GroupExecType.NORMAL))
    }
}
