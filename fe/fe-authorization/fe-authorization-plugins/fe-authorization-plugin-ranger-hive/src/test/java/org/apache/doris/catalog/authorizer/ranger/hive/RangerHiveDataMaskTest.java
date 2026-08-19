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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.apache.doris.authorization.spi.AuthorizationContext;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A mask type on a Hive service carries an expression written for Hive, and Doris cannot bind a single one of
 * them: the stock Hive definition's transformers are Hive UDFs ({@code mask_show_last_n}, {@code mask_hash},
 * a nine-argument {@code mask}). What reaches the planner has to be Doris dialect, so this source translates,
 * and what it translates to is pinned here - a wrong expression is a column returned in a form nobody asked
 * for, and a missing one is a statement failing on an unknown function with nothing in the error naming
 * Ranger.
 */
public class RangerHiveDataMaskTest {

    /**
     * What each mask type of the stock Hive service definition has to render as.
     *
     * <p>Written out rather than read off the map under test: a test deriving the answer from the code cannot
     * fail when the code changes.
     */
    private static final Map<String, String> EXPECTED = expected();

    private static Map<String, String> expected() {
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("MASK", "regexp_replace(regexp_replace(regexp_replace({col},'([A-Z])', 'x'),"
                + "'([a-z])','x'),'([0-9])','n')");
        expected.put("MASK_SHOW_LAST_4", "LPAD(RIGHT({col}, 4), CHAR_LENGTH({col}), 'X')");
        expected.put("MASK_SHOW_FIRST_4", "RPAD(LEFT({col}, 4), CHAR_LENGTH({col}), 'X')");
        expected.put("MASK_HASH", "hex(sha2({col}, 256))");
        expected.put("MASK_DATE_SHOW_YEAR", "date_trunc({col}, 'year')");
        return expected;
    }

    @Test
    public void testEachMaskTypeRendersInDorisDialect() {
        withController(controller -> {
            for (Map.Entry<String, String> maskType : EXPECTED.entrySet()) {
                Assert.assertEquals("mask type " + maskType.getKey() + " no longer renders the expression"
                                + " Doris applies for it",
                        maskType.getValue(), controller.dataMaskExpressionOf(null, maskType.getKey()));
            }
        });
    }

    /**
     * A mask type with no Doris equivalent fails the statement naming itself.
     *
     * <p>The alternative is passing the definition's own transformer through, which is a Hive UDF: the
     * statement then fails on an unknown function, with nothing in the error pointing at Ranger. Worse would
     * be answering "no mask", which is the column in the clear.
     */
    @Test
    public void testAnUnknownMaskTypeIsRefusedNamingIt() {
        withController(controller -> {
            IllegalStateException refused = Assert.assertThrows(IllegalStateException.class,
                    () -> controller.dataMaskExpressionOf(null, "MASK_SHOW_LAST_7"));
            Assert.assertTrue("the refusal does not name the mask type it could not apply, which is the only"
                            + " thing pointing at the Ranger policy to fix: " + refused.getMessage(),
                    refused.getMessage().contains("MASK_SHOW_LAST_7"));
        });
    }

    /**
     * The same mask type has to mean the same thing whether the catalog is bound to this source or to
     * {@code ranger-doris}, and what {@code ranger-doris} applies is the transformer in
     * {@code ranger-servicedef-doris.json}.
     *
     * <p>That file is not in this repository: {@code run-thirdparties-docker.sh} downloads it into a
     * gitignored cache directory, and it is the one artifact that changes in place - a corrected transformer
     * arrives under a name that never changes. So a drift between the two sources would reach
     * {@code ranger-doris} at once and this source never. This case is the only thing that can notice, and it
     * can only notice on a machine that has run the docker environment; elsewhere there is nothing to compare
     * against and it is skipped rather than made to pass vacuously.
     */
    @Test
    public void testTheExpressionsMatchTheDorisServiceDefinitionWhenOneIsCached() throws IOException {
        Path definition = cachedDorisServiceDefinition();
        Assume.assumeTrue("no downloaded ranger-servicedef-doris.json to compare against", definition != null);

        String json = new String(Files.readAllBytes(definition), StandardCharsets.UTF_8);
        for (Map.Entry<String, String> maskType : EXPECTED.entrySet()) {
            Assert.assertEquals("the Doris service definition and this source disagree about what mask type "
                            + maskType.getKey() + " means, so the same Ranger policy masks a column one way"
                            + " through ranger-doris and another way through " + RangerHiveAccessController.NAME,
                    maskType.getValue(), transformerOf(json, maskType.getKey()));
        }
    }

    /**
     * The transformer the definition declares for {@code maskType}.
     *
     * <p>Read with a regex rather than a JSON parser so that this module needs no parser on its test path;
     * the values are plain strings with no escapes, which is asserted by the parse failing loudly if the
     * shape ever changes.
     */
    private static String transformerOf(String json, String maskType) {
        Matcher named = Pattern.compile("\"name\"\\s*:\\s*\"" + Pattern.quote(maskType) + "\"").matcher(json);
        Assert.assertTrue("the Doris service definition no longer declares mask type " + maskType,
                named.find());
        Matcher transformer = Pattern.compile("\"transformer\"\\s*:\\s*\"([^\"]*)\"").matcher(json);
        Assert.assertTrue("the Doris service definition declares no transformer after mask type " + maskType,
                transformer.find(named.end()));
        return transformer.group(1);
    }

    /** The downloaded Doris service definition, or null on a machine that has never run the docker setup. */
    private static Path cachedDorisServiceDefinition() {
        for (File dir = new File("").getAbsoluteFile(); dir != null; dir = dir.getParentFile()) {
            File cached = new File(dir,
                    "docker/thirdparties/docker-compose/ranger/cache/ranger-servicedef-doris.json");
            if (cached.isFile()) {
                return cached.toPath();
            }
        }
        return null;
    }

    /** A controller over mocked Ranger machinery: nothing below is asked anything about a policy. */
    private static void withController(ControllerCase body) {
        AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
        try (MockedConstruction<RangerHivePlugin> plugin = Mockito.mockConstruction(RangerHivePlugin.class);
                MockedConstruction<RangerHiveAuditHandler> audit =
                        Mockito.mockConstruction(RangerHiveAuditHandler.class)) {
            RangerHiveAccessController controller = new RangerHiveAccessController(
                    ImmutableMap.of("ranger.service.name", "hive"), context);
            try {
                body.run(controller);
            } finally {
                controller.close();
            }
        }
    }

    @FunctionalInterface
    private interface ControllerCase {
        void run(RangerHiveAccessController controller);
    }
}
