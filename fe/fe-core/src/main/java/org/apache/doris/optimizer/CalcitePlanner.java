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

package org.apache.doris.optimizer;

import com.google.common.base.Preconditions;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class CalcitePlanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlVolcanoTest.class);

    private static final SqlParser.Config SQL_PARSER_CONFIG =
            SqlParser.configBuilder(SqlParser.Config.DEFAULT)
                    .setCaseSensitive(false)
                    .setConformance(SqlConformanceEnum.MYSQL_5)
                    .setQuoting(Quoting.BACK_TICK)
                    .build();

    public RelNode plan(String sql, SchemaPlus rootSchema) {
        final FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SQL_PARSER_CONFIG)
                .defaultSchema(rootSchema)
                .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
                .build();

        // use VolcanoPlanner
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
        // add rules
        planner.addRule(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN);
        planner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);
        planner.addRule(PruneEmptyRules.PROJECT_INSTANCE);
        // add ConverterRule
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_INTERSECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_UNION_RULE);

        try {
            SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            // sql parser
            SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
            SqlNode parsed = parser.parseStmt();
            LOGGER.info("The SqlNode after parsed is:\n{}", parsed.toString());
            Properties props = new Properties();
            // props.put(CalciteConnectionProperty.CASE_SENSITIVE, true);
            CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
                    CalciteSchema.from(rootSchema),
                    CalciteSchema.from(rootSchema).path(null),
                    factory,
                    new CalciteConnectionConfigImpl(props));

            // sql validate
            SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), calciteCatalogReader,
                    factory, CalciteMetaMgr.conformance(frameworkConfig));
            boolean isCaseSensitive = validator.getCatalogReader().nameMatcher().isCaseSensitive();
            SqlNode validated = validator.validate(parsed);
            LOGGER.info("The SqlNode after validated is:\n{} boolean isCaseSensitive{}", validated.toString(), isCaseSensitive);

            final RexBuilder rexBuilder = CalciteMetaMgr.createRexBuilder(factory);
            final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

            // init SqlToRelConverter config
            final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
                    .withConfig(frameworkConfig.getSqlToRelConverterConfig())
                    .withTrimUnusedFields(false)
                    .withConvertTableAccess(false)
                    .build();
            // SqlNode toRelNode
            final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(new CalciteMetaMgr.ViewExpanderImpl(),
                    validator, calciteCatalogReader, cluster, frameworkConfig.getConvertletTable(), config);
            RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);

            root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
            final RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
            root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
            RelNode relNode = root.rel;
            LOGGER.info("The relational expression string before optimized is:\n{}", RelOptUtil.toString(relNode));

            RelTraitSet desiredTraits =
                    relNode.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
            relNode = planner.changeTraits(relNode, desiredTraits);

            planner.setRoot(relNode);
            relNode = planner.findBestExp();
            return relNode;
        } catch (Exception e) {
            e.printStackTrace();
            Preconditions.checkState(false);
            return null;
        }
    }
}
