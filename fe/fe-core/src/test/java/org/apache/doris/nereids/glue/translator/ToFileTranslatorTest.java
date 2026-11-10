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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionToThriftConverter;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.S3Resource;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToFile;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.tablefunction.ListFilesTableValuedFunction;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TS3Resource;
import org.apache.doris.thrift.TTVFListFileScanRange;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ToFileTranslatorTest extends TestWithFeService {
    @Test
    void s3ResourceReachesQueryParametersWithoutUriValidationOrRewrite() throws Exception {
        S3Resource resource = createResource("file_translation_wire_resource");
        PlanTranslatorContext translation = newTranslation();
        String uri = "hdfs://OtherHost/a/../b%2f?token=value#fragment";
        ToFile expression = new ToFile(new StringLiteral(resource.getName()), new StringLiteral(uri));
        Expr first = ExpressionTranslator.translate(expression, translation);
        TExpr wire = ExprToThriftVisitor.treeToThrift(first);
        String backendSecretKey = "AWS_SECRET_KEY";
        assertFunctionHasNoResource(wire.getNodes().get(0).getFn(), backendSecretKey);
        Assertions.assertEquals(uri, wire.getNodes().get(2).getStringLiteral().getValue());

        Map<String, TS3Resource> queryResources = translation.getS3Resources();
        Assertions.assertEquals(1, queryResources.size());
        TS3Resource config = queryResources.get(resource.getName());
        Assertions.assertEquals("first", config.getProperties().get(backendSecretKey));
        TPipelineFragmentParams params = new TPipelineFragmentParams()
                .setProtocolVersion(PaloInternalServiceVersion.V1).setQueryId(new TUniqueId(1, 2))
                .setPerExchNumSenders(Map.of()).setDestinations(List.of()).setS3Resources(queryResources);
        TPipelineFragmentParams received = new TPipelineFragmentParams();
        new TDeserializer().deserialize(received, new TSerializer().serialize(params));
        Assertions.assertEquals(queryResources, received.getS3Resources());
        ByteArrayOutputStream persisted = new ByteArrayOutputStream();
        first.getFn().write(new DataOutputStream(persisted));
        Assertions.assertFalse(persisted.toString(java.nio.charset.StandardCharsets.UTF_8).contains(backendSecretKey));

        resource.modifyProperties(new HashMap<>(Map.of("s3.secret_key", "second", "s3_validity_check", "false")));
        config.getProperties().put(backendSecretKey, "mutated wire copy");
        Assertions.assertEquals("first", translation.getS3Resources()
                .get(resource.getName()).getProperties().get(backendSecretKey));
        Expr samePlan = ExpressionTranslator.translate(expression, translation);
        assertFunctionHasNoResource(wireSnapshot(samePlan), backendSecretKey);
        Assertions.assertEquals("first", translation.getS3Resources()
                .get(resource.getName()).getProperties().get(backendSecretKey));

        TFunction cloned = FunctionToThriftConverter.toThrift(first.getFn().clone(), first.getType(),
                new Type[] {Type.STRING, Type.STRING}, new Boolean[] {false, false});
        assertFunctionHasNoResource(cloned, backendSecretKey);

        PlanTranslatorContext nextTranslation = newTranslation();
        Assertions.assertTrue(nextTranslation.getS3Resources().isEmpty());
        Expr nextExecute = ExpressionTranslator.translate(expression, nextTranslation);
        assertFunctionHasNoResource(wireSnapshot(nextExecute), backendSecretKey);
        Assertions.assertEquals("second", nextTranslation.getS3Resources()
                .get(resource.getName()).getProperties().get(backendSecretKey));
        Assertions.assertEquals("first", received.getS3Resources()
                .get(resource.getName()).getProperties().get(backendSecretKey));
    }

    @Test
    void listFilesSharesThePlanResourceSnapshotWithToFile() throws Exception {
        S3Resource resource = createResource("file_translation_list_resource");
        PlanTranslatorContext translation = newTranslation();
        ToFile expression = new ToFile(new StringLiteral(resource.getName()), new StringLiteral("s3://bucket/a"));
        ExpressionTranslator.translate(expression, translation);
        ListFilesTableValuedFunction function = new ListFilesTableValuedFunction(
                Map.of("resource", resource.getName(), "uri", "s3://bucket/"));
        DescriptorTable descriptors = new DescriptorTable();
        ScanNode scan = function.getScanNode(new PlanNodeId(0), descriptors.createTupleDescriptor(),
                connectContext.getSessionVariable(), translation.getOrRegisterS3Resource(resource.getName()));
        resource.modifyProperties(new HashMap<>(Map.of("s3.secret_key", "second", "s3_validity_check", "false")));
        scan.finalizeForNereids();
        TTVFListFileScanRange first = scan.getScanRangeLocations(0).get(0)
                .getScanRange().getDataGenScanRange().getListFileParams();
        Assertions.assertEquals("first", first.getResource().getProperties().get("AWS_SECRET_KEY"));
        first.getResource().getProperties().put("AWS_SECRET_KEY", "mutated wire copy");
        scan.finalizeForNereids();
        Assertions.assertEquals("first", scan.getScanRangeLocations(0).get(0)
                .getScanRange().getDataGenScanRange().getListFileParams()
                .getResource().getProperties().get("AWS_SECRET_KEY"));
        Assertions.assertEquals("second", newTranslation().getOrRegisterS3Resource(resource.getName())
                .getProperties().get("AWS_SECRET_KEY"));
    }

    private S3Resource createResource(String name) throws Exception {
        CreateResourceCommand command = new CreateResourceCommand(new CreateResourceInfo(false, false, name,
                ImmutableMap.copyOf(Map.of("type", "s3", "s3.endpoint", "https://s3.us-east-1.amazonaws.com",
                "s3.region", "us-east-1", "s3.bucket", "bucket", "s3.access_key", "access-key",
                "s3.secret_key", "first", "s3_validity_check", "false"))));
        command.getInfo().analyzeResourceType();
        S3Resource resource = (S3Resource) Resource.fromCommand(command);
        Env.getCurrentEnv().getResourceMgr().createResource(resource, false);
        return resource;
    }

    @Test
    void physicalPlanUsesTheSameResourceForToFileAndListFiles() throws Exception {
        S3Resource resource = createResource("file_translation_physical_resource");
        String sql = "select element_at(to_file('" + resource.getName() + "', path), 'size') "
                + "from list_files('resource'='" + resource.getName() + "', 'uri'='s3://bucket/')";
        NereidsPlanner planner = PlanChecker.from(connectContext).plan(sql);
        Assertions.assertEquals(1, planner.getS3Resources().size());
        TS3Resource configuration = planner.getS3Resources().get(resource.getName());
        TS3Resource scanConfiguration = planner.getScanNodes().get(0).getScanRangeLocations(0).get(0)
                .getScanRange().getDataGenScanRange().getListFileParams().getResource();
        Assertions.assertEquals(configuration, scanConfiguration);
        resource.modifyProperties(new HashMap<>(Map.of("s3.secret_key", "second", "s3_validity_check", "false")));
        NereidsPlanner nextPlanner = PlanChecker.from(connectContext).plan(sql);
        Assertions.assertEquals("first", configuration.getProperties().get("AWS_SECRET_KEY"));
        Assertions.assertEquals("second", nextPlanner.getS3Resources().get(resource.getName())
                .getProperties().get("AWS_SECRET_KEY"));
    }

    private PlanTranslatorContext newTranslation() {
        return new PlanTranslatorContext(MemoTestUtils.createCascadesContext(connectContext, "select 1"));
    }

    private void assertFunctionHasNoResource(TFunction function, String propertyKey) throws Exception {
        String bytes = new String(new TSerializer().serialize(function),
                java.nio.charset.StandardCharsets.ISO_8859_1);
        Assertions.assertFalse(bytes.contains(propertyKey));
    }

    private TFunction wireSnapshot(Expr expression) {
        return ExprToThriftVisitor.treeToThrift(expression).getNodes().get(0).getFn();
    }
}
