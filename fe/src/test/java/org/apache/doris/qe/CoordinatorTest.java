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

package org.apache.doris.qe;

import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.persist.EditLog;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.Planner;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.BeforeClass;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoordinatorTest extends Coordinator {
    static Planner planner = new Planner();
    static ConnectContext context = new ConnectContext(null);
    static {
        context.setQueryId(new TUniqueId(1, 2));
    }
    @Mocked
    static Catalog catalog;
    @Mocked
    static EditLog editLog;
    @Mocked
    static FrontendOptions frontendOptions;
    static Analyzer analyzer = new Analyzer(catalog, null);
    static Backend backendA;
    static Backend backendB;
    static Backend backendC;
    static Backend backendD;

    public CoordinatorTest() {
        super(context, analyzer, planner);
    }

    private static Coordinator coor;

    @BeforeClass
    public static void beforeTest() throws IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchFieldException,
            SecurityException, NoSuchMethodException {
        coor = new Coordinator(context, analyzer, planner);
        new Expectations() {
            {
                editLog.logAddBackend((Backend) any);
                minTimes = 0;

                editLog.logDropBackend((Backend) any);
                minTimes = 0;

                editLog.logBackendStateChange((Backend) any);
                minTimes = 0;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                Catalog.getCurrentCatalogJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;
            }
        };

        new Expectations(frontendOptions) {
            {
                FrontendOptions.getLocalHostAddress();
                minTimes = 0;
                result = "127.0.0.1";
            }
        };

        FeConstants.heartbeat_interval_second = Integer.MAX_VALUE;
        backendA = new Backend(0, "machineA", 0);
        backendA.updateOnce(10000, 0, 0);
        backendB = new Backend(1, "machineB", 0);
        backendB.updateOnce(10000, 0, 0);
        backendC = new Backend(2, "machineC", 0);
        backendC.updateOnce(10000, 0, 0);
        backendD = new Backend(3, "machineD", 0);
        backendD.updateOnce(10000, 0, 0);

        // private 方法赋值
        Field field = coor.getClass().getDeclaredField("idToBackend");
        field.setAccessible(true);
        Map<Long, Backend> backendMap = new HashMap<Long, Backend>();
        backendMap.put(Long.valueOf(0), backendA);
        backendMap.put(Long.valueOf(1), backendB);
        ImmutableMap<Long, Backend> idToBackendAB = ImmutableMap.copyOf(backendMap);
        field.set(coor, idToBackendAB);
    }

    static void invokeFunction(String functionName) throws IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchFieldException,
            SecurityException, NoSuchMethodException {
        Method method = coor.getClass().getDeclaredMethod(functionName);
        method.setAccessible(true);
        method.invoke(coor);
    }

    static Object getField(Object object, String fieldName) throws NoSuchFieldException,
            SecurityException, IllegalArgumentException, IllegalAccessException {
        Field field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        Object after = field.get(object);
        return after;
    }

    /*
     * 场景1：扫描2个scanRange，每个scanRange都分布在两台机器上（MachineA，machineB）
     * 返回结果： 根据调度策略，machineA和machineB各扫描1个scanRange
     *
     * 场景2：扫描3个scanRange，每个scanRange都分布在两台机器上（MachineA，machineB）
     * 返回结果： 根据调度策略，machineA扫描2个scanRange，machineB扫描1个scanRange
     *
     * 场景3：扫描3个scanRange，每个scanRange分布在不同的两台机器上（分别分布在（MachineA，machineB），（MachineA，machineC）
     *      （MachineC，machineB）上。
     * 返回结果：根据调度策略，machineA，machineB，machineC分布扫描1个scanRange
     */
    // TODO(lingbin): PALO-2051.
    // Comment out these code temporatily.
    // @Test
    public void testComputeScanRangeAssignment() throws IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchFieldException,
            SecurityException, NoSuchMethodException {
        Method method = coor.getClass().getDeclaredMethod(
                "computeScanRangeAssignment",
                PlanNodeId.class,
                List.class,
                Coordinator.FragmentScanRangeAssignment.class);
        method.setAccessible(true);
        int planNodeId = 2;
        // 输出参数
        FragmentScanRangeAssignment assignment = coor.new FragmentScanRangeAssignment();
        // 输入参数
        List<TScanRangeLocations> locations = new ArrayList<TScanRangeLocations>();

        TScanRangeLocations scanRangeLocationsA = new TScanRangeLocations();
        List<TScanRangeLocation> listLocationsA = new ArrayList<TScanRangeLocation>();
        listLocationsA.add((new TScanRangeLocation())
                .setServer(new TNetworkAddress("machineA", 10000)).setBackend_id(0));
        listLocationsA.add((new TScanRangeLocation())
                .setServer(new TNetworkAddress("machineB", 10000)).setBackend_id(1));
        scanRangeLocationsA.setLocations(listLocationsA).setScan_range(new TScanRange());

        TScanRangeLocations scanRangeLocationsB = new TScanRangeLocations();
        List<TScanRangeLocation> listLocationsB = new ArrayList<TScanRangeLocation>();
        listLocationsB.add((new TScanRangeLocation())
                .setServer(new TNetworkAddress("machineB", 10000)).setBackend_id(1));
        listLocationsB.add((new TScanRangeLocation())
                .setServer(new TNetworkAddress("machineC", 10000)).setBackend_id(2));
        scanRangeLocationsB.setLocations(listLocationsB).setScan_range(new TScanRange());

        TScanRangeLocations scanRangeLocationsC = new TScanRangeLocations();
        List<TScanRangeLocation> listLocationsC = new ArrayList<TScanRangeLocation>();
        listLocationsC.add((new TScanRangeLocation())
                .setServer(new TNetworkAddress("machineC", 10000)).setBackend_id(2));
        listLocationsC.add((new TScanRangeLocation())
                .setServer(new TNetworkAddress("machineA", 10000)).setBackend_id(0));
        scanRangeLocationsC.setLocations(listLocationsC).setScan_range(new TScanRange());

        // 场景1： 2个scanRange
        {
            assignment.clear();
            locations.clear();
            locations.add(scanRangeLocationsA);
            locations.add(scanRangeLocationsA);
            // 调用函数
            method.invoke(
                    coor,
                    new PlanNodeId(planNodeId),
                    locations,
                    assignment);
            // 判断返回值
            Assert.assertEquals(assignment.get(new TNetworkAddress("machineA", 10000))
                    .get(planNodeId).size(), 1);
            Assert.assertEquals(assignment.get(new TNetworkAddress("machineB", 10000))
                    .get(planNodeId).size(), 1);
        }
        // 场景2： 3个scanRange，每个scan_range都分布在两台机器上（A和B）
        {
            assignment.clear();
            locations.clear();
            locations.add(scanRangeLocationsA);
            locations.add(scanRangeLocationsA);
            locations.add(scanRangeLocationsA);

            // 调用函数
            method.invoke(
                    coor,
                    new PlanNodeId(planNodeId),
                    locations,
                    assignment);
            // 判断返回值
            Assert.assertEquals(assignment.get(new TNetworkAddress("machineA", 10000))
                    .get(planNodeId).size(), 2);
            Assert.assertEquals(assignment.get(new TNetworkAddress("machineB", 10000))
                    .get(planNodeId).size(), 1);
        }
        // 场景3： 3个scanRange，scan_range分别分布在（A，B）（A，C）（C,B）上
        {
            Field field = coor.getClass().getDeclaredField("idToBackend");
            field.setAccessible(true);
            Map<Long, Backend> backendMap = new HashMap<Long, Backend>();
            backendMap.put(Long.valueOf(0), backendA);
            backendMap.put(Long.valueOf(1), backendB);
            backendMap.put(Long.valueOf(2), backendC);
            ImmutableMap<Long, Backend> idToBackendAB = ImmutableMap.copyOf(backendMap);
            field.set(coor, idToBackendAB);

            assignment.clear();
            locations.clear();
            locations.add(scanRangeLocationsA);
            locations.add(scanRangeLocationsB);
            locations.add(scanRangeLocationsC);
            // 调用函数
            method.invoke(
                    coor,
                    new PlanNodeId(planNodeId),
                    locations,
                    assignment);
            // 判断返回值
            Assert.assertEquals(assignment.get(new TNetworkAddress("machineA", 10000))
                    .get(planNodeId).size(), 1);
            Assert.assertEquals(assignment.get(new TNetworkAddress("machineB", 10000))
                    .get(planNodeId).size(), 1);
            Assert.assertEquals(assignment.get(new TNetworkAddress("machineC", 10000))
                    .get(planNodeId).size(), 1);
        }
    }
    /*
     * 场景1：扫描UNPARTITIONED的fragment
     * 返回结果： fragment执行参数的host列表为随机分配fragment
     *
     * 场景2：fragment的最左节点为ScanNode
     * 返回结果：fragment执行参数的host列表为ScanNode的host列表
     *
     * 场景3：fragment的最左节点为非ScanNode
     * 返回结果：fragment执行参数的host列表为来源fragment的执行参数的host列表
     */
    // TODO(lingbin): PALO-2051.
    // Comment out these code temporatily.
    // @Test
    public void testcomputeFragmentHosts() throws NoSuchMethodException, SecurityException,
            IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchFieldException {
        Method method = coor.getClass().getDeclaredMethod(
                "computeFragmentHosts");
        method.setAccessible(true);
        // 场景1：UNPARTITIONED
        {
            PlanFragment fragment = new PlanFragment(new PlanFragmentId(1),
                    new OlapScanNode(new PlanNodeId(1), new TupleDescriptor(new TupleId(10)), "null scanNode"),
                    DataPartition.UNPARTITIONED );
            List<PlanFragment> privateFragments =
                    (ArrayList<PlanFragment>) getField(coor, "fragments");
            Map<PlanFragmentId, FragmentExecParams> privateFragmentExecParams =
                    (HashMap<PlanFragmentId, FragmentExecParams>) getField(
                            coor, "fragmentExecParams");
            privateFragments.clear();
            privateFragments.add(fragment);
            privateFragmentExecParams.put(new PlanFragmentId(1), new FragmentExecParams(fragment));

            // 调用函数
            method.invoke(coor);
            // 判断返回值
            // Assert.assertEquals(privateFragmentExecParams.get(new PlanFragmentId(1))
            // .hosts.get(0).hostname, "machineA");
            // Assert.assertEquals(privateFragmentExecParams.get(new PlanFragmentId(1))
            // .hosts.get(0).port, 10000);
        }
        // 场景2： ScanNode
        {
            PlanFragment fragment = new PlanFragment(new PlanFragmentId(1),
                    new OlapScanNode(new PlanNodeId(1), new TupleDescriptor(new TupleId(10)), "null scanNode"),
                    DataPartition.RANDOM );
            List<PlanFragment> privateFragments =
                    (ArrayList<PlanFragment>) getField(coor, "fragments");
            Map<PlanFragmentId, FragmentExecParams> privateFragmentExecParams =
                    (HashMap<PlanFragmentId, FragmentExecParams>) getField(
                            coor, "fragmentExecParams");
            Map<PlanFragmentId, FragmentScanRangeAssignment> privateScanRangeAssignment =
                    (HashMap<PlanFragmentId, FragmentScanRangeAssignment>) getField(
                            coor, "scanRangeAssignment");
            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            assignment.put(new TNetworkAddress("machineC", 10000), null);
            assignment.put(new TNetworkAddress("machineD", 10000), null);

            privateScanRangeAssignment.put(new PlanFragmentId(1), assignment);

            privateFragments.clear();
            privateFragments.add(fragment);
            privateFragmentExecParams.put(new PlanFragmentId(1), new FragmentExecParams(fragment));

            // 调用函数
            method.invoke(coor);
            // 判断返回值
            // Assert.assertEquals(2, privateFragmentExecParams.get(new PlanFragmentId(1))
            // .hosts.size());
            // String hostname1 = privateFragmentExecParams.get(new PlanFragmentId(1))
            // .hosts.get(0).hostname;
            // String hostname2 = privateFragmentExecParams.get(new PlanFragmentId(1))
            // .hosts.get(1).hostname;
            // Assert.assertTrue(hostname1.equals("machineC") || hostname1.equals("machineD"));
            // Assert.assertTrue(hostname2.equals("machineC") || hostname1.equals("machineD"));
            // Assert.assertFalse(hostname1.equals(hostname2));
        }
        // 场景3： 非ScanNode
        {
             /*     fragmentFather(UNPARITIONED) fragmentID=0
              *     exchangeNode
              *
              *     fragmentSon(RANDOM) fragmentID=1
              *     olapScannode
              * */

            Field field = coor.getClass().getDeclaredField("idToBackend");
            field.setAccessible(true);
            Map<Long, Backend> backendMap = new HashMap<Long, Backend>();
            backendMap.put(Long.valueOf(0), backendA);
            backendMap.put(Long.valueOf(1), backendB);
            backendMap.put(Long.valueOf(2), backendC);
            backendMap.put(Long.valueOf(3), backendD);
            ImmutableMap<Long, Backend> idToBackendAB = ImmutableMap.copyOf(backendMap);
            field.set(coor, idToBackendAB);

            PlanNode olapNode = new OlapScanNode(new PlanNodeId(1), new TupleDescriptor(new TupleId(10)),
                    "null scanNode");
            PlanFragment fragmentFather = new PlanFragment(new PlanFragmentId(0),
                    new ExchangeNode(new PlanNodeId(10), olapNode,  false),
                    DataPartition.UNPARTITIONED);
            PlanFragment fragmentSon = new PlanFragment(new PlanFragmentId(1),
                    olapNode,
                    DataPartition.RANDOM );
            // fragmentSon.setDestination(fragmentFather, new PlanNodeId(10));

            List<PlanFragment> privateFragments = (ArrayList<PlanFragment>) getField(
                    coor, "fragments");
            Map<PlanFragmentId, FragmentExecParams> privateFragmentExecParams =
                    (HashMap<PlanFragmentId, FragmentExecParams>) getField(
                            coor, "fragmentExecParams");
            Map<PlanFragmentId, FragmentScanRangeAssignment> privateScanRangeAssignment =
                    (HashMap<PlanFragmentId, FragmentScanRangeAssignment>) getField(
                            coor, "scanRangeAssignment");

            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            assignment.put(new TNetworkAddress("machineC", 10000), null);
            assignment.put(new TNetworkAddress("machineD", 10000), null);
            privateScanRangeAssignment.put(new PlanFragmentId(1), assignment);

            privateFragments.clear();
            privateFragments.add(fragmentFather);
            privateFragments.add(fragmentSon);

            privateFragmentExecParams.put(new PlanFragmentId(1),
                    new FragmentExecParams(fragmentSon));
            privateFragmentExecParams.put(new PlanFragmentId(0),
                    new FragmentExecParams(fragmentFather));

            // 调用函数
            method.invoke(coor);
            // 判断返回值
            // Assert.assertEquals(privateFragmentExecParams.get(new PlanFragmentId(0))
            // .hosts.get(0).hostname, "machineB");
            // Assert.assertEquals(privateFragmentExecParams.get(new PlanFragmentId(0))
            // .hosts.get(0).port, 10000);
            // Assert.assertEquals(2, privateFragmentExecParams.get(new PlanFragmentId(1))
            // .hosts.size());
            // String hostname1 = privateFragmentExecParams.get(new PlanFragmentId(1))
            // .hosts.get(0).hostname;
            // String hostname2 = privateFragmentExecParams.get(new PlanFragmentId(1))
            // .hosts.get(1).hostname;
            // Assert.assertTrue(hostname1.equals("machineC") || hostname1.equals("machineD"));
            // Assert.assertTrue(hostname2.equals("machineC") || hostname2.equals("machineD"));
            // Assert.assertFalse(hostname1.equals(hostname2));
        }
    }

    /*
    public void testNetworkException() throws TException, NoSuchFieldException,
            SecurityException, IllegalArgumentException, IllegalAccessException,
            NoSuchMethodException, InvocationTargetException {
        Map<PlanFragmentId, FragmentExecParams> privateFragmentExecParams =
                (HashMap<PlanFragmentId, FragmentExecParams>) getField(
                        coor, "fragmentExecParams");
        ConcurrentMap<TUniqueId, BackendExecState> privateBackendExecStateMap =
                (ConcurrentMap<TUniqueId, BackendExecState>) getField(coor, "backendExecStateMap");
        TQueryOptions privateQueryOptions = (TQueryOptions) getField(coor, "queryOptions");
        // 设置超时时间为2s，尽快返回连接超时
        privateQueryOptions.setQuery_timeout(2);
        // Configure.qe_query_timeout_s  = 2;

        privateFragmentExecParams.clear();
        privateFragmentExecParams.put(new PlanFragmentId(23), new FragmentExecParams(null));
        // privateFragmentExecParams.get(new PlanFragmentId(23)).hosts.add(
        // new TNetworkAddress("machine", 10000));
        privateBackendExecStateMap.put(new TUniqueId(11, 12), coor.new BackendExecState(
                new PlanFragmentId(23), 0, 0, new TExecPlanFragmentParams(),
                new HashMap<TNetworkAddress, Long>()));
        // 调用函数
        boolean isException = false;
        try {
            privateBackendExecStateMap.get(new TUniqueId(11, 12)).execRemoteFragment();
        } catch (org.apache.thrift.transport.TTransportException e) {
            isException = true;
        } catch (Exception e) {
            isException = false;
        }
        Assert.assertTrue("need get the TTransportException", isException);
    }
    */
}

