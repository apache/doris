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

package org.apache.doris.cloud.catalog;

import org.apache.doris.common.DdlException;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ComputeGroupTest {
    private ComputeGroup computeGroup;

    @BeforeEach
    public void setUp() {
        computeGroup = new ComputeGroup("test_id", "test_group", ComputeGroup.ComputeTypeEnum.COMPUTE);
    }

    @Test
    public void testCheckPropertiesWithNull() throws DdlException {
        computeGroup.checkProperties(null);
        computeGroup.checkProperties(Maps.newHashMap());
    }

    @Test
    public void testCheckPropertiesWithValidBalanceType() throws DdlException {
        // 测试有效的balance_type
        Map<String, String> properties = Maps.newHashMap();
        properties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.WITHOUT_WARMUP.getValue());
        computeGroup.checkProperties(properties);

        properties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.ASYNC_WARMUP.getValue());
        computeGroup.checkProperties(properties);

        properties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.SYNC_WARMUP.getValue());
        computeGroup.checkProperties(properties);

        properties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.PEER_READ_ASYNC_WARMUP.getValue());
        computeGroup.checkProperties(properties);
    }

    @Test
    public void testCheckPropertiesWithInvalidBalanceType() {
        // 测试无效的balance_type
        Map<String, String> properties = Maps.newHashMap();
        properties.put(ComputeGroup.BALANCE_TYPE, "invalid_type");

        Assertions.assertThrows(DdlException.class, () -> {
            computeGroup.checkProperties(properties);
        });
    }

    @Test
    public void testCheckPropertiesWithValidTimeout() throws DdlException {
        // 测试有效的timeout
        Map<String, String> properties = Maps.newHashMap();
        properties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.ASYNC_WARMUP.getValue());
        properties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "300");
        computeGroup.checkProperties(properties);

        properties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "1");
        computeGroup.checkProperties(properties);
    }

    @Test
    public void testCheckPropertiesWithInvalidTimeout() {
        // 测试无效的timeout
        Map<String, String> properties = Maps.newHashMap();
        properties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "-1");

        Assertions.assertThrows(DdlException.class, () -> {
            computeGroup.checkProperties(properties);
        });

        properties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "invalid");
        Assertions.assertThrows(DdlException.class, () -> {
            computeGroup.checkProperties(properties);
        });
    }

    @Test
    public void testCheckPropertiesWithUnsupportedProperty() {
        // 测试不支持的属性
        Map<String, String> properties = Maps.newHashMap();
        properties.put("unsupported_property", "value");

        Assertions.assertThrows(DdlException.class, () -> {
            computeGroup.checkProperties(properties);
        });
    }

    @Test
    public void testModifyPropertiesWithDirectSwitch() throws DdlException {
        // 测试without_warmup类型，应该删除timeout
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.WITHOUT_WARMUP.getValue());
        inputProperties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT,
                String.valueOf(ComputeGroup.DEFAULT_BALANCE_WARM_UP_TASK_TIMEOUT));

        // 先设置timeout到properties中
        computeGroup.getProperties().put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT,
                String.valueOf(ComputeGroup.DEFAULT_BALANCE_WARM_UP_TASK_TIMEOUT));
        Assertions.assertTrue(computeGroup.getProperties().containsKey(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT));

        computeGroup.modifyProperties(inputProperties);

        // 验证timeout被删除
        Assertions.assertFalse(computeGroup.getProperties().containsKey(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT));
    }

    @Test
    public void testModifyPropertiesWithSyncCache() throws DdlException {
        // 测试sync_cache类型，应该删除timeout
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.SYNC_WARMUP.getValue());
        inputProperties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT,
                String.valueOf(ComputeGroup.DEFAULT_BALANCE_WARM_UP_TASK_TIMEOUT));

        // 先设置timeout到properties中
        computeGroup.getProperties().put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT,
                String.valueOf(ComputeGroup.DEFAULT_BALANCE_WARM_UP_TASK_TIMEOUT));
        Assertions.assertTrue(computeGroup.getProperties().containsKey(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT));

        computeGroup.modifyProperties(inputProperties);

        // 验证timeout被删除
        Assertions.assertFalse(computeGroup.getProperties().containsKey(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT));
    }

    @Test
    public void testCheckPropertiesWithBalanceTypeTransition() throws DdlException {
        computeGroup.getProperties().put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.WITHOUT_WARMUP.getValue());
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.ASYNC_WARMUP.getValue());
        computeGroup.checkProperties(inputProperties);
    }

    @Test
    public void testCheckPropertiesWithWarmupCacheToWarmupCache() throws DdlException {
        computeGroup.getProperties().put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.ASYNC_WARMUP.getValue());
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.ASYNC_WARMUP.getValue());
        computeGroup.checkProperties(inputProperties);
    }

    @Test
    public void testCheckPropertiesWithDirectSwitchToDirectSwitch() throws DdlException {
        // 测试从direct_switch转换到direct_switch，不需要设置timeout
        computeGroup.getProperties().put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.WITHOUT_WARMUP.getValue());
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.WITHOUT_WARMUP.getValue());
        computeGroup.checkProperties(inputProperties);
    }

    @Test
    public void testModifyPropertiesWithWarmupCacheAndExistingTimeout() throws DdlException {
        // 测试async_warmup类型，已存在timeout，不应该添加默认值
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.ASYNC_WARMUP.getValue());
        inputProperties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "600");

        // 先设置timeout到properties中
        computeGroup.getProperties().put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "500");
        String originalTimeout = computeGroup.getProperties().get(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT);

        computeGroup.modifyProperties(inputProperties);

        // 验证timeout没有被修改, 这里的意思是用户已经设置过timeout了，就不应该被覆盖
        Assertions.assertEquals(originalTimeout, computeGroup.getProperties().get(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT));
    }

    @Test
    public void testModifyPropertiesWithWarmupCacheAndNoTimeout() throws DdlException {
        // 测试async_warmup类型，不存在timeout，应该添加默认值
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.ASYNC_WARMUP.getValue());

        // 确保properties中没有timeout
        computeGroup.getProperties().remove(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT);
        Assertions.assertFalse(computeGroup.getProperties().containsKey(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT));

        computeGroup.modifyProperties(inputProperties);
        // 验证默认值被添加
        Assertions.assertTrue(computeGroup.getProperties().containsKey(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT));
        Assertions.assertEquals(String.valueOf(ComputeGroup.DEFAULT_BALANCE_WARM_UP_TASK_TIMEOUT),
                computeGroup.getProperties().get(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT));
    }

    @Test
    public void testModifyPropertiesWithNullBalanceType() throws DdlException {
        // 测试null balance_type，不应该修改properties
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put("other_property", "value");

        Map<String, String> originalProperties = Maps.newHashMap(computeGroup.getProperties());

        computeGroup.modifyProperties(inputProperties);

        // 验证properties没有被修改
        Assertions.assertEquals(originalProperties, computeGroup.getProperties());
    }

    @Test
    public void testModifyPropertiesWithEmptyInput() throws DdlException {
        // 测试空输入，不应该修改properties
        Map<String, String> inputProperties = Maps.newHashMap();

        Map<String, String> originalProperties = Maps.newHashMap(computeGroup.getProperties());

        computeGroup.modifyProperties(inputProperties);

        // 验证properties没有被修改
        Assertions.assertEquals(originalProperties, computeGroup.getProperties());
    }

    @Test
    public void testCheckPropertiesWithSyncCacheToWarmupCache() throws DdlException {
        computeGroup.getProperties().put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.SYNC_WARMUP.getValue());
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.ASYNC_WARMUP.getValue());
        computeGroup.checkProperties(inputProperties);
    }

    @Test
    public void testValidateTimeoutRestrictionWithNoCurrentBalanceType() throws DdlException {
        // 测试当前没有设置balance_type的情况
        computeGroup.getProperties().remove(ComputeGroup.BALANCE_TYPE);
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "500");
        computeGroup.checkProperties(inputProperties);
    }

    @Test
    public void testValidateTimeoutRestrictionWithCurrentWarmupCache() throws DdlException {
        // 测试当前balance_type是warmup_cache的情况
        computeGroup.getProperties().put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.ASYNC_WARMUP.getValue());
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "500");
        computeGroup.checkProperties(inputProperties);
    }

    @Test
    public void testValidateTimeoutRestrictionWithDirectSwitchToWarmupCache() throws DdlException {
        // 测试从direct_switch转换到warmup_cache并设置timeout
        computeGroup.getProperties().put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.WITHOUT_WARMUP.getValue());
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.ASYNC_WARMUP.getValue());
        inputProperties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "500");
        computeGroup.checkProperties(inputProperties);
    }

    @Test
    public void testValidateTimeoutRestrictionWithSyncCacheToWarmupCacheWithTimeout() throws DdlException {
        // 测试从sync_cache转换到warmup_cache并设置timeout
        computeGroup.getProperties().put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.SYNC_WARMUP.getValue());
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.ASYNC_WARMUP.getValue());
        inputProperties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "500");
        computeGroup.checkProperties(inputProperties);
    }

    @Test
    public void testValidateTimeoutRestrictionWithDirectSwitchAndOnlyTimeout() {
        // 测试当前是direct_switch，仅设置timeout应该失败
        computeGroup.getProperties().put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.WITHOUT_WARMUP.getValue());
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "500");

        Assertions.assertThrows(DdlException.class, () -> {
            computeGroup.checkProperties(inputProperties);
        });
    }

    @Test
    public void testValidateTimeoutRestrictionWithSyncCacheAndOnlyTimeout() {
        // 测试当前是sync_cache，仅设置timeout应该失败
        computeGroup.getProperties().put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.SYNC_WARMUP.getValue());
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "500");

        Assertions.assertThrows(DdlException.class, () -> {
            computeGroup.checkProperties(inputProperties);
        });
    }

    @Test
    public void testValidateTimeoutRestrictionWithDirectSwitchToSyncCacheAndTimeout() {
        // 测试从direct_switch转换到sync_cache并设置timeout应该失败
        computeGroup.getProperties().put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.WITHOUT_WARMUP.getValue());
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.SYNC_WARMUP.getValue());
        inputProperties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "500");

        Assertions.assertThrows(DdlException.class, () -> {
            computeGroup.checkProperties(inputProperties);
        });
    }

    @Test
    public void testValidateTimeoutRestrictionWithDirectSwitchToSameAndTimeout() {
        // 测试从direct_switch转换到direct_switch并设置timeout应该失败
        computeGroup.getProperties().put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.WITHOUT_WARMUP.getValue());
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.WITHOUT_WARMUP.getValue());
        inputProperties.put(ComputeGroup.BALANCE_WARM_UP_TASK_TIMEOUT, "500");

        Assertions.assertThrows(DdlException.class, () -> {
            computeGroup.checkProperties(inputProperties);
        });
    }

    @Test
    public void testValidateTimeoutRestrictionWithNoInputTimeout() throws DdlException {
        // 测试输入中没有timeout的情况
        computeGroup.getProperties().put(ComputeGroup.BALANCE_TYPE, BalanceTypeEnum.WITHOUT_WARMUP.getValue());
        Map<String, String> inputProperties = Maps.newHashMap();
        inputProperties.put("other_property", "value");
        Assertions.assertThrows(DdlException.class, () -> {
            // Property other_property is not supported
            computeGroup.checkProperties(inputProperties);
        });
    }
}
