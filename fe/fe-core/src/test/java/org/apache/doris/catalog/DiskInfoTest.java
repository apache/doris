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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.thrift.TStorageMedium;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for DiskInfo class, focusing on exceedLimit() method
 * and debug point functionality for storage medium capacity checks.
 */
public class DiskInfoTest {

    @Before
    public void setUp() {
        // Enable debug points for testing
        Config.enable_debug_points = true;

        // Set DEBUG level for DiskInfo logger to cover debug log statements
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig("org.apache.doris.catalog.DiskInfo");
        if (loggerConfig.getName().equals("org.apache.doris.catalog.DiskInfo")) {
            loggerConfig.setLevel(Level.DEBUG);
        } else {
            // Create new logger config if it doesn't exist
            LoggerConfig newLoggerConfig = new LoggerConfig("org.apache.doris.catalog.DiskInfo", Level.DEBUG, true);
            config.addLogger("org.apache.doris.catalog.DiskInfo", newLoggerConfig);
        }
        ctx.updateLoggers();

        // Set test-friendly config values
        // Note: We directly set values without saving originals because
        // in unit test environment Config may not be fully initialized
        Config.storage_min_left_capacity_bytes = 100L * 1024 * 1024; // 100MB
        Config.storage_high_watermark_usage_percent = 85;
        Config.storage_flood_stage_left_capacity_bytes = 50L * 1024 * 1024; // 50MB
        Config.storage_flood_stage_usage_percent = 95;

        // Clear any existing debug points
        DebugPointUtil.clearDebugPoints();
    }

    @After
    public void tearDown() {
        // Restore logger level to INFO
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig("org.apache.doris.catalog.DiskInfo");
        if (loggerConfig.getName().equals("org.apache.doris.catalog.DiskInfo")) {
            loggerConfig.setLevel(Level.INFO);
            ctx.updateLoggers();
        }

        // Disable debug points
        Config.enable_debug_points = false;

        // Reset config values to reasonable defaults
        Config.storage_min_left_capacity_bytes = 2L * 1024 * 1024 * 1024; // 2GB (default)
        Config.storage_high_watermark_usage_percent = 85; // default
        Config.storage_flood_stage_left_capacity_bytes = 1L * 1024 * 1024 * 1024; // 1GB (default)
        Config.storage_flood_stage_usage_percent = 95; // default

        // Clear all debug points
        DebugPointUtil.clearDebugPoints();
    }

    /**
     * Test basic capacity check without debug points
     */
    @Test
    public void testBasicCapacityCheck() {
        // Case 1: Disk has enough capacity
        DiskInfo diskInfo = new DiskInfo("/data/disk1");
        diskInfo.setTotalCapacityB(1024L * 1024 * 1024); // 1GB
        diskInfo.setAvailableCapacityB(500L * 1024 * 1024); // 500MB available
        diskInfo.setStorageMedium(TStorageMedium.HDD);

        Assert.assertFalse("Disk with 500MB available should not exceed limit",
                diskInfo.exceedLimit(false));

        // Case 2: Disk exceeds capacity limit (low available space)
        diskInfo.setAvailableCapacityB(50L * 1024 * 1024); // 50MB available (< 100MB min)
        Assert.assertTrue("Disk with only 50MB available should exceed limit",
                diskInfo.exceedLimit(false));

        // Case 3: Disk exceeds capacity limit (high usage percentage)
        diskInfo.setAvailableCapacityB(100L * 1024 * 1024); // 100MB available
        diskInfo.setTotalCapacityB(500L * 1024 * 1024); // 500MB total (80% used)
        Assert.assertFalse("Disk with 80% usage should not exceed 85% limit",
                diskInfo.exceedLimit(false));

        diskInfo.setAvailableCapacityB(50L * 1024 * 1024); // 50MB available (90% used)
        Assert.assertTrue("Disk with 90% usage should exceed 85% limit",
                diskInfo.exceedLimit(false));
    }

    /**
     * Test flood stage capacity check
     */
    @Test
    public void testFloodStageCheck() {
        DiskInfo diskInfo = new DiskInfo("/data/disk1");
        diskInfo.setTotalCapacityB(1024L * 1024 * 1024); // 1GB
        diskInfo.setStorageMedium(TStorageMedium.HDD);

        // Flood stage uses AND condition (both must be true)
        // Case 1: Low space but not high percentage -> should not exceed
        // 40MB available (< 50MB), but 984MB/1024MB = 96% used is still high
        diskInfo.setAvailableCapacityB(40L * 1024 * 1024); // 40MB available (< 50MB flood stage)
        // Used = Total - Available = 1024MB - 40MB = 984MB (96% > 95%)
        // This WILL exceed because both conditions are met!
        Assert.assertTrue("Low space AND high percentage -> should exceed",
                diskInfo.exceedLimit(true));

        // Case 2: High percentage but enough space -> should not exceed
        diskInfo.setAvailableCapacityB(100L * 1024 * 1024); // 100MB available (> 50MB)
        // Used = 1024MB - 100MB = 924MB (90% < 95%)
        Assert.assertFalse("High percentage but enough space -> should not exceed",
                diskInfo.exceedLimit(true));

        // Case 3: Low percentage but not enough space -> should not exceed
        diskInfo.setTotalCapacityB(500L * 1024 * 1024); // 500MB total
        diskInfo.setAvailableCapacityB(40L * 1024 * 1024); // 40MB available (< 50MB)
        // Used = 500MB - 40MB = 460MB (92% < 95%)
        Assert.assertFalse("Low space but low percentage -> should not exceed",
                diskInfo.exceedLimit(true));

        // Case 4: Both conditions met -> should exceed
        diskInfo.setTotalCapacityB(1024L * 1024 * 1024); // 1GB
        diskInfo.setAvailableCapacityB(30L * 1024 * 1024); // 30MB available (< 50MB)
        // Used = 1024MB - 30MB = 994MB (97% > 95%)
        Assert.assertTrue("Flood stage with both conditions should exceed",
                diskInfo.exceedLimit(true));
    }

    /**
     * Test debug point: DiskInfo.exceedLimit.ssd.alwaysTrue
     * Forces SSD disks to report as exceed limit
     */
    @Test
    public void testDebugPointSsdAlwaysTrue() {
        DiskInfo diskInfo = new DiskInfo("/data/ssd1");
        diskInfo.setTotalCapacityB(1024L * 1024 * 1024); // 1GB
        diskInfo.setAvailableCapacityB(500L * 1024 * 1024); // 500MB available (plenty)
        diskInfo.setStorageMedium(TStorageMedium.SSD);

        // Without debug point - should not exceed
        Assert.assertFalse("SSD with plenty space should not exceed",
                diskInfo.exceedLimit(false));

        // Enable debug point
        DebugPointUtil.addDebugPoint("DiskInfo.exceedLimit.ssd.alwaysTrue");

        // With debug point - should always exceed
        Assert.assertTrue("SSD with debug point should always exceed",
                diskInfo.exceedLimit(false));

        // HDD should not be affected
        diskInfo.setStorageMedium(TStorageMedium.HDD);
        Assert.assertFalse("HDD should not be affected by SSD debug point",
                diskInfo.exceedLimit(false));

        // Clear debug point
        DebugPointUtil.clearDebugPoints();
    }

    /**
     * Test debug point: DiskInfo.exceedLimit.ssd.alwaysFalse
     * Forces SSD disks to report as available
     */
    @Test
    public void testDebugPointSsdAlwaysFalse() {
        DiskInfo diskInfo = new DiskInfo("/data/ssd1");
        diskInfo.setTotalCapacityB(1024L * 1024 * 1024); // 1GB
        diskInfo.setAvailableCapacityB(50L * 1024 * 1024); // 50MB available (low)
        diskInfo.setStorageMedium(TStorageMedium.SSD);

        // Without debug point - should exceed
        Assert.assertTrue("SSD with low space should exceed",
                diskInfo.exceedLimit(false));

        // Enable debug point
        DebugPointUtil.addDebugPoint("DiskInfo.exceedLimit.ssd.alwaysFalse");

        // With debug point - should never exceed
        Assert.assertFalse("SSD with debug point should never exceed",
                diskInfo.exceedLimit(false));

        // HDD should not be affected
        diskInfo.setStorageMedium(TStorageMedium.HDD);
        Assert.assertTrue("HDD should not be affected by SSD debug point",
                diskInfo.exceedLimit(false));

        // Clear debug point
        DebugPointUtil.clearDebugPoints();
    }

    /**
     * Test debug point: DiskInfo.exceedLimit.hdd.alwaysTrue
     * Forces HDD disks to report as exceed limit
     */
    @Test
    public void testDebugPointHddAlwaysTrue() {
        DiskInfo diskInfo = new DiskInfo("/data/hdd1");
        diskInfo.setTotalCapacityB(1024L * 1024 * 1024); // 1GB
        diskInfo.setAvailableCapacityB(500L * 1024 * 1024); // 500MB available (plenty)
        diskInfo.setStorageMedium(TStorageMedium.HDD);

        // Without debug point - should not exceed
        Assert.assertFalse("HDD with plenty space should not exceed",
                diskInfo.exceedLimit(false));

        // Enable debug point
        DebugPointUtil.addDebugPoint("DiskInfo.exceedLimit.hdd.alwaysTrue");

        // With debug point - should always exceed
        Assert.assertTrue("HDD with debug point should always exceed",
                diskInfo.exceedLimit(false));

        // SSD should not be affected
        diskInfo.setStorageMedium(TStorageMedium.SSD);
        Assert.assertFalse("SSD should not be affected by HDD debug point",
                diskInfo.exceedLimit(false));

        // Clear debug point
        DebugPointUtil.clearDebugPoints();
    }

    /**
     * Test debug point: DiskInfo.exceedLimit.hdd.alwaysFalse
     * Forces HDD disks to report as available
     */
    @Test
    public void testDebugPointHddAlwaysFalse() {
        DiskInfo diskInfo = new DiskInfo("/data/hdd1");
        diskInfo.setTotalCapacityB(1024L * 1024 * 1024); // 1GB
        diskInfo.setAvailableCapacityB(50L * 1024 * 1024); // 50MB available (low)
        diskInfo.setStorageMedium(TStorageMedium.HDD);

        // Without debug point - should exceed
        Assert.assertTrue("HDD with low space should exceed",
                diskInfo.exceedLimit(false));

        // Enable debug point
        DebugPointUtil.addDebugPoint("DiskInfo.exceedLimit.hdd.alwaysFalse");

        // With debug point - should never exceed
        Assert.assertFalse("HDD with debug point should never exceed",
                diskInfo.exceedLimit(false));

        // SSD should not be affected
        diskInfo.setStorageMedium(TStorageMedium.SSD);
        Assert.assertTrue("SSD should not be affected by HDD debug point",
                diskInfo.exceedLimit(false));

        // Clear debug point
        DebugPointUtil.clearDebugPoints();
    }

    /**
     * Test multiple debug points interaction
     */
    @Test
    public void testMultipleDebugPoints() {
        DiskInfo ssdDisk = new DiskInfo("/data/ssd1");
        ssdDisk.setTotalCapacityB(1024L * 1024 * 1024);
        ssdDisk.setAvailableCapacityB(500L * 1024 * 1024);
        ssdDisk.setStorageMedium(TStorageMedium.SSD);

        DiskInfo hddDisk = new DiskInfo("/data/hdd1");
        hddDisk.setTotalCapacityB(1024L * 1024 * 1024);
        hddDisk.setAvailableCapacityB(500L * 1024 * 1024);
        hddDisk.setStorageMedium(TStorageMedium.HDD);

        // Enable both alwaysTrue debug points
        DebugPointUtil.addDebugPoint("DiskInfo.exceedLimit.ssd.alwaysTrue");
        DebugPointUtil.addDebugPoint("DiskInfo.exceedLimit.hdd.alwaysTrue");

        // Both should report exceed limit
        Assert.assertTrue("SSD should exceed with debug point", ssdDisk.exceedLimit(false));
        Assert.assertTrue("HDD should exceed with debug point", hddDisk.exceedLimit(false));

        // Clear and set alwaysFalse for both
        DebugPointUtil.clearDebugPoints();
        DebugPointUtil.addDebugPoint("DiskInfo.exceedLimit.ssd.alwaysFalse");
        DebugPointUtil.addDebugPoint("DiskInfo.exceedLimit.hdd.alwaysFalse");

        // Both should report available
        Assert.assertFalse("SSD should not exceed with debug point", ssdDisk.exceedLimit(false));
        Assert.assertFalse("HDD should not exceed with debug point", hddDisk.exceedLimit(false));

        // Clear debug points
        DebugPointUtil.clearDebugPoints();
    }
}

