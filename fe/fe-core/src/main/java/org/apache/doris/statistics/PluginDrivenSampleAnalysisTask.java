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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.text.StringSubstitutor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Sample-capable analyze task for a flipped plugin-driven file-scan table (plain-hive after the HMS cutover).
 *
 * <p>The generic {@link ExternalAnalysisTask} throws {@code NotImplementedException} from {@link #doSample()},
 * which is correct for connectors that cannot serve raw per-file sizes (iceberg/paimon/JDBC/ES). A plain-hive
 * table CAN (legacy {@code HMSExternalTask.doSample}), so {@code PluginDrivenExternalTable.createAnalysisTask}
 * hands back THIS task when the connector declares {@code SUPPORTS_SAMPLE_ANALYZE} per-table. The
 * {@code doSample}/{@code getSampleInfo}/{@code needLimit} bodies are a verbatim port of the legacy
 * {@code HMSAnalysisTask} equivalents — they touch only base-class members ({@link #table}, {@code tbl},
 * {@code col}, {@code tableSample} and the shared templates/helpers), never an HMS-specific field, so the scale
 * factor, limit heuristic and linear-vs-DUJ1 estimator choice stay byte-identical to legacy. The raw file sizes
 * come from {@code PluginDrivenExternalTable.getChunkSizes()} (connector's {@code listFileSizes}); the
 * Doris-type slot-width math stays here. Full (non-sample) analyze falls through to the base {@link #doFull()}.
 */
public class PluginDrivenSampleAnalysisTask extends ExternalAnalysisTask {

    public PluginDrivenSampleAnalysisTask() {
        super();
    }

    public PluginDrivenSampleAnalysisTask(AnalysisInfo info) {
        super(info);
    }

    @Override
    protected void doSample() {
        StringBuilder sb = new StringBuilder();
        Map<String, String> params = buildSqlParams();
        params.put("min", getMinFunction());
        params.put("max", getMaxFunction());
        params.put("dataSizeFunction", getDataSizeFunction(col, false));
        Pair<Double, Long> sampleInfo = getSampleInfo();
        params.put("scaleFactor", String.valueOf(sampleInfo.first));
        params.put("hotValueCollectCount", String.valueOf(SessionVariable.getHotValueCollectCount()));
        if (LOG.isDebugEnabled()) {
            LOG.debug("Will do sample collection for column {}", col.getName());
        }
        boolean limitFlag = false;
        boolean bucketFlag = false;
        // If sample size is too large, use limit to control the sample size.
        if (needLimit(sampleInfo.second, sampleInfo.first)) {
            limitFlag = true;
            long columnSize = 0;
            for (Column column : table.getFullSchema()) {
                columnSize += column.getDataType().getSlotSize();
            }
            double targetRows = (double) sampleInfo.second / columnSize;
            // Estimate the new scaleFactor based on the schema.
            if (targetRows > StatisticsUtil.getHugeTableSampleRows()) {
                params.put("limit", "limit " + StatisticsUtil.getHugeTableSampleRows());
                params.put("scaleFactor",
                        String.valueOf(sampleInfo.first * targetRows / StatisticsUtil.getHugeTableSampleRows()));
            }
        }
        // Single distribution column is not fit for DUJ1 estimator, use linear estimator.
        Set<String> distributionColumns = tbl.getDistributionColumnNames();
        if (distributionColumns.size() == 1 && distributionColumns.contains(col.getName().toLowerCase())) {
            bucketFlag = true;
            sb.append(LINEAR_ANALYZE_TEMPLATE);
            params.put("ndvFunction", "ROUND(NDV(`${colName}`) * ${scaleFactor})");
            params.put("rowCount", "ROUND(COUNT(1) * ${scaleFactor})");
            params.put("rowCount2", "(SELECT COUNT(1) FROM cte1 WHERE `${colName}` IS NOT NULL)");
        } else {
            sb.append(DUJ1_ANALYZE_TEMPLATE);
            params.put("subStringColName", getStringTypeColName(col));
            params.put("dataSizeFunction", getDataSizeFunction(col, true));
            params.put("ndvFunction", getNdvFunction("ROUND(SUM(t1.count) * ${scaleFactor})"));
            params.put("rowCount", "ROUND(SUM(t1.count) * ${scaleFactor})");
            params.put("rowCount2", "(SELECT SUM(`count`) FROM cte1 WHERE `col_value` IS NOT NULL)");
        }
        LOG.info("Sample for column [{}]. Scale factor [{}], "
                + "limited [{}], is distribute column [{}]",
                col.getName(), params.get("scaleFactor"), limitFlag, bucketFlag);
        StringSubstitutor stringSubstitutor = new StringSubstitutor(params);
        String sql = stringSubstitutor.replace(sb.toString());
        runQuery(sql);
    }

    /**
     * Get the pair of sample scale factor and the file size going to sample. While analyzing, the result of
     * count, null count and data size need to multiply this scale factor to get a more accurate result.
     * @return Pair of sample scale factor and the file size going to sample.
     */
    protected Pair<Double, Long> getSampleInfo() {
        if (tableSample == null) {
            return Pair.of(1.0, 0L);
        }
        long target;
        // Get list of all files' size in this table (from the connector, via getChunkSizes()).
        List<Long> chunkSizes = table.getChunkSizes();
        Collections.shuffle(chunkSizes, new Random(tableSample.getSeek()));
        long total = 0;
        // Calculate the total size of this table.
        for (long size : chunkSizes) {
            total += size;
        }
        if (total == 0) {
            return Pair.of(1.0, 0L);
        }
        // Calculate the sample target size for percent and rows sample.
        if (tableSample.isPercent()) {
            target = total * tableSample.getSampleValue() / 100;
        } else {
            int columnSize = 0;
            for (Column column : table.getFullSchema()) {
                columnSize += column.getDataType().getSlotSize();
            }
            target = columnSize * tableSample.getSampleValue();
        }
        // Calculate the actual sample size (cumulate).
        long cumulate = 0;
        for (long size : chunkSizes) {
            cumulate += size;
            if (cumulate >= target) {
                break;
            }
        }
        return Pair.of(Math.max(((double) total) / cumulate, 1), cumulate);
    }

    /**
     * If the size to sample is larger than LIMIT_SIZE (1GB) and is much larger (1.2*) than the size the user
     * wants to sample, use limit to control the total sample size.
     * @param sizeToRead The file size to sample.
     * @param factor sizeToRead * factor = Table total size.
     * @return True if need to limit.
     */
    protected boolean needLimit(long sizeToRead, double factor) {
        long total = (long) (sizeToRead * factor);
        long target;
        if (tableSample.isPercent()) {
            target = total * tableSample.getSampleValue() / 100;
        } else {
            int columnSize = 0;
            for (Column column : table.getFullSchema()) {
                columnSize += column.getDataType().getSlotSize();
            }
            target = columnSize * tableSample.getSampleValue();
        }
        return sizeToRead > LIMIT_SIZE && sizeToRead > target * LIMIT_FACTOR;
    }
}
