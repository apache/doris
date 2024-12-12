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

package doris.arrowflight.demo;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


/**
 * Iterate over each batch in ArrowReader.
 * ArrowReader is the iterator returned by ADBC Client when executing a query.
 */
public class ArrowBatchReader {

    @FunctionalInterface
    public interface LoadArrowBatchFunc {
        void load(ArrowReader reader) throws IOException;
    }

    /**
     * Print one row in VectorSchemaRoot, if the output format is incorrect, may need to modify
     * the output method of different types of ValueVector.
     */
    public static void printRow(VectorSchemaRoot root, int rowIndex) {
        if (root == null || rowIndex < 0 || rowIndex >= root.getRowCount()) {
            System.out.println("Invalid row index: " + rowIndex);
            return;
        }

        System.out.print("> ");
        for (Field field : root.getSchema().getFields()) {
            ValueVector vector = root.getVector(field.getName());
            if (vector != null) {
                if (vector instanceof org.apache.arrow.vector.DateDayVector) {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                    int dayOffset = ((org.apache.arrow.vector.DateDayVector) vector).get(rowIndex);
                    LocalDate date = LocalDate.ofEpochDay(dayOffset);
                    System.out.print(date.format(formatter));
                } else if (vector instanceof org.apache.arrow.vector.BitVector) {
                    System.out.print(((org.apache.arrow.vector.BitVector) vector).get(rowIndex) == 1);
                } else {
                    // other types field
                    System.out.print(vector.getObject(rowIndex).toString());
                }
                System.out.print(", ");
            }
        }
        System.out.println();
    }

    /**
     * Iterate over each batch in ArrowReader with the least cost, only record the number of rows and batches,
     * usually used to test performance.
     */
    public static LoadArrowBatchFunc loadArrowBatch = reader -> {
        int rowCount = 0;
        int batchCount = 0;
        while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            if (batchCount == 0) {
                System.out.println("> " + root.getSchema().toString());
                printRow(root, 1);  // only print first line
            }
            rowCount += root.getRowCount();
            batchCount += 1;
        }
        System.out.println("> batchCount: " + batchCount + ", rowCount: " + rowCount);
    };

    /**
     * Iterate over each batch in ArrowReader and convert the batch to String, this will take more time.
     */
    public static LoadArrowBatchFunc loadArrowBatchToString = reader -> {
        int rowCount = 0;
        List<String> result = new ArrayList<>();
        while (reader.loadNextBatch()) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            if (result.size() == 0) {
                System.out.println("> " + root.getSchema().toString());
                printRow(root, 0);  // only print first line
            }
            result.add(root.contentToTSVString());
            rowCount += root.getRowCount();
        }
        System.out.println("> batchCount: " + result.size() + ", rowCount: " + rowCount);
    };
}
