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

package org.apache.doris.sdk.examples;

/**
 * Entry point for running all examples.
 *
 * <p>Usage:
 * <pre>
 * # Run all examples
 * java -cp java-doris-sdk-1.0.0.jar org.apache.doris.sdk.examples.ExamplesMain all
 *
 * # Run individual examples
 * java -cp java-doris-sdk-1.0.0.jar org.apache.doris.sdk.examples.ExamplesMain simple
 * java -cp java-doris-sdk-1.0.0.jar org.apache.doris.sdk.examples.ExamplesMain single
 * java -cp java-doris-sdk-1.0.0.jar org.apache.doris.sdk.examples.ExamplesMain json
 * java -cp java-doris-sdk-1.0.0.jar org.apache.doris.sdk.examples.ExamplesMain concurrent
 * java -cp java-doris-sdk-1.0.0.jar org.apache.doris.sdk.examples.ExamplesMain gzip
 * </pre>
 */
public class ExamplesMain {

    public static void main(String[] args) {
        String mode = (args.length > 0) ? args[0].toLowerCase() : "all";

        switch (mode) {
            case "all":
                System.out.println("\n>>> Running: simple");
                SimpleConfigExample.run();
                System.out.println("\n>>> Running: single");
                SingleBatchExample.run();
                System.out.println("\n>>> Running: json");
                JsonExample.run();
                System.out.println("\n>>> Running: concurrent");
                ConcurrentExample.run();
                System.out.println("\n>>> Running: gzip");
                GzipExample.run();
                break;
            case "simple":
                SimpleConfigExample.run();
                break;
            case "single":
                SingleBatchExample.run();
                break;
            case "json":
                JsonExample.run();
                break;
            case "concurrent":
                ConcurrentExample.run();
                break;
            case "gzip":
                GzipExample.run();
                break;
            default:
                System.err.println("Unknown example: " + mode);
                System.err.println("Available: all | simple | single | json | concurrent | gzip");
                System.exit(1);
        }
    }
}
