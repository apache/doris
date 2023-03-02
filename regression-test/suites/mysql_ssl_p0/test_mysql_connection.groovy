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

suite("test_mysql_connection") {

    def executeMySQLCommand = { String command ->
        try {
            String line;
            StringBuilder errMsg = new StringBuilder();
            StringBuilder msg = new StringBuilder();
            Process p = Runtime.getRuntime().exec(new String[]{"/bin/bash", "-c", command});

            BufferedReader errInput = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            while ((line = errInput.readLine()) != null) {
                errMsg.append(line);
            }
            assert errMsg.length() == 0: "error occurred!" + errMsg.toString();
            errInput.close();

            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((line = input.readLine()) != null) {
                msg.append(line);
            }
            assert msg.toString().contains("version"): "error occurred!" + errMsg.toString();
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    String jdbcUrlConfig = context.config.jdbcUrl;
    String tempString = jdbcUrlConfig.substring(jdbcUrlConfig.indexOf("jdbc:mysql://") + 13);
    String mysqlHost = tempString.substring(0, tempString.indexOf(":"));
    String mysqlPort = tempString.substring(tempString.indexOf(":") + 1, tempString.indexOf("/"));
    String cmdDefault = "mysql -uroot -h" + mysqlHost + " -P" + mysqlPort + " -e \"show variables\"";
    String cmdDisabledSsl = "mysql --ssl-mode=DISABLE -uroot -h" + mysqlHost + " -P" + mysqlPort + " -e \"show variables\"";
    String cmdSsl12 = "mysql --ssl-mode=REQUIRED -uroot -h" + mysqlHost + " -P" + mysqlPort + " --tls-version=TLSv1.2 -e \"show variables\"";
    // The current mysql-client version of the test environment is 5.7.32, which does not support TLSv1.3, so comment this part.
    // String cmdSsl13 = "mysql --ssl-mode=REQUIRED -uroot -h" + mysqlHost + " -P" + mysqlPort +  " --tls-version=TLSv1.3 -e \"show variables\"";
    executeMySQLCommand(cmdDefault);
    executeMySQLCommand(cmdDisabledSsl);
    executeMySQLCommand(cmdSsl12);
    // executeMySQLCommand(cmdSsl13);
}
