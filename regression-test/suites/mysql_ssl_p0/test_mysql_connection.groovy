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
import org.apache.doris.regression.Config

suite("test_mysql_connection") { suite ->
    // NOTE: this suite need you install mysql client 5.7 + to support --ssl-mode parameter

    def executeMySQLCommand = { String command ->
        def cmds = ["/bin/bash", "-c", command]
        logger.info("Execute: ${cmds}".toString())
        Process p = cmds.execute()

        def errMsg = new StringBuilder()
        def msg = new StringBuilder()
        p.waitForProcessOutput(msg, errMsg)

        assert errMsg.length() == 0: "error occurred!" + errMsg
        assert msg.toString().contains("version"): "error occurred!" + errMsg
        assert p.exitValue() == 0
    }

    String jdbcUrlConfig = context.config.jdbcUrl;
    String tempString = jdbcUrlConfig.substring(jdbcUrlConfig.indexOf("jdbc:mysql://") + 13);
    String mysqlHost = tempString.substring(0, tempString.indexOf(":"));
    String mysqlPort = tempString.substring(tempString.indexOf(":") + 1, tempString.indexOf("/"));
    String cmdDefault = "mysql -uroot -h" + mysqlHost + " -P" + mysqlPort + " -e \"show variables\"";
    String cmdDisabledSsl = "mysql --ssl-mode=DISABLE -uroot -h" + mysqlHost + " -P" + mysqlPort + " -e \"show variables\"";
    String cmdSsl12 = "mysql --ssl-mode=REQUIRED -uroot -h" + mysqlHost + " -P" + mysqlPort + " --tls-version=TLSv1.2 -e \"show variables\"";
    // client verifies server certificate
    String cmdv1 = "mysql --ssl-mode=VERIFY_CA --ssl-ca=" + context.config.sslCertificatePath + "/ca.pem -uroot -h" + mysqlHost + " -P" + mysqlPort + " --tls-version=TLSv1.2 -e \"show variables\"";
    
    // two-way ssl auth (client and server both verify their respective certificates)
    String cmdv2 = "mysql --ssl-mode=VERIFY_CA --ssl-ca=" + context.config.sslCertificatePath + "/ca.pem \
                    --ssl-cert=" + context.config.sslCertificatePath + "/client-cert.pem \
                    --ssl-key=" + context.config.sslCertificatePath + "/client-key.pem -uroot -h" + mysqlHost + " -P" + mysqlPort + " --tls-version=TLSv1.2 -e \"show variables\"";

    // The current mysql-client version of the test environment is 5.7.32, which does not support TLSv1.3, so comment this part.
    // String cmdSsl13 = "mysql --ssl-mode=REQUIRED -uroot -h" + mysqlHost + " -P" + mysqlPort +  " --tls-version=TLSv1.3 -e \"show variables\"";
    executeMySQLCommand(cmdDefault);
    executeMySQLCommand(cmdDisabledSsl);
    executeMySQLCommand(cmdSsl12);
    // executeMySQLCommand(cmdSsl13);
    executeMySQLCommand(cmdv1);
    executeMySQLCommand(cmdv2);
}
