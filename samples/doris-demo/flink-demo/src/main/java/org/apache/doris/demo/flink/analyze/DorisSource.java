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
package org.apache.doris.demo.flink.analyze;

import org.apache.doris.demo.flink.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * custom flink jdbc source
 */
public class DorisSource extends RichSourceFunction<User> {
    private static final Logger logger = LoggerFactory.getLogger(DorisSource.class);

    private Connection connection = null;
    private PreparedStatement ps = null;
    private Properties properties;


    public DorisSource(Properties properties){
        this.properties=properties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection(properties.getProperty("url"),
                    properties.getProperty("username"),
                    properties.getProperty("password"));
        ps = connection.prepareStatement(properties.getProperty("sql"));
    }


    @Override
    public void run(SourceContext<User> sourceContext) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                User user = new User();
                user.setName(resultSet.getString("name"));
                user.setAge(resultSet.getInt("age"));
                user.setPrice(resultSet.getString("price"));
                user.setSale(resultSet.getString("sale"));
                sourceContext.collect(user);
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        } finally {
            connection.close();
        }

    }

    @Override
    public void cancel() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}


