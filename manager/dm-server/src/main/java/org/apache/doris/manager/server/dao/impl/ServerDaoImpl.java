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
package org.apache.doris.manager.server.dao.impl;

import org.apache.doris.manager.server.constants.AgentStatus;
import org.apache.doris.manager.server.dao.ServerDao;
import org.apache.doris.manager.server.entity.AgentEntity;
import org.apache.doris.manager.server.entity.AgentRoleEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * server dao
 **/
@Repository
public class ServerDaoImpl implements ServerDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public List<AgentEntity> queryAgentNodes(List<String> hosts) {
        String sql = "select id,host,port,status,register_time,last_reported_time" +
                " from t_agent ";
        if (hosts != null && !hosts.isEmpty()) {
            sql = sql + " where host in (%s)";
            String inSql = String.join(",", Collections.nCopies(hosts.size(), "?"));
            sql = String.format(sql, inSql);
        }
        List<AgentEntity> agentEntities = jdbcTemplate.query(
                sql, (resultSet, i) -> {
                    Integer id = resultSet.getInt("id");
                    String ht = resultSet.getString("host");
                    Integer pt = resultSet.getInt("port");
                    String status = resultSet.getString("status");
                    Date registerTime = resultSet.getTimestamp("register_time");
                    Date lastReportedTime = resultSet.getTimestamp("last_reported_time");
                    return new AgentEntity(id, ht, pt, status, registerTime, lastReportedTime);
                }, hosts.toArray());
        return agentEntities;
    }

    @Override
    public AgentEntity agentInfo(String host, Integer port) {
        String sql = "select id,host,port,status,register_time,last_reported_time from t_agent where host = ? and port = ?";
        List<AgentEntity> agents = jdbcTemplate.query(sql, (resultSet, i) -> {
            Integer id = resultSet.getInt("id");
            String ht = resultSet.getString("host");
            Integer pt = resultSet.getInt("port");
            String status = resultSet.getString("status");
            Date registerTime = resultSet.getTimestamp("register_time");
            Date lastReportedTime = resultSet.getTimestamp("last_reported_time");
            return new AgentEntity(id, ht, pt, status, registerTime, lastReportedTime);
        }, host, port);
        if (agents != null && !agents.isEmpty()) {
            return agents.get(0);
        }
        return null;
    }

    @Override
    public int refreshAgentStatus(String host, Integer port) {
        String sql = "update t_agent set last_reported_time = now(),status = ? where host = ? and port = ?";
        return jdbcTemplate.update(sql, AgentStatus.RUNNING.name(), host, port);
    }

    @Override
    public int registerAgent(String host, Integer port) {
        String sql = "insert into t_agent(host,port,status,register_time) values(?,?,?,now())";
        return jdbcTemplate.update(sql, host, port, AgentStatus.RUNNING.name());
    }

    @Override
    public int updateBatchAgentStatus(List<AgentEntity> agents) {
        String sql = "update t_agent set status = ? where id = ?";
        int[] i = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                AgentEntity agent = agents.get(i);
                ps.setString(1, agent.getStatus());
                ps.setInt(2, agent.getId());
            }

            public int getBatchSize() {
                return agents.size();
            }
        });
        return i.length;
    }

    @Override
    public int insertAgentRole(List<AgentRoleEntity> agentRoles) {
        String sql = "insert into t_agent_role(host,role,install_dir) values(?,?,?)";
        int[] i = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                AgentRoleEntity node = agentRoles.get(i);
                ps.setString(1, node.getHost());
                ps.setString(2, node.getRole());
                ps.setString(3, node.getInstallDir());
            }

            public int getBatchSize() {
                return agentRoles.size();
            }
        });
        return i.length;
    }

    @Override
    public String agentRole(String host) {
        String sql = "select host,role,install_dir from t_agent_role where host = ?";
        List<AgentRoleEntity> agentRoles = jdbcTemplate.query(sql, (resultSet, i) -> {
            String ht = resultSet.getString("host");
            String role = resultSet.getString("role");
            String installDir = resultSet.getString("install_dir");
            return new AgentRoleEntity(ht, role, installDir);
        }, host);
        if (agentRoles != null && !agentRoles.isEmpty()) {
            return agentRoles.get(0).getRole();
        }
        return null;
    }

    @Override
    public List<AgentRoleEntity> agentRoles() {
        String sql = "select host,role,install_dir from t_agent_role";
        List<AgentRoleEntity> agentRoles = jdbcTemplate.query(sql, (resultSet, i) -> {
            String ht = resultSet.getString("host");
            String role = resultSet.getString("role");
            String installDir = resultSet.getString("install_dir");
            return new AgentRoleEntity(ht, role, installDir);
        });
        if (agentRoles == null) {
            return new ArrayList<>();
        }
        return agentRoles;
    }
}
