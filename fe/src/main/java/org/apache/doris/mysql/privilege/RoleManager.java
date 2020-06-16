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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.PaloAuth.PrivLevel;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RoleManager implements Writable {
    private Map<String, PaloRole> roles = Maps.newHashMap();

    public RoleManager() {
        roles.put(PaloRole.OPERATOR.getRoleName(), PaloRole.OPERATOR);
        roles.put(PaloRole.ADMIN.getRoleName(), PaloRole.ADMIN);
    }

    public PaloRole getRole(String role) {
        return roles.get(role);
    }

    public PaloRole addRole(PaloRole newRole, boolean errOnExist) throws DdlException {
        PaloRole existingRole = roles.get(newRole.getRoleName());
        if (existingRole != null) {
            if (errOnExist) {
                throw new DdlException("Role " + newRole + " already exists");
            }
            // merge
            existingRole.merge(newRole);
            return existingRole;
        } else {
            roles.put(newRole.getRoleName(), newRole);
            return newRole;
        }
    }

    public void dropRole(String qualifiedRole, boolean errOnNonExist) throws DdlException {
        if (!roles.containsKey(qualifiedRole)) {
            if (errOnNonExist) {
                throw new DdlException("Role " + qualifiedRole + " does not exist");
            }
            return;
        }

        // we just remove the role from this map and remain others unchanged(privs, etc..)
        roles.remove(qualifiedRole);
    }

    public PaloRole revokePrivs(String role, TablePattern tblPattern, PrivBitSet privs, boolean errOnNonExist)
            throws DdlException {
        PaloRole existingRole = roles.get(role);
        if (existingRole == null) {
            if (errOnNonExist) {
                throw new DdlException("Role " + role + " does not exist");
            }
            return null;
        }

        Map<TablePattern, PrivBitSet> map = existingRole.getTblPatternToPrivs();
        PrivBitSet existingPriv = map.get(tblPattern);
        if (existingPriv == null) {
            if (errOnNonExist) {
                throw new DdlException(tblPattern + " does not exist in role " + role);
            }
            return null;
        }

        existingPriv.remove(privs);
        return existingRole;
    }

    public PaloRole revokePrivs(String role, ResourcePattern resourcePattern, PrivBitSet privs, boolean errOnNonExist)
            throws DdlException {
        PaloRole existingRole = roles.get(role);
        if (existingRole == null) {
            if (errOnNonExist) {
                throw new DdlException("Role " + role + " does not exist");
            }
            return null;
        }

        Map<ResourcePattern, PrivBitSet> map = existingRole.getResourcePatternToPrivs();
        PrivBitSet existingPriv = map.get(resourcePattern);
        if (existingPriv == null) {
            if (errOnNonExist) {
                throw new DdlException(resourcePattern + " does not exist in role " + role);
            }
            return null;
        }

        existingPriv.remove(privs);
        return existingRole;
    }

    public void dropUser(UserIdentity userIdentity) {
        for (PaloRole role : roles.values()) {
            role.dropUser(userIdentity);
        }
    }

    public void getRoleInfo(List<List<String>> results) {
        for (PaloRole role : roles.values()) {
            List<String> info = Lists.newArrayList();
            info.add(role.getRoleName());
            info.add(Joiner.on(", ").join(role.getUsers()));
            
            // global
            boolean hasGlobal = false;
            for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
                if (entry.getKey().getPrivLevel() == PrivLevel.GLOBAL) {
                    hasGlobal = true;
                    info.add(entry.getValue().toString());
                    // global priv should only has one
                    break;
                }
            }
            if (!hasGlobal) {
                info.add(FeConstants.null_string);
            }

            // db
            List<String> tmp = Lists.newArrayList();
            for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
                if (entry.getKey().getPrivLevel() == PrivLevel.DATABASE) {
                    tmp.add(entry.getKey().toString() + ": " + entry.getValue().toString());
                }
            }
            if (tmp.isEmpty()) {
                info.add(FeConstants.null_string);
            } else {
                info.add(Joiner.on("; ").join(tmp));
            }
            
            
            // tbl
            tmp.clear();
            for (Map.Entry<TablePattern, PrivBitSet> entry : role.getTblPatternToPrivs().entrySet()) {
                if (entry.getKey().getPrivLevel() == PrivLevel.TABLE) {
                    tmp.add(entry.getKey().toString() + ": " + entry.getValue().toString());
                }
            }
            if (tmp.isEmpty()) {
                info.add(FeConstants.null_string);
            } else {
                info.add(Joiner.on("; ").join(tmp));
            }

            // resource
            tmp.clear();
            for (Map.Entry<ResourcePattern, PrivBitSet> entry : role.getResourcePatternToPrivs().entrySet()) {
                if (entry.getKey().getPrivLevel() == PrivLevel.RESOURCE) {
                    tmp.add(entry.getKey().toString() + ": " + entry.getValue().toString());
                }
            }
            if (tmp.isEmpty()) {
                info.add(FeConstants.null_string);
            } else {
                info.add(Joiner.on("; ").join(tmp));
            }

            results.add(info);
        }
    }

    public static RoleManager read(DataInput in) throws IOException {
        RoleManager roleManager = new RoleManager();
        roleManager.readFields(in);
        return roleManager;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // minus 2 to ignore ADMIN and OPERATOR role
        out.writeInt(roles.size() - 2);
        for (PaloRole role : roles.values()) {
            if (role == PaloRole.ADMIN || role == PaloRole.OPERATOR) {
                continue;
            }
            role.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            PaloRole role = PaloRole.read(in);
            roles.put(role.getRoleName(), role);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Roles: ");
        for (PaloRole role : roles.values()) {
            sb.append(role).append("\n");
        }
        return sb.toString();
    }
}
