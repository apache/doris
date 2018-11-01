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

import org.apache.doris.common.DdlException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Random;

public class PrivilegeTest {

    private static int userNum = 250;
    private static int dbNum = 250;
    private static int tblNum = 3000;

    private static String[] dbs = new String[dbNum];
    private static String[] tbls = new String[tblNum];
    private static String[] users = new String[userNum];

    private static PaloAuth auth = new PaloAuth();
    private static Random random = new Random(System.currentTimeMillis());

    private static Method grantGlobalPrivsM;
    private static Method grantDbPrivsM;
    private static Method grantTblPrivsM;

    static {
        Method[] methods = PaloAuth.class.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getName().equals("grantGlobalPrivs")) {
                method.setAccessible(true);
                grantGlobalPrivsM = method;
            } else if (method.getName().equals("grantDbPrivs")) {
                method.setAccessible(true);
                grantDbPrivsM = method;
            } else if (method.getName().equals("grantTblPrivs")) {
                method.setAccessible(true);
                grantTblPrivsM = method;
            }
        }
    }

    private static String getRandomString(String prefix, int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer(prefix);
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    private static void genNames() {
        for (int i = 0; i < dbNum; i++) {
            dbs[i] = getRandomString("db_", 10);
        }

        for (int i = 0; i < tblNum; i++) {
            tbls[i] = getRandomString("tbl_", 20);
        }

        for (int i = 0; i < userNum; i++) {
            users[i] = getRandomString("user_", 20);
        }
    }

    public static void genAuth() throws DdlException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        long start = System.currentTimeMillis();

        byte[] passwd = new byte[] { 'a', 'b' };

        genNames();

        // global privs
        Object[] params = { "%", "root", passwd, false, false, false, PrivBitSet.of(PaloPrivilege.NODE_PRIV) };
        grantGlobalPrivsM.invoke(auth, params);


        Object[] params2 = { "%", "superuser", passwd, false, false, false,
                PrivBitSet.of(PaloPrivilege.GRANT_PRIV, PaloPrivilege.SELECT_PRIV, PaloPrivilege.LOAD_PRIV,
                                       PaloPrivilege.ALTER_PRIV, PaloPrivilege.CREATE_PRIV, PaloPrivilege.DROP_PRIV) };
        grantGlobalPrivsM.invoke(auth, params2);

        // db privs
        PrivBitSet dbPrivs = PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.LOAD_PRIV,
                                                    PaloPrivilege.ALTER_PRIV, PaloPrivilege.CREATE_PRIV,
                                                    PaloPrivilege.DROP_PRIV);
        for (int i = 0; i < dbs.length; i++) {
            for (int j = 0; j < 2; j++) {
                int idx = Math.abs(random.nextInt()) % userNum;
                Object[] params3 = { "%", dbs[i], users[idx], false, false, false, dbPrivs };
                grantDbPrivsM.invoke(auth, params3);
            }
        }
        
        // tbl privs
        PrivBitSet tblPrivs = PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.LOAD_PRIV);
        for (int i = 0; i < tbls.length; i++) {
            int dbIdx = Math.abs(random.nextInt()) % dbNum;
            int userIdx = Math.abs(random.nextInt()) % userNum;
            int tblIdx = Math.abs(random.nextInt()) % tblNum;

            Object[] params4 = { "%", dbs[dbIdx], users[userIdx], tbls[tblIdx], false, false, false, tblPrivs };
            grantTblPrivsM.invoke(auth, params4);
        }

        // System.out.println("gen auth cost: " + (System.currentTimeMillis() - start));
    }

    private static long randomCheckTablePrivs(PrivPredicate predicate)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        int dbIdx = Math.abs(random.nextInt()) % dbNum;
        int userIdx = Math.abs(random.nextInt()) % userNum;
        int tblIdx = Math.abs(random.nextInt()) % tblNum;
        
        long start = System.nanoTime();
        boolean res = auth.checkTblPriv("192.168.1.1", users[userIdx], dbs[dbIdx], tbls[tblIdx],
                                        predicate);

        if (res) {
            System.out.println(res);
        }
        return System.nanoTime() - start;
        // System.out.println("check auth cost: " + (System.currentTimeMillis() - start));
    }

    public static void main(String[] args) throws DdlException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        genAuth();

        // System.out.println(auth.getUserPrivTable());
        // System.out.println(auth.getDbPrivTable());
        // System.out.println(auth.getTablePrivTable());

        PrivPredicate predicate = PrivPredicate.ADMIN;
        int num = 10000;
        long cost = 0;
        for (int i = 0; i < num; i++) {
            cost += randomCheckTablePrivs(predicate);
        }
        System.out.println("total auth cost: " + cost);
        System.out.println("avg auth cost: " + cost / num);
    }

}
