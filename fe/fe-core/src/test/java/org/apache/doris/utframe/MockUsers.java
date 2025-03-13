package org.apache.doris.utframe;

import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class MockUsers {
    public static UserIdentity getUserWithNoAllPrivilege() throws UserException {
        UserIdentity user = new UserIdentity("user_with_no_all_privilege", "%");
        TablePattern tablePattern = new TablePattern("*", "*", "*");
        List<AccessPrivilegeWithCols> privileges = ImmutableList.of();
        createUser(user, tablePattern, privileges);
        return user;
    }

    private static void createUser(UserIdentity userIdentity, TablePattern tablePattern,
            List<AccessPrivilegeWithCols> privileges) throws UserException {
        userIdentity.analyze();
        tablePattern.analyze();
        CreateUserStmt createUserStmt = new CreateUserStmt(new UserDesc(userIdentity));
        Env.getCurrentEnv().getAuth().createUser(createUserStmt);
        GrantStmt grantStmt = new GrantStmt(userIdentity, null, tablePattern, privileges);
        Env.getCurrentEnv().getAuth().grant(grantStmt);
    }

    public static void changeUser(UserIdentity user) {
        ConnectContext.get().setCurrentUserIdentity(user);
    }
}
