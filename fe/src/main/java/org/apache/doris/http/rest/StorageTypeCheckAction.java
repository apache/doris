package org.apache.doris.http.rest;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.thrift.TStorageType;

import com.google.common.base.Strings;

import org.json.JSONObject;

import java.util.List;
import java.util.Map;

import io.netty.handler.codec.http.HttpMethod;

public class StorageTypeCheckAction extends RestBaseAction {
    public StorageTypeCheckAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        StorageTypeCheckAction action = new StorageTypeCheckAction(controller);
        controller.registerHandler(HttpMethod.GET, "/api/_check_storagetype", action);
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) throws DdlException {
        AuthorizationInfo authInfo = getAuthorizationInfo(request);
        checkGlobalAuth(authInfo, PrivPredicate.ADMIN);

        String dbName = request.getSingleParameter("db");
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("Parameter db is missing");
        }

        String fullDbName = ClusterNamespace.getFullName(authInfo.cluster, dbName);
        Database db = catalog.getDb(fullDbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        JSONObject root = new JSONObject();
        db.readLock();
        try {
            List<Table> tbls = db.getTables();
            for (Table tbl : tbls) {
                if (tbl.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTbl = (OlapTable) tbl;
                JSONObject indexObj = new JSONObject();
                for (Map.Entry<Long, TStorageType> entry : olapTbl.getIndexIdToStorageType().entrySet()) {
                    if (entry.getValue() == TStorageType.ROW) {
                        String idxName = olapTbl.getIndexNameById(entry.getKey());
                        indexObj.put(idxName, entry.getValue().name());
                    }
                }
                root.put(tbl.getName(), indexObj);
            }
        } finally {
            db.readUnlock();
        }

        // to json response
        String result = root.toString();

        // send result
        response.setContentType("application/json");
        response.getContent().append(result);
        sendResult(request, response);
    }
}
