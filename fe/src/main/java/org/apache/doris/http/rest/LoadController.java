package org.apache.doris.http.rest;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.netty.handler.codec.http.HttpHeaders;

@RestController
public class LoadController extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(LoadController.class);

    public static final String SUB_LABEL_NAME_PARAM = "sub_label";

    private ExecuteEnv execEnv = ExecuteEnv.getInstance();

    private boolean isStreamLoad = false;

    @RequestMapping(path = "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_load",method = RequestMethod.PUT)
    public Object load(HttpServletRequest request,
                       HttpServletResponse response, @PathVariable String db, @PathVariable String table)throws DdlException{
        this.isStreamLoad = false;
        executeCheckPassword(request,response);
        return executeWithoutPassword(request,response,db,table);
    }

    @RequestMapping(path =  "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load",method = RequestMethod.PUT)
    public Object streamLoad(HttpServletRequest request,
                           HttpServletResponse response,@PathVariable String db,@PathVariable String table)throws DdlException{
        this.isStreamLoad = true;
        executeCheckPassword(request,response);
       return executeWithoutPassword(request,response,db,table);
    }

    private Object executeWithoutPassword(HttpServletRequest request,
                                       HttpServletResponse response,String db,String table) throws DdlException {
        String dbName = db;
        String tableName = table;
        String urlStr = request.getRequestURI();
        // A 'Load' request must have 100-continue header
        if (request.getHeader(HttpHeaders.Names.EXPECT) == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build("There is no 100-continue header");
        }

        final String clusterName = ConnectContext.get().getClusterName();
        if (Strings.isNullOrEmpty(clusterName)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build("No cluster selected.");
        }


        if (Strings.isNullOrEmpty(dbName)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build("No database selected.");
        }

        if (Strings.isNullOrEmpty(tableName)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build("No table selected.");
        }

        String fullDbName = ClusterNamespace.getFullName(clusterName, dbName);

        String label = request.getParameter(LABEL_KEY);

        if (!isStreamLoad) {
            if (Strings.isNullOrEmpty(label)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build("No label selected.");

            }
        } else {
            label = request.getHeader(LABEL_KEY);
        }

        // check auth
        checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), fullDbName, tableName, PrivPredicate.LOAD);

        if (!isStreamLoad && !Strings.isNullOrEmpty(request.getParameter(SUB_LABEL_NAME_PARAM))) {
            // only multi mini load need to redirect to Master, because only Master has the info of table to
            // the Backend which the file exists.
            try {
                RedirectView redirectView = redirectToMaster(request, response);
                if (redirectView != null) {
                    return redirectView;
                }
            } catch (Exception e){
                e.printStackTrace();
            }

        }

        // Choose a backend sequentially.
        List<Long> backendIds = Catalog.getCurrentSystemInfo().seqChooseBackendIds(1, true, false, clusterName);
        if (backendIds == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build("No backend alive.");
        }

        Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendIds.get(0));
        if (backend == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build("No backend alive.");
        }

        TNetworkAddress redirectAddr = new TNetworkAddress(backend.getHost(), backend.getHttpPort());

        if (!isStreamLoad) {
            String subLabel = request.getParameter(SUB_LABEL_NAME_PARAM);
            if (!Strings.isNullOrEmpty(subLabel)) {
                redirectAddr = execEnv.getMultiLoadMgr().redirectAddr(fullDbName, label, tableName, redirectAddr);
            }
        }

        LOG.info("redirect load action to destination={}, stream: {}, db: {}, tbl: {}, label: {}",
                redirectAddr.toString(), isStreamLoad, dbName, tableName, label);

        RedirectView redirectView  = redirectTo(request,redirectAddr);
        return redirectView;

    }




}
