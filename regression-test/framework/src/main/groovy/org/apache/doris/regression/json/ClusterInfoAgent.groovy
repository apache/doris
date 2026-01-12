package org.apache.doris.regression.json

import com.google.gson.Gson
import com.google.gson.annotations.SerializedName

import java.lang.reflect.Field

class ClusterInfoAgent {
    static ClusterInfoAgent LoadFromFile(String path) {
        Gson gson = new Gson();
        ClusterInfoAgent info = gson.fromJson(new FileReader(path), ClusterInfoAgent.class);

        if (info.prometheus == null) {
            // by convention prometheus is deployed on the same node as the master FE
            Prometheus prom = new Prometheus();
            for (FeNode node : info.fe.node) {
                if (node.isMaster) {
                    prom.BaseURL = "http://${node.ip}:9090";
                    info.prometheus = prom;
                    break;
                }
            }
        }

        return info;
    }

    List<DorisNode> AllNodes() {
        List<DorisNode> nodes = new ArrayList();
        nodes.addAll(this.fe.node);
        if (this.metaService) {
            nodes.addAll(this.metaService.node)
        }

        if (this.recycler) {
            nodes.addAll(this.recycler.node)
        }
        this.be.cluster.each { cluster ->
            nodes.addAll(cluster.node);
        }
        return nodes;
    }

    Set<String> collectAllModuleIPs() {
        Set<String> ips = new LinkedHashSet<>()

        this.fe.node.each { n ->
            if (n?.ip) {
                ips.add(n.ip as String)
            }
        }

        this.be.cluster.each { c ->
            c.node.each { n ->
                if (n?.ip) {
                    ips.add(n.ip as String)
                }
            }
        }

        this.metaService?.node?.each { n ->
            if (n?.ip) {
                ips.add(n.ip as String)
            }
        }

        this.recycler?.node?.each { n ->
            if (n?.ip) {
                ips.add(n.ip as String)
            }
        }

        return ips
    }

    // Randomly get N nodes of specified types
    // nodeType: node type fe|be|metaService|recycler (must match the variable name defined below)
    // num: number of nodes to get (default 1)
    // cluster: when getting BE info, cluster can be specified (default 0)
    List<DorisNode> RandomNodeByType(ArrayList<String> nodeTypes, int num = 1, int cluster = 0) throws NoSuchFieldException, IllegalAccessException {
        List<DorisNode> nodes = new ArrayList()

        nodeTypes.each { nodeType ->
            Field field = ClusterInfoAgent.class.getDeclaredField(nodeType)
            field.setAccessible(true)
            def service = field.get(this)
            def node = (nodeType == "be") ? service.cluster[cluster].node : service.node
            nodes.addAll(node)
        }

        List<DorisNode> ret = new ArrayList()
        def random = new Random()
        Set<Integer> uniqueNumbers = new HashSet<>()

        // Generate N different random numbers
        if (num > nodes.size()) {
            // If num is greater than the number of nodes in the cluster, use all nodes
            uniqueNumbers.addAll(0..(nodes.size()-1))
        } else {
            while (uniqueNumbers.size() < num) {
                int randomNumber = random.nextInt(nodes.size())
                uniqueNumbers.add(randomNumber)
            }
        }

        for (Integer index : uniqueNumbers) {
            ret.add(nodes.get(index))
        }

        return ret
    }


    // Randomly get N nodes from all nodes
    // num: number of nodes to get (default 1)
    // include_fdb: whether to include FDB node information
    List<DorisNode> RandomNode(int num = 1, boolean include_fdb = false) throws Exception {
        List<DorisNode> ret = new ArrayList()
        def nodes = this.AllNodes()
        if (include_fdb) {
            nodes.addAll(this.fdb.node)
        }
        if (num > nodes.size()) {
            throw new Exception("Num:${num} is greater than the number of all nodes:${nodes.size()} ")
        }

        // Generate random numbers
        Random random = new Random()
        // Use Set to store different random numbers
        Set<Integer> uniqueNumbers = new HashSet<>();

        // Generate N different random numbers
        while (uniqueNumbers.size() < num) {
            int randomNumber = random.nextInt(nodes.size())
            uniqueNumbers.add(randomNumber)
        }

        for (Integer index : uniqueNumbers) {
            ret.add(nodes.get(index))
        }

        return ret
    }

    String JAVAHome() {
        return this.javaHome
    }


    @SerializedName("java_home")
    String javaHome
    @SerializedName("fe")
    Fe fe
    @SerializedName("be")
    Be be
    @SerializedName("prometheus")
    Prometheus prometheus
    @SerializedName("meta_service")
    MetaService metaService
    @SerializedName("recycler")
    Recycler recycler
    @SerializedName("fdb")
    FDB fdb


    class Fe {
        @SerializedName("http_port")
        String httpPort;
        @SerializedName("rpc_port")
        String rpcPort;
        @SerializedName("query_port")
        String queryPort;
        @SerializedName("edit_log_port")
        String editLogPort;
        @SerializedName("cloud_http_port")
        String cloudHTTPPort;
        @SerializedName("cloud_unique_id")
        String cloudUniqueId;
        @SerializedName("node")
        List<FeNode> node;
    }

    class Be {
        @SerializedName("cluster")
        List<BeCluster> cluster;

        class BeCluster {
            @SerializedName("be_port")
            String bePort;
            @SerializedName("brpc_port")
            String brpcPort;
            @SerializedName("webserver_port")
            String webserverPort;
            @SerializedName("heartbeat_service_port")
            String heartbeatServicePort;
            @SerializedName("cloud_unique_id")
            String cloudUniqueId;
            @SerializedName("cluster_name")
            String clusterName;
            @SerializedName("cluster_id")
            String clusterId;
            @SerializedName("node")
            List<BeNode> node;
        }
    }


    class MetaService {
        @SerializedName("brpc_listen_port")
        String brpc_port

        @SerializedName("node")
        List<MetaServiceNode> node
    }


    class Recycler {
        @SerializedName("brpc_listen_port")
        String brpc_port

        @SerializedName("node")
        List<MetaServiceNode> node;
    }

    class FDB {
        @SerializedName("node")
        List<FDBNode> node;
    }

    static class Prometheus {
        @SerializedName("base_url")
        String BaseURL;
        @SerializedName("cluster_name")
        String ClusterName;
    }
}

final class FeNode extends DorisNode {
    @SerializedName("is_master")
    boolean isMaster;

    String nodeType() {
        return "fe";
    }

    String processKey() {
        return "org.apache.doris.DorisFE"
    }

    String pidFilePath() {
        return "bin/fe.pid"
    }
}

final class BeNode extends DorisNode {
    String nodeType() {
        return "be";
    }

    String processKey() {
        return "doris_be"
    }

    String pidFilePath() {
        return "bin/be.pid"
    }
}

final class MetaServiceNode extends DorisNode {
    String nodeType() {
        return "meta_service";
    }

    String processKey() {
        return "meta-service"
    }

    String pidFilePath() {
        return "bin/*_cloud.pid"
    }
}

final class RecyclerNode extends DorisNode {
    String nodeType() {
        return "recycler"
    }

    String processKey() {
        return "recycler"
    }

    String pidFilePath() {
        return "bin/*_cloud.pid"
    }
}


final class FDBNode extends DorisNode {
    String nodeType() {
        return "FDB"
    }

    String processKey() {
        return "fdbserver"
    }

    String pidFilePath() {
        return ""
    }
}

final class OtherNode extends DorisNode {
    OtherNode(String ip) {
        this.ip = ip
        this.installPath = "/opt/doris"
    }

    @Override
    String nodeType() {
        return "other"
    }

    @Override
    String processKey() {
        return "other"
    }

    @Override
    String pidFilePath() {
        return ""
    }
}


abstract class DorisNode {
    @SerializedName("ip")
    String ip
    @SerializedName("install_path")
    String installPath

    @SerializedName("disks")
    List<Disk> disks

    // Disk information for disk-related fault injection
    // lsblk -a | grep disk
    // name: vdb num:252:16 path:/mnt/hdd01
    class Disk {
        @SerializedName("name")
        String name
        @SerializedName("num")
        String num
        @SerializedName("path")
        String path
    }

    abstract String nodeType();

    // Process key information used by ps -ef | grep [key]
    // Better approach: get pid from ssh
    abstract String processKey();

    // Path of pid file relative to installPath
    abstract String pidFilePath();
}