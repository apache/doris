# 查看正在执行的SQL功能设计

当前Palo在线上出现压力过大查询卡住时，难以定位当时是哪些查询导致集群压力过大，为了能在发生问题时，快速的定位并找到集群正在执行的大查询，所以需要提供查看正在执行的SQL的功能。

## 用户接口

### 1、查看正在执行的查询

	show proc "/current_queries"
	
![Mou_icon](../resources/current_queries.png)
	
### 2、查看某个查询的具体SQL

	show proc "/current_queries/{query_id}"
	
![Mou_icon](../resources/current_queries_id.png)
	
###	3、查看某个查询的FRAGMENT的执行情况

	show proc "/current_queries/{query_id}/fragments"
	
![Mou_icon](../resources/current_queries_fragments.png)

FRAGMENT ID与EXPLAIN里面的FRAGMENT ID是一一对应的，方便查看查询卡到哪一步。

### 4、查看所有BE上正在执行的INSTANCE

	show proc "/current_backend_instances"
	
![Mou_icon](../resources/current_backend_instances.png)

	
	
## 详细设计

为了可以实现上述的功能，牵扯了三个方面：

*	查询信息的统计
*	BE执行状态的引入
*	查询的展示

查询的展示不做过多的说明，即通过实现ProcDirInterface接口然后注册到ProcService来实现4种接口的schema展示。

### 查询信息的统计

为了统计查询，在SQL下发执行时取到SQL、FRAGMENT的所有信息，这里需要有一个全局的查询信息统计的结构，重写了一下现有的QeProcessor作为一个接口：
	
	public interface QeProcessor {

   	 	 TReportExecStatusResult reportExecStatus(TReportExecStatusParams params);

    	 void registerQuery(TUniqueId queryId, Coordinator coord) throws InternalException;

   		 void registerQuery(TUniqueId queryId, QeProcessorImpl.QueryInfo info) throws InternalException;

   		 void unregisterQuery(TUniqueId queryId);

    	 Map<String, QueryStatisticsItem> getQueryStatistics();
	}

通过registerQuery来注册查询，unregisterQuery来反注册查询，然后proc下的各个实现都调用getQueryStatistics来获取当前查询的信息，对于接口3需要查询SQL的INSTANCE在BE上的执行情况，所以对于BE的执行这里引入了两种状态。

*	running : 正在进行计算
*	wait：等待数据的状态，对于EXCHANGE以及SCANNODE来讲读取数据的时候需要设置为此状态。
