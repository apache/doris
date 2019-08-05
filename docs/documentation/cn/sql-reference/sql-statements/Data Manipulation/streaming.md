# STREAM LOAD
## description
    NAME:
        stream-load: load data to table in streaming
        
    SYNOPSIS
        curl --location-trusted -u user:passwd [-H ""...] -T data.file -XPUT http://fe_host:http_port/api/{db}/{table}/_stream_load

    DESCRIPTION
        该语句用于向指定的 table 导入数据，与普通Load区别是，这种导入方式是同步导入。
        这种导入方式仍然能够保证一批导入任务的原子性，要么全部数据导入成功，要么全部失败。
        该操作会同时更新和此 base table 相关的 rollup table 的数据。
        这是一个同步操作，整个数据导入工作完成后返回给用户导入结果。    
        当前支持HTTP chunked与非chunked上传两种方式，对于非chunked方式，必须要有Content-Length来标示上传内容长度，这样能够保证数据的完整性。
        另外，用户最好设置Expect Header字段内容100-continue，这样可以在某些出错场景下避免不必要的数据传输。

    OPTIONS
        用户可以通过HTTP的Header部分来传入导入参数
        
        label: 一次导入的标签，相同标签的数据无法多次导入。用户可以通过指定Label的方式来避免一份数据重复导入的问题。
        当前Palo内部保留30分钟内最近成功的label。
        
        column_separator：用于指定导入文件中的列分隔符，默认为\t。如果是不可见字符，则需要加\x作为前缀，使用十六进制来表示分隔符。
        如hive文件的分隔符\x01，需要指定为-H "column_separator:\x01"
        
        columns：用于指定导入文件中的列和 table 中的列的对应关系。如果源文件中的列正好对应表中的内容，那么是不需要指定这个字段的内容的。
        如果源文件与表schema不对应，那么需要这个字段进行一些数据转换。这里有两种形式column，一种是直接对应导入文件中的字段，直接使用字段名表示；
        一种是衍生列，语法为 `column_name` = expression。举几个例子帮助理解。
        例1: 表中有3个列“c1, c2, c3”，源文件中的三个列一次对应的是"c3,c2,c1"; 那么需要指定-H "columns: c3, c2, c1"
        例2: 表中有3个列“c1, c2, c3", 源文件中前三列依次对应，但是有多余1列；那么需要指定-H "columns: c1, c2, c3, xxx"; 
        最后一个列随意指定个名称占位即可
        例3: 表中有3个列“year, month, day"三个列，源文件中只有一个时间列，为”2018-06-01 01:02:03“格式；
        那么可以指定-H "columns: col, year = year(col), month=month(col), day=day(col)"完成导入
        
        where: 用于抽取部分数据。用户如果有需要将不需要的数据过滤掉，那么可以通过设定这个选项来达到。
        例1: 只导入大于k1列等于20180601的数据，那么可以在导入时候指定-H "where: k1 = 20180601"

        max_filter_ratio：最大容忍可过滤（数据不规范等原因）的数据比例。默认零容忍。数据不规范不包括通过 where 条件过滤掉的行。

        partitions: 用于指定这次导入所设计的partition。如果用户能够确定数据对应的partition，推荐指定该项。不满足这些分区的数据将被过滤掉。
        比如指定导入到p1, p2分区，-H "partitions: p1, p2"

    RETURN VALUES
        导入完成后，会以Json格式返回这次导入的相关内容。当前包括一下字段
        Status: 导入最后的状态。
            Success：表示导入成功，数据已经可见；
            Publish Timeout：表述导入作业已经成功Commit，但是由于某种原因并不能立即可见。用户可以视作已经成功不必重试导入
            Label Already Exists: 表明该Label已经被其他作业占用，可能是导入成功，也可能是正在导入。
            用户需要通过get label state命令来确定后续的操作
            其他：此次导入失败，用户可以指定Label重试此次作业
        Message: 导入状态详细的说明。失败时会返回具体的失败原因。
        NumberTotalRows: 从数据流中读取到的总行数
        NumberLoadedRows: 此次导入的数据行数，只有在Success时有效
        NumberFilteredRows: 此次导入过滤掉的行数，即数据质量不合格的行数
        NumberUnselectedRows: 此次导入，通过 where 条件被过滤掉的行数
        LoadBytes: 此次导入的源文件数据量大小
        LoadTimeMs: 此次导入所用的时间
        ErrorURL: 被过滤数据的具体内容，仅保留前1000条
        
    ERRORS
        可以通过以下语句查看导入错误详细信息：

        SHOW LOAD WARNINGS ON 'url'

        其中 url 为 ErrorURL 给出的 url。
    
## example

    1. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表，使用Label用于去重
        curl --location-trusted -u root -H "label:123" -T testData http://host:port/api/testDb/testTbl/_stream_load
        
    2. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表，使用Label用于去重, 并且只导入k1等于20180601的数据
        curl --location-trusted -u root -H "label:123" -H "where: k1=20180601" -T testData http://host:port/api/testDb/testTbl/_stream_load

    3. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表, 允许20%的错误率（用户是defalut_cluster中的）
        curl --location-trusted -u root -H "label:123" -H "max_filter_ratio:0.2" -T testData http://host:port/api/testDb/testTbl/_stream_load
        
    4. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表, 允许20%的错误率，并且指定文件的列名（用户是defalut_cluster中的）
        curl --location-trusted -u root  -H "label:123" -H "max_filter_ratio:0.2" -H "columns: k2, k1, v1" -T testData http://host:port/api/testDb/testTbl/_stream_load
        
    5. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表中的p1, p2分区, 允许20%的错误率。
        curl --location-trusted -u root  -H "label:123" -H "max_filter_ratio:0.2" -H "partitions: p1, p2" -T testData http://host:port/api/testDb/testTbl/_stream_load

    6. 使用streaming方式导入（用户是defalut_cluster中的）
        seq 1 10 | awk '{OFS="\t"}{print $1, $1 * 10}' | curl --location-trusted -u root -T - http://host:port/api/testDb/testTbl/_stream_load

    7. 导入含有HLL列的表，可以是表中的列或者数据中的列用于生成HLL列
        curl --location-trusted -u root -H "columns: k1, k2, v1=hll_hash(k1)" -T testData http://host:port/api/testDb/testTbl/_stream_load
 
## keyword
    STREAM,LOAD

# GET LABEL STATE
## description
    NAME:
        get_label_state: get label's state
        
    SYNOPSIS
        curl -u user:passwd http://host:port/api/{db}/{label}/_state

    DESCRIPTION
        该命令用于查看一个Label对应的事务状态

    RETURN VALUES
        执行完毕后，会以Json格式返回这次导入的相关内容。当前包括一下字段
        Label：本次导入的 label，如果没有指定，则为一个 uuid。
        Status：此命令是否成功执行，Success表示成功执行
        Message： 具体的执行信息
        State: 只有在Status为Success时才有意义
           UNKNOWN: 没有找到对应的Label
           PREPARE: 对应的事务已经prepare，但尚未提交
           COMMITTED: 事务已经提交，不能被cancel
           VISIBLE: 事务提交，并且数据可见，不能被cancel
           ABORTED: 事务已经被ROLLBACK，导入已经失败。
        
    ERRORS
    
## example

    1. 获得testDb, testLabel的状态
        curl -u root http://host:port/api/testDb/testLabel/_state
 
## keyword
    GET, LABEL, STATE

# CANCEL LABEL
## description
    NAME:
        cancel_label: cancel a transaction with label
        
    SYNOPSIS
        curl -u user:passwd -XPOST http://host:port/api/{db}/{label}/_cancel

    DESCRIPTION
        该命令用于cancel一个指定Label对应的事务，事务在Prepare阶段能够被成功cancel

    RETURN VALUES
        执行完成后，会以Json格式返回这次导入的相关内容。当前包括一下字段
        Status: 是否成功cancel
            Success: 成功cancel事务
            其他: cancel失败
        Message: 具体的失败信息
           
    ERRORS
    
## example

    1. cancel testDb, testLabel的作业
        curl -u root -XPOST http://host:port/api/testDb/testLabel/_cancel
 
## keyword
    CANCEL，LABEL






