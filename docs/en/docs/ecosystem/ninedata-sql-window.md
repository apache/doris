# NineData SQL Console

This article introduces how to efficiently manage and access Doris data sources using the NineData SQL Console. The SQL Console is the simplest and most efficient way to access Doris. With SQL , you can perform the following operations:<br>
1. Log in and access your Doris database. During the SQL Console development process, it supports intelligent suggestions and content, enabling fast navigation and development efficiency.<br>
2. Query and manage Doris data.<br>
3. Conveniently create and modify data tables in Doris.<br>
4. Collaborative development allows efficient collaboration with other members of your team for Doris data querying and modification.<br>
5. Data security: Automatically scan and dynamically anonymize sensitive data in Doris to ensure data security.<br>

## SQL Console Login Address
[https://console.ninedata.cloud/home](https://console.ninedata.cloud/dataQuery/workbench)

## Prerequisites

You have registered a NineData platform account. For more information, see registration of a NineData account.

## Features

NineData's SQL Console provides multiple convenient features to help you easily manage your data sources.<br>

**Function**  | **Description**
------------- | -------------
**SQL Intelligent Prompt** | When entering SQL statements in the SQL Console, the system will automatically prompt keywords, functions, and object information such as database and table field names based on the semantics you enter. In addition, it provides convenient functions such as SQL templates (semantic structure), and syntax help.
**AI Intelligence** | NineData provides AI services that support the following functions: <br>- **SQL Statement Generation:** Quickly convert short natural language expressions into SQL queries.<br>- **SQL Query** Optimization: Help you optimize SQL statements by providing the original SQL statement and indicating the need for optimization.<br>- **Question Answering**: Answer various types of questions for you."""
**Save SQL <br> View SQL** | For frequently used SQL statements, you can save them in NineData. The saved SQL statements can also be viewed. And because the SQL is saved in the cloud, it is not limited by the browser or host, allowing you to view and use the saved SQL anytime, anywhere.
**Workspace Saving** | For scenarios such as unexpected browser exits or sudden crashes, you only need to log in to the NineData console and open the SQL Console again. The previously opened data sources and executed SQL commands will be automatically loaded. Since the page state is saved in the cloud, it is not limited by the browser or host, allowing you to restore your workspace anytime, anywhere.


## Step 1: Add Data Source


* Log in to the NineData console. 
* In the left navigation bar, click on DataSource > DataSource.
* Select the location and environment of the data source as needed, and choose Doris as the database type.


![datasource_en](/images/ninedata/datasource.png)


*  After completing all the configurations, click on "Connection Test" to test if the data source can be accessed properly. If the connection is successful, click on "Create Data Source" to finish adding the data source. Otherwise, please recheck the connection settings until the connection test succeeds.<br>

For more information on adding a data source, see [creation of data sources](https://docs.ninedata.cloud/en/configuration/datasource).

## Step 2: Using the SQL Console
*  Log in to the NineData console. 
*  the left navigation bar, click on SQL Dev > SQL Console.<br>
If you have previously logged in to a data source and haven't closed it, you will automatically enter the page of that data source.
* Click on the text box below the SQL Console. A pop-up will appear with available data sources. Click on the target data source and then click on Start Query to jump to the SQL Console.<br/>
If you have multiple data sources, you can enter complete or partial keywords in the box for precise or fuzzy searching. The searchable fields include:<br>

	1）  Data source name <br>
	2）    IP address

* Once the SQL Console is open, you can perform data management operations on the data source as shown in the image below.

![sql_window_en](/images/ninedata/sql_window_en.png)

* For detailed instructions on using the SQL Console, see appendix of this article.
In SQL Development Enterprise Edition (Organization Mode), if sensitive columns are added to the target data source, you will not be able to view the complete content of those columns. If you need to view them, please apply for sensitive column permissions.

![sentitive_data_en.png](/images/ninedata/sentitive_data_en.png)

Appendix: Introduction to the SQL Console Interface.

![sql_window_extention_en](/images/ninedata/sql_window_extention_en.png)


## Related Links
- [Accessing Doris Data Source through Gateway]()
- [Ninedata Help Documentation](https://docs.ninedata.cloud/en/)