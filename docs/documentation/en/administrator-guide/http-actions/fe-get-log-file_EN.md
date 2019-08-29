# get\_log\_file

Users can access FE log files through the HTTP interface.

## Log type

The following types of FE logs are supported:

1. fe.audit.log (audit log)

	The audit log records information that has been requested by all request statements for the corresponding FE node. The file naming rules for audit logs are as follows:

    ```
    fe.audit.log                # Current Latest Log
    fe.audit.log.20190603.1     # The audit log of the corresponding date generates a serial suffix when the log size of the corresponding date exceeds 1GB. The smaller the serial number, the newer the content.
    fe.audit.log.20190603.2
    fe.audit.log.20190602.1
    ...
    ```

## Interface examples

1. Get a list of log files of the corresponding type

	Examples:

	`curl -X HEAD -uuser:passwd http://fe_host:http_port/api/get_log_file?type=fe.audit.log`

	Result:

    ```
    HTTP/1.1 200 OK
    file_infos: {"fe.audit.log":24759,"fe.audit.log.20190528.1":132934}
    content-type: text/html
    connection: keep-alive
    ```
	In the returned header, the `file_infos'field displays the list of files in JSON format and the corresponding file size (in bytes)

2. Download log files

	Examples:

    ```
    curl -X GET -uuser:passwd http://fe_host:http_port/api/get_log_file?type=fe.audit.log\&file=fe.audit.log.20190528.1
    ```
	Result:

	Download the specified file as a file.

## Interface description

The interface requires admin privileges.
