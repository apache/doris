# get\_log\_file

To get FE log via HTTP

## Types of FE log

1. fe.audit.log (Audit log)

    The audit log records the all statements executed. Audit log's name format as follow:

    ```
    fe.audit.log                # The latest audit log
    fe.audit.log.20190603.1     # The historical audit log. The smaller the sequence number, the newer the log.
    fe.audit.log.20190603.2
    fe.audit.log.20190602.1
    ...
    ```

## Example

1. Get the list of specified type of logs

    Example
    
    `curl -X HEAD -uuser:passwd http://fe_host:http_port/api/get_log_file?type=fe.audit.log`
    
    Returns:
    
    ```
    HTTP/1.1 200 OK
    file_infos: {"fe.audit.log":24759,"fe.audit.log.20190528.1":132934}
    content-type: text/html
    connection: keep-alive
    ```
    
    In the header of result, the `file_infos` section saves the file list and file size in JSON format.
    
2. Download files

    Example:
    
    ```
    curl -X GET -uuser:passwd http://fe_host:http_port/api/get_log_file?type=fe.audit.log\&file=fe.audit.log.20190528.1
    ```

## Notification

Need ADMIN priviledge.
