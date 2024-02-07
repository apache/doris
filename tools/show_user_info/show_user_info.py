import configparser
from fe_user_resolver import FeUserResolver

"""
@Author : wei.zhang
@Email: zhangweiwhim@gmail.com
@CreateTime : 2024/2/7 14:40
"""

if __name__ == '__main__':
    cf = configparser.ConfigParser()
    cf.read("./conf")
    fe_host = cf.get('cluster', 'fe_host')
    query_port = int(cf.get('cluster', 'query_port'))
    user = cf.get('cluster', 'user')
    query_pwd = cf.get('cluster', 'query_pwd')

    print("============= CONF =============")
    print("fe_host =", fe_host)
    print("fe_query_port =", query_port)
    print("user =", user)
    print("====================================")
    fe_meta = FeUserResolver(fe_host, query_port, user, query_pwd)
    fe_meta.init()
    fe_meta.print_list(False)  # True 显示为table 格式，False 为列表格式
