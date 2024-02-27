新增测试case：
1. 在SQL目录下，创建SQL文件sample.sql
2. 生成结果文件，将结果放在result目录下，
   执行python implement.py ./sql/sample.sql ./result/sample.result --gen
3. 将测试添加到execute_test.py中，回归测试时，将执行该测试case

执行测试case：
1. 修改var.sh文件中的fe的host，query Port，web port, user和password，source var.sh使生效
2. 执行python implement.py ./sql/sample.sql ./result/sample.result，查看输出结果
3. 根据数据结果，结合./result/sample.result，定位错误的SQL

执行回归：
source var.sh
nosetests test_execute.py -sv

提示：
1.使用mini load时，将数据放到data目录下，URL中使用参数{FE_HOST}:{HTTP_PORT}
2.不检查导入是否成功，只等待导入结束后，执行查询
3.不支持使用alter等异步请求
4.建议数据量尽量小
5.建议使用脚本生成结果数据
