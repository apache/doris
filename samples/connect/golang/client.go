package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	host := "127.0.0.1"
	port := 9030
	user := "root"
	password := ""
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port)

	//connect to doris
	driver, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("open mysql driver failed, error[%v]\n", err)
		return
	}
	if err := driver.Ping(); err != nil {
		fmt.Printf("ping doris failed, error[%v]\n", err)
		return
	}
	fmt.Printf("connect to doris successfully\n")

	//create database
	if _, err := driver.Exec("CREATE DATABASE IF NOT EXISTS  db_test"); err != nil {
		fmt.Printf("create database failed, error[%v]\n", err)
		return
	}
	fmt.Printf("create database successfully\n")

	//set db context
	if _, err := driver.Exec("USE db_test"); err != nil {
		fmt.Printf("set db context failed, error[%v]\n", err)
		return
	}
	fmt.Printf("set db context successfully\n")

	//create table
	SQL := "CREATE TABLE IF NOT EXISTS table_test(siteid INT, citycode SMALLINT, pv BIGINT SUM) " +
		"AGGREGATE KEY(siteid, citycode) " +
		"DISTRIBUTED BY HASH(siteid) BUCKETS 10 " +
		"PROPERTIES(\"replication_num\" = \"1\")"
	if _, err := driver.Exec(SQL); err != nil {
		fmt.Printf("create table failed, error[%v]\n", err)
		return
	}
	fmt.Printf("create table successfully\n")

	//insert data
	SQL = "INSERT INTO table_test values(1, 2, 3), (4, 5, 6), (1, 2, 4)"
	if _, err := driver.Exec(SQL); err != nil {
		fmt.Printf("insert data failed, error[%v]\n", err)
		return
	}
	fmt.Printf("insert data successfully\n")

	//query data
	rows, err := driver.Query("SELECT * FROM table_test")
	if err != nil {
		fmt.Printf("query data from doris failed, error[%v]\n", err)
		return
	}
	for rows.Next() {
		var siteId, cityCode int
		var pv int64
		if err := rows.Scan(&siteId, &cityCode, &pv); err != nil {
			fmt.Printf("scan columns failed, error[%v]\n", err)
			return
		}
		fmt.Printf("%d\t%d\t%d\n", siteId, cityCode, pv)
	}
	fmt.Printf("query data successfully\n")

	//drop database
	if _, err := driver.Exec("DROP DATABASE IF EXISTS db_test"); err != nil {
		fmt.Printf("drop database failed, error[%v]\n", err)
		return
	}
	fmt.Printf("drop database successfully\n")
}
