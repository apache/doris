<!DOCTYPE html>
<html>
<body>
<?php

$host = "192.168.100.80";
$user = "root";
$password = "";
$database = "db_test";
$port = 9030;

# connect to doris
$conn = new mysqli($host, $user, $password, "", $port);
if ($conn->connect_errno) {
	echo "<p> connect to doris failed. " . $conn->connect_errno . "</p>";
}
echo "<p> connect to doris successfully </p>";

# create database
$sql = "CREATE DATABASE IF NOT EXISTS " . $database;
if ($conn->query($sql) === TRUE) {
	echo "<p> create database successfully </p>";
} else {
	echo "<p> create database failed. " . $conn->error . "</p>";
}

# set db context
if ($conn->select_db($database) === TRUE) {
	echo "<p> set db context successfully </p>";
} else {
	echo "<p> set db context failed. " . $conn->error . "</p>";
}

# create table
$sql = "CREATE TABLE IF NOT EXISTS table_test(siteid INT, citycode SMALLINT, pv BIGINT SUM) " .
	"AGGREGATE KEY(siteid, citycode) " .
	"DISTRIBUTED BY HASH(siteid) BUCKETS 10 " .
	"PROPERTIES(\"replication_num\" = \"1\")";
if ($conn->query($sql) === TRUE) {
	echo "<p> create table successfully </p>";
} else {
	echo "<p> create table failed. " . $conn->error . "</p>";
}

# insert data
$sql = "INSERT INTO table_test values(1, 2, 3), (4, 5, 6), (1, 2, 4)";
if ($conn->query($sql) === TRUE) {
	echo "<p> insert data successfully </p>";
} else {
	echo "<p> insert data failed. " . $conn->error . "</p>";
}

# query data
$sql = "SELECT siteid, citycode, pv FROM table_test";
$result = $conn->query($sql);
if ($result) {
	echo "<p> query data successfully </p>";
	echo "<table><tr><th>siteid</th><th>citycode</th><th>pv</th></tr>";
	while($row = $result->fetch_assoc()) {
		echo "<tr><td>" . $row["siteid"] . "</td><td>" . $row["citycode"] . "</td><td>" . $row["pv"] . "</td></tr>";
	}
	echo "</table>";
} else {
	echo "<p> query data failed. " . $conn->error . "</p>";
}

# drop database
$sql = "DROP DATABASE IF EXISTS " . $database;
if ($conn->query($sql) === TRUE) {
	echo "<p> drop database successfully </p>";
} else {
	echo "<p> drop database failed. " . $conn->error . "</p>";
}

# close connection
$conn->close();

?>
</body>
</html>
