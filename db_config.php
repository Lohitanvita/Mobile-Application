<?php

$servername = "rds-mysql-project1-dbds.chl55zwgoe4a.us-east-1.rds.amazonaws.com";
$username = "masterUsers6";
$password = "mysqlrds1";
$dbname = "rdsMysql";
$port = "3306";


// Create connection
$link = mysqli_connect($servername, $username, $password, $dbname);

mysqli_set_charset($link,"utf8");
?>