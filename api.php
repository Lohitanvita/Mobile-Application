<?php

ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);
header("Access-Control-Allow-Origin: *");

$method = $_GET['method'];

$obj = new backend();

if ($method == 'getdata') {
    $obj->getdata();
}  else {
    echo 'why are you here';
}
class backend
{
    public function __construct()
    {
    }

    public function getdata()
    {
        $query = isset($_POST['query']) ? $_POST['query'] : "Select * from cat_id";
        $req = isset($_POST['req']) ? $_POST['req'] : "";
        include 'db_config.php';
        $result = mysqli_query($link, $query);
        if($req == 'GET'){
        while ($row = mysqli_fetch_assoc($result)) {
            $data[] = $row;
        }
        echo json_encode($data);
    } else{
        print_r($result);
    }
    }
}
