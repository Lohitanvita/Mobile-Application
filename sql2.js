var formdata = new FormData();
formdata.append("req", "GET");
formdata.append("query", "select * from Product");

var requestOptions = {
  method: 'POST',
  body: formdata,
  redirect: 'follow'
};

fetch("http://54.89.100.188/DBDS_Team/api.php?method=getdata", requestOptions)
  .then(response => response.text())
  .then(result => console.log(result))
  .catch(error => console.log('error', error));