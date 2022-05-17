document.getElementById("mySQLButton").onclick = function() {getCityData()};
var formdata = new FormData();

// function mySQLFunction() {
//   console.log("You clicked mySQL button");
//   var SQLinput = document.getElementById("SQLQuery").value;
//   console.log("SQL input ",SQLinput);
//   formdata.append("req", "GET");
//   formdata.append("query", SQLinput);

//   var requestOptions = {
//     method: 'POST',
//     body: formdata,
//     redirect: 'follow'
//   };
  

// fetch("http://100.25.124.235/api.php?method=getdata", requestOptions)
//     .then(response => response.text())
//     .then(result => console.log(result))
//     .catch(error => console.log('error', error));

//     console.log("result from ",result);
// }

function getCityData() {

  console.log("You clicked my city data");
  var SQLinput = document.getElementById("SQLQuery").value;
  console.log("SQL input ",SQLinput);
  formdata.append("req", "GET");
  formdata.append("query", SQLinput);

    $.ajax({
        url: "http://100.25.124.235/api.php?method=getdata",
        method: "POST",
        data: {
            req: 'get',
            query: SQLinput,
        },
        datatype: "json",
        success: function (response) {
            console.log(response);
            let data = JSON.parse(response);
            console.log(data);
            $.each(data, function (city, city) {
                console.log(city.product_id);
                
            });
        },
        error: function (response) {
            // console.log(response);
        },
    });
    /*  }*/
}










