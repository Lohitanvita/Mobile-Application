
**Database for Data Science Android Application Development**
The Android Mobile application retrievs data from AWS MySQL and Redshift containing two databases named Instacart, ABC Retails on to the front end by SQL commands given on the UI frontend. There are radio buttons to choose between DBMS, and dropdown to choose databses. A text box to run SQL query and Run, Reset buttons.

**Project Development**
This Android Application involves AWS services like EC2, RDS MySQL, Redshift, S3 bucket, Android Studio, PHP, Python (pyodbc), Kotlin
Intially using ERDPlus two ER Diagrams and their Relational diagrams are made for two different databases (Instacart, ABC Retails).
Then using those diagrams by python to_csv module the data files are generated and uploaded to S3 bucket in AWS.
Later using pyodbc connection and pandas, the files from S3 are uploaded to MySQl database.
With the help of copy function the data is uploaded to Redshift from S3.
The frontend UI is created using HTML, Bootstrap, CSS, Jquery, Javascript.
The backend connection from UI to Database is given using PHP APIs.
Finally using AWS EC2 the app is hosted and an apk is developed that runs the app in any android mobile phones.
