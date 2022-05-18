import boto3
import pandas
import pandas as pd
import pyodbc

class S3MySQLABCExport:

    def __init__(self):
        # connecting to the MySQL database
        self.conn = pyodbc.connect(
                            "DRIVER={MySQL ODBC 8.0 Unicode Driver}; "
                            "User=masterUsers6;"
                            "Password=mysqlrds1;"
                            "Server=rds-mysql-project1-dbds.chl55zwgoe4a.us-east-1.rds.amazonaws.com;"
                            "Database=ABC_Retail;"
                            "Port=3306;")

    def read_and_write_abc_data(self, transform=False):
        '''
        # Creating the low level functional client
        client = boto3.client('s3', aws_access_key_id='AKIAYPDYUHAWHTREKHOI',
                              aws_secret_access_key='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',
                              region_name='us-east-1')

        # Fetch the list of existing buckets
        clientResponse = client.list_buckets()

        # Print the bucket names one by one
        print('Printing bucket names...')
        for bucket in clientResponse['Buckets']:
            print(f'Bucket Name: {bucket["Name"]}')

        bucket = 'abcr-etail-data'
        files = client.list_objects_v2(Bucket=bucket)['Contents']
        files_list = [file['Key'] for file in files]

        for f in files_list:
            # Create the S3 object
            obj = client.get_object(Bucket=bucket, Key=f)
            # Read data from the S3 object
            data = pd.read_csv(obj['Body'])
            #prefix = 'Instacart_original/Instacart/' if transform else ''
            if f == 'customer.csv':
                df_customers = pandas.DataFrame(data)
            elif f == 'employee.csv':
                df_employees = pandas.DataFrame(data)
            elif f == 'product.csv':
                df_products = pandas.DataFrame(data)
            elif f == 'orders.csv':
                df_orders = pandas.DataFrame(data)
            elif f == 'order_product.csv':
                df_order_prods = pandas.DataFrame(data)
        '''
        file_path = r"C:\Users\rlohi\Downloads\ABC_Retail_Orders.xlsx"
        df = pd.read_excel(file_path, sheet_name='ABC_Retail_Orders')

        if transform:

            df_customer = df[['Customer_ContactName', 'Customer_Country','CompanyName','Customer_Phone','Customer_City']]
            df_customer.drop_duplicates(inplace = True)
            df_customer.reset_index(inplace= True)
            df_customer.drop(columns=['index'], inplace=True)
            df_customer['CustomerID'] = df_customer.index + 1
            # df_customer.to_excel('C://Users//rlohi//Downloads/customer.xlsx', index=False)

            df_employee = df[['Employee_LastName', 'Employee_FirstName', 'Employee_Title']]
            df_employee.drop_duplicates(inplace=True)
            df_employee.reset_index(inplace=True)
            df_employee.drop(columns=['index'], inplace=True)
            df_employee['EmployeeID'] = df_employee.index + 1
            #df_employee.to_excel('C://Users//rlohi//Downloads/employee.xlsx', index=False)

            df_product = df[['ProductName']]
            df_product.drop_duplicates(inplace=True)
            df_product.reset_index(inplace=True)
            df_product.drop(columns=['index'], inplace=True)
            df_product['ProductID'] = df_product.index + 1
            #df_product.to_excel('C://Users//rlohi//Downloads/product.xlsx', index=False)

            df_order = pd.merge(df_customer, df, how='inner', on='Customer_ContactName')
            df_order = pd.merge(df_employee, df_order, how='inner', on='Employee_FirstName')
            df_order = df_order[['OrderID', 'OrderDate', 'Order_ShippedDate', 'Order_ShipCountry', 'Order_Freight',
                           'Order_ShipCity', 'EmployeeID', 'CustomerID']]
            df_order = df_order.drop_duplicates().sort_values(by=['OrderID'])
            df_order.reset_index(inplace=True)
            # df_order.to_excel('C://Users//rlohi//Downloads/orders.xlsx', index=False)

            df_order_prod = pd.merge(df_product, df, how='inner', on='ProductName')
            df_order_prod = df_order_prod[['Order_Quantity', 'Order_UnitPrice', 'Order_Amount', 'ProductID', 'OrderID']]
            # df_order_prod.to_excel('C://Users//rlohi//Downloads/order_product.xlsx', index=False)

        # self.customer_reader(df_customer)
        # self.employee_reader(df_employee)
        # self.product_reader(df_product)
        # self.orders_reader(df_order)
        self.order_product(df_order_prod)

        self.conn.close()

    def customer_reader(self, data):
        insert_stmt = 'INSERT INTO Customer (CustomerID,Customer_ContactName,Customer_Country,' \
                      'CompanyName,Customer_Phone,Customer_City) VALUES '
        df_customer = pd.DataFrame(data)
        dataset_size = df_customer.shape[0]
        print('Dataset size of Customer', dataset_size)
        start = 0
        batch_size = 250000
        batch_insert_stmt = None
        for row in df_customer.itertuples():
            if not batch_insert_stmt:
                batch_insert_stmt = insert_stmt
            if row.Index >= start and row.Index <= (start + batch_size):
                vals = f'({row.CustomerID}, "{row.Customer_ContactName}", "{row.Customer_Country}", "{row.CompanyName}",' \
                       f'"{row.Customer_Phone}", "{row.Customer_City}"),'
                batch_insert_stmt += vals
            if row.Index == min(start + batch_size, dataset_size - 1):
                batch_insert_stmt = batch_insert_stmt.rsplit(',', 1)[0] + ';'
                cursor = self.conn.cursor()
                print(f'Going to execute customer sql for rows starting {start}')
                cursor.execute(batch_insert_stmt)
                print('Execution finished')
                cursor.commit()
                cursor.close()
                batch_insert_stmt = None
                start += batch_size + 1

    def employee_reader(self, data):
        insert_stmt = 'INSERT INTO Employee (EmployeeID,Employee_LastName, Employee_FirstName, Employee_Title) VALUES '
        df_employee = pd.DataFrame(data)
        dataset_size = df_employee.shape[0]
        print('Dataset size for department', dataset_size)
        start = 0
        batch_size = 250000
        batch_insert_stmt = None
        for row in df_employee.itertuples():
            if not batch_insert_stmt:
                batch_insert_stmt = insert_stmt
            if row.Index >= start and row.Index <= (start + batch_size):
                vals = f'({row.EmployeeID}, "{row.Employee_LastName}", "{row.Employee_FirstName}", "{row.Employee_Title}"),'
                batch_insert_stmt += vals
            # print(row.Index)
            if row.Index == min(start + batch_size, dataset_size - 1):
                batch_insert_stmt = batch_insert_stmt.rsplit(',', 1)[0] + ';'
                cursor = self.conn.cursor()
                print(f'Going to execute employee sql for rows starting {start}')
                cursor.execute(batch_insert_stmt)
                print('Execution finished')
                cursor.commit()
                cursor.close()
                batch_insert_stmt = None
                start += batch_size + 1

    def product_reader(self, data):
        insert_stmt = 'INSERT INTO Product (ProductID,ProductName) VALUES '
        df_product = pd.DataFrame(data)
        dataset_size = df_product.shape[0]
        print('Dataset size for product', dataset_size)
        start = 0
        batch_size = 250000
        batch_insert_stmt = None
        for row in df_product.itertuples():
            if not batch_insert_stmt:
                batch_insert_stmt = insert_stmt
            if row.Index >= start and row.Index <= (start + batch_size):
                vals = f'({row.ProductID}, "{row.ProductName}"),'
                batch_insert_stmt += vals
            if row.Index == min(start + batch_size, dataset_size - 1):
                batch_insert_stmt = batch_insert_stmt.rsplit(',', 1)[0] + ';'
                cursor = self.conn.cursor()
                print(f'Going to execute product sql for rows starting {start}')
                cursor.execute(batch_insert_stmt)
                print('Execution finished')
                cursor.commit()
                cursor.close()
                batch_insert_stmt = None
                start += batch_size + 1

    def orders_reader(self, data):
        insert_stmt = 'INSERT INTO Orders (OrderID, OrderDate, Order_ShippedDate, Order_ShipCountry, ' \
                      'Order_Freight,Order_ShipCity, EmployeeID, CustomerID) VALUES '
        df_order = pd.DataFrame(data)
        dataset_size = df_order.shape[0]
        print('Dataset size for orders', dataset_size)
        start = 0
        batch_size = 250000
        batch_insert_stmt = None
        for row in df_order.itertuples():
            if not batch_insert_stmt:
                batch_insert_stmt = insert_stmt
            if row.Index >= start and row.Index <= (start + batch_size):
                vals = f'({row.OrderID}, "{row.OrderDate}", "{row.Order_ShippedDate}", "{row.Order_ShipCountry}", ' \
                       f'{row.Order_Freight}, "{row.Order_ShipCity}", {row.EmployeeID}, {row.CustomerID}),'
                batch_insert_stmt += vals
            if row.Index == min(start + batch_size, dataset_size - 1):
                batch_insert_stmt = batch_insert_stmt.rsplit(',', 1)[0] + ';'
                cursor = self.conn.cursor()
                print(f'Going to execute orders sql for rows starting {start}')
                cursor.execute(batch_insert_stmt)
                print('Execution finished')
                cursor.commit()
                cursor.close()
                batch_insert_stmt = None
                start += batch_size + 1

    def order_product(self, data):
        insert_stmt = 'INSERT INTO Order_Product (Order_Quantity,Order_UnitPrice,Order_Amount,ProductID,OrderID) VALUES '
        df_order_prod = pd.DataFrame(data)
        dataset_size = df_order_prod.shape[0]
        print('Dataset size for order product', dataset_size)
        start = 0
        batch_size = 2500
        batch_insert_stmt = None
        for row in df_order_prod.itertuples():
            if not batch_insert_stmt:
                batch_insert_stmt = insert_stmt
            if row.Index >= start and row.Index <= (start + batch_size):
                vals = f'({row.Order_Quantity}, {row.Order_UnitPrice}, {row.Order_Amount},' \
                       f' {row.ProductID},{row.OrderID}),'
                batch_insert_stmt += vals
            if row.Index == min(start + batch_size, dataset_size - 1):
                batch_insert_stmt = batch_insert_stmt.rsplit(',', 1)[0] + ';'
                cursor = self.conn.cursor()
                print(f'Going to execute order product sql for rows starting {start}')
                cursor.execute(batch_insert_stmt)
                print('Execution finished')
                cursor.commit()
                cursor.close()
                batch_insert_stmt = None
                start += batch_size + 1




if __name__ == '__main__':
    transform = True
    s3_export = S3MySQLABCExport()
    s3_export.read_and_write_abc_data(transform)
