import boto3
import pandas
import pandas as pd
import pyodbc

class S3MySQLExport:

    def __init__(self):
        # connecting to the MySQL database
        self.conn = pyodbc.connect(
                            "DRIVER={MySQL ODBC 8.0 Unicode Driver}; "
                            "User=masterUsers6;"
                            "Password=mysqlrds1;"
                            "Server=rds-mysql-project1-dbds.chl55zwgoe4a.us-east-1.rds.amazonaws.com;"
                            "Database=Instacart;"
                            "Port=3306;")

    def read_and_write_data(self, transform=False):
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

        bucket = 'project-team5'
        files = client.list_objects_v2(Bucket=bucket)['Contents']
        files_list = [file['Key'] for file in files]

        for f in files_list:
            # Create the S3 object
            obj = client.get_object(Bucket=bucket, Key=f)
            # Read data from the S3 object
            data = pd.read_csv(obj['Body'])
            prefix = 'Instacart_original/Instacart/' if transform else ''
            if f == f'{prefix}aisles.csv':
                df_aisle = pandas.DataFrame(data)
            elif f == f'{prefix}departments.csv':
                df_dept = pandas.DataFrame(data)
            elif f == f'{prefix}products.csv':
                df_prod = pandas.DataFrame(data)
            elif f == f'{prefix}orders.csv':
                df_order = pandas.DataFrame(data)
            elif f == f'{prefix}order_products.csv':
                df_order_prod = pandas.DataFrame(data)
            elif f == 'users.csv':
                df_user = pandas.DataFrame(data)

        if transform:
            # aisle.csv
            df_aisle_dept = pd.merge(df_prod, df_aisle, how='inner', on='aisle_id')
            df_aisle_dept = df_aisle_dept[['aisle_id', 'aisle', 'department_id']]
            df_aisle_dept = df_aisle_dept.drop_duplicates().sort_values(by=['aisle_id'])
            df_aisle_dept.to_csv('C://Users//rlohi//Downloads/transformed/aisles.csv', encoding='utf-8',
                                 index=False)

            # products
            df_product = df_prod[['product_id', 'product_name', 'aisle_id']]
            df_product.to_csv('C://Users//rlohi//Downloads/transformed/products.csv', encoding='utf-8',
                              index=False)

            # users
            df_users = df_order[['user_id', 'order_id']]
            df_users.to_csv('C://Users//rlohi//Downloads/transformed/users.csv', encoding='utf-8', index=False)

            # orders
            df_order = df_order[
                ['order_id', 'order_dow', 'order_number', 'order_hour_of_day', 'days_since_prior_order']]
            df_order.to_csv('C://Users//rlohi//Downloads/transformed/orders.csv', encoding='utf-8', index=False)

        # self.department_reader(df_dept)
        # self.aisles_reader(df_aisle)
        # self.product_reader(df_prod)
        # self.orders_reader(df_order)
        # self.order_product(df_order_prod)
        # self.users_reader(df_user)

        self.conn.close()


    def aisles_reader(self, data):
        insert_stmt = 'INSERT INTO Aisle (aisle_id, aisle, department_id) VALUES '
        df_aisle = pd.DataFrame(data)
        dataset_size = df_aisle.shape[0]
        print('Dataset size of Aisle', dataset_size)
        start = 0
        batch_size = 250000
        batch_insert_stmt = None
        for row in df_aisle.itertuples():
            if not batch_insert_stmt:
                batch_insert_stmt = insert_stmt
            if row.Index >= start and row.Index <= (start + batch_size):
                vals = f'({row.aisle_id}, "{row.aisle}", {row.department_id}),'
                batch_insert_stmt += vals
            if row.Index == min(start + batch_size, dataset_size - 1):
                batch_insert_stmt = batch_insert_stmt.rsplit(',', 1)[0] + ';'
                cursor = self.conn.cursor()
                print(f'Going to execute aisle sql for rows starting {start}')
                cursor.execute(batch_insert_stmt)
                print('Execution finished')
                cursor.commit()
                cursor.close()
                batch_insert_stmt = None
                start += batch_size + 1

    def department_reader(self, data):
        insert_stmt = 'INSERT INTO Department (department_id, department) VALUES '
        df_dept = pd.DataFrame(data)
        dataset_size = df_dept.shape[0]
        print('Dataset size for department', dataset_size)
        start = 0
        batch_size = 250000
        batch_insert_stmt = None
        for row in df_dept.itertuples():
            if not batch_insert_stmt:
                batch_insert_stmt = insert_stmt
            if row.Index >= start and row.Index <= (start + batch_size):
                vals = f'({row.department_id}, "{row.department}"),'
                batch_insert_stmt += vals
            # print(row.Index)
            if row.Index == min(start + batch_size, dataset_size - 1):
                batch_insert_stmt = batch_insert_stmt.rsplit(',', 1)[0] + ';'
                cursor = self.conn.cursor()
                print(f'Going to execute department sql for rows starting {start}')
                cursor.execute(batch_insert_stmt)
                print('Execution finished')
                cursor.commit()
                cursor.close()
                batch_insert_stmt = None
                start += batch_size + 1

    def product_reader(self, data):
        insert_stmt = 'INSERT INTO Product (product_id, product_name, aisle_id) VALUES '
        df_prod = pd.DataFrame(data)
        dataset_size = df_prod.shape[0]
        print('Dataset size for product', dataset_size)
        start = 0
        batch_size = 250000
        batch_insert_stmt = None
        for row in df_prod.itertuples():
            if not batch_insert_stmt:
                batch_insert_stmt = insert_stmt
            if row.Index >= start and row.Index <= (start + batch_size):
                vals = f'({row.product_id}, "{row.product_name}", {row.aisle_id}),'
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
        insert_stmt = 'INSERT INTO Orders (order_id, order_dow, order_hour_of_day, order_number, days_since_prior_order) VALUES '
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
                vals = f'({row.order_id}, {row.order_dow}, {row.order_hour_of_day}, {row.order_number}, {row.days_since_prior_order}),'
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
        insert_stmt = 'INSERT INTO Order_Product (order_id, product_id, add_to_cart_order, reordered) VALUES '
        df_order_prod = pd.DataFrame(data)
        dataset_size = df_order_prod.shape[0]
        print('Dataset size for order product', dataset_size)
        start = 0
        batch_size = 250000
        batch_insert_stmt = None
        for row in df_order_prod.itertuples():
            if not batch_insert_stmt:
                batch_insert_stmt = insert_stmt
            if row.Index >= start and row.Index <= (start + batch_size):
                vals = f'({row.order_id}, {row.product_id}, {row.add_to_cart_order}, {row.reordered}),'
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

    def users_reader(self, data):
        insert_stmt = 'INSERT INTO Users (order_id, user_id) VALUES '
        df_order_user = pd.DataFrame(data)
        dataset_size = df_order_user.shape[0]
        print('Dataset size for order user', dataset_size)
        start = 0
        batch_size = 250000
        batch_insert_stmt = None
        for row in df_order_user.itertuples():
            if not batch_insert_stmt:
                batch_insert_stmt = insert_stmt
            if row.Index >= start and row.Index <= (start + batch_size):
                vals = f'({row.order_id}, {row.user_id}),'
                batch_insert_stmt += vals
            if row.Index == min(start + batch_size, dataset_size - 1):
                batch_insert_stmt = batch_insert_stmt.rsplit(',', 1)[0] + ';'
                cursor = self.conn.cursor()
                print(f'Going to execute order user sql for rows starting {start}')
                cursor.execute(batch_insert_stmt)
                print('Execution finished')
                cursor.commit()
                cursor.close()
                batch_insert_stmt = None
                start += batch_size + 1


if __name__ == '__main__':
    transform = False
    s3_export = S3MySQLExport()
    s3_export.read_and_write_data(transform)
