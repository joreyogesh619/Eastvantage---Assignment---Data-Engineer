import sqlite3
import pandas as pd

def getConnection(db_path):
    try:
        return sqlite3.connect(db_path)
    except sqlite3.Error as e:
        print(f"Database connection failed: {e}")
        raise


def pandaSolution(db_path, output_path):

    conn = None
    try:
        conn = getConnection(db_path)

        customers = pd.read_sql("SELECT * FROM Customer", conn)
        sales = pd.read_sql("SELECT * FROM Sales", conn)
        orders = pd.read_sql("SELECT * FROM Orders", conn)
        items = pd.read_sql("SELECT * FROM Items", conn)

        customers = customers[
            (customers['age'] >= 18) &
            (customers['age'] <= 35)
        ]

        df = customers.merge(sales, on='customer_id') \
                      .merge(orders, on='sales_id') \
                      .merge(items, on='item_id')

        df = df[df['quantity'].notna()]

        result = df.groupby(
            ['customer_id', 'age', 'item_name'],
            as_index=False
        )['quantity'].sum()

        result = result[result['quantity'] > 0]

        result.columns = ['Customer', 'Age', 'Item', 'Quantity']

        result['Quantity'] = result['Quantity'].astype(int)

        result = result.sort_values(['Customer', 'Item'])

        result.to_csv(output_path, sep=';', index=False)

    except Exception as e:
        print(f"Error : {e}")

    finally:
        if conn:
            conn.close()