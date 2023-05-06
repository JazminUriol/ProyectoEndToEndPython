import pandas as pd
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, locals())

class Transform():
    
    def _init_(self) -> None:
        self.process = 'Transform Process'
        
    def enunciado1(self,products, order_items, orders, categories, departments):
        q =""" select department_name, product_name, anio, quantity, total,rank, row
        FROM (
        select d.department_name,p.product_name, 
        strftime('%Y', o.order_date) as anio,
        sum(i.order_item_quantity) as quantity,
        cast(sum(i.order_item_subtotal) as INT) as total,
        DENSE_RANK () OVER ( 
                                PARTITION BY d.department_name
                                ORDER BY sum(i.order_item_subtotal) DESC
                            ) rank,
        DENSE_RANK() OVER (
                            PARTITION BY d.department_name
                            ORDER BY strftime('%Y', o.order_date) DESC
                          ) row
        from products as p
        inner join order_items as i on i.order_item_product_id = p.product_id
        inner join orders as o on o.order_id = i.order_item_order_id
        inner join categories as c on c.category_id = p.product_category_id
        inner join departments as d on d.department_id =c.category_department_id
        group by d.department_name, p.product_name, anio) t
        where rank <=5 and row = 1
        """
        result = sqldf(q) 
        return result
    
    def enunciado2(self, df_orders,df_customers, df_order_items):
        df_orders['order_date'] = pd.to_datetime(df_orders['order_date'])
        df_sort_orders = df_orders.sort_values('order_date', ascending=0)
        df_sort_orders.query("order_status == 'PENDING' | order_status == 'PENDING_PAYMENT'")
        df_sort_orders_pendientes = df_sort_orders.query("order_status == 'PENDING' | order_status == 'PENDING_PAYMENT'").merge(df_customers, left_on='order_customer_id', right_on='customer_id').merge(df_order_items, left_on='order_id', right_on='order_item_order_id')
        df_sort_orders_pendientes = df_sort_orders_pendientes[['customer_city', 'order_status', 'order_item_quantity']]
        df = df_sort_orders_pendientes.groupby(['customer_city','order_status']).sum('order_item_quantity').reset_index()

        return df
    
