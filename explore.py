import os
#from functools import wraps
# from typing import Any, Callable, List, Optional
import psycopg2
from queryPlan import QueryPlan

class Explore: 
    def __init__(self, host, port, database, user, password):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        self.cursor = self.conn.cursor()

    # Explain a query
    def explain(self, query) -> QueryPlan:
        self.cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
        plan = self.cursor.fetchall()
        query_plan_dict: dict = plan[0][0][0]["Plan"]
        return QueryPlan(query_plan_dict, query)

    
    def close_connection(self):
        # Close the cursor and connection
        self.cursor.close()
        self.conn.close()
    
if __name__ == '__main__':
    exploration = Explore()
    input_query =  """
    select 
      l_returnflag,
      l_linestatus,
      sum(l_quantity) as sum_qty,
      sum(l_extendedprice) as sum_base_price,
      sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      avg(l_quantity) as avg_qty,
      avg(l_extendedprice) as avg_price,
      avg(l_discount) as avg_disc,
      count(*) as count_order
    from
      lineitem
    where
      l_extendedprice > 100
    group by
      l_returnflag,
      l_linestatus
    order by
      l_returnflag,
      l_linestatus;
"""
    query_plan_instance = exploration.explain(input_query)
    qep_tree = query_plan_instance.save_graph_file()
    exploration.close_connection()
    explanation = query_plan_instance.create_explanation(query_plan_instance.root)
    totalCost = query_plan_instance.calculate_total_cost()
    print(explanation)
    print(totalCost)
