import os
#from functools import wraps
# from typing import Any, Callable, List, Optional
import psycopg2
from queryPlan import QueryPlan
import math
import re
from matplotlib.colors import ListedColormap
import matplotlib.pyplot as plt
import numpy as np

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

        self.incomplete_conditions = {}
        self.conditions = []
        self.intermediate_table_queries = {}
        self.ctid_queries = []

    # Explain a query
    def explain(self, query) -> QueryPlan:
        self.cursor.execute(f"EXPLAIN (ANALYSE, COSTS, BUFFERS, TIMING, FORMAT JSON) {query}")
        plan = self.cursor.fetchall()
        query_plan_dict: dict = plan[0][0][0]["Plan"]
        return QueryPlan(query_plan_dict, query)
    
    def close_connection(self):
        # Close the cursor and connection
        self.cursor.close()
        self.conn.close()

    def extract_conditions(self,plan, level):
      
      conditions = []

      # Recursively call the function to start from the root
      if 'Plans' in plan:
          for subplan in plan['Plans']:
              conditions.extend(exploration.extract_conditions(subplan, level+1))

      # Check for incomplete queries in children node
      for condition in self.incomplete_conditions:
      
          if condition['Level'] == (level + 1) and "Checked" not in condition:
              # Marked that the query has been checked - to prevent other branches from checking a node that is not its child
              condition["Checked"] = True

              # Check if current level is "Hash Join"
              if plan['Node Type'] == "Hash Join":
                  
                  complete_condition = condition
                  complete_condition["Hash Cond"] = plan["Hash Cond"][1:-1]
                  conditions.append(complete_condition)

                  # Remove from incomplete_conditions
                  self.incomplete_conditions.remove(condition)

      # Check for Nodes with Sequential Scan
      if plan['Node Type'] in ['Seq Scan', 'CTE Scan']:
          selected_keys = ["Node Type", "Relation Name", "Alias"]
          if 'Filter' in plan:
              selected_keys.append("Filter")
              selected_pairs = {key: value for key, value in plan.items() if key in selected_keys}
              
              # Remove outer parenthesis for Filter
              selected_pairs["Filter"] = selected_pairs["Filter"][1:-1]
              # Add the current level
              selected_pairs["Level"] = level

              conditions.append(selected_pairs)

          else:
              # Some nodes are sequentially scan 
              incomplete_condition = {key: value for key, value in plan.items() if key in selected_keys}
              incomplete_condition["Level"] = level
              self.incomplete_conditions.append(incomplete_condition)

      # Check for Nodes with Index Scan
      elif plan['Node Type'] in ['Index Scan', 'Index Only Scan', 'Bitmap Index Scan']:
          selected_keys = ["Node Type", "Relation Name", "Alias", "Index Cond"]

          if "Filter" in plan:
              selected_keys.append("Filter")

          selected_pairs = {key: value for key, value in plan.items() if key in selected_keys}

          # Remove outer parenthesis for Filter and  Index Cond
          selected_pairs["Filter"] = selected_pairs["Filter"][1:-1]
          selected_pairs["Index Cond"] = selected_pairs["Index Cond"][1:-1]
          # Add the current level
          selected_pairs["Level"] = level

          conditions.append(selected_pairs)

      # Check for Nodes with Bitmap Heap Scan
      elif plan['Node Type'] == 'Bitmap Heap Scan':
          pass    

      return conditions
    
    def construct_query(conditions):
      intermediate_table_queries = {}
      queries = []
      for condition in conditions:
          current_alias = condition["Alias"]
          table = condition["Relation Name"]
          
          if condition["Node Type"] in ["Seq Scan", "CTE Scan"]:
              if "Filter" in condition:
                  filter_condition = condition["Filter"]
                  query_for_intermediate = f"(SELECT * FROM {table} {current_alias} WHERE {filter_condition})"
                  query = f"SELECT ctid, * FROM {table} {current_alias} ORDER BY ctid"

                  queries.append(query) # Sequential Scan will access all blocks
                  intermediate_table_queries[f"{current_alias}"] = query_for_intermediate # Stored to create a query if and when this table is used for other scans

              elif "Hash Cond" in condition:
                  hash_condition = condition["Hash Cond"]

                  # Extracting table aliases                  
                  join_condition_parts = hash_condition.split('=')
                  left_table_col = join_condition_parts[0].strip()
                  right_table_col = join_condition_parts[1].strip()

                  left_table_alias = left_table_col.split('.')[0]
                  right_table_alias = right_table_col.split('.')[0]
                  intermediate_table_alias = right_table_alias if current_alias == left_table_alias else right_table_alias

                  # Get the query of this intermediate table
                  intermediate_table_query = intermediate_table_queries[f"{intermediate_table_alias}"]

                  # Crafting the SQL queries
                  query_for_intermediate = f"""
                      (SELECT * 
                      FROM {table} {current_alias} 
                      JOIN {intermediate_table_query} {intermediate_table_alias} 
                      ON {hash_condition})
                  """
                  query = f"""
                      SELECT {current_alias}.ctid, * 
                      FROM {table} {current_alias} 
                      JOIN {intermediate_table_query} {intermediate_table_alias} 
                      ON {hash_condition} 
                      ORDER BY {current_alias}.ctid
                  """
                  
                  queries.append(query)
                  intermediate_table_queries[f"{current_alias}"] = query_for_intermediate
                  
          elif condition["Node Type"] in ["Index Scan", "Index Only Scan", "Bitmap Index Scan"]:
              
              index_condition = condition["Index Cond"] # Filtering the field that is the primary key
              filter_condition = "" # Filtering fields that are not the primary key

              if "Filter" in condition:
                  filter_condition = condition["Filter"] 

              # Regex to add alias for current table
              # Identify conditions that already table alias
              pattern = re.compile(r'\b\w+\.\w+\b') 
              table_alias = f"{current_alias}."

              # Find all matches in the condition
              regex_exclusions = ["AND", "OR"]
              regex_exclusions.extend(pattern.findall(index_condition))
              regex_exclusions.extend(pattern.findall(filter_condition))

              # Updated regular expression pattern
              pattern = re.compile(r'(\w+\.?\w*)')

              # Use regular expressions to add the table alias
              index_condition = pattern.sub(
                  lambda match: f"{table_alias}{match.group(1)}"
                  if '.' not in match.group(1) and match.group(1) not in regex_exclusions and not match.group(1).isalpha()
                  else match.group(1),
                  index_condition
              )

              filter_condition = pattern.sub(
                  lambda match: f"{table_alias}{match.group(1)}"
                  if '.' not in match.group(1) and match.group(1) not in regex_exclusions and not match.group(1).isalpha()
                  else match.group(1),
                  filter_condition
              )

              # Extracting intermediate table alias
              join_condition_parts = index_condition.split('=')
              left_table_col = join_condition_parts[0].strip()
              right_table_col = join_condition_parts[1].strip()

              intermediate_table_alias = right_table_col.split('.')[0]

              # Get the query of this intermediate table
              intermediate_table_query = intermediate_table_queries[f"{intermediate_table_alias}"]

              # Crafting the SQL queries
              if "Filter" in condition:
                  query_for_intermediate = f"""
                      (SELECT * 
                      FROM {table} {current_alias} 
                      JOIN {intermediate_table_query} {intermediate_table_alias} 
                      ON {index_condition} 
                      WHERE {filter_condition})
                  """

                  query = f"""
                      SELECT {current_alias}.ctid, * 
                      FROM {table} {current_alias} 
                      JOIN {intermediate_table_query} {intermediate_table_alias} 
                      ON {index_condition} 
                      WHERE {filter_condition}
                      ORDER BY {current_alias}.ctid
                  """
              else:
                  query_for_intermediate = f"""
                      (SELECT * 
                      FROM {table} {current_alias} 
                      JOIN {intermediate_table_query} {intermediate_table_alias} 
                      ON {index_condition})
                  """

                  query = f"""
                      SELECT {current_alias}.ctid, * 
                      FROM {table} {current_alias} 
                      JOIN {intermediate_table_query} {intermediate_table_alias} 
                      ON {index_condition}
                      ORDER BY {current_alias}.ctid
                  """

              queries.append(query)
              intermediate_table_queries[f"{current_alias}"] = query_for_intermediate

      return intermediate_table_queries, queries
    
    def prep_visualise (self, sql_query):
      # Execute the query and get the plan
      self.cursor.execute(f"EXPLAIN (FORMAT JSON) {sql_query}")
      qep_list = self.cursor.fetchone()[0]

      
      self.conditions = exploration.extract_conditions(qep_list[0]["Plan"], 0)

      # For each table, construct a query using ctid
      
      self.intermediate_table_queries, self.ctid_queries = exploration.construct_query(self.conditions)

    def get_table_details(self,query,table_name):
      
      # Execute the SQL query
      self.cursor.execute(query)

      # Fetch all rows
      rows = self.cursor.fetchall()

      # Extract ctid values
      ctids = [row[0] for row in rows]

      # Visualize the ctid values
      blocks_accessed = set()
      for temp_tuple in ctids:
          temp_tuple = tuple(map(int, temp_tuple.strip('()').split(',')))
          block_num = temp_tuple[0]
          blocks_accessed.add(block_num)

      num_blocks_query = f"SELECT ctid FROM {table_name} ORDER BY ctid DESC LIMIT 1"
      num_blocks = exploration.get_num_blocks(num_blocks_query)

      max_width = 40
      height = math.ceil(num_blocks/max_width)

      return table_name, height
    
    def get_num_blocks(self, query):
      
      self.cursor.execute(query)
      num_blocks = self.cursor.fetchall()
      num_blocks = tuple(map(int, num_blocks[0][0].strip('()').split(',')))

      return num_blocks[0] + 1
    
    def visualise_block_all_tables (ctid_queries, conditions):
      # Execute the query and visualize ctid values
      for i in range(len(ctid_queries)):
          exploration.visualise_block_grid(ctid_queries[i], conditions[i]["Relation Name"], conditions[i]["Alias"])

    def visualise_block_grid(self, query, table_name, alias, limit = None):
      
      # Execute the SQL query
      self.cursor.execute(query)

      # Fetch all rows
      rows = self.cursor.fetchall()

      # Extract ctid values
      ctids = [row[0] for row in rows]

      # Visualize the ctid values
      blocks_accessed = set()
      for temp_tuple in ctids:
          temp_tuple = tuple(map(int, temp_tuple.strip('()').split(',')))
          block_num = temp_tuple[0]
          blocks_accessed.add(block_num)

      num_blocks_query = f"SELECT ctid FROM {table_name} ORDER BY ctid DESC LIMIT 1"
      num_blocks = exploration.get_num_blocks(num_blocks_query)

      max_width = 40
      height = math.ceil(num_blocks/max_width)/20
      gridmap = np.zeros((math.ceil(num_blocks/max_width), max_width), dtype=int)

      for i in range(num_blocks):
          row, col = divmod(i, max_width)
          gridmap[row][col] = 1

      for block in blocks_accessed:
          row, col = divmod(block, max_width)
          gridmap[row][col] = 2

      
      # Plot the gridmap
      plt.figure(figsize=(18,3 * height))
      custom_cmap = ListedColormap(['white', 'brown', 'green'])
      plt.pcolor(gridmap[::-1],cmap=custom_cmap,edgecolors='k', linewidths=1)

      # Customize plot
      plt.title(f'Disk Memory Gridmap - Accessed Blocks in {table_name} ({alias})')
      # Label x-axis intervals every 5th block
      x_ticks = np.arange(0, max_width, 5) + 0.5
      x_labels = np.arange(1, max_width + 1)[::5]
      plt.xticks(x_ticks, x_labels)

      # Label y-axis intervals every 5th row
      y_ticks = np.arange(gridmap.shape[0] - 1, -1, -5) + 0.5
      y_labels = np.arange(1, gridmap.shape[0] + 1, 5)
      plt.yticks(y_ticks, y_labels)

      plt.tick_params(axis='x', which='both', bottom=False, top=True, labelbottom=False, labeltop=True)
      plt.show()

    
    
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

    exploration.prep_visualise(input_query)
    table_details = {}
    for i in range(len(exploration.ctid_queries)):
      table_name, height = exploration.get_table_details(exploration.ctid_queries[i],exploration.conditions[i]["Relation Name"])
      alias = exploration.conditions[i]["Alias"]
      table_name_with_alias = f"{table_name} - {alias}"
      table_details[f"{table_name_with_alias}"] = height

    exploration.visualise_block_all_tables(exploration.ctid_queries, exploration.conditions)
    # For visualisation of only 1 table (Change the index accordingly)
    # visualise_block_grid(ctid_queries[1], conditions[1]["Relation Name"], conditions[1]["Alias"])

    exploration.close_connection()
    explanation = query_plan_instance.create_explanation(query_plan_instance.root)
    totalCost = query_plan_instance.calculate_total_cost()
    print(explanation)
    print(totalCost)
