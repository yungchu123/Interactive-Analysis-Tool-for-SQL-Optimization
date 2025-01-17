from descriptions.aggregate_explain import aggregate_explain
from descriptions.append_explain import append_explain
from descriptions.cte_explain import cte_explain
from descriptions.function_scan_explain import function_scan_explain
from descriptions.group_explain import group_explain
from descriptions.index_scan_explain import index_scan_explain
from descriptions.index_scan_explain import index_only_scan_explain

from descriptions.limit_explain import limit_explain
from descriptions.materializer_explain import materialize_explain
from descriptions.unique_explain import unique_explain
from descriptions.merge_join_explain import merge_join_explain
from descriptions.setop_explain import setop_explain
from descriptions.subquery_scan_explain import subquery_scan_explain
from descriptions.values_scan_explain import values_scan_explain
from descriptions.seq_scan_explain import seq_scan_explain
from descriptions.nested_loop_explain import nested_loop_explain
from descriptions.sort_explain import sort_explain
from descriptions.hash_explain import hash_explain
from descriptions.hash_join_explain import hash_join_explain

class Descriptors(object):
    # Static Explainer class to store a hashmap of node types to explain functions
    explainer_map = {
        "Aggregate": aggregate_explain,
        "Append": append_explain,
        "CTE Scan": cte_explain,
        "Function Scan": function_scan_explain,
        "Group": group_explain,
        "Index Scan": index_scan_explain,
        "Index Only Scan": index_only_scan_explain,
        "Limit": limit_explain,
        "Materialize": materialize_explain,
        "Unique": unique_explain,
        "Merge Join": merge_join_explain,
        "SetOp": setop_explain,
        "Subquery Scan": subquery_scan_explain,
        "Values Scan": values_scan_explain,
        "Seq Scan": seq_scan_explain,
        "Nested Loop": nested_loop_explain,
        "Sort": sort_explain,
        "Hash": hash_explain,
        "Hash Join": hash_join_explain,
    }


if __name__ == "__main__":
    print(Descriptors.explainer_map)