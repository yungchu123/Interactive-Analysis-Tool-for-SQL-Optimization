import os
import random
from typing import List

import networkx as nx


def get_tree_node_pos(
    G, root=None, width=1.0, height=1, vert_gap=0.1, vert_loc=0, xcenter=0.5
):
    """
    Get the positions of nodes in a hierarchical layout for a tree.

    Parameters:
    - G: NetworkX graph
    - root: The root node of the tree (default: None, will use a random node)
    - width: Width of each level in the hierarchy (default: 1.0)
    - height: Height of the tree (default: 1)
    - vert_gap: Gap between vertical levels (default: 0.1)
    - vert_loc: Starting vertical location (default: 0)
    - xcenter: Horizontal center for the root node (default: 0.5)

    Returns:
    - pos: Dictionary with node positions
    """

    # hierarchical plot of QEP tree

    if not nx.is_tree(G):
        raise TypeError(
            "cannot use hierarchy_pos on a graph that is not a tree"
        )

    if root is None:
        if isinstance(G, nx.DiGraph):
            root = next(
                iter(nx.topological_sort(G))
            )  # allows back compatibility with nx version 1.11
        else:
            root = random.choice(list(G.nodes))

    path_dict = dict(nx.all_pairs_shortest_path(G))
    max_height = 0
    for value in path_dict.values():
        max_height = max(max_height, len(value))
    vert_gap = height / max_height

    def _hierarchy_pos(
        G,
        root,
        width,
        vert_gap,
        vert_loc,
        xcenter,
        pos=None,
        parent=None,
        min_dx=0.05,
    ):
        """
        Recursively compute node positions in the hierarchical layout.

        Parameters:
        - G: NetworkX graph
        - root: Current root node
        - width: Width of the current level
        - vert_gap: Gap between vertical levels
        - vert_loc: Current vertical location
        - xcenter: Horizontal center for the current level
        - pos: Dictionary with node positions
        - parent: Parent node
        - min_dx: Minimum horizontal gap

        Returns:
        - pos: Updated dictionary with node positions
        """

        if pos is None:
            pos = {root: (xcenter, vert_loc)}
        else:
            pos[root] = (xcenter, vert_loc)
        children = list(G.neighbors(root))
        if not isinstance(G, nx.DiGraph) and parent is not None:
            children.remove(parent)
        if len(children) != 0:
            dx = max(min_dx, width / len(children))
            nextx = xcenter - width / 2 - max(min_dx, dx / 2)
            for child in children:
                nextx += dx
                pos = _hierarchy_pos(
                    G,
                    child,
                    width=dx,
                    vert_gap=vert_gap,
                    vert_loc=vert_loc - vert_gap,
                    xcenter=nextx,
                    pos=pos,
                    parent=root,
                )
        return pos

    return _hierarchy_pos(G, root, width, vert_gap, vert_loc, xcenter)



