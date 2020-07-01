import queue
from copy import copy, deepcopy
from collections import defaultdict

# This file exists in order to deal with generic graph structures. The `dbtree.py`
# file also implements a graph structure, but it is inherently tied to the schema data
# and graph algorithms often like adding temporary data to nodes in order to obtain
# more efficient algorithms.

# https://stackoverflow.com/a/30747003
# With thanks to author mVChr for the basic Graph structure.
# I've only added the "distance_from_leaf", "topological_sort" and "copy" methods.
class Graph(object):
    """ Graph data structure, undirected by default. """

    def __init__(self, connections, directed=False):
        self._graph = defaultdict(set)
        self._directed = directed
        self.add_connections(connections)
        self.data = {}

    def add_connections(self, connections):
        """ Add connections (list of tuple pairs) to graph """

        for node1, node2 in connections:
            self.add(node1, node2)

    def add(self, node1, node2):
        """ Add connection between node1 and node2 """

        self._graph[node1].add(node2)
        # if node1 not in self.data:
        #     self.data[node1] = 0
        if not self._directed:
            self._graph[node2].add(node1)
            # if node2 not in self.data:
            #     self.data[node2] = 0


    def remove(self, node):
        """ Remove all references to node """

        for n, cxns in self._graph.items():  # python3: items(); python2: iteritems()
            try:
                cxns.remove(node)
            except KeyError:
                pass
        try:
            del self._graph[node]
            del self.data[node]
        except KeyError:
            pass

    def is_connected(self, node1, node2):
        """ Is node1 directly connected to node2 """

        return node1 in self._graph and node2 in self._graph[node1]

    def find_path(self, node1, node2, path=[]):
        """ Find any path between node1 and node2 (may not be shortest) """

        path = path + [node1]
        if node1 == node2:
            return path
        if node1 not in self._graph:
            return None
        for node in self._graph[node1]:
            if node not in path:
                new_path = self.find_path(node, node2, path)
                if new_path:
                    return new_path
        return None

    def is_leaf(self, node):
        return len(self._graph[node]) <= 1

    def calculate_dist_from_leaves(self):
        Q = queue.Queue()
        visited = set()

        # set up leaves
        for v in self._graph.keys():
            if self.is_leaf(v):
                self.data[v] = 0
                Q.put(v)
                visited.add(v)

        # perform BFS from all leaves
        while not Q.empty():
            v = Q.get()
            for w in self._graph[v]:
                if w not in visited:
                    self.data[w] = self.data[v] + 1
                    visited.add(w)
                    Q.put(w)

    def topological_sort(self, leave_until_last=None):
        assert not self._directed, "currently only implemented for undirected graphs"
        result = []
        G = self.copy()
        # if leave_until_last:
        #     # get the copy of leave_until_last object (ow. will fail for eq further down)
        #     try:
        #         leave_until_last = [v for v in G._graph.keys()
        #                             if v.name == leave_until_last.name]
        #     except AttributeError:
        #         leave_until_last = [v for v in G._graph.keys() if v == leave_until_last]
        #     assert len(leave_until_last) == 1, "leave_until_last value is not unique in G"
        #     leave_until_last = leave_until_last[0]
        # Need to take copy since we need to mutate (rm edges) graph
        # ENSURE NO REFERENCES TO SELF IN THE BELOW CODE (subtle bugs).
        Q = queue.Queue()
        # add leaf nodes to the queue:
        for k in G._graph.keys():
            if G.is_leaf(k):
                Q.put(k)
        # loop over queue
        while not Q.empty():
            v = Q.get()
            if (v == leave_until_last) and (not Q.empty()):
                Q.put(v)
                continue
            result.append(v)
            while len(G._graph[v]) > 0:
                m = G._graph[v].pop()
                G._graph[m].remove(v)
                if len(G._graph[m]) == 1:
                    Q.put(m)
        return result

    def __str__(self):
        return '{}({})'.format(self.__class__.__name__, dict(self._graph))

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        obj = type(self).__new__(self.__class__)
        obj._graph = copy(self._graph)
        obj._directed = self._directed
        obj.data = deepcopy(self.data)
        return obj


if __name__ == "__main__":
    # BASIC TESTING
    tmp = Graph([('a', 'b'), ('a', 'c'), ('c', 'd'), ('c', 'e'), ('c', 'f'), ('e', 'g'),
                 ('a', 'h'), ('h', 'i'), ('i', 'j'), ('i', 'k'), ('e', 'l')])
    out = tmp.topological_sort(leave_until_last='a')
    print(out)
