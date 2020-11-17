import typing as tp
from . import operations as ops
from . import external_sort as es
from copy import deepcopy


class Node:
    """Parent class for nodes of graph"""
    def __init(self) -> None:
        pass

    def run(self) -> ops.TRowsGenerator:
        """
        create generator of rows through this node
        """
        pass

    def add_source(self, source: tp.Callable[[], ops.TRowsGenerator]) -> None:
        """
        add fabric of generators of rows to node
        """
        pass


class SourceNode(Node):
    """Graph node receiving date from source"""
    def __init__(self) -> None:
        self.source: tp.Callable[[], ops.TRowsGenerator]

    def add_source(self, source: tp.Callable[[], ops.TRowsGenerator]) -> None:
        """
        :param source: fabric of generators of rows
        """
        self.source = source

    def run(self) -> ops.TRowsGenerator:
        for row in self.source():
            yield row


class MapNode(Node):
    """ Graph node applying map to date from previous node"""
    def __init__(self, source: Node, mapper: ops.Mapper) -> None:
        """
        :param source: previous node
        :param mapper: mapper to construct map operation from
        """
        self.source = source
        self.map = ops.Map(mapper)

    def run(self) -> ops.TRowsGenerator:
        yield from self.map(self.source.run())


class ReduceNode(Node):
    """Graph node applying reduce operation to date from previous node"""
    def __init__(self, source: Node, reducer: ops.Reducer, keys: tp.Sequence[str]) -> None:
        """
        :param source: previous node
        :param reducer: reducer to construct reduce operation from
        :param keys: name of columns for reduce operation
        """
        self.source = source
        self.reduce = ops.Reduce(reducer, keys)

    def run(self) -> ops.TRowsGenerator:
        for row in self.reduce(self.source.run()):
            yield row


class JoinNode(Node):
    """Graph node applying join operation to date from two previous nodes"""
    def __init__(self, left: Node, right: Node, joiner: ops.Joiner, keys: tp.Sequence[str]) -> None:
        """
        :param left: previous left node
        :param right: previous right node
        :param joiner: particular joiner for join operation
        :param keys: name of columns for join operation
        """
        self.left = left
        self.right = right
        self.join = ops.Join(joiner, keys)

    def run(self) -> ops.TRowsGenerator:
        for row in self.join(self.left.run(), self.right.run()):
            yield row


class SortNode(Node):
    """Graph node which sort date from previous node"""
    def __init__(self, source: Node, keys: tp.Sequence[str]) -> None:
        """
        :param source: previous node
        :param keys: name of columns to sort by
        """
        self.es = es.ExternalSort(keys)
        self.source = source

    def run(self) -> ops.TRowsGenerator:
        for row in self.es(self.source.run()):
            yield row


class Graph:
    """Computational graph implementation"""

    def __init__(self, name: str):
        self.last_node: Node = SourceNode()
        self.sources = {name: self.last_node}

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        :param name: name of kwarg to use as data source
        """
        return Graph(name)

    @staticmethod
    def fabric(name: str, parser: tp.Callable[[str], ops.TRow]) -> tp.Callable[[], ops.TRowsGenerator]:
        f_name = name

        def source() -> ops.TRowsGenerator:
            file = open(f_name, 'r')
            while True:
                line = file.readline()
                if not line:
                    file.close()
                    break
                else:
                    yield parser(line)

        return source

    @staticmethod
    def graph_from_file(filename: str, parser: tp.Callable[[str], ops.TRow]) -> 'Graph':
        """Construct new graph extended with operation for reading rows from file
        :param filename: filename to read from
        :param parser: parser from string to Row
        """

        graph = Graph('')
        graph.last_node.add_source(graph.fabric(filename, parser))
        return graph

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        graph = deepcopy(self)
        graph.last_node = MapNode(graph.last_node, mapper)
        return graph

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        graph = deepcopy(self)
        graph.last_node = ReduceNode(graph.last_node, reducer, keys)
        return graph

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        graph = deepcopy(self)
        graph.last_node = SortNode(graph.last_node, keys)
        return graph

    def join(self, joiner: ops.Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        graph = deepcopy(self)
        graph.last_node = JoinNode(graph.last_node, join_graph.last_node, joiner, keys)
        for key in join_graph.sources.keys():
            new_key = key
            while new_key in graph.sources.keys():
                new_key += '_'
            graph.sources[new_key] = join_graph.sources[key]
        return graph

    def run(self, **kwargs: tp.Any) -> tp.List[ops.TRow]:
        """Single method to start execution; data sources passed as kwargs"""
        for key in kwargs.keys():
            new_key = key
            while new_key in self.sources.keys():
                self.sources[new_key].add_source(kwargs[key])
                new_key += '_'
        result: tp.List[ops.TRow] = []
        for row in self.last_node.run():
            print(row)
            result.append(row)
        return result
