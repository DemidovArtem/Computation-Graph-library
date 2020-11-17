from . import graphs


def test_map() -> None:
    data = [
        {'1': 1, '2': 2, '3': 3},
        {'1': 4, '2': 5, '3': 6}
    ]
    graph = graphs.Graph.graph_from_iter('input')\
        .map(graphs.operations.DummyMapper())

    correct_result = data

    result = graph.run(input=lambda: iter(data))

    assert correct_result == result


def test_reduce() -> None:
    data = [
        {'1': 1, '2': 2, '3': 3},
        {'1': 1, '2': 2, '3': 6},
        {'1': 2, '2': 3, '3': 6}
    ]

    keys = tuple(['1', '2'])
    graph = graphs.Graph.graph_from_iter('input')\
        .reduce(graphs.operations.TopN('1', 2), keys)

    correct_result = data

    result = graph.run(input=lambda: iter(data))

    assert correct_result == result


def test_join() -> None:
    data_1 = [
        {'1': 1, '2': 2, '3': 3},
        {'1': 1, '2': 2, '3': 6},
        {'1': 2, '2': 3, '3': 6}
    ]

    data_2 = [
        {'1': 1, '2': 2, '3': 3},
        {'1': 4, '2': 5, '3': 6}
    ]

    graph_1 = graphs.Graph.graph_from_iter('data_1')

    graph_2 = graphs.Graph.graph_from_iter('data_2')

    keys = tuple(['1', '2'])
    graph = graph_1.join(graphs.operations.InnerJoiner(), graph_2, keys)

    correct_result = [
        {'1': 1, '2': 2, '3_1': 3, '3_2': 3},
        {'1': 1, '2': 2, '3_1': 6, '3_2': 3}
    ]
    result = graph.run(data_1=lambda: iter(data_1), data_2=lambda: iter(data_2))
    assert correct_result == result


def test_creation() -> None:
    data = [
        {'1': 1, '2': 2, '3': 3},
        {'1': 4, '2': 5, '3': 6}
    ]
    graph = graphs.Graph.graph_from_iter('input')

    correct_result = data

    result = graph.run(input=lambda: iter(data))

    assert correct_result == result
