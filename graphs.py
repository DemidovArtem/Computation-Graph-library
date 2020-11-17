from .lib import Graph, operations
import typing as tp


def word_count_graph(input_stream_name: str, text_column: str = 'text', count_column: str = 'count') -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    return Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                         result_column: str = 'tf_idf') -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""
    graph_word: Graph = Graph.graph_from_iter(input_stream_name)\
        .map(operations.FilterPunctuation(text_column))\
        .map(operations.LowerCase(text_column))\
        .map(operations.Split(text_column))

    doc_count = 'doc_count'

    graph_doc: Graph = Graph.graph_from_iter(input_stream_name)\
        .sort([doc_column])\
        .reduce(operations.Count(doc_count), [])

    suffix = '_1'

    graph_idf: Graph = graph_word \
        .sort([doc_column, text_column])\
        .reduce(operations.FirstReducer(), [doc_column, text_column])\
        .sort([text_column])\
        .reduce(operations.Count(doc_count), [text_column])\
        .join(operations.InnerJoiner('', suffix), graph_doc, [])\
        .map(operations.Idf(doc_count + suffix, doc_count))

    graph_result: Graph = graph_word\
        .sort([doc_column])\
        .reduce(operations.TermFrequency(text_column), [doc_column]) \
        .sort([text_column])

    return graph_result\
        .join(operations.InnerJoiner(), graph_idf, [text_column])\
        .map(operations.Product(['tf', 'idf'], result_column))\
        .reduce(operations.TopN(result_column, 3), [text_column])\
        .sort([doc_column])\
        .map(operations.Project([doc_column, text_column, result_column]))


def pmi_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
              result_column: str = 'pmi') -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""

    word_in_doc = 'count'

    graph: Graph = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))\
        .sort([doc_column, text_column])

    filtered_graph: Graph = graph\
        .map(operations.Filter(lambda row: len(row[text_column]) > 4))\
        .sort([doc_column, text_column])\
        .reduce(operations.Count(word_in_doc), [doc_column, text_column])\
        .map(operations.Filter(lambda row: row[word_in_doc] >= 2))
    filtered_graph = filtered_graph.join(operations.InnerJoiner(), graph, [doc_column, text_column])

    tf_graph = filtered_graph\
        .reduce(operations.TermFrequency(text_column), []) \
        .sort([text_column])

    suffix = '_1'
    return filtered_graph \
        .sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column), [doc_column]) \
        .sort([text_column]) \
        .join(operations.InnerJoiner('', suffix),
              tf_graph
              .sort([text_column]),
              [text_column])\
        .map(operations.Idf('tf', 'tf' + suffix, result_column))\
        .sort([doc_column, result_column])\
        .reduce(operations.TopN(result_column, 10), [doc_column])\
        .map(operations.Project([doc_column, text_column, result_column]))


def yandex_maps_graph(input_stream_name_time: str, input_stream_name_length: str,
                      enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
                      edge_id_column: str = 'edge_id', start_coord_column: str = 'start', end_coord_column: str = 'end',
                      weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
                      speed_result_column: str = 'speed') -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""
    length_column = 'length'
    dt_column = 'dt'

    length_graph: Graph = Graph.graph_from_iter(input_stream_name_length)\
        .map(operations.Length(start_coord_column, end_coord_column, length_column))

    suffix = '_datetime'

    time_graph: Graph = Graph.graph_from_iter(input_stream_name_time)\
        .map(operations.FormatDate(enter_time_column, enter_time_column + suffix))\
        .map(operations.FormatDate(leave_time_column, leave_time_column + suffix))\
        .map(operations.WeekDay(enter_time_column + suffix, weekday_result_column))\
        .map(operations.Hour(enter_time_column + suffix, hour_result_column))\
        .map(operations.DeltaTime(enter_time_column + suffix, leave_time_column + suffix, dt_column))

    return length_graph.join(operations.InnerJoiner(),
                             time_graph,
                             [edge_id_column])\
        .map(operations.Speed(length_column, dt_column, speed_result_column)).\
        sort([weekday_result_column, hour_result_column])\
        .reduce(operations.Mean(speed_result_column), [weekday_result_column, hour_result_column])\
        .map(operations.Project([weekday_result_column, hour_result_column, speed_result_column]))


def word_count_graph_file(input_stream_name: str, parser: tp.Callable[[str], operations.TRow],
                          text_column: str = 'text', count_column: str = 'count') -> Graph:
    """
    Constructs graph which counts words in text_column of all rows passed.
    Data is reading from file.
    """
    return Graph.graph_from_file(input_stream_name, parser) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph_file(input_stream_name: str, parser: tp.Callable[[str], operations.TRow],
                              doc_column: str = 'doc_id', text_column: str = 'text',
                              result_column: str = 'tf_idf') -> Graph:
    """
    Constructs graph which calculates td-idf for every word/document pair.
    Data is reading from file.
    """
    graph_word: Graph = Graph.graph_from_file(input_stream_name, parser)\
        .map(operations.FilterPunctuation(text_column))\
        .map(operations.LowerCase(text_column))\
        .map(operations.Split(text_column))

    doc_count = 'doc_count'

    graph_doc: Graph = Graph.graph_from_file(input_stream_name, parser)\
        .sort([doc_column])\
        .reduce(operations.Count(doc_count), [])

    suffix = '_1'

    graph_idf: Graph = graph_word \
        .sort([doc_column, text_column])\
        .reduce(operations.FirstReducer(), [doc_column, text_column])\
        .sort([text_column])\
        .reduce(operations.Count(doc_count), [text_column])\
        .join(operations.InnerJoiner('', suffix), graph_doc, [])\
        .map(operations.Idf(doc_count + suffix, doc_count))

    graph_result: Graph = graph_word\
        .sort([doc_column])\
        .reduce(operations.TermFrequency(text_column), [doc_column]) \
        .sort([text_column])

    return graph_result\
        .join(operations.InnerJoiner(), graph_idf, [text_column])\
        .map(operations.Product(['tf', 'idf'], result_column))\
        .reduce(operations.TopN(result_column, 3), [text_column])\
        .sort([doc_column])\
        .map(operations.Project([doc_column, text_column, result_column]))


def pmi_graph_file(input_stream_name: str, parser: tp.Callable[[str], operations.TRow],
                   doc_column: str = 'doc_id', text_column: str = 'text',
                   result_column: str = 'pmi') -> Graph:
    """
    Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information.
    Data is reading from file.
    """

    word_in_doc = 'count'

    graph: Graph = Graph.graph_from_file(input_stream_name, parser) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))\
        .sort([doc_column, text_column])

    filtered_graph: Graph = graph\
        .map(operations.Filter(lambda row: len(row[text_column]) > 4))\
        .sort([doc_column, text_column])\
        .reduce(operations.Count(word_in_doc), [doc_column, text_column])\
        .map(operations.Filter(lambda row: row[word_in_doc] >= 2))
    filtered_graph = filtered_graph.join(operations.InnerJoiner(), graph, [doc_column, text_column])

    tf_graph = filtered_graph\
        .reduce(operations.TermFrequency(text_column), []) \
        .sort([text_column])

    suffix = '_1'
    return filtered_graph \
        .sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column), [doc_column]) \
        .sort([text_column]) \
        .join(operations.InnerJoiner('', suffix),
              tf_graph
              .sort([text_column]),
              [text_column])\
        .map(operations.Idf('tf', 'tf' + suffix, result_column))\
        .sort([doc_column, result_column])\
        .reduce(operations.TopN(result_column, 10), [doc_column])\
        .map(operations.Project([doc_column, text_column, result_column]))


def yandex_maps_graph_file(input_stream_name_time: str,
                           input_stream_name_length: str,
                           parser: tp.Callable[[str], operations.TRow],
                           enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
                           edge_id_column: str = 'edge_id', start_coord_column: str = 'start',
                           end_coord_column: str = 'end',
                           weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
                           speed_result_column: str = 'speed') -> Graph:
    """
    Constructs graph which measures average speed in km/h depending on the weekday and hour.
    Data is reading from file.
    """
    length_column = 'length'
    dt_column = 'dt'

    length_graph: Graph = Graph.graph_from_file(input_stream_name_length, parser)\
        .map(operations.Length(start_coord_column, end_coord_column, length_column))

    suffix = '_datetime'

    time_graph: Graph = Graph.graph_from_file(input_stream_name_time, parser)\
        .map(operations.FormatDate(enter_time_column, enter_time_column + suffix))\
        .map(operations.FormatDate(leave_time_column, leave_time_column + suffix))\
        .map(operations.WeekDay(enter_time_column + suffix, weekday_result_column))\
        .map(operations.Hour(enter_time_column + suffix, hour_result_column))\
        .map(operations.DeltaTime(enter_time_column + suffix, leave_time_column + suffix, dt_column))

    return length_graph.join(operations.InnerJoiner(),
                             time_graph,
                             [edge_id_column])\
        .map(operations.Speed(length_column, dt_column, speed_result_column)).\
        sort([weekday_result_column, hour_result_column])\
        .reduce(operations.Mean(speed_result_column), [weekday_result_column, hour_result_column])\
        .map(operations.Project([weekday_result_column, hour_result_column, speed_result_column]))
