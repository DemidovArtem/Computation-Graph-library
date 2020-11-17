from . import graphs
from .lib.testing import parser, make_reader, text_path


def test_tf_idf_file() -> None:
    other_graph = graphs.inverted_index_graph('texts',
                                              doc_column='doc_id', text_column='text', result_column='tf_idf')
    correct_result = other_graph.run(texts=make_reader(text_path))

    graph = graphs.inverted_index_graph_file(text_path, parser,
                                             doc_column='doc_id', text_column='text', result_column='tf_idf')
    result = graph.run()

    assert correct_result == result
