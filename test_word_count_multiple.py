from . import graphs
from .lib.testing import parser, text_path


def test_word_count_multiple_call_file() -> None:
    graph = graphs.word_count_graph_file(text_path, parser, text_column='text', count_column='count')
    result_1 = graph.run()
    result_2 = graph.run()
    assert result_1 == result_2
