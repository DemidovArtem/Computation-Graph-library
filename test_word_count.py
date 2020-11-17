from . import graphs
from .lib.testing import make_reader, parser, text_path


def test_word_count_file() -> None:
    other_graph = graphs.word_count_graph('data', text_column='text', count_column='count')
    correct_result = other_graph.run(data=make_reader(text_path))

    graph = graphs.word_count_graph_file(text_path, parser, text_column='text', count_column='count')
    result = graph.run()

    assert correct_result == result
