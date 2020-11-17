from . import graphs
from .lib.testing import make_reader, parser, text_path


def test_pmi_file() -> None:
    other_graph = graphs.pmi_graph('texts',
                                   doc_column='doc_id', text_column='text', result_column='pmi')
    correct_result = other_graph.run(texts=make_reader(text_path))

    graph = graphs.pmi_graph_file(text_path, parser,
                                  doc_column='doc_id', text_column='text', result_column='pmi')
    result = graph.run()

    assert correct_result == result
