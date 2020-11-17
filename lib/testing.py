import typing as tp
from . import operations as ops

MiB = 1024 ** 2

text_path = './resource/text_corpus.txt'
road_path = './resource/road_graph_data.txt'
travel_path = './resource/travel_times.txt'


def parser(line: str) -> ops.TRow:
    """Convert string to TRow"""
    return eval(line)


def make_reader(filename: str) -> tp.Callable[[], ops.TRowsGenerator]:
    """Create function that returns Generator, reading from file"""
    def reader() -> ops.TRowsGenerator:
        """Work as generator, reading from file"""
        file = open(filename, 'r')
        while True:
            line = file.readline()
            if not line:
                file.close()
                break
            else:
                yield parser(line)
    return reader
