from . import graphs
from .lib.testing import make_reader, parser, road_path, travel_path


def test_yandex_maps_file() -> None:
    other_graph = graphs.yandex_maps_graph(
        'travel_time', 'edge_length',
        enter_time_column='enter_time', leave_time_column='leave_time', edge_id_column='edge_id',
        start_coord_column='start', end_coord_column='end',
        weekday_result_column='weekday', hour_result_column='hour', speed_result_column='speed'
    )
    correct_result = other_graph.run(travel_time=make_reader(travel_path), edge_length=make_reader(road_path))

    graph = graphs.yandex_maps_graph_file(
        travel_path, road_path, parser,
        enter_time_column='enter_time', leave_time_column='leave_time', edge_id_column='edge_id',
        start_coord_column='start', end_coord_column='end',
        weekday_result_column='weekday', hour_result_column='hour', speed_result_column='speed'
    )

    result = graph.run()

    assert correct_result == result
