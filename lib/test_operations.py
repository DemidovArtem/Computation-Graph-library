from operator import itemgetter

from pytest import approx

from . import operations as ops
from datetime import datetime as dt
from math import log


def test_dummy_map() -> None:
    tests: ops.TRowsIterable = [
        {'test_id': 1, 'text': 'one two three'},
        {'test_id': 2, 'text': 'testing out stuff'}
    ]

    result = ops.Map(ops.DummyMapper())

    assert tests == list(result(tests))


def test_lower_case() -> None:
    tests: ops.TRowsIterable = [
        {'test_id': 1, 'text': 'camelCaseTest'},
        {'test_id': 2, 'text': 'UPPER_CASE_TEST'},
        {'test_id': 3, 'text': 'wEiRdTeSt'}
    ]

    etalon: ops.TRowsIterable = [
        {'test_id': 1, 'text': 'camelcasetest'},
        {'test_id': 2, 'text': 'upper_case_test'},
        {'test_id': 3, 'text': 'weirdtest'}
    ]

    result = ops.Map(ops.LowerCase(column='text'))(tests)

    assert etalon == list(result)


def test_filtering_punctuation() -> None:
    tests: ops.TRowsIterable = [
        {'test_id': 1, 'text': 'Hello, world!'},
        {'test_id': 2, 'text': 'Test. with. a. lot. of. dots.'},
        {'test_id': 3, 'text': r'!"#$%&\'()*+,-./:;<=>?@[\]^_`{|}~'}
    ]

    etalon: ops.TRowsIterable = [
        {'test_id': 1, 'text': 'Hello world'},
        {'test_id': 2, 'text': 'Test with a lot of dots'},
        {'test_id': 3, 'text': ''}
    ]

    result = ops.Map(ops.FilterPunctuation(column='text'))(tests)

    assert etalon == list(result)


def test_splitting() -> None:
    tests: ops.TRowsIterable = [
        {'test_id': 1, 'text': 'one two three'},
        {'test_id': 2, 'text': 'tab\tsplitting\ttest'},
        {'test_id': 3, 'text': 'more\nlines\ntest'},
        {'test_id': 4, 'text': 'tricky\u00A0test'}
    ]

    etalon: ops.TRowsIterable = [
        {'test_id': 1, 'text': 'one'},
        {'test_id': 1, 'text': 'three'},
        {'test_id': 1, 'text': 'two'},

        {'test_id': 2, 'text': 'splitting'},
        {'test_id': 2, 'text': 'tab'},
        {'test_id': 2, 'text': 'test'},

        {'test_id': 3, 'text': 'lines'},
        {'test_id': 3, 'text': 'more'},
        {'test_id': 3, 'text': 'test'},

        {'test_id': 4, 'text': 'test'},
        {'test_id': 4, 'text': 'tricky'}
    ]

    result = ops.Map(ops.Split(column='text'))(tests)

    assert etalon == sorted(result, key=itemgetter('test_id', 'text'))


def test_product() -> None:
    tests: ops.TRowsIterable = [
        {'test_id': 1, 'speed': 5, 'distance': 10},
        {'test_id': 2, 'speed': 60, 'distance': 2},
        {'test_id': 3, 'speed': 3, 'distance': 15},
        {'test_id': 4, 'speed': 100, 'distance': 0.5},
        {'test_id': 5, 'speed': 48, 'distance': 15},
    ]

    etalon: ops.TRowsIterable = [
        {'test_id': 1, 'speed': 5, 'distance': 10, 'time': 50},
        {'test_id': 2, 'speed': 60, 'distance': 2, 'time': 120},
        {'test_id': 3, 'speed': 3, 'distance': 15, 'time': 45},
        {'test_id': 4, 'speed': 100, 'distance': 0.5, 'time': 50},
        {'test_id': 5, 'speed': 48, 'distance': 15, 'time': 720},
    ]

    result = ops.Map(ops.Product(columns=['speed', 'distance'], result_column='time'))(tests)

    assert etalon == list(result)


def test_filter() -> None:
    tests: ops.TRowsIterable = [
        {'test_id': 1, 'f': 0, 'g': 0},
        {'test_id': 2, 'f': 0, 'g': 1},
        {'test_id': 3, 'f': 1, 'g': 0},
        {'test_id': 4, 'f': 1, 'g': 1}
    ]

    etalon: ops.TRowsIterable = [
        {'test_id': 2, 'f': 0, 'g': 1},
        {'test_id': 3, 'f': 1, 'g': 0}
    ]

    def xor(row: ops.TRow) -> bool:
        return row['f'] ^ row['g']

    result = ops.Map(ops.Filter(condition=xor))(tests)

    assert etalon == list(result)


def test_projection() -> None:
    tests: ops.TRowsIterable = [
        {'test_id': 1, 'junk': 'x', 'value': 42},
        {'test_id': 2, 'junk': 'y', 'value': 1},
        {'test_id': 3, 'junk': 'z', 'value': 144}
    ]

    etalon: ops.TRowsIterable = [
        {'value': 42},
        {'value': 1},
        {'value': 144}
    ]

    result = ops.Map(ops.Project(columns=['value']))(tests)

    assert etalon == list(result)


def test_dummy_reduce() -> None:
    tests: ops.TRowsIterable = [
        {'test_id': 1, 'text': 'hello, world'},
        {'test_id': 2, 'text': 'bye!'}
    ]

    result = ops.Reduce(ops.FirstReducer(), keys=['test_id'])(tests)

    assert tests == list(result)


def test_top_n() -> None:
    matches: ops.TRowsIterable = [
        {'match_id': 1, 'player_id': 1, 'rank': 42},
        {'match_id': 1, 'player_id': 2, 'rank': 7},
        {'match_id': 1, 'player_id': 3, 'rank': 0},
        {'match_id': 1, 'player_id': 4, 'rank': 39},

        {'match_id': 2, 'player_id': 5, 'rank': 15},
        {'match_id': 2, 'player_id': 6, 'rank': 39},
        {'match_id': 2, 'player_id': 7, 'rank': 27},
        {'match_id': 2, 'player_id': 8, 'rank': 7}
    ]

    etalon: ops.TRowsIterable = [
        {'match_id': 1, 'player_id': 1, 'rank': 42},
        {'match_id': 1, 'player_id': 2, 'rank': 7},
        {'match_id': 1, 'player_id': 4, 'rank': 39},

        {'match_id': 2, 'player_id': 5, 'rank': 15},
        {'match_id': 2, 'player_id': 6, 'rank': 39},
        {'match_id': 2, 'player_id': 7, 'rank': 27}
    ]

    presorted_matches = sorted(matches, key=itemgetter('match_id'))
    result = ops.Reduce(ops.TopN(column='rank', n=3), keys=['match_id'])(presorted_matches)

    assert etalon == sorted(result, key=itemgetter('match_id', 'player_id'))


def test_term_frequency() -> None:
    docs: ops.TRowsIterable = [
        {'doc_id': 1, 'text': 'hello', 'count': 1},
        {'doc_id': 1, 'text': 'little', 'count': 1},
        {'doc_id': 1, 'text': 'world', 'count': 1},

        {'doc_id': 2, 'text': 'little', 'count': 1},

        {'doc_id': 3, 'text': 'little', 'count': 3},
        {'doc_id': 3, 'text': 'little', 'count': 3},
        {'doc_id': 3, 'text': 'little', 'count': 3},

        {'doc_id': 4, 'text': 'little', 'count': 2},
        {'doc_id': 4, 'text': 'hello', 'count': 1},
        {'doc_id': 4, 'text': 'little', 'count': 2},
        {'doc_id': 4, 'text': 'world', 'count': 1},

        {'doc_id': 5, 'text': 'hello', 'count': 2},
        {'doc_id': 5, 'text': 'hello', 'count': 2},
        {'doc_id': 5, 'text': 'world', 'count': 1},

        {'doc_id': 6, 'text': 'world', 'count': 4},
        {'doc_id': 6, 'text': 'world', 'count': 4},
        {'doc_id': 6, 'text': 'world', 'count': 4},
        {'doc_id': 6, 'text': 'world', 'count': 4},
        {'doc_id': 6, 'text': 'hello', 'count': 1}
    ]

    etalon: ops.TRowsIterable = [
        {'doc_id': 1, 'text': 'hello', 'tf': approx(0.3333, abs=0.001)},
        {'doc_id': 1, 'text': 'little', 'tf': approx(0.3333, abs=0.001)},
        {'doc_id': 1, 'text': 'world', 'tf': approx(0.3333, abs=0.001)},

        {'doc_id': 2, 'text': 'little', 'tf': approx(1.0)},

        {'doc_id': 3, 'text': 'little', 'tf': approx(1.0)},

        {'doc_id': 4, 'text': 'hello', 'tf': approx(0.25)},
        {'doc_id': 4, 'text': 'little', 'tf': approx(0.5)},
        {'doc_id': 4, 'text': 'world', 'tf': approx(0.25)},

        {'doc_id': 5, 'text': 'hello', 'tf': approx(0.666, abs=0.001)},
        {'doc_id': 5, 'text': 'world', 'tf': approx(0.333, abs=0.001)},

        {'doc_id': 6, 'text': 'hello', 'tf': approx(0.2)},
        {'doc_id': 6, 'text': 'world', 'tf': approx(0.8)}
    ]

    presorted_docs = sorted(docs, key=itemgetter('doc_id'))  # !!!
    result = ops.Reduce(ops.TermFrequency(words_column='text'), keys=['doc_id'])(presorted_docs)

    assert etalon == sorted(result, key=itemgetter('doc_id', 'text'))


def test_counting() -> None:
    sentences: ops.TRowsIterable = [
        {'sentence_id': 1, 'word': 'hello'},
        {'sentence_id': 1, 'word': 'my'},
        {'sentence_id': 1, 'word': 'little'},
        {'sentence_id': 1, 'word': 'world'},

        {'sentence_id': 2, 'word': 'hello'},
        {'sentence_id': 2, 'word': 'my'},
        {'sentence_id': 2, 'word': 'little'},
        {'sentence_id': 2, 'word': 'little'},
        {'sentence_id': 2, 'word': 'hell'}
    ]

    etalon: ops.TRowsIterable = [
        {'count': 1, 'word': 'hell'},
        {'count': 1, 'word': 'world'},
        {'count': 2, 'word': 'hello'},
        {'count': 2, 'word': 'my'},
        {'count': 3, 'word': 'little'}
    ]

    presorted_words = sorted(sentences, key=itemgetter('word'))  # !!!
    result = ops.Reduce(ops.Count(column='count'), keys=['word'])(presorted_words)

    assert etalon == sorted(result, key=itemgetter('count', 'word'))


def test_sum() -> None:
    matches: ops.TRowsIterable = [
        {'match_id': 1, 'player_id': 1, 'score': 42},
        {'match_id': 1, 'player_id': 2, 'score': 7},
        {'match_id': 1, 'player_id': 3, 'score': 0},
        {'match_id': 1, 'player_id': 4, 'score': 39},

        {'match_id': 2, 'player_id': 5, 'score': 15},
        {'match_id': 2, 'player_id': 6, 'score': 39},
        {'match_id': 2, 'player_id': 7, 'score': 27},
        {'match_id': 2, 'player_id': 8, 'score': 7}
    ]

    etalon: ops.TRowsIterable = [
        {'match_id': 1, 'score': 88},
        {'match_id': 2, 'score': 88}
    ]

    presorted_matches = sorted(matches, key=itemgetter('match_id'))  # !!!
    result = ops.Reduce(ops.Sum(column='score'), keys=['match_id'])(presorted_matches)

    assert etalon == sorted(result, key=itemgetter('match_id'))


def test_mean() -> None:
    matches: ops.TRowsIterable = [
        {'match_id': 1, 'player_id': 1, 'score': 42},
        {'match_id': 1, 'player_id': 2, 'score': 7},
        {'match_id': 1, 'player_id': 3, 'score': 0},
        {'match_id': 1, 'player_id': 4, 'score': 39},

        {'match_id': 2, 'player_id': 5, 'score': 15},
        {'match_id': 2, 'player_id': 6, 'score': 39},
        {'match_id': 2, 'player_id': 7, 'score': 27},
        {'match_id': 2, 'player_id': 8, 'score': 7}
    ]

    etalon: ops.TRowsIterable = [
        {'match_id': 1, 'score': 22},
        {'match_id': 2, 'score': 22}
    ]

    presorted_matches = sorted(matches, key=itemgetter('match_id'))  # !!!
    result = ops.Reduce(ops.Mean(column='score'), keys=['match_id'])(presorted_matches)

    assert etalon == sorted(result, key=itemgetter('match_id'))


def test_speed() -> None:
    races: ops.TRowsIterable = [
        {'race_id': 1, 'length': 10, 'time': 0.2},
        {'race_id': 2, 'length': 5, 'time': 0.2},
        {'race_id': 3, 'length': 20, 'time': 0.2},
        {'race_id': 4, 'length': 10, 'time': 2},
        {'race_id': 5, 'length': 10, 'time': 20}
    ]

    etalon: ops.TRowsIterable = [
        {'race_id': 1, 'length': 10, 'time': 0.2, 'speed': 50},
        {'race_id': 2, 'length': 5, 'time': 0.2, 'speed': 25},
        {'race_id': 3, 'length': 20, 'time': 0.2, 'speed': 100},
        {'race_id': 4, 'length': 10, 'time': 2, 'speed': 5},
        {'race_id': 5, 'length': 10, 'time': 20, 'speed': 0.5}
    ]

    presorted_races = sorted(races, key=itemgetter('race_id'))  # !!!
    result = ops.Map(ops.Speed(dt_column='time', length_column='length', result_column='speed'))(presorted_races)

    assert etalon == sorted(result, key=itemgetter('race_id'))


def test_format_date() -> None:
    visitors: ops.TRowsIterable = [
        {'id': 1, 'date': '20171020T112238.723000'},
        {'id': 2, 'date': '20171011T145553.040000'},
        {'id': 3, 'date': '20171020T090548.939000'},
        {'id': 4, 'date': '20171020T090548'},
        {'id': 5, 'date': '20171022T131828.330000'}
    ]

    etalon: ops.TRowsIterable = [
        {'id': 1, 'date': '20171020T112238.723000',
         'datetime': dt.strptime('20171020T112238.723000', "%Y%m%dT%H%M%S.%f")},
        {'id': 2, 'date': '20171011T145553.040000',
         'datetime': dt.strptime('20171011T145553.040000', "%Y%m%dT%H%M%S.%f")},
        {'id': 3, 'date': '20171020T090548.939000',
         'datetime': dt.strptime('20171020T090548.939000', "%Y%m%dT%H%M%S.%f")},
        {'id': 4, 'date': '20171020T090548',
         'datetime': dt.strptime('20171020T090548', "%Y%m%dT%H%M%S")},
        {'id': 5, 'date': '20171022T131828.330000',
         'datetime': dt.strptime('20171022T131828.330000', "%Y%m%dT%H%M%S.%f")}
    ]

    presorted_visitors = sorted(visitors, key=itemgetter('id'))  # !!!
    result = ops.Map(ops.FormatDate(date_column='date', result_column='datetime'))(presorted_visitors)

    assert etalon == sorted(result, key=itemgetter('id'))


def test_week_day() -> None:
    visitors: ops.TRowsIterable = [
        {'id': 1, 'datetime': dt.strptime('20200429T112238.723000', "%Y%m%dT%H%M%S.%f")},
        {'id': 2, 'datetime': dt.strptime('20200430T145553.040000', "%Y%m%dT%H%M%S.%f")},
        {'id': 3, 'datetime': dt.strptime('20200501T090548.939000', "%Y%m%dT%H%M%S.%f")},
        {'id': 4, 'datetime': dt.strptime('20200502T090548', "%Y%m%dT%H%M%S")},
        {'id': 5, 'datetime': dt.strptime('20200503T131828.330000', "%Y%m%dT%H%M%S.%f")}
    ]

    etalon: ops.TRowsIterable = [
        {'id': 1, 'datetime': dt.strptime('20200429T112238.723000', "%Y%m%dT%H%M%S.%f"), 'weekday': 'Wed'},
        {'id': 2, 'datetime': dt.strptime('20200430T145553.040000', "%Y%m%dT%H%M%S.%f"), 'weekday': 'Thu'},
        {'id': 3, 'datetime': dt.strptime('20200501T090548.939000', "%Y%m%dT%H%M%S.%f"), 'weekday': 'Fri'},
        {'id': 4, 'datetime': dt.strptime('20200502T090548', "%Y%m%dT%H%M%S"), 'weekday': 'Sat'},
        {'id': 5, 'datetime': dt.strptime('20200503T131828.330000', "%Y%m%dT%H%M%S.%f"), 'weekday': 'Sun'}
    ]

    presorted_visitors = sorted(visitors, key=itemgetter('id'))  # !!!
    result = ops.Map(ops.WeekDay(date_column='datetime', result_column='weekday'))(presorted_visitors)

    assert etalon == sorted(result, key=itemgetter('id'))


def test_hour() -> None:
    visitors: ops.TRowsIterable = [
        {'id': 1, 'datetime': dt.strptime('20200429T112238.723000', "%Y%m%dT%H%M%S.%f")},
        {'id': 2, 'datetime': dt.strptime('20200430T145553.040000', "%Y%m%dT%H%M%S.%f")},
        {'id': 3, 'datetime': dt.strptime('20200501T090548.939000', "%Y%m%dT%H%M%S.%f")},
        {'id': 4, 'datetime': dt.strptime('20200502T090548', "%Y%m%dT%H%M%S")},
        {'id': 5, 'datetime': dt.strptime('20200503T131828.330000', "%Y%m%dT%H%M%S.%f")}
    ]

    etalon: ops.TRowsIterable = [
        {'id': 1, 'datetime': dt.strptime('20200429T112238.723000', "%Y%m%dT%H%M%S.%f"), 'hour': 11},
        {'id': 2, 'datetime': dt.strptime('20200430T145553.040000', "%Y%m%dT%H%M%S.%f"), 'hour': 14},
        {'id': 3, 'datetime': dt.strptime('20200501T090548.939000', "%Y%m%dT%H%M%S.%f"), 'hour': 9},
        {'id': 4, 'datetime': dt.strptime('20200502T090548', "%Y%m%dT%H%M%S"), 'hour': 9},
        {'id': 5, 'datetime': dt.strptime('20200503T131828.330000', "%Y%m%dT%H%M%S.%f"), 'hour': 13}
    ]

    presorted_visitors = sorted(visitors, key=itemgetter('id'))  # !!!
    result = ops.Map(ops.Hour(date_column='datetime', result_column='hour'))(presorted_visitors)

    assert etalon == sorted(result, key=itemgetter('id'))


def test_delta_time() -> None:
    visitors: ops.TRowsIterable = [
        {'id': 1,
         'start': dt(year=2019, month=6, day=16, hour=6, minute=33, second=26),
         'end': dt(year=2014, month=4, day=3, hour=12, minute=3, second=5)},
        {'id': 2,
         'start': dt(year=2022, month=11, day=10, hour=17, minute=45, second=56),
         'end': dt(year=2032, month=12, day=13, hour=2, minute=4, second=12)}
    ]

    etalon: ops.TRowsIterable = [
        {'id': 1,
         'start': dt(year=2019, month=6, day=16, hour=6, minute=33, second=26),
         'end': dt(year=2014, month=4, day=3, hour=12, minute=3, second=5),
         'dt': (dt(year=2014, month=4, day=3, hour=12, minute=3, second=5) -
                dt(year=2019, month=6, day=16, hour=6, minute=33, second=26)).total_seconds()/(60**2)},
        {'id': 2,
         'start': dt(year=2022, month=11, day=10, hour=17, minute=45, second=56),
         'end': dt(year=2032, month=12, day=13, hour=2, minute=4, second=12),
         'dt': (dt(year=2032, month=12, day=13, hour=2, minute=4, second=12) -
                dt(year=2022, month=11, day=10, hour=17, minute=45, second=56)).total_seconds()/(60**2)}
    ]

    presorted_visitors = sorted(visitors, key=itemgetter('id'))  # !!!
    result = ops.Map(ops.DeltaTime(enter_time='start', leave_time='end', result_column='dt'))(presorted_visitors)

    assert etalon == sorted(result, key=itemgetter('id'))


def test_idf() -> None:
    data: ops.TRowsIterable = [
        {'id': 1, '1': 10, '2': 100},
        {'id': 2, '1': 30, '2': 14},
        {'id': 3, '1': 13, '2': 0.7}
    ]

    etalon: ops.TRowsIterable = [
        {'id': 1, '1': 10, '2': 100, 'idf': log(10/100)},
        {'id': 2, '1': 30, '2': 14, 'idf': log(30/14)},
        {'id': 3, '1': 13, '2': 0.7, 'idf': log(13/0.7)}
    ]

    presorte_data = sorted(data, key=itemgetter('id'))
    result = ops.Map(ops.Idf('1', '2', 'idf'))(presorte_data)

    assert etalon == sorted(result, key=itemgetter('id'))


def test_simple_join() -> None:
    players: ops.TRowsIterable = [
        {'player_id': 1, 'username': 'XeroX'},
        {'player_id': 2, 'username': 'jay'},
        {'player_id': 3, 'username': 'Destroyer'},
    ]

    games: ops.TRowsIterable = [
        {'game_id': 1, 'player_id': 3, 'score': 99},
        {'game_id': 2, 'player_id': 1, 'score': 17},
        {'game_id': 3, 'player_id': 1, 'score': 22}
    ]

    etalon: ops.TRowsIterable = [
        {'game_id': 1, 'player_id': 3, 'score': 99, 'username': 'Destroyer'},
        {'game_id': 2, 'player_id': 1, 'score': 17, 'username': 'XeroX'},
        {'game_id': 3, 'player_id': 1, 'score': 22, 'username': 'XeroX'}
    ]

    presorted_games = sorted(games, key=itemgetter('player_id'))    # !!!
    presorted_players = sorted(players, key=itemgetter('player_id'))  # !!!
    result = ops.Join(ops.InnerJoiner(), keys=['player_id'])(presorted_games, presorted_players)
    # print(sorted(result, key=itemgetter('game_id')))
    assert etalon == sorted(result, key=itemgetter('game_id'))


def test_inner_join() -> None:
    players: ops.TRowsIterable = [
        {'player_id': 0, 'username': 'root'},
        {'player_id': 1, 'username': 'XeroX'},
        {'player_id': 2, 'username': 'jay'}
    ]

    games: ops.TRowsIterable = [
        {'game_id': 1, 'player_id': 3, 'score': 9999999},
        {'game_id': 2, 'player_id': 1, 'score': 17},
        {'game_id': 3, 'player_id': 2, 'score': 22}
    ]

    etalon: ops.TRowsIterable = [
        # player 3 is unknown
        # no games for player 0
        {'game_id': 2, 'player_id': 1, 'score': 17, 'username': 'XeroX'},
        {'game_id': 3, 'player_id': 2, 'score': 22, 'username': 'jay'}
    ]

    presorted_games = sorted(games, key=itemgetter('player_id'))    # !!!
    presorted_players = sorted(players, key=itemgetter('player_id'))  # !!!
    result = ops.Join(ops.InnerJoiner(), keys=['player_id'])(presorted_games, presorted_players)

    assert etalon == sorted(result, key=itemgetter('game_id'))


def test_outer_join() -> None:
    players: ops.TRowsIterable = [
        {'player_id': 0, 'username': 'root'},
        {'player_id': 1, 'username': 'XeroX'},
        {'player_id': 2, 'username': 'jay'}
    ]

    games: ops.TRowsIterable = [
        {'game_id': 1, 'player_id': 3, 'score': 9999999},
        {'game_id': 2, 'player_id': 1, 'score': 17},
        {'game_id': 3, 'player_id': 2, 'score': 22}
    ]

    etalon: ops.TRowsIterable = [
        {'player_id': 0, 'username': 'root'},              # no such game
        {'game_id': 1, 'player_id': 3, 'score': 9999999},  # no such player
        {'game_id': 2, 'player_id': 1, 'score': 17, 'username': 'XeroX'},
        {'game_id': 3, 'player_id': 2, 'score': 22, 'username': 'jay'}
    ]

    presorted_games = sorted(games, key=itemgetter('player_id'))    # !!!
    presorted_players = sorted(players, key=itemgetter('player_id'))  # !!!
    result = ops.Join(ops.OuterJoiner(), keys=['player_id'])(presorted_games, presorted_players)

    assert etalon == sorted(result, key=lambda x: x.get('game_id', -1))


def test_left_join() -> None:
    players: ops.TRowsIterable = [
        {'player_id': 0, 'username': 'root'},
        {'player_id': 1, 'username': 'XeroX'},
        {'player_id': 2, 'username': 'jay'}
    ]

    games: ops.TRowsIterable = [
        {'game_id': 1, 'player_id': 3, 'score': 0},
        {'game_id': 2, 'player_id': 1, 'score': 17},
        {'game_id': 3, 'player_id': 2, 'score': 22},
        {'game_id': 4, 'player_id': 2, 'score': 41}
    ]

    etalon: ops.TRowsIterable = [
        # ignore player 0 with 0 games
        {'game_id': 1, 'player_id': 3, 'score': 0},  # unknown player 3
        {'game_id': 2, 'player_id': 1, 'score': 17, 'username': 'XeroX'},
        {'game_id': 3, 'player_id': 2, 'score': 22, 'username': 'jay'},
        {'game_id': 4, 'player_id': 2, 'score': 41, 'username': 'jay'}
    ]

    presorted_games = sorted(games, key=itemgetter('player_id'))    # !!!
    presorted_players = sorted(players, key=itemgetter('player_id'))  # !!!
    result = ops.Join(ops.LeftJoiner(), keys=['player_id'])(presorted_games, presorted_players)

    assert etalon == sorted(result, key=itemgetter('game_id'))


def test_right_join() -> None:
    players: ops.TRowsIterable = [
        {'player_id': 0, 'username': 'root'},
        {'player_id': 1, 'username': 'XeroX'},
        {'player_id': 2, 'username': 'jay'}
    ]

    games: ops.TRowsIterable = [
        {'game_id': 1, 'player_id': 3, 'score': 0},
        {'game_id': 2, 'player_id': 1, 'score': 17},
        {'game_id': 3, 'player_id': 2, 'score': 22},
        {'game_id': 4, 'player_id': 2, 'score': 41},
        {'game_id': 5, 'player_id': 1, 'score': 34}
    ]

    etalon: ops.TRowsIterable = [
        # ignore game with unknown player 3
        {'player_id': 0, 'username': 'root'},  # no games for root
        {'game_id': 2, 'player_id': 1, 'score': 17, 'username': 'XeroX'},
        {'game_id': 3, 'player_id': 2, 'score': 22, 'username': 'jay'},
        {'game_id': 4, 'player_id': 2, 'score': 41, 'username': 'jay'},
        {'game_id': 5, 'player_id': 1, 'score': 34, 'username': 'XeroX'}
    ]

    presorted_games = sorted(games, key=itemgetter('player_id'))    # !!!
    presorted_players = sorted(players, key=itemgetter('player_id'))  # !!!
    result = ops.Join(ops.RightJoiner(), keys=['player_id'])(presorted_games, presorted_players)

    assert etalon == sorted(result, key=lambda x: x.get('game_id', -1))


def test_simple_join_with_collision() -> None:
    players: ops.TRowsIterable = [
        {'player_id': 1, 'username': 'XeroX', 'score': 400},
        {'player_id': 2, 'username': 'jay', 'score': 451},
        {'player_id': 3, 'username': 'Destroyer', 'score': 999},
    ]

    games: ops.TRowsIterable = [
        {'game_id': 1, 'player_id': 3, 'score': 99},
        {'game_id': 2, 'player_id': 1, 'score': 17},
        {'game_id': 3, 'player_id': 1, 'score': 22}
    ]

    etalon: ops.TRowsIterable = [
        {'game_id': 1, 'player_id': 3, 'score_game': 99, 'score_max': 999, 'username': 'Destroyer'},
        {'game_id': 2, 'player_id': 1, 'score_game': 17, 'score_max': 400, 'username': 'XeroX'},
        {'game_id': 3, 'player_id': 1, 'score_game': 22, 'score_max': 400, 'username': 'XeroX'}
    ]

    presorted_games = sorted(games, key=itemgetter('player_id'))    # !!!
    presorted_players = sorted(players, key=itemgetter('player_id'))  # !!!
    result = ops.Join(ops.InnerJoiner(suffix_a='_game', suffix_b='_max'),
                      keys=['player_id'])(presorted_games, presorted_players)

    assert etalon == sorted(result, key=itemgetter('game_id'))
