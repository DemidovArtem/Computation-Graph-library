from abc import ABC, abstractmethod
from heapq import nlargest
from itertools import groupby
import typing as tp
import string
import math
import datetime

TRow = tp.Dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


# Operations


class Mapper(ABC):
    """Base class for mappers"""
    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in rows:
            for new_row in self.mapper(row):
                yield new_row


class Reducer(ABC):
    """Base class for reducers"""
    @abstractmethod
    def __call__(self, group_key: tp.Sequence[str], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


def key_func_maker(keys: tp.Sequence[str]) -> tp.Callable[[tp.Dict[str, tp.Any]], tp.Tuple[tp.Any, ...]]:
    f_keys = keys

    def key_func(row: tp.Dict[str, tp.Any]) -> tp.Tuple[tp.Any, ...]:
        return get_key_value(f_keys, row)
    return key_func


class Reduce(Operation):
    """Class that implement reduce operation"""
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        """
        :param reducer: used reducer
        :param keys: column names for reducer
        """
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        for key, group in groupby(rows, key_func_maker(self.keys)):
            for row in self.reducer(self.keys, group):
                yield row


class Joiner(ABC):
    """Base class for joiners"""
    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        """
        :param suffix_a: suffix for first table columns:
        :param suffix_b: suffix for second table columns:
        """
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass

    def join_row_pair(self, keys: tp.Sequence[str], row_a: tp.Dict[str, tp.Any], row_b: tp.Dict[str, tp.Any]) \
            -> tp.Dict[str, tp.Any]:
        """
        :param keys: name of columns to use for join
        :param row_a: row of left table
        :param row_b: row of right table
        """
        new_row: tp.Dict[str, tp.Any] = {}
        for key in row_a.keys():
            if key not in keys and key in row_b.keys():
                new_row[key + self._a_suffix] = row_a[key]
                new_row[key + self._b_suffix] = row_b[key]
            else:
                new_row[key] = row_a[key]
        for key in row_b.keys():
            if key not in row_a.keys():
                new_row[key] = row_b[key]
        return new_row

    def join_rows(self, keys: tp.Sequence[str],
                  rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: name of columns to use for join
        :param rows_a: set of rows of left table with the same value in columns from keys
        :param rows_b: set of rows of right table with the same value in columns from keys
        """
        if not rows_a:
            for row in rows_b:
                yield row
        elif not rows_b:
            for row in rows_a:
                yield row
        else:
            for row_a in rows_a:
                for row_b in rows_b:
                    yield self.join_row_pair(keys, row_a, row_b)


class Join(Operation):
    """Class that implement join operation"""
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        """
        :param keys: name of columns for join
        :param joiner: joiner with particular strategy
        """
        self.keys = keys
        self.joiner = joiner

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param rows: left table with data
        :param args: contain right table with data
        """

        iterator_a = groupby(rows, key_func_maker(self.keys))
        iterator_b = groupby(args[0], key_func_maker(self.keys))
        key_a, group_a = get_next(iterator_a)
        key_b, group_b = get_next(iterator_b)

        while key_a is not None or key_b is not None:
            if not (key_a is None) and (key_b is None or key_a < key_b):
                for row in self.joiner(self.keys, group_a, []):
                    yield row
                key_a, group_a = get_next(iterator_a)
            elif key_a is None or key_b < key_a:
                for row in self.joiner(self.keys, [], group_b):
                    yield row
                key_b, group_b = get_next(iterator_b)
            else:
                for row in self.joiner(self.keys, list(group_a), list(group_b)):
                    yield row
                key_a, group_a = get_next(iterator_a)
                key_b, group_b = get_next(iterator_b)


# Dummy operators


class DummyMapper(Mapper):
    """Yield exactly the row passed"""
    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""
    def __call__(self, group_key: tp.Sequence[str], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            yield row
            break


# Mappers


class Idf(Mapper):
    """Add idf column log(row[col_1]/row[col_2])"""
    def __init__(self, total_doc_count: str, doc_with_word_count: str, result_column: str = 'idf'):
        """
        :param total_doc_count: number of words in all docs
        :param doc_with_word_count: number of word in particular doc
        :param result_column: name of column to write idf
        """
        self.total_doc_count = total_doc_count
        self.doc_with_word_count = doc_with_word_count
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.result_column] = math.log(row[self.total_doc_count]/row[self.doc_with_word_count])
        yield row


class FormatDate(Mapper):
    """Add column with datetime converted from string"""
    def __init__(self, date_column: str, result_column: str = 'date'):
        """
        :param date_column: name of column with datetime in string format
        :param result_column: name of column to write format datetime in
        """
        self.date_column = date_column
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        date = row[self.date_column]
        if '.' not in date:
            date += '.0'
        row[self.result_column] = datetime.datetime.strptime(date, "%Y%m%dT%H%M%S.%f")
        yield row


class WeekDay(Mapper):
    """Add column with weekday of datetime column"""
    def __init__(self, date_column: str, result_column: str = 'day'):
        """
        :param date_column: name of column with date in datetime format
        :param result_column: name of column to write weekday in
        """
        self.date_column = date_column
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.result_column] = row[self.date_column].strftime("%A")[:3]
        yield row


class Hour(Mapper):
    """Add column with hour of datetime column"""
    def __init__(self, date_column: str, result_column: str = 'hour'):
        """
        :param date_column: name of column with date in datetime format
        :param result_column: name of column to write hour in
        """
        self.date_column = date_column
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.result_column] = row[self.date_column].hour
        yield row


class DeltaTime(Mapper):
    """Add column with delta time for datetime columns in hours"""
    def __init__(self, enter_time: str, leave_time: str, result_column: str = 'dt'):
        """
        :param enter_time: name of column with start time in datetime format
        :param leave_time: name of column with end time in datetime format
        :param result_column: name of column to write delta time in
        """
        self.enter_time = enter_time
        self.leave_time = leave_time
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.result_column] = (row[self.leave_time] - row[self.enter_time]).total_seconds() / (60 ** 2)
        yield row


class Length(Mapper):
    """
    Add column with distance between two points on Earth in km.
    Points position defined by column with [longitude, latitude]
    """
    def __init__(self, start_column: str, end_column: str, length_column: str):
        """
        :param start_column: name of column with position of start point
        :param end_column: name of column with position of end point
        :param length_column: name of column to write calculated length in
        """
        self.start_column = start_column
        self.end_column = end_column
        self.length_column = length_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        lon_1, lat_1, lon_2, lat_2 = map(math.radians, [*row[self.start_column], *row[self.end_column]])
        d_lon = lon_2 - lon_1
        d_lat = lat_2 - lat_1
        a = math.sin(d_lat/2)**2 + math.cos(lat_1) * math.cos(lat_2) * math.sin(d_lon/2)**2
        angle = 2 * math.asin(math.sqrt(a))
        radius = 6371
        row[self.length_column] = radius * angle
        yield row


class Speed(Mapper):
    """Calculate speed from path length and path time in km/h"""
    def __init__(self, length_column: str, dt_column: str, result_column: str):
        """
        :param length_column: name of column with length of path
        :param dt_column: name of column with spent time
        :param result_column: name of column to write speed in
        """
        self.length_column = length_column
        self.dt_column = dt_column
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.result_column] = row[self.length_column] / (row[self.dt_column])
        yield row


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = row[self.column].translate(str.maketrans('', '', string.punctuation))
        yield row


class LowerCase(Mapper):
    """Replace column value with value in lower case"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        return txt.lower()

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = self._lower_case(row[self.column])
        yield row


class Split(Mapper):
    """Split row on multiple rows by separator"""
    def __init__(self, column: str, separator: tp.Optional[str] = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator

    def __call__(self, row: TRow) -> TRowsGenerator:
        for sub_str in row[self.column].split(self.separator):
            new_row = row.copy()
            new_row[self.column] = sub_str
            yield new_row


class Product(Mapper):
    """Calculates product of multiple columns"""
    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        res = 1.
        for col in self.columns:
            res *= row[col]
        row[self.result_column] = res
        yield row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""
    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""
    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {col: row[col] for col in self.columns}


# Reducers


class TopN(Reducer):
    """Calculate top N by value"""
    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max = column
        self.n = n

    def __call__(self, group_key: tp.Sequence[str], rows: TRowsIterable) -> TRowsGenerator:
        for row in nlargest(self.n, rows, lambda row_val: row_val[self.column_max]):
            yield row


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""
    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tp.Sequence[str], rows: TRowsIterable) -> TRowsGenerator:
        words_count: tp.Dict[str, int] = {}
        row_number = 0
        first_raw: TRow = {}
        for row in rows:
            if not first_raw:
                first_raw = row.copy()
            row_number += 1
            if row[self.words_column] in words_count.keys():
                words_count[row[self.words_column]] += 1
            else:
                words_count[row[self.words_column]] = 1

        for word in words_count:
            new_row: tp.Dict[str, tp.Any] = {}
            for key in group_key:
                new_row[key] = first_raw[key]
            new_row[self.words_column] = word
            new_row[self.result_column] = words_count[word] / row_number
            yield new_row


class Count(Reducer):
    """Count rows passed and yield single row as a result"""
    def __init__(self, column: str) -> None:
        """
        :param column: name of column to count
        """
        self.column = column

    def __call__(self, group_key: tp.Sequence[str], rows: TRowsIterable) -> TRowsGenerator:
        row_number: int = 0
        new_row: TRow = {}
        first_row: TRow = {}
        for row in rows:
            if not first_row:
                first_row = row.copy()
            row_number += 1

        for key in first_row:
            if key in group_key:
                new_row[key] = first_row[key]

        new_row[self.column] = row_number
        yield new_row


class Sum(Reducer):
    """Sum values in column passed and yield single row as a result"""
    def __init__(self, column: str) -> None:
        """
        :param column: name of column to sum
        """
        self.column = column

    def __call__(self, group_key: tp.Sequence[str], rows: TRowsIterable) -> TRowsGenerator:
        sum_value: int = 0
        new_row: TRow = {}
        first_row: TRow = {}
        for row in rows:
            if not first_row:
                first_row = row.copy()
            sum_value += row[self.column]
        for key in first_row:
            if key in group_key:
                new_row[key] = first_row[key]
        new_row[self.column] = sum_value
        yield new_row


class Mean(Reducer):
    """Find mean value in column passed and yield single row as a result"""
    def __init__(self, column: str) -> None:
        """
        :param column: name of column to calculate mean
        """
        self.column = column

    def __call__(self, group_key: tp.Sequence[str], rows: TRowsIterable) -> TRowsGenerator:
        sum_value: int = 0
        num_rows = 0
        new_row: TRow = {}
        first_row: TRow = {}
        for row in rows:
            if not first_row:
                first_row = row.copy()
            num_rows += 1
            sum_value += row[self.column]
        for key in first_row:
            if key in group_key:
                new_row[key] = first_row[key]
        new_row[self.column] = sum_value / num_rows
        yield new_row

# Joiners


def check_equal(keys: tp.Sequence[str], row_a: tp.Dict[str, tp.Any], row_b: tp.Dict[str, tp.Any]) -> bool:
    """
    :param keys: names of columns used to compare value in
    :param row_a: row of left table
    :param row_b: row of right table
    """
    return [row_a[key] for key in keys] == [row_b[key] for key in keys]


def get_key_value(keys: tp.Sequence[str], row: tp.Dict[str, tp.Any]) -> tp.Tuple[tp.Any, ...]:
    """
    :param keys: names of columns used to create list of value in them
    :param row: row of table
    """
    return tuple([row[key] for key in keys])


def get_next(iterator):     # type: ignore
    """Tries to get next value of iterator, return Nones if stopped iteration"""
    try:
        return next(iterator)
    except StopIteration:
        return [None, None]


class InnerJoiner(Joiner):
    """Join with inner strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        if rows_a and rows_b:
            for row in self.join_rows(keys, rows_a, rows_b):
                yield row


class OuterJoiner(Joiner):
    """Join with outer strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        for row in self.join_rows(keys, rows_a, rows_b):
            yield row


class LeftJoiner(Joiner):
    """Join with left strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        if rows_a:
            for row in self.join_rows(keys, rows_a, rows_b):
                yield row


class RightJoiner(Joiner):
    """Join with right strategy"""
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        if rows_b:
            for row in self.join_rows(keys, rows_a, rows_b):
                yield row
