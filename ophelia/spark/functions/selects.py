import re
from itertools import chain
from pyspark.sql import DataFrame
from pyspark.sql.column import _to_seq
from pyspark.sql.functions import create_map, lit
from ophelia.spark.utils.list_utils import ListUtils


__all__ = ["Selects", "SelectWrapper"]


class Selects(object):

    def __init__(self, jdf, sql_ctx):
        self._jdf = jdf
        self.sql_ctx = sql_ctx
        self._sc = sql_ctx and sql_ctx._sc

    @staticmethod
    def regex_expr(regex_name):
        if isinstance(regex_name, list):
            return [f'.*{regex}' for regex in regex_name]
        return [f'.*{regex_name}']

    @staticmethod
    def select_regex(self, regex_expr):
        # Todo: es posible llamar el atributo _jdf sin necesidad de quitar el decorador @staticmethod, se remueven por
        # Todo: que producen error de ejecucion por el momento
        # Todo: se deja el codigo muestra de version anterior
        """
        stream = self._jdf.columns
        regex_list = [line for regex in regex_expr for line in stream if re.compile(regex).match(line)]
        clean_regex_list = ListUtils.remove_duplicated_elements(regex_list)
        return DataFrame(self._jdf.select(clean_regex_list), self.sql_ctx)
        """
        stream = self.columns
        regex_list = [line for regex in regex_expr for line in stream if re.compile(regex).match(line)]
        clean_regex_list = ListUtils.remove_duplicate_element(regex_list)
        return self.select(clean_regex_list)

    @staticmethod
    def select_startswith(self, regex):
        cols_list = self.columns
        if isinstance(regex, list):
            return self.select([c for c in cols_list for reg in regex if c.startswith(reg)])
        return self.select([c for c in cols_list if c.startswith(regex)])

    @staticmethod
    def select_endswith(self, regex):
        cols_list = self.columns
        if isinstance(regex, list):
            return self.select([c for c in cols_list for reg in regex if c.endswith(reg)])
        return self.select([c for c in cols_list if c.endswith(regex)])

    @staticmethod
    def select_contains(self, regex):
        if isinstance(regex, list):
            contain_cols = [c for c in self.columns for reg in regex if reg in c]
            clean_regex_list = ListUtils.remove_duplicate_element(contain_cols)
            return self.select(clean_regex_list)
        contain_cols = [c for c in self.columns if regex in c]
        clean_regex_list = ListUtils.remove_duplicate_element(contain_cols)
        return self.select(clean_regex_list)

    @staticmethod
    def sort_columns_asc(self):
        # Todo: es posible llamar el atributo _jdf sin necesidad de quitar el decorador @staticmethod, se remueven por
        # Todo: que producen error de ejecucion por el momento
        # Todo: se deja el codigo muestra de version anterior
        """
        DataFrame(self._jdf.select(self._jdf.schema.fields), self.sql_ctx)

        cols = self._jdf.column
        print("java jdf object", cols)
        print("java jdf type", cols.__class__.__name__)
        return DataFrame(self._jdf.select(self._sc, cols), self.sql_ctx)
        """
        return self.select(*sorted(self.columns))

    @staticmethod
    def select_freqitems(self, cols, support=None):
        if isinstance(cols, tuple):
            cols = list(cols)
        if not isinstance(cols, list):
            raise ValueError("cols must be a list or tuple of column names as strings.")
        if not support:
            support = 0.01
        return DataFrame(self._jdf.stat().freqItems(_to_seq(self._sc, cols), support), self.sql_ctx)

    @staticmethod
    def __type_list(datatype, select_type):
        return [k for k, v in datatype if v in select_type]

    @staticmethod
    def select_strings(self):
        dtype = self.dtypes
        stype = ['string', 'str', 'AnyStr', 'char']
        return Selects.__type_list(datatype=dtype, select_type=stype)

    @staticmethod
    def select_integers(self):
        dtype = self.dtypes
        stype = ['int', 'integer']
        return Selects.__type_list(datatype=dtype, select_type=stype)

    @staticmethod
    def select_floats(self):
        dtype = self.dtypes
        stype = ['float']
        return Selects.__type_list(datatype=dtype, select_type=stype)

    @staticmethod
    def select_doubles(self):
        dtype = self.dtypes
        stype = ['double']
        return Selects.__type_list(datatype=dtype, select_type=stype)

    @staticmethod
    def select_decimals(self):
        dtype = self.dtypes
        stype = ['decimal']
        return Selects.__type_list(datatype=dtype, select_type=stype)

    @staticmethod
    def select_longs(self):
        dtype = self.dtypes
        stype = ['long', 'bigint']
        return Selects.__type_list(datatype=dtype, select_type=stype)

    @staticmethod
    def select_dates(self):
        dtype = self.dtypes
        stype = ['date', 'timestamp']
        return self.select(Selects.__type_list(datatype=dtype, select_type=stype))

    @staticmethod
    def select_complex(self):
        dtype = self.dtypes
        stype = ['complex']
        return self.select(Selects.__type_list(datatype=dtype, select_type=stype))

    @staticmethod
    def select_structs(self):
        dtype = self.dtypes
        stype = ['list', 'tuple', 'array', 'vector']
        return self.select(Selects.__type_list(datatype=dtype, select_type=stype))

    @staticmethod
    def select_categorical(self):
        dtype = self.dtypes
        stype = ['string', 'long', 'bigint']
        return self.select(Selects.__type_list(datatype=dtype, select_type=stype))

    @staticmethod
    def select_numerical(self):
        dtype = self.dtypes
        stype = ['double', 'decimal', 'integer', 'int', 'float', 'complex']
        return self.select(Selects.__type_list(datatype=dtype, select_type=stype))

    def select_features(self, df):
        return {'string': self.select_strings(df),
                'int': self.select_integers(df),
                'long': self.select_longs(df),
                'double': self.select_doubles(df),
                'float': self.select_floats(df),
                'date': self.select_dates(df),
                'complex': self.select_complex(df),
                'struct': self.select_structs(df),
                'categorical': self.select_categorical(df),
                'numeric': self.select_numerical(df)}

    @staticmethod
    def map_item(self, origin_col, map_col, map_val):
        map_expr = create_map([lit(x) for x in chain(*map_val.items())])
        return self.select('*', (map_expr[self[origin_col]]).alias(map_col))


class SelectWrapper(object):

    DataFrame.selectRegex = Selects.select_regex
    DataFrame.selectStartswith = Selects.select_startswith
    DataFrame.selectEndswith = Selects.select_endswith
    DataFrame.selectContains = Selects.select_contains
    DataFrame.sortColAsc = Selects.sort_columns_asc
    DataFrame.selectFreqItems = Selects.select_freqitems
    DataFrame.selectStrings = Selects.select_strings
    DataFrame.selectInts = Selects.select_integers
    DataFrame.selectFloats = Selects.select_floats
    DataFrame.selectDoubles = Selects.select_doubles
    DataFrame.selectDecimals = Selects.select_decimals
    DataFrame.selectLongs = Selects.select_longs
    DataFrame.selectDates = Selects.select_dates
    DataFrame.selectFeatures = Selects.select_features
    DataFrame.mapItem = Selects.map_item
