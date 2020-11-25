import re
from pyspark.sql import DataFrame
from pyspark.sql.column import _to_seq
from ophelia.ophelib.utils.list_utils import ListUtils


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
        clean_regex_list = ListUtils.remove_duplicated_elements(regex_list)
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
            clean_regex_list = ListUtils.remove_duplicated_elements(contain_cols)
            return self.select(clean_regex_list)
        contain_cols = [c for c in self.columns if regex in c]
        clean_regex_list = ListUtils.remove_duplicated_elements(contain_cols)
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


class SelectWrapper(object):

    DataFrame.selectRegex = Selects.select_regex
    DataFrame.selectStartswith = Selects.select_startswith
    DataFrame.selectEndswith = Selects.select_endswith
    DataFrame.selectContains = Selects.select_contains
    DataFrame.sortColAsc = Selects.sort_columns_asc
    DataFrame.select_freqitems = Selects.select_freqitems
