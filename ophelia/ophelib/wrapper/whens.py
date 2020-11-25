from pyspark.sql.functions import col


class Whens(object):

    @staticmethod
    def __list_match_sign(lst):
        return [sign for sign in lst
                if sign == '=='
                or sign == '!='
                or sign == '>='
                or sign == '<='
                or sign == '>'
                or sign == '<']

    @staticmethod
    def __spark_condition_col_dict(w_col, c_col):
        return {'==': col(w_col) == c_col, '!=': col(w_col) != c_col, '>=': col(w_col) >= c_col,
                '<=': col(w_col) <= c_col, '>': col(w_col) > c_col, '<': col(w_col) < c_col}

    @staticmethod
    def string_match(string_condition):
        str_split = string_condition.split(' ')
        match_op = Whens.__list_match_sign(str_split)
        search_index_condition_sign = [str_split.index(op) for op in match_op]
        where_col = str_split[search_index_condition_sign[0]-1]
        condition = str_split[search_index_condition_sign[0]+1]
        return Whens.__spark_condition_col_dict(where_col, condition)[match_op[0]]
