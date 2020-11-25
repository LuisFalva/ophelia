from ophelia.ophelib.utils.list_utils import ListUtils

__all__ = ['regex_expr', 'feature_picking', 'remove_duplicated_elements']


def regex_expr(regex_name):
    return ListUtils.regex_expr(regex_name)


def feature_picking(df):
    return ListUtils.feature_picking(df)


def remove_duplicated_elements(lst):
    return ListUtils.remove_duplicated_elements(lst)
