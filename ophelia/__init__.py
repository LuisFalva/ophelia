

__all__ = ["ClassType", "ClassName", "OpheliaToolsException", "OpheliaMLException", "OpheliaEvaluatorException",
           "OpheliaReadException", "OpheliaSessionException", "OpheliaStudioException", "OpheliaUtilsException",
           "OpheliaWrapperException", "OpheliaWriteException"]


def ClassType(dtype):
    return dtype.__class__


def ClassName(dtype):
    return dtype.__class__.__name__


def InstanceError(obj, t):
    if not isinstance(obj, t):
        raise TypeError("Unsupported type {}".format(ClassName(obj)))
    return None


class OpheliaToolsException(Exception):
    """
    Ophelia Tools Exception
    """
    pass


class OpheliaMLException(Exception):
    """
    Ophelia ML Exception
    """
    pass


class OpheliaEvaluatorException(Exception):
    """
    Ophelia Evaluator Exception
    """
    pass


class OpheliaReadException(Exception):
    """
    Ophelia Read Exception
    """
    pass


class OpheliaSessionException(Exception):
    """
    Ophelia Session Exception
    """
    pass


class OpheliaStudioException(Exception):
    """
    Ophelia Studio Exception
    """
    pass


class OpheliaUtilsException(Exception):
    """
    Ophelia Utils Exception
    """
    pass


class OpheliaWrapperException(Exception):
    """
    Ophelia Wrapper Exception
    """
    pass


class OpheliaWriteException(Exception):
    """
    Ophelia Write Exception
    """
    pass
