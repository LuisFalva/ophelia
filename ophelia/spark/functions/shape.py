from pyspark.sql import DataFrame

__all__ = ["ShapeDF", "ShapeDFWrapper"]


class ShapeDF(object):

    @staticmethod
    def shape(self):
        if len(self.columns) == 1:
            return self.count(),
        return self.count(), len(self.columns)


class ShapeDFWrapper(object):

    DataFrame.Shape = property(lambda self: ShapeDF.shape(self))
