from pyspark.sql import DataFrame


class ShapeDF:

    @staticmethod
    def add_shape():
        def shape(self):
            if len(self.columns) == 1:
                return self.count(),
            return self.count(), len(self.columns)
        DataFrame.shape = property(lambda self: shape(self))
