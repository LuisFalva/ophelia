from spark.read.spark_read import Read, SparkReadWrapper


class PathRead:
    def __init__(self):
        self.root = "data"
        self.dir = "ophelia"
        self.load = "load"
        self.engine = "engine"
        self.model = "model"

    def ReadPath(self, opt, project):
        return f"{self.root}/{self.dir}/{self.load}/{opt}/{project}"


class OpheliaReadWrapper(object):

    __all__ = ["Read", "SparkReadWrapper", "PathRead"]
