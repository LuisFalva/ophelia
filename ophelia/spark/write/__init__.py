
__all__ = ["PathWrite", "FormatWrite"]


class FormatWrite:
    def __init__(self):
        self.parquet = "parquet"
        self.excel = "excel"
        self.csv = "csv"
        self.json = "json"
        self.all = [self.parquet, self.excel, self.csv, self.json]


class PathWrite:
    def __init__(self):
        self.root = "data"
        self.dir = "ophelia"
        self.out = "out"
        self.engine = "engine"
        self.model = "model"

    def WritePath(self, opt, project):
        return f"{self.root}/{self.dir}/{self.out}/{opt}/{project}/"
