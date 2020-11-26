
__all__ = [
    "ReadPath",
    "FormatRead",
    "PathRead",
]


class FormatRead:
    def __init__(self):
        self.parquet = "parquet"
        self.excel = "excel"
        self.csv = "csv"
        self.json = "json"
        self.all = [self.parquet, self.excel, self.csv, self.json]


class PathRead:
    def __init__(self):
        self.root = "data"
        self.dir = "ophelia"
        self.load = "load"
        self.engine = "engine"
        self.model = "model"


ReadPath = (
    lambda opt, project:
    PathRead().root + "/" + PathRead().dir + "/" +
    PathRead().load + "/" + opt + "/" + project + "/"
)
