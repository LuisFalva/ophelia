
__all__ = [
    "WritePath",
    "PathWrite",
    "FormatWrite",
]


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


WritePath = (
    lambda opt, project:
    PathWrite().root + "/" + PathWrite().dir + "/" +
    PathWrite().out + "/" + opt + "/" + project + "/"
)
