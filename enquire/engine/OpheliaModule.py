class KeyStructure:

    def __init__(self):
        """
        Constructor object input parameters for Key Structure
        """
        self.index = 1
        self.vote = "vote"
        self.weight = "weight"
        self.risk_label = "risk_label"
        self.id = "customer_id"
        self.age = "age"
        self.education = "education"
        self.gender = "gender"
        self.marital = "marital"
        self.job = "job"
        self.child = "child"
        self.saving = "saving"
        self.insight = "insight"
        self.backup = "backup"
        self.prob = "prob"
        self.weighted_prob = "weighted_prob"
        self.final_vote = "final_vote"
        self.sum_vote = "sum_vote"
        self.sum_weight = "sum_weight"
        self.sum_prob = "sum_prob"
        self.sum_weighted_prob = "sum_weighted_prob"
        self.grouping = [
            self.id,
            self.risk_label,
            self.age,
            self.education,
            self.gender,
            self.marital,
            self.job,
            self.child,
            self.saving,
            self.insight,
            self.backup
        ]


class Voting:

    def __init__(self):
        """
        Constructor object input parameters for Voting
        """
        self.unit = 1
        self.min_vote = lambda w: float(len(w) / 2) + self.unit
        self.mid_vote = lambda w: float((len(w) / 2) + (len(w) / 5)) + self.unit
        self.max_vote = lambda w: float(len(w) + 1) + self.unit


class RiskLabel:

    def __init__(self):
        """
        Constructor object input parameters for Risk Label
        """
        self.Aggressive = "A"
        self.ModerateAggressive = "MA"
        self.Moderate = "M"
        self.ModerateConservative = "MC"
        self.Conservative = "C"


class Feature:

    def __init__(self):
        """
        Constructor object input parameters for Feature
        """
        self.super_boss = ["management"]
        self.employee = ["admin.", "technician", "services", "blue-collar"]
        self.no_employee = ["housemaid", "student"]
        self.self_employee = ["self-employed", "entrepreneur"]
        self.unemployed = ["unemployed", "retired", "unknown"]
        self.married = ["married"]
        self.single = ["single"]
        self.divorced = ["divorced"]
        self.unknown = ["unknown"]
        self.middle = ["primary"]
        self.university = ["secondary"]
        self.master = ["tertiary"]
        self.female = ["female"]
        self.male = ["male"]


class ModelParameters:

    def __init__(self):
        """
        Constructor object input parameters for Tree Model Parameters
        """
        self.gender = 0.0072
        self.age = 0.3205
        self.education = 0.0009
        self.job = 0.3905
        self.marital = 0.0024
        self.child = 0.1404
        self.saving = 0.0423
        self.insight = 0.0335
        self.backup = 0.0623
