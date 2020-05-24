class KeyStructure:

    def __init__(self):
        """
        Constructor object for input parameters.
        :param NAME_PARAM: "DESCRIPTION HERE"        
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
        Constructor object for input parameters.
        :param NAME_PARAM: "DESCRIPTION HERE"        
        """
        self.vote = 1
        self.min_vote = lambda w: float(len(w) / 2)
        self.mid_vote = lambda w: float((len(w) / 2) + (len(w) / 5))
        self.max_vote = lambda w: float(len(w) + 1)


class RiskLabel:

    def __init__(self):
        """
        Constructor object for input parameters.
        :param NAME_PARAM: "DESCRIPTION HERE"        
        """
        self.Aggressive = "A"
        self.ModerateAggressive = "MA"
        self.Moderate = "M"
        self.ModerateConservative = "MC"
        self.Conservative = "C"


class Feature:

    def __init__(self):
        """
        Constructor object for input parameters.
        :param NAME_PARAM: "DESCRIPTION HERE"        
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
