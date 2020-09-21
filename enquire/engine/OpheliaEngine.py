from pyspark.sql.types import Row
from enquire.engine.OpheliaModule import Feature, RiskLabel, Voting


class OpheliaEngine:
    """
    Docstring class
    """

    feature = Feature()
    risk = RiskLabel()
    vote = Voting()

    @staticmethod
    def key_value(vote: float, w: float, cls: str) -> Row:
        return Row(vote=float(vote), weight=float(w), risk_label=str(cls))

    @staticmethod
    def score_gender(gender: str, w: dict) -> Row:
        """
        Score gender function helps to impute scoring ratio for gender factor analysis
        :param gender: str, gender class
        :param w: dict, with weight factor for gender
        :return: Row, with one node of information from gender tree
        """
        w_i = w["gender"]
        vote = OpheliaEngine.vote.unit
        if gender in OpheliaEngine.feature.female:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.ModerateAggressive)
        elif gender in OpheliaEngine.feature.male:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.Aggressive)

    @staticmethod
    def score_age(age: int, w: dict) -> Row:
        """
        Score age function helps to impute scoring ratio for age factor analysis
        :param age: int, age number
        :param w: dict, with weight factor for age
        :return: Row, with one node of information from age tree
        """
        w_i = w["age"]
        vote = OpheliaEngine.vote.unit
        min_vote = OpheliaEngine.vote.min_vote(w)
        mid_vote = OpheliaEngine.vote.mid_vote(w)
        max_vote = OpheliaEngine.vote.max_vote(w)

        def two_bound(a: int, min_bound: int, upper_bound: int) -> int:
            if min_bound <= a < upper_bound:
                return a

        if two_bound(age, min_bound=60, upper_bound=100):
            if two_bound(age, min_bound=67, upper_bound=100):
                return OpheliaEngine.key_value(max_vote, w_i, OpheliaEngine.risk.Conservative)
            elif two_bound(age, min_bound=60, upper_bound=67):
                return OpheliaEngine.key_value(max_vote, w_i, OpheliaEngine.risk.ModerateConservative)
        elif two_bound(age, min_bound=50, upper_bound=60):
            if two_bound(age, min_bound=57, upper_bound=60):
                return OpheliaEngine.key_value(mid_vote, w_i, OpheliaEngine.risk.ModerateConservative)
            elif two_bound(age, min_bound=50, upper_bound=57):
                return OpheliaEngine.key_value(mid_vote, w_i, OpheliaEngine.risk.Moderate)
        elif two_bound(age, min_bound=40, upper_bound=50):
            if two_bound(age, min_bound=47, upper_bound=50):
                return OpheliaEngine.key_value(min_vote, w_i, OpheliaEngine.risk.Moderate)
            elif two_bound(age, min_bound=40, upper_bound=47):
                return OpheliaEngine.key_value(min_vote, w_i, OpheliaEngine.risk.ModerateAggressive)
        elif two_bound(age, min_bound=30, upper_bound=40):
            if two_bound(age, min_bound=37, upper_bound=40):
                return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.ModerateAggressive)
            elif two_bound(age, min_bound=30, upper_bound=37):
                return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.Aggressive)
        else:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.Aggressive)

    @staticmethod
    def score_education(education: str, w: dict) -> Row:
        """
        Score education function helps to impute scoring ratio for education factor analysis
        :param education: str, number of education
        :param w: dict, with weight factor for education
        :return: Row, with one node of information from education tree
        """
        w_i = w["education"]
        vote = OpheliaEngine.vote.unit
        if education in OpheliaEngine.feature.unknown:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.Conservative)
        elif education in OpheliaEngine.feature.middle:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.ModerateConservative)
        elif education in OpheliaEngine.feature.university:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.ModerateAggressive)
        elif education in OpheliaEngine.feature.master:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.Aggressive)

    @staticmethod
    def score_occupation(occupation: str, w: dict) -> Row:
        """
        Score occupation function helps to impute scoring ratio for occupation factor analysis
        :param occupation: str, number of occupation
        :param w: dict, with weight factor for occupation
        :return: Row, with one node of information from occupation tree
        """
        w_i = w["job"]
        vote = OpheliaEngine.vote.unit
        min_vote = OpheliaEngine.vote.min_vote(w)
        mid_vote = OpheliaEngine.vote.mid_vote(w)
        max_vote = OpheliaEngine.vote.max_vote(w)
        if occupation in OpheliaEngine.feature.unemployed:
            return OpheliaEngine.key_value(max_vote, w_i, OpheliaEngine.risk.Conservative)
        elif occupation in OpheliaEngine.feature.no_employee:
            return OpheliaEngine.key_value(mid_vote, w_i, OpheliaEngine.risk.ModerateConservative)
        elif occupation in OpheliaEngine.feature.self_employee:
            return OpheliaEngine.key_value(min_vote, w_i, OpheliaEngine.risk.Moderate)
        elif occupation in OpheliaEngine.feature.employee:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.ModerateAggressive)
        else:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.Aggressive)

    @staticmethod
    def score_marital(marital: str, w: dict) -> Row:
        """
        Score marital function helps to impute scoring ratio for marital factor analysis
        :param marital: str, number of marital
        :param w: dict, with weight factor for marital
        :return: Row, with one node of information from marital tree
        """
        w_i = w["marital"]
        vote = OpheliaEngine.vote.unit
        min_vote = OpheliaEngine.vote.min_vote(w)
        mid_vote = OpheliaEngine.vote.mid_vote(w)
        if marital in OpheliaEngine.feature.married:
            return OpheliaEngine.key_value(min_vote, w_i, OpheliaEngine.risk.ModerateAggressive)
        elif marital in OpheliaEngine.feature.single:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.Aggressive)
        elif marital in OpheliaEngine.feature.divorced:
            return OpheliaEngine.key_value(mid_vote, w_i, OpheliaEngine.risk.ModerateConservative)

    @staticmethod
    def score_child(child: float, w: dict) -> Row:
        """
        Score child function helps to impute scoring ratio for child factor analysis
        :param child: float, 1.0 for yes 0.0 for no
        :param w: dict, with weight factor for child
        :return: Row, with one node of information from child tree
        """
        w_i = w["child"]
        vote = OpheliaEngine.vote.unit
        min_vote = OpheliaEngine.vote.min_vote(w)
        if child == float(1.0):
            return OpheliaEngine.key_value(min_vote, w_i, OpheliaEngine.risk.ModerateConservative)
        else:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.ModerateAggressive)

    @staticmethod
    def score_saving(saving: float, w: dict) -> Row:
        """
        Score saving function helps to impute scoring ratio for saving factor analysis
        :param saving: float, 1.0 for yes 0.0 for no
        :param w: dict, with weight factor for saving
        :return: Row, with one node of information from saving tree
        """
        w_i = w["saving"]
        vote = OpheliaEngine.vote.unit
        min_vote = OpheliaEngine.vote.min_vote(w)
        if saving == float(1.0):
            return OpheliaEngine.key_value(min_vote, w_i, OpheliaEngine.risk.ModerateConservative)
        else:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.ModerateAggressive)

    @staticmethod
    def score_insight(insight: float, w: dict) -> Row:
        """
        Score insight function helps to impute scoring ratio for insight factor analysis
        :param insight: float, 1.0 for yes 0.0 for no
        :param w: dict, with weight factor for insight
        :return: Row, with one node of information from insight tree
        """
        w_i = w["insight"]
        vote = OpheliaEngine.vote.unit
        min_vote = OpheliaEngine.vote.min_vote(w)
        if insight == float(1.0):
            return OpheliaEngine.key_value(min_vote, w_i, OpheliaEngine.risk.Aggressive)
        else:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.Moderate)

    @staticmethod
    def score_backup(backup: float, w: dict) -> Row:
        """
        Score backup function helps to impute scoring ratio for backup factor analysis
        :param backup: float, 1.0 for yes 0.0 for no
        :param w: dict, with weight factor for backup
        :return: Row, with one node of information from backup tree
        """
        w_i = w["backup"]
        vote = OpheliaEngine.vote.unit
        min_vote = OpheliaEngine.vote.min_vote(w)
        if backup == float(1.0):
            return OpheliaEngine.key_value(min_vote, w_i, OpheliaEngine.risk.Aggressive)
        else:
            return OpheliaEngine.key_value(vote, w_i, OpheliaEngine.risk.Moderate)
