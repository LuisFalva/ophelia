from pyspark import Row
from .Module import Feature, RiskLabel, Voting


class Scrutiny:

    ft = Feature()
    rl = RiskLabel()
    vt = Voting()

    @staticmethod
    def key_value(vote, w, clss):
        return Row(vote=round(vote), weight=float(w), risk_label=str(clss))

    @staticmethod
    def score_gender(gender, w):

        w_i = w["gender"]
        vote = Scrutiny.vt.vote
        min_vote = Scrutiny.vt.min_vote(w)

        if gender in Scrutiny.ft.female:
            return Scrutiny.key_value(min_vote, w_i, Scrutiny.rl.ModerateAggressive)
        elif gender in Scrutiny.ft.male:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.Aggressive)

    @staticmethod
    def score_age(age, w):

        w_i = w["age"]
        vote = Scrutiny.vt.vote
        min_vote = Scrutiny.vt.min_vote(w)
        mid_vote = Scrutiny.vt.mid_vote(w)
        max_vote = Scrutiny.vt.max_vote(w)

        def two_bound(age, min_bound, upper_bound):
            if min_bound <= age < upper_bound:
                return age

        if two_bound(age, min_bound=60, upper_bound=100):
            if two_bound(age, min_bound=67, upper_bound=100):
                return Scrutiny.key_value(max_vote, w_i, Scrutiny.rl.Conservative)
            elif two_bound(age, min_bound=60, upper_bound=67):
                return Scrutiny.key_value(max_vote, w_i, Scrutiny.rl.ModerateConservative)

        elif two_bound(age, min_bound=50, upper_bound=60):
            if two_bound(age, min_bound=57, upper_bound=60):
                return Scrutiny.key_value(mid_vote, w_i, Scrutiny.rl.ModerateConservative)
            elif two_bound(age, min_bound=50, upper_bound=57):
                return Scrutiny.key_value(mid_vote, w_i, Scrutiny.rl.Moderate)

        elif two_bound(age, min_bound=40, upper_bound=50):
            if two_bound(age, min_bound=47, upper_bound=50):
                return Scrutiny.key_value(min_vote, w_i, Scrutiny.rl.Moderate)
            elif two_bound(age, min_bound=40, upper_bound=47):
                return Scrutiny.key_value(min_vote, w_i, Scrutiny.rl.ModerateAggressive)

        elif two_bound(age, min_bound=30, upper_bound=40):
            if two_bound(age, min_bound=37, upper_bound=40):
                return Scrutiny.key_value(vote, w_i, Scrutiny.rl.ModerateAggressive)
            elif two_bound(age, min_bound=30, upper_bound=37):
                return Scrutiny.key_value(vote, w_i, Scrutiny.rl.Aggressive)
        else:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.Aggressive)

    @staticmethod
    def score_education(education, w):

        w_i = w["education"]
        vote = Scrutiny.vt.vote

        if education in Scrutiny.ft.unknown:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.Conservative)
        elif education in Scrutiny.ft.middle:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.ModerateConservative)
        elif education in Scrutiny.ft.university:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.ModerateAggressive)
        elif education in Scrutiny.ft.master:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.Aggressive)

    @staticmethod
    def score_occupation(occupation, w):

        w_i = w["job"]
        vote = Scrutiny.vt.vote
        min_vote = Scrutiny.vt.min_vote(w)
        mid_vote = Scrutiny.vt.mid_vote(w)
        max_vote = Scrutiny.vt.max_vote(w)

        if occupation in Scrutiny.ft.unemployed:
            return Scrutiny.key_value(max_vote, w_i, Scrutiny.rl.Conservative)
        elif occupation in Scrutiny.ft.no_employee:
            return Scrutiny.key_value(mid_vote, w_i, Scrutiny.rl.ModerateConservative)
        elif occupation in Scrutiny.ft.self_employee:
            return Scrutiny.key_value(min_vote, w_i, Scrutiny.rl.Moderate)
        elif occupation in Scrutiny.ft.employee:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.ModerateAggressive)
        else:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.Aggressive)

    @staticmethod
    def score_marital(marital, w):

        w_i = w["marital"]
        vote = Scrutiny.vt.vote
        min_vote = Scrutiny.vt.min_vote(w)
        mid_vote = Scrutiny.vt.mid_vote(w)

        if marital in Scrutiny.ft.married:
            return Scrutiny.key_value(min_vote, w_i, Scrutiny.rl.ModerateAggressive)
        elif marital in Scrutiny.ft.single:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.Aggressive)
        elif marital in Scrutiny.ft.divorced:
            return Scrutiny.key_value(mid_vote, w_i, Scrutiny.rl.ModerateConservative)

    @staticmethod
    def score_child(child, w):

        w_i = w["child"]
        vote = Scrutiny.vt.vote
        min_vote = Scrutiny.vt.min_vote(w)

        if child == 1:
            return Scrutiny.key_value(min_vote, w_i, Scrutiny.rl.ModerateConservative)
        else:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.ModerateAggressive)

    @staticmethod
    def score_saving(saving, w):

        w_i = w["saving"]
        vote = Scrutiny.vt.vote
        min_vote = Scrutiny.vt.min_vote(w)

        if saving == 1:
            return Scrutiny.key_value(min_vote, w_i, Scrutiny.rl.ModerateConservative)
        else:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.ModerateAggressive)

    @staticmethod
    def score_insight(insight, w):

        w_i = w["insight"]
        vote = Scrutiny.vt.vote
        min_vote = Scrutiny.vt.min_vote(w)

        if insight == 1:
            return Scrutiny.key_value(min_vote, w_i, Scrutiny.rl.Aggressive)
        else:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.Moderate)

    @staticmethod
    def score_backup(backup, w):

        w_i = w["backup"]
        vote = Scrutiny.vt.vote
        min_vote = Scrutiny.vt.min_vote(w)

        if backup == 1:
            return Scrutiny.key_value(min_vote, w_i, Scrutiny.rl.Aggressive)
        else:
            return Scrutiny.key_value(vote, w_i, Scrutiny.rl.Moderate)
