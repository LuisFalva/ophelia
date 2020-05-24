import operator
from .Config import ModelParameters
from .Module import RiskLabel, KeyStructure
from .ScrutinyFeatures import Scrutiny


class Classification:

    rl = RiskLabel()
    ks = KeyStructure()
    mp = ModelParameters()

    @staticmethod
    def factor_weights(**kargs):
        none = float(0.0)
        gender = float(kargs.get(Classification.ks.gender))
        age = float(kargs.get(Classification.ks.age))
        education = float(kargs.get(Classification.ks.education))
        job = float(kargs.get(Classification.ks.job))
        marital = float(kargs.get(Classification.ks.marital))
        child = float(kargs.get(Classification.ks.child))
        saving = float(kargs.get(Classification.ks.saving))
        insight = float(kargs.get(Classification.ks.insight))
        backup = float(kargs.get(Classification.ks.backup))

        dict_weight = {
            Classification.ks.gender: none if gender is None else gender,
            Classification.ks.age: none if age is None else age,
            Classification.ks.education: none if education is None else education,
            Classification.ks.job: none if job is None else job,
            Classification.ks.marital: none if marital is None else marital,
            Classification.ks.child: none if child is None else child,
            Classification.ks.saving: none if saving is None else saving,
            Classification.ks.insight: none if insight is None else insight,
            Classification.ks.backup: none if backup is None else backup
        }
        return dict_weight

    @staticmethod
    def tree_generator(obj):
        w = Classification.factor_weights(
            gender=Classification.mp.gender,
            age=Classification.mp.age,
            education=Classification.mp.education,
            job=Classification.mp.job,
            marital=Classification.mp.marital,
            child=Classification.mp.child,
            saving=Classification.mp.saving,
            insight=Classification.mp.insight,
            backup=Classification.mp.backup
        )

        tree = [
            Scrutiny.score_gender(gender=obj.gender, w=w),
            Scrutiny.score_age(age=obj.age, w=w),
            Scrutiny.score_education(education=obj.education, w=w),
            Scrutiny.score_occupation(occupation=obj.job, w=w),
            Scrutiny.score_marital(marital=obj.marital, w=w),
            Scrutiny.score_child(child=obj.child, w=w),
            Scrutiny.score_saving(saving=obj.saving, w=w),
            Scrutiny.score_insight(insight=obj.insight, w=w),
            Scrutiny.score_backup(backup=obj.backup, w=w)
        ]
        return tree

    @staticmethod
    def dict_filter(dic):
        filter_dic = {}
        for item in dic:
            if dic[item] > 0:
                filter_dic[item] = dic[item]
        return filter_dic

    @staticmethod
    def group_key(a, ma, m, mc, c, counter=0.0):
        group_key_value = {
            Classification.rl.Aggressive: float(a),
            Classification.rl.ModerateAggressive: float(ma),
            Classification.rl.Moderate: float(m),
            Classification.rl.ModerateConservative: float(mc),
            Classification.rl.Conservative: float(c),
            "tot": float(counter)
        }
        return group_key_value

    @staticmethod
    def group_by_key(tree, key, a=0.0, ma=0.0, m=0.0, mc=0.0, c=0.0, counter=0.0):
        for item in tree:
            counter += tree.count(item)
            if item[Classification.ks.risk_label] == Classification.rl.Aggressive:
                a += item[key]
            elif item[Classification.ks.risk_label] == Classification.rl.ModerateAggressive:
                ma += item[key]
            elif item[Classification.ks.risk_label] == Classification.rl.Moderate:
                m += item[key]
            elif item[Classification.ks.risk_label] == Classification.rl.ModerateConservative:
                mc += item[key]
            elif item[Classification.ks.risk_label] == Classification.rl.Conservative:
                c += item[key]

        return Classification.dict_filter(Classification.group_key(a, ma, m, mc, c, counter))

    @staticmethod
    def prob_label(dic, a=0.0, ma=0.0, m=0.0, mc=0.0, c=0.0):
        for item in dic:
            if item == Classification.rl.Aggressive:
                a = dic[item] / dic["tot"]
            elif item == Classification.rl.ModerateAggressive:
                ma = dic[item] / dic["tot"]
            elif item == Classification.rl.Moderate:
                m = dic[item] / dic["tot"]
            elif item == Classification.rl.ModerateConservative:
                mc = dic[item] / dic["tot"]
            elif item == Classification.rl.Conservative:
                c = dic[item] / dic["tot"]

        return Classification.dict_filter(Classification.group_key(a, ma, m, mc, c))

    @staticmethod
    def matmul_dict(weight, prob, w_a=0.0, w_ma=0.0, w_m=0.0, w_mc=0.0, w_c=0.0):
        for item in weight:
            if item == Classification.rl.Aggressive:
                w_a = (weight[item] * prob[item]) + weight[item]
            elif item == Classification.rl.ModerateAggressive:
                w_ma = (weight[item] * prob[item]) + weight[item]
            elif item == Classification.rl.Moderate:
                w_m = (weight[item] * prob[item]) + weight[item]
            elif item == Classification.rl.ModerateConservative:
                w_mc = (weight[item] * prob[item]) + weight[item]
            elif item == Classification.rl.Conservative:
                w_c = (weight[item] * prob[item]) + weight[item]

        return Classification.dict_filter(Classification.group_key(w_a, w_ma, w_m, w_mc, w_c))

    @staticmethod
    def consensus(dot, freq, threshold=0.6, truncate=0.5, w_a=0.0, w_ma=0.0, w_m=0.0, w_mc=0.0, w_c=0.0):

        def activation_func(x, thr=threshold, trunc=truncate):
            return round(x - thr + trunc)

        for item in dot:
            if item == Classification.rl.Aggressive:
                w_a = activation_func(dot[item] + freq[item])
            elif item == Classification.rl.ModerateAggressive:
                w_ma = activation_func(dot[item] + freq[item])
            elif item == Classification.rl.Moderate:
                w_m = activation_func(dot[item] + freq[item])
            elif item == Classification.rl.ModerateConservative:
                w_mc = activation_func(dot[item] + freq[item])
            elif item == Classification.rl.Conservative:
                w_c = activation_func(dot[item] + freq[item])

        return Classification.dict_filter(Classification.group_key(w_a, w_ma, w_m, w_mc, w_c))

    @staticmethod
    def validate_values(dic):
        equal_validation = len(dic.values()) == len(set(dic.values()))
        if equal_validation is False:
            return str(1)
        return str(0)

    @staticmethod
    def assign_label(dic, dot_dic):
        if len(dic) is 0:
            return str("null")
        if Classification.validate_values(dic) is "1":
            return str(max(dot_dic.items(), key=operator.itemgetter(1))[0])
        else:
            return str(max(dic.items(), key=operator.itemgetter(1))[0])

    @staticmethod
    def run_classification_risk(tree):
        aggregate_weight = Classification.group_by_key(tree, "weight")
        label_frequency = Classification.group_by_key(tree, "vote")
        label_probability = Classification.prob_label(label_frequency)
        dot = Classification.matmul_dict(aggregate_weight, label_probability)
        consensus_vote = Classification.consensus(dot, label_frequency)
        return str(Classification.assign_label(consensus_vote, dot))
