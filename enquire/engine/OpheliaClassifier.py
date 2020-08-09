import operator
from .OpheliaModule import RiskLabel, KeyStructure, ModelParameters
from .OpheliaEngine import OpheliaEngine


class OpheliaClassifier:

    rl = RiskLabel()
    ks = KeyStructure()
    mp = ModelParameters()

    @staticmethod
    def factor_weights(**kargs):
        none = float(0.0)
        gender = float(kargs.get(OpheliaClassifier.ks.gender))
        age = float(kargs.get(OpheliaClassifier.ks.age))
        education = float(kargs.get(OpheliaClassifier.ks.education))
        job = float(kargs.get(OpheliaClassifier.ks.job))
        marital = float(kargs.get(OpheliaClassifier.ks.marital))
        child = float(kargs.get(OpheliaClassifier.ks.child))
        saving = float(kargs.get(OpheliaClassifier.ks.saving))
        insight = float(kargs.get(OpheliaClassifier.ks.insight))
        backup = float(kargs.get(OpheliaClassifier.ks.backup))

        dict_weight = {
            OpheliaClassifier.ks.gender: none if gender is None else gender,
            OpheliaClassifier.ks.age: none if age is None else age,
            OpheliaClassifier.ks.education: none if education is None else education,
            OpheliaClassifier.ks.job: none if job is None else job,
            OpheliaClassifier.ks.marital: none if marital is None else marital,
            OpheliaClassifier.ks.child: none if child is None else child,
            OpheliaClassifier.ks.saving: none if saving is None else saving,
            OpheliaClassifier.ks.insight: none if insight is None else insight,
            OpheliaClassifier.ks.backup: none if backup is None else backup
        }
        return dict_weight

    @staticmethod
    def tree_generator(obj):
        w = OpheliaClassifier.factor_weights(
            gender=OpheliaClassifier.mp.gender,
            age=OpheliaClassifier.mp.age,
            education=OpheliaClassifier.mp.education,
            job=OpheliaClassifier.mp.job,
            marital=OpheliaClassifier.mp.marital,
            child=OpheliaClassifier.mp.child,
            saving=OpheliaClassifier.mp.saving,
            insight=OpheliaClassifier.mp.insight,
            backup=OpheliaClassifier.mp.backup
        )

        tree = [
            OpheliaEngine.score_gender(gender=obj.gender, w=w),
            OpheliaEngine.score_age(age=obj.age, w=w),
            OpheliaEngine.score_education(education=obj.education, w=w),
            OpheliaEngine.score_occupation(occupation=obj.job, w=w),
            OpheliaEngine.score_marital(marital=obj.marital, w=w),
            OpheliaEngine.score_child(child=obj.child, w=w),
            OpheliaEngine.score_saving(saving=obj.saving, w=w),
            OpheliaEngine.score_insight(insight=obj.insight, w=w),
            OpheliaEngine.score_backup(backup=obj.backup, w=w)
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
            OpheliaClassifier.rl.Aggressive: float(a),
            OpheliaClassifier.rl.ModerateAggressive: float(ma),
            OpheliaClassifier.rl.Moderate: float(m),
            OpheliaClassifier.rl.ModerateConservative: float(mc),
            OpheliaClassifier.rl.Conservative: float(c),
            "tot": float(counter)
        }
        return group_key_value

    @staticmethod
    def group_by_key(tree, key, a=0.0, ma=0.0, m=0.0, mc=0.0, c=0.0, counter=0.0):
        for item in tree:
            counter += tree.count(item)
            if item[OpheliaClassifier.ks.risk_label] == OpheliaClassifier.rl.Aggressive:
                a += item[key]
            elif item[OpheliaClassifier.ks.risk_label] == OpheliaClassifier.rl.ModerateAggressive:
                ma += item[key]
            elif item[OpheliaClassifier.ks.risk_label] == OpheliaClassifier.rl.Moderate:
                m += item[key]
            elif item[OpheliaClassifier.ks.risk_label] == OpheliaClassifier.rl.ModerateConservative:
                mc += item[key]
            elif item[OpheliaClassifier.ks.risk_label] == OpheliaClassifier.rl.Conservative:
                c += item[key]

        return OpheliaClassifier.dict_filter(OpheliaClassifier.group_key(a, ma, m, mc, c, counter))

    @staticmethod
    def prob_label(dic, a=0.0, ma=0.0, m=0.0, mc=0.0, c=0.0):
        for item in dic:
            if item == OpheliaClassifier.rl.Aggressive:
                a = dic[item] / dic["tot"]
            elif item == OpheliaClassifier.rl.ModerateAggressive:
                ma = dic[item] / dic["tot"]
            elif item == OpheliaClassifier.rl.Moderate:
                m = dic[item] / dic["tot"]
            elif item == OpheliaClassifier.rl.ModerateConservative:
                mc = dic[item] / dic["tot"]
            elif item == OpheliaClassifier.rl.Conservative:
                c = dic[item] / dic["tot"]

        return OpheliaClassifier.dict_filter(OpheliaClassifier.group_key(a, ma, m, mc, c))

    @staticmethod
    def matmul_dict(weight, prob, w_a=0.0, w_ma=0.0, w_m=0.0, w_mc=0.0, w_c=0.0):
        for item in weight:
            if item == OpheliaClassifier.rl.Aggressive:
                w_a = (weight[item] * prob[item]) + weight[item]
            elif item == OpheliaClassifier.rl.ModerateAggressive:
                w_ma = (weight[item] * prob[item]) + weight[item]
            elif item == OpheliaClassifier.rl.Moderate:
                w_m = (weight[item] * prob[item]) + weight[item]
            elif item == OpheliaClassifier.rl.ModerateConservative:
                w_mc = (weight[item] * prob[item]) + weight[item]
            elif item == OpheliaClassifier.rl.Conservative:
                w_c = (weight[item] * prob[item]) + weight[item]

        return OpheliaClassifier.dict_filter(OpheliaClassifier.group_key(w_a, w_ma, w_m, w_mc, w_c))

    @staticmethod
    def consensus(dot, freq, threshold=0.6, truncate=0.5, w_a=0.0, w_ma=0.0, w_m=0.0, w_mc=0.0, w_c=0.0):

        def activation_func(x, thr=threshold, trunc=truncate):
            return round(x - thr + trunc)

        for item in dot:
            if item == OpheliaClassifier.rl.Aggressive:
                w_a = activation_func(dot[item] + freq[item])
            elif item == OpheliaClassifier.rl.ModerateAggressive:
                w_ma = activation_func(dot[item] + freq[item])
            elif item == OpheliaClassifier.rl.Moderate:
                w_m = activation_func(dot[item] + freq[item])
            elif item == OpheliaClassifier.rl.ModerateConservative:
                w_mc = activation_func(dot[item] + freq[item])
            elif item == OpheliaClassifier.rl.Conservative:
                w_c = activation_func(dot[item] + freq[item])

        return OpheliaClassifier.dict_filter(OpheliaClassifier.group_key(w_a, w_ma, w_m, w_mc, w_c))

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
        if OpheliaClassifier.validate_values(dic) is "1":
            return str(max(dot_dic.items(), key=operator.itemgetter(1))[0])
        else:
            return str(max(dic.items(), key=operator.itemgetter(1))[0])

    @staticmethod
    def run_classification_risk(tree):
        aggregate_weight = OpheliaClassifier.group_by_key(tree, "weight")
        label_frequency = OpheliaClassifier.group_by_key(tree, "vote")
        label_probability = OpheliaClassifier.prob_label(label_frequency)
        dot = OpheliaClassifier.matmul_dict(aggregate_weight, label_probability)
        consensus_vote = OpheliaClassifier.consensus(dot, label_frequency)
        return str(OpheliaClassifier.assign_label(consensus_vote, dot))
