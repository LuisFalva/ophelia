import operator
from enquire.engine.OpheliaModule import RiskLabel, KeyStructure, ModelParameters
from enquire.engine.OpheliaEngine import OpheliaEngine


class OpheliaClassifier:

    label = RiskLabel()
    key = KeyStructure()
    mp = ModelParameters()

    @staticmethod
    def factor_weights(**kargs):
        none = float(0.0)
        gender = float(kargs.get(OpheliaClassifier.key.gender))
        age = float(kargs.get(OpheliaClassifier.key.age))
        education = float(kargs.get(OpheliaClassifier.key.education))
        job = float(kargs.get(OpheliaClassifier.key.job))
        marital = float(kargs.get(OpheliaClassifier.key.marital))
        child = float(kargs.get(OpheliaClassifier.key.child))
        saving = float(kargs.get(OpheliaClassifier.key.saving))
        insight = float(kargs.get(OpheliaClassifier.key.insight))
        backup = float(kargs.get(OpheliaClassifier.key.backup))

        dict_weight = {
            OpheliaClassifier.key.gender: none if gender is None else gender,
            OpheliaClassifier.key.age: none if age is None else age,
            OpheliaClassifier.key.education: none if education is None else education,
            OpheliaClassifier.key.job: none if job is None else job,
            OpheliaClassifier.key.marital: none if marital is None else marital,
            OpheliaClassifier.key.child: none if child is None else child,
            OpheliaClassifier.key.saving: none if saving is None else saving,
            OpheliaClassifier.key.insight: none if insight is None else insight,
            OpheliaClassifier.key.backup: none if backup is None else backup
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
            OpheliaClassifier.label.Aggressive: float(a),
            OpheliaClassifier.label.ModerateAggressive: float(ma),
            OpheliaClassifier.label.Moderate: float(m),
            OpheliaClassifier.label.ModerateConservative: float(mc),
            OpheliaClassifier.label.Conservative: float(c),
            "tot": float(counter)
        }
        return group_key_value

    @staticmethod
    def group_by_key(tree, ky, a=0.0, ma=0.0, m=0.0, mc=0.0, c=0.0, counter=0.0):
        for item in tree:
            counter += tree.count(item)
            if item[OpheliaClassifier.key.risk_label] == OpheliaClassifier.label.Aggressive:
                a += item[ky]
            elif item[OpheliaClassifier.key.risk_label] == OpheliaClassifier.label.ModerateAggressive:
                ma += item[ky]
            elif item[OpheliaClassifier.key.risk_label] == OpheliaClassifier.label.Moderate:
                m += item[ky]
            elif item[OpheliaClassifier.key.risk_label] == OpheliaClassifier.label.ModerateConservative:
                mc += item[ky]
            elif item[OpheliaClassifier.key.risk_label] == OpheliaClassifier.label.Conservative:
                c += item[ky]

        return OpheliaClassifier.dict_filter(OpheliaClassifier.group_key(a, ma, m, mc, c, counter))

    @staticmethod
    def prob_label(dic, a=0.0, ma=0.0, m=0.0, mc=0.0, c=0.0):
        for item in dic:
            if item == OpheliaClassifier.label.Aggressive:
                a = dic[item] / dic["tot"]
            elif item == OpheliaClassifier.label.ModerateAggressive:
                ma = dic[item] / dic["tot"]
            elif item == OpheliaClassifier.label.Moderate:
                m = dic[item] / dic["tot"]
            elif item == OpheliaClassifier.label.ModerateConservative:
                mc = dic[item] / dic["tot"]
            elif item == OpheliaClassifier.label.Conservative:
                c = dic[item] / dic["tot"]

        return OpheliaClassifier.dict_filter(OpheliaClassifier.group_key(a, ma, m, mc, c))

    @staticmethod
    def matmul_dict(weight, prob, w_a=0.0, w_ma=0.0, w_m=0.0, w_mc=0.0, w_c=0.0):
        for item in weight:
            if item == OpheliaClassifier.label.Aggressive:
                w_a = (weight[item] * prob[item]) + weight[item]
            elif item == OpheliaClassifier.label.ModerateAggressive:
                w_ma = (weight[item] * prob[item]) + weight[item]
            elif item == OpheliaClassifier.label.Moderate:
                w_m = (weight[item] * prob[item]) + weight[item]
            elif item == OpheliaClassifier.label.ModerateConservative:
                w_mc = (weight[item] * prob[item]) + weight[item]
            elif item == OpheliaClassifier.label.Conservative:
                w_c = (weight[item] * prob[item]) + weight[item]

        return OpheliaClassifier.dict_filter(OpheliaClassifier.group_key(w_a, w_ma, w_m, w_mc, w_c))

    @staticmethod
    def consensus(dot, freq, threshold=0.6, truncate=0.5, w_a=0.0, w_ma=0.0, w_m=0.0, w_mc=0.0, w_c=0.0):

        def activation_func(x, thr=threshold, trunc=truncate):
            return round(x - thr + trunc)

        for item in dot:
            if item == OpheliaClassifier.label.Aggressive:
                w_a = activation_func(dot[item] + freq[item])
            elif item == OpheliaClassifier.label.ModerateAggressive:
                w_ma = activation_func(dot[item] + freq[item])
            elif item == OpheliaClassifier.label.Moderate:
                w_m = activation_func(dot[item] + freq[item])
            elif item == OpheliaClassifier.label.ModerateConservative:
                w_mc = activation_func(dot[item] + freq[item])
            elif item == OpheliaClassifier.label.Conservative:
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
        if len(dic) == 0:
            return str("null")
        if OpheliaClassifier.validate_values(dic) == "1":
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
