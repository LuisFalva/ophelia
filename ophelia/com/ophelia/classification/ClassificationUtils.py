from collections import Counter
from .Module import KeyStructure, RiskLabel, Feature, WeightLabel


class Classification(object):
    
    key = KeyStructure()
    rsk = RiskLabel()
    feat = Feature()
    wgt = WeightLabel()

    small_weight = feat.small_weight
    mid_weight = feat.mid_weight
    big_weight = feat.big_weight

    @staticmethod
    def score_gender(gender, w):
        if gender in Classification.feat.female:
            return dict({Classification.key.vote: 1,
                         Classification.key.weight: w,
                         Classification.key.risk_label: Classification.rsk.ModerateAggresive})
        elif gender in Classification.feat.male:
            return dict({Classification.key.vote: 1,
                         Classification.key.weight: w,
                         Classification.key.risk_label: Classification.rsk.Aggresive})
    
    @staticmethod
    def score_age(age, w):
        if age > 60:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w+Classification.big_weight, 
                    Classification.key.risk_label: Classification.rsk.Conservative}
        elif 50 < age <= 60:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w+Classification.mid_weight, 
                    Classification.key.risk_label: Classification.rsk.ModerateConservative}
        elif 40 < age <= 50:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w+Classification.small_weight, 
                    Classification.key.risk_label: Classification.rsk.Moderate}
        elif 30 < age <= 40:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w, 
                    Classification.key.risk_label: Classification.rsk.ModerateAggresive}
        else:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w, 
                    Classification.key.risk_label: Classification.rsk.Aggresive}
    
    @staticmethod
    def score_marital(marital, w):
        if marital in Classification.feat.married:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w, 
                    Classification.key.risk_label: Classification.rsk.ModerateAggresive}
        if marital in Classification.feat.single:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w, 
                    Classification.key.risk_label: Classification.rsk.Aggresive}
        else:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w, 
                    Classification.key.risk_label: Classification.rsk.Moderate}
    
    @staticmethod
    def score_education(education, w):
        if education in Classification.feat.primary:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w, 
                    Classification.key.risk_label: Classification.rsk.Conservative}
        elif education in Classification.feat.secondary:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w, 
                    Classification.key.risk_label: Classification.rsk.ModerateConservative}
        elif education in Classification.feat.middle:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w, 
                    Classification.key.risk_label: Classification.rsk.Moderate}
        elif education in Classification.feat.bachelor:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w, 
                    Classification.key.risk_label: Classification.rsk.ModerateAggresive}
        else:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w, 
                    Classification.key.risk_label: Classification.rsk.Aggresive}
    
    @staticmethod
    def score_occupation(occupation, w):
        if (occupation in Classification.feat.retired) | (occupation in Classification.feat.unemployed):
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w+Classification.big_weight, 
                    Classification.key.risk_label: Classification.rsk.Conservative}
        elif (occupation in Classification.feat.housemaid) | (occupation in Classification.feat.student):
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w+Classification.mid_weight, 
                    Classification.key.risk_label: Classification.rsk.ModerateConservative}
        elif (occupation in Classification.feat.self_employed) | (occupation in Classification.feat.blue_collar):
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w+Classification.small_weight, 
                    Classification.key.risk_label: Classification.rsk.Moderate}
        elif occupation in Classification.feat.employee:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w, 
                    Classification.key.risk_label: Classification.rsk.ModerateAggresive}
        else:
            return {Classification.key.vote: 1, 
                    Classification.key.weight: w, 
                    Classification.key.risk_label: Classification.rsk.Aggresive}
    
    @staticmethod
    def __label_picking(tree):
        label_picking = [tree[i][Classification.key.risk_label] for i in range(len(tree))]
        return label_picking

    @staticmethod
    def __count_elements(tree):
        c = Counter()
        for v in tree:
            c[v[Classification.key.risk_label]] += v[Classification.key.vote]
        return {riskLabel: round(vote) for riskLabel, vote in c.items()}
    
    @staticmethod
    def probability_label(tree):
        label_list = Classification.__label_picking(tree)
        probability = Classification.__count_elements(tree)
        for p in set(label_list):
            probability[p] /= len(label_list)
        return probability
    
    @staticmethod
    def tree_generator(obj):
        
        first_round_dtree = [
            Classification.score_gender(obj.gender, Classification.wgt.gender),
            Classification.score_age(obj.age, Classification.wgt.age),
            Classification.score_education(obj.education, Classification.wgt.education),
            Classification.score_occupation(obj.job, Classification.wgt.occupation),
            Classification.score_marital(obj.marital, Classification.wgt.marital)
        ]

        second_round_dtree = [{"Second tree empty"}]
        third_round_dtree = [{"Third tree empty"}]
        
        return first_round_dtree
    
    @staticmethod
    def dict_filter(dic):
        filter_dic = {}
        for item in dic:
            if dic[item] > 0:
                filter_dic[item] = dic[item]
        return filter_dic
    
    @staticmethod
    def groupBy_key(tree, key, a=0, ma=0, m=0, mc=0, c=0, counter=0):
        for item in tree:
            counter += tree.count(item) 
            if item["risk_label"] == "A":
                a += item[key]
            elif item["risk_label"] == "MA":
                ma += item[key]
            elif item["risk_label"] == "M":
                m += item[key]
            elif item["risk_label"] == "MC":
                mc += item[key]
            elif item["risk_label"] == "C":
                c += item[key]
        return Classification.dict_filter({"A": a, "MA": ma, "M": m, "MC": mc, "C": c, "tot": counter})
    
    @staticmethod
    def prob_label(dic, a=0, ma=0, m=0, mc=0, c=0):
        for item in dic:
            if item == "A":
                a = dic[item] / dic["tot"]
            elif item == "MA":
                ma = dic[item] / dic["tot"]
            elif item == "M":
                m = dic[item] / dic["tot"]
            elif item == "MC":
                mc = dic[item] / dic["tot"]
            elif item == "C":
                c = dic[item] / dic["tot"]
        return Classification.dict_filter({"A": a, "MA": ma, "M": m, "MC": mc, "C": c})
    
    @staticmethod
    def solver(w):
        N = len(w)-1
        if N is 0:
            return float(1.0)
        return float(1 / N)
    
    @staticmethod
    def matmul_dict(weight, prob, w_a=0, w_ma=0, w_m=0, w_mc=0, w_c=0):
        for item in weight:
            if item == "A":
                w_a = (weight[item] * prob[item]) + weight[item]
            elif item == "MA":
                w_ma = (weight[item] * prob[item]) + weight[item]
            elif item == "M":
                w_m = (weight[item] * prob[item]) + weight[item]
            elif item == "MC":
                w_mc = (weight[item] * prob[item]) + weight[item]
            elif item == "C":
                w_c = (weight[item] * prob[item]) + weight[item]
        return Classification.dict_filter({"A": w_a, "MA": w_ma, "M": w_m, "MC": w_mc, "C": w_c})
    
    @staticmethod
    def truncate(x, threshold=0.6, truncate=0.5):
        return round(x - threshold + truncate)
    
    @staticmethod
    def consensus(dot, freq, w_a=0, w_ma=0, w_m=0, w_mc=0, w_c=0):
        for item in dot:
            if item == "A":
                w_a = Classification.truncate(dot[item] + freq[item])
            elif item == "MA":
                w_ma = Classification.truncate(dot[item] + freq[item])
            elif item == "M":
                w_m = Classification.truncate(dot[item] + freq[item])
            elif item == "MC":
                w_mc = Classification.truncate(dot[item] + freq[item])
            elif item == "C":
                w_c = Classification.truncate(dot[item] + freq[item])
        return Classification.dict_filter({"A": w_a, "MA": w_ma, "M": w_m, "MC": w_mc, "C": w_c})
    
    @staticmethod
    def assign_label(dic):
        if len(dic) is 0:
            return str("null")
        return str(max(dic))
    
    @staticmethod
    def run_classification_risk(tree):
        aggregate_weight = Classification.groupBy_key(tree, "weight")
        label_frequency = Classification.groupBy_key(tree, "vote")
        label_probability = Classification.prob_label(label_frequency)
        dot = Classification.matmul_dict(aggregate_weight, label_probability)
        consensus_vote = Classification.consensus(dot, label_frequency)
        return Classification.assign_label(consensus_vote)