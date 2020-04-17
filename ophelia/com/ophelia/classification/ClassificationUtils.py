from collections import Counter

from .Module import KeyNameStructure, RiskLabelClass, DemographFeature, WeightLabelClass


class DemographicScoring:
    
    key = KeyNameStructure()
    label_class = RiskLabelClass()
    demograph = DemographFeature()
    weight = WeightLabelClass()

    small_weight = demograph.small_weight
    mid_weight = demograph.mid_weight
    big_weight = demograph.big_weight

    @staticmethod
    def score_gender(gender, w):
        if gender in DemographicScoring.demograph.female:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.ModerateAggresive}
        elif gender in DemographicScoring.demograph.male:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.Aggresive}
    
    @staticmethod
    def score_age(age, w):
        if age > 60:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w+DemographicScoring.big_weight, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.Conservative}
        elif 50 < age <= 60:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w+DemographicScoring.mid_weight, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.ModerateConservative}
        elif 40 < age <= 50:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w+DemographicScoring.small_weight, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.Moderate}
        elif 30 < age <= 40:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.ModerateAggresive}
        else:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.Aggresive}
    
    @staticmethod
    def score_marital(marital, w):
        if marital in DemographicScoring.demograph.married:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.ModerateAggresive}
        if marital in DemographicScoring.demograph.single:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.Aggresive}
        else:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.Moderate}
    
    @staticmethod
    def score_education(education, w):
        if education in DemographicScoring.demograph.primary:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.Conservative}
        elif education in DemographicScoring.demograph.secondary:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.ModerateConservative}
        elif education in DemographicScoring.demograph.middle:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.Moderate}
        elif education in DemographicScoring.demograph.bachelor:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.ModerateAggresive}
        else:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.Aggresive}
    
    @staticmethod
    def score_occupation(occupation, w):
        if (occupation in DemographicScoring.demograph.retired) | (occupation in DemographicScoring.demograph.unemployed):
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w+DemographicScoring.big_weight, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.Conservative}
        elif (occupation in DemographicScoring.demograph.housemaid) | (occupation in DemographicScoring.demograph.student):
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w+DemographicScoring.mid_weight, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.ModerateConservative}
        elif (occupation in DemographicScoring.demograph.self_employed) | (occupation in DemographicScoring.demograph.blue_collar):
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w+DemographicScoring.small_weight, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.Moderate}
        elif occupation in DemographicScoring.demograph.employee:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.ModerateAggresive}
        else:
            return {DemographicScoring.key.vote: 1, 
                    DemographicScoring.key.weight: w, 
                    DemographicScoring.key.risk_label: DemographicScoring.label_class.Aggresive}
    
    @staticmethod
    def __label_picking(tree):
        label_picking = [tree[i][DemographicScoring.key.risk_label] for i in range(len(tree))]
        return label_picking

    @staticmethod
    def __count_elements(tree):
        c = Counter()
        for v in tree:
            c[v[DemographicScoring.key.risk_label]] += v[DemographicScoring.key.vote]
        return {riskLabel: round(vote) for riskLabel, vote in c.items()}
    
    @staticmethod
    def probability_label(tree):
        label_list = DemographicScoring.__label_picking(tree)
        probability = DemographicScoring.__count_elements(tree)
        for p in set(label_list):
            probability[p] /= len(label_list)
        return probability
    
    @staticmethod
    def tree_generator(obj):
        
        first_round_dtree = [
            DemographicScoring.score_gender(obj.gender, DemographicScoring.weight.gender),
            DemographicScoring.score_age(obj.age, DemographicScoring.weight.age),
            DemographicScoring.score_education(obj.education, DemographicScoring.weight.education),
            DemographicScoring.score_occupation(obj.job, DemographicScoring.weight.occupation),
            DemographicScoring.score_marital(obj.marital, DemographicScoring.weight.marital)
        ]

        second_round_dtree = [{"Second tree empty"}]
        third_round_dtree = [{"Third tree empty"}]
        
        return first_round_dtree