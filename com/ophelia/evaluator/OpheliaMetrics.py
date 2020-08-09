from math import log
from com.ophelia.utils.logger import OpheliaLogger


class OpheliaMetrics:

    __logger = OpheliaLogger()

    @staticmethod
    def probability_class(node):
        node_sum = sum(node.values())
        percents = {c: v / node_sum for c, v in node.items()}
        return node_sum, percents

    @staticmethod
    def gini_score(node):
        _, percents = OpheliaMetrics.probability_class(node)
        # donde i contiene la probabilidad calculada del nodo en cuestión
        score = round(1 - sum([i**2 for i in percents.values()]), 3)
        OpheliaMetrics.__logger.info("Gini Score for node {} : {}".format(node, score))
        return score

    @staticmethod
    def entropy_score(node):
        _, percents = OpheliaMetrics.probability_class(node)
        score = round(sum([-i * log(i, 2) for i in percents.values()]), 3)
        OpheliaMetrics.__logger.info("Entropy Score for node {} : {}".format(node, score))
        return score

    @staticmethod
    def information_gain(parent, children, criterion):
        score = {'gini': OpheliaMetrics.gini_score, 'entropy': OpheliaMetrics.entropy_score}
        metric = score[criterion]
        parent_score = metric(parent)
        parent_sum = sum(parent.values())
        weighted_child_score = sum([metric(i) * sum(i.values()) / parent_sum for i in children])
        gain = round((parent_score - weighted_child_score), 2)
        OpheliaMetrics.__logger.info("Information gain: {}".format(gain))
        return gain
