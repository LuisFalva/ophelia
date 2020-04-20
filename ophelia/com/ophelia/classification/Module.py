import numpy as np


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
        self.occupation = "job"
        self.prob = "prob"
        self.weighted_prob = "weighted_prob"
        self.final_vote = "final_vote"
        self.sum_vote = "sum_vote"
        self.sum_weight = "sum_weight"
        self.sum_prob = "sum_prob"
        self.sum_weighted_prob = "sum_weighted_prob"
        self.grouping = [self.id, self.risk_label, 
                         self.age, self.education, 
                         self.gender, self.marital, 
                         self.occupation]


class WeightLabel:
    
    def __init__(self, W=None):
        """
        Constructor object for input parameters.
        :param NAME_PARAM: "DESCRIPTION HERE"        
        """
        if W == None:
            self.W = np.random.dirichlet(np.ones(5),size=1).tolist()[0]
        else:
            # valores que pueden venir de otro origen .conf
            self.W = W
        
        self.gender = self.W[0]
        self.age = self.W[1]
        self.education = self.W[2]
        self.occupation = self.W[3]
        self.marital = self.W[4]
    

class RiskLabel:
    
    def __init__(self):
        """
        Constructor object for input parameters.
        :param NAME_PARAM: "DESCRIPTION HERE"        
        """
        self.Aggresive = "A"
        self.ModerateAggresive = "MA"
        self.Moderate = "M"
        self.ModerateConservative = "MC"
        self.Conservative = "C"
        
class Feature:
    
    def __init__(self):
        """
        Constructor object for input parameters.
        :param NAME_PARAM: "DESCRIPTION HERE"        
        """
        self.female = {
            "femenino":"female", 
            "mujer":"female", 
            "women":"female", 
            "female":"female"
        }
        self.male = {
            "masculino":"male",
            "hombre":"male",
            "men":"male",
            "male":"male"
        }
        self.married = {
            "casado":"married",
            "married":"married"
        }
        self.single = {
            "soltero":"single",
            "single":"single"
        }
        self.primary = {
            "primaria":"elementary school",
            "primary":"elementary school",
            "elementary school":"elementary school",
            "elementary-school":"elementary school"
        }
        self.secondary = {
            "secundaria":"junior higschool",
            "secondary":"junior higschool",
            "junior higschool":"junior higschool",
            "junior-higschool":"junior higschool"
        }
        self.middle = {
            "tertiary":"higschool",
            "media":"higschool",
            "media-superior":"higschool",
            "media superior":"higschool",
            "bachillerato":"higschool",
            "preparatoria":"higschool"
        }
        self.bachelor = {
            "unknown":"bachelor",
            "superior":"bachelor",
            "universidad":"bachelor",
            "bachelor":"bachelor",
            "college":"bachelor"
        }
        self.retired = {
            "retirado":"retired",
            "retired":"retired",
            "jubilado":"retired",
            "pensionado":"retired"
        }
        self.unemployed = {
            "desempleado":"unemployed",
            "unemployed":"unemployed"
        }
        self.housemaid = {
            "hogar":"housemaid",
            "casa":"housemaid",
            "housemaid":"housemaid",
            "house":"housemaid",
            "home":"housemaid"
        }
        self.student = {
            "estudiante":"student",
            "academico":"student",
            "academia":"student",
            "escuela":"student",
            "colegio":"student",
            "student":"student",
            "college":"student",
            "school":"student"
        }
        self.self_employed = {
            "independiente":"self-employed",
            "freelance":"self-employed",
            "autoempleado":"self-employed",
            "auto empleado":"self-employed",
            "auto-empleado":"self-employed",
            "self-employed":"self-employed",
            "servicios":"self-employed",
            "services":"self-employed",
            "entrepreneur":"self-employed",
            "emprendedor":"self-employed"
        }
        self.employee = {
            "empleado":"employee",
            "employee":"employee",
            "technician":"employee",
            "admin.":"employee",
            "management":"employee"
        }
        self.blue_collar = {
            "blue-collar":"blue-collar",
            "trabajador":"blue-collar",
            "obrero":"blue-collar"
        }
        self.small_weight = 1.25
        self.mid_weight = 2
        self.big_weight = 3
        