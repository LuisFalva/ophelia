from datetime import datetime


class OpheliaLogger:

    @staticmethod
    def get_current_time():
        time_format = '%H:%M:%S.%f'
        return datetime.now().time().strftime(time_format)[:-3]

    def get_message(self, message, level):
        time = self.get_current_time()
        ophelia_name = "Ophelia"
        return "{0} {1} [{2}] {3}".format(time, ophelia_name, level, message)

    def debug(self, message):
        print(self.get_message(message, "DEBUG"))

    def info(self, message):
        print(self.get_message(message, "INFO"))

    def warning(self, message):
        print(self.get_message(message, "WARN"))

    def error(self, message):
        print(self.get_message(message, "ERROR"))
