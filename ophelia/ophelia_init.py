from ..ophelia.ophelia_logger import OpheliaLogger


class OpheliaInit:

    def __init__(self):
        self.__logger = OpheliaLogger()
        self.__version = 'Ophelia.0.0.1'
        self.__info = {
            "who is": "Hello! This API builds data mining & ml pipelines with pyspark",
            "welcome": "Welcome to Ophelia pyspark miner engine",
            "version": f"API Version {self.__version}",
            "warn": "V for Vendata..."
        }

    def __auto_space(self, msg):
        max_info = self.__info[max(self.__info)]
        default_tape = "█ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █"
        if len(default_tape) > len(max_info):
            space = " "*(abs(len(default_tape) - len(msg)))
            return msg + space
        elif len(default_tape) < len(max_info):
            space = " "*(abs(len(max_info) - len(msg)))
            return msg + space

    def __build_info(self):
        self.__logger.tape(self.__info[max(self.__info)], adjust_tape = -2)
        for i in self.__info.keys():
            if i != "warn":
                self.__logger.info('| ' + self.__auto_space(self.__info[i]) + ' |')
            else:
                self.__logger.warning('| ' + self.__auto_space(self.__info[i]) + ' |')
        self.__logger.tape(self.__info[max(self.__info)], adjust_tape = -2)

    def __build_mask(self):
        self.__logger.tape(self.__info[max(self.__info)], adjust_tape = -2)
        self.__logger.warning(self.__auto_space("                - Ophelia's Fellowship Gentleman -"))
        self.__logger.mask(self.__auto_space("  █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ █ █ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ █ █ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ █ ╬ ╬ ╬ █ ╬ ╬ ╬ █ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ █ █ ╬ ╬ ╬ █ ╬ ╬ ╬ █ █ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ ╬ ╬ █ █ █ █ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ █ █ █ █ ╬ ╬ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"))
        self.__logger.mask(self.__auto_space("  █ █ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ ╬ ╬ ▓ █ █ █ ╬ ╬ ╬ █ █ █ █ ╬ █ █ █ █ ╬ ╬ ╬ █ █ █ ▓ ╬ ╬ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ █ ╬ ╬ ▓ ▓ █ █ █ █ █ █ █ ╬ ╬ ╬ █ █ █ █ █ █ █ ▓ ▓ ╬ ╬ █ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ █ ╬ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ ╬ ╬ █ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █ █ █"))
        self.__logger.mask(self.__auto_space("  █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █"))
        self.__logger.warning(self.__auto_space(""))
        self.__logger.tape(self.__info[max(self.__info)], adjust_tape = -2)

    def __build_ophelia_message(self):
        self.__build_info()
        self.__build_mask()

    def print_info(self, no_mask=True):
        if no_mask:
            return self.__build_info()
        return self.__build_ophelia_message()
