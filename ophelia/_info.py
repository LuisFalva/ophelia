from ._logger import OpheliaLogger


class OpheliaInfo:

    def __init__(self):
        self.__logger = OpheliaLogger()
        self.__version = "Ophelia.0.0.1"
        self.__info = {
            "who is": "Hello! This engine is for data mining & ml pipelines in PySpark",
            "welcome": "Welcome to Ophelia Spark miner engine",
            "version": f"Package Version {self.__version}",
            "warn": "V for Vendata...",
        }

    def __auto_space(self, msg):
        max_info = self.__info[max(self.__info)]
        default_tape = "█ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █"
        if len(default_tape) > len(max_info):
            space = " " * (abs(len(default_tape) - len(msg)))
            return msg + space
        elif len(default_tape) < len(max_info):
            space = " " * (abs(len(max_info) - len(msg)))
            return msg + space

    def __build_info(self):
        self.__logger.tape(self.__info[max(self.__info)], adjust_tape=-2)
        for i in self.__info.keys():
            if i != "warn":
                self.__logger.info("| " + self.__auto_space(self.__info[i]) + " |")
            else:
                self.__logger.warning("| " + self.__auto_space(self.__info[i]) + " |")
        self.__logger.tape(self.__info[max(self.__info)], adjust_tape=-2)

    def __build_mask(self):
        self.__logger.warning(
            self.__auto_space(
                "                      - Ophelia Spark -                     "
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ █ █ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ █ █ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ █ ╬ ╬ ╬ █ ╬ ╬ ╬ █ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ █ █ ╬ ╬ ╬ █ ╬ ╬ ╬ █ █ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ ╬ ╬ █ █ █ █ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ █ █ █ █ ╬ ╬ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ ╬ ╬ ▓ █ █ █ ╬ ╬ ╬ █ █ █ █ ╬ █ █ █ █ ╬ ╬ ╬ █ █ █ ▓ ╬ ╬ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ █ ╬ ╬ ▓ ▓ █ █ █ █ █ █ █ ╬ ╬ ╬ █ █ █ █ █ █ █ ▓ ▓ ╬ ╬ █ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ █ ╬ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ ╬ ╬ █ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █ █ █"
            )
        )
        self.__logger.mask(
            self.__auto_space(
                "  █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █\n"
            )
        )

    def __build_ophelia_message(self):
        self.__build_info()
        self.__build_mask()

    def print_info(self, mask):
        if mask:
            return self.__build_ophelia_message()
        return self.__build_info()
