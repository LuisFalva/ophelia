import traceback
import datetime as dt
from enquire.transfromer.ConfigModule import EnvironModule


class BaseControl:
    """
        Ophelia Base Control class is the main controller class to execute transformer module
    """
    __logger = sparkutils.get_logger('BaseControl')

    def __init__(self):
        self.__logger = sparkutils.get_logger(BaseControl.__qualname__)
        self.__envmod = EnvironModule()

    def save_table(self, path, dataframe):
        self.__logger.info("Writing in HDFS: " + str(dt.datetime.now()))
        dataframe.write.mode('overwrite').parquet(path)
        self.__logger.info("Saved in: " + path)

    def run(self, spark, app_config):
        try:
            param_date = str(app_config.getString("transformer.target_date"))
            year, month, day = map(int, param_date.split("-"))
            activation_date = dt.date(year, month, day)
            self.__logger.info("*** "+str(activation_date)+" *** ")

            self.__logger.info("Start process: " + str(dt.datetime.now()))
            output_path = self.__envmod.OUTPUT_MODULE_PATH + "/" + "information_date=" + str(activation_date)

            self.__logger.info("Loading tables from HDFS: " + str(dt.datetime.now()))
            df = self.df(app_config, spark, activation_date)
            self.save_table(df, df)

            self.__logger.info("End process: " + str(dt.datetime.now()))
        except NetBalanceException as chcex:
            self.__logger.error('Error creating NetBalance ABT: ' + str(chcex))
            return -1

        except Exception as e:
            traceback.print_exc()
            self.__logger.error('Error creating NetBalance ABT: ' + str(e))
            return -1

        return 0
