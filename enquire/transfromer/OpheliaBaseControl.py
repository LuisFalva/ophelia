import sparkutils
import traceback

import datetime as dt
from net_balance.abt_netbalance.loading.table_loader import TableLoader
from net_balance.abt_netbalance.process.net_balance import NetBalance
from net_balance import NetBalanceException
from net_balance.conf.module import Module


class OpheliaBaseControl:
    """
        This class generates the net balance of a specific date
    """
    __logger = sparkutils.get_logger('NetBalanceProcess')

    def __init__(self):
        self.__logger = sparkutils.get_logger(NetBalanceProcess.__qualname__)

    def save_table(self, path, dataframe):
        self.__logger.info("Writing in HDFS: " + str(dt.datetime.now()))
        dataframe.write.mode('overwrite').parquet(path)
        self.__logger.info("Saved in: " + path)

    @staticmethod
    def make_net_balance_tables(params, spark_session, activation_date):
        try:
            time_frame = Module.TIME_FRAME
            fide_time_frame = Module.FIDE_TIME_FRAME
            months_back = Module.USER_ACTIVE_MONTHS
            update_day = Module.MONTHLY_UPDATE_DATE
            table_loader = TableLoader()
            table_loader.load_params(params)
            net_balance = NetBalance(spark_session)
            (tmlara14_data, tla018_data, tla1872_data,
             tla1199_data, tla026_data,
             latest_tla1872_date, latest_tla026_date,
             customer_liabilities_tables, tla1705_data,
             tla938_data, latest_tla938_date, tla539_data) = (table_loader.load_net_balance_tables(spark_session,
                                                                                                   activation_date,
                                                                                                   time_frame,
                                                                                                   fide_time_frame,
                                                                                                   update_day))

            customers_net_balance = net_balance.make_calendar(tmlara14_data, tla018_data,
                                                              tla1872_data, tla1199_data,
                                                              tla026_data,
                                                              latest_tla1872_date,
                                                              latest_tla026_date,
                                                              customer_liabilities_tables, tla1705_data,
                                                              tla938_data, latest_tla938_date, tla539_data,
                                                              months_back)
            return customers_net_balance
        except Exception as e:
            traceback.print_exc()
            NetBalanceProcess.__logger.error("Error in make_net_balance_tables method: " + str(e))
            raise NetBalanceException('Error in make_net_balance_tables method: ' + str(e))

    def run(self, spark, app_config):
        try:
            param_date = str(app_config.getString("net_balance.target_date"))
            year, month, day = map(int, param_date.split("-"))
            activation_date = dt.date(year, month, day)
            self.__logger.info("*** "+str(activation_date)+" *** ")

            self.__logger.info("Start process: " + str(dt.datetime.now()))
            net_balance_path = Module.OUTPUT_MODULE_GOV_PATH + "/" + "information_date=" + str(activation_date)

            self.__logger.info("Loading tables from HDFS: " + str(dt.datetime.now()))
            net_balance_table = self.make_net_balance_tables(app_config, spark, activation_date)
            self.save_table(net_balance_path, net_balance_table)

            self.__logger.info("End process: " + str(dt.datetime.now()))
        except NetBalanceException as chcex:
            self.__logger.error('Error creating NetBalance ABT: ' + str(chcex))
            return -1

        except Exception as e:
            traceback.print_exc()
            self.__logger.error('Error creating NetBalance ABT: ' + str(e))
            return -1

        return 0
