from datetime import datetime
import logging
import logging.config
import json
import os
from dotenv import load_dotenv
from persistence.dbconnector.db_connector import get_connected
from persistence.dbio.cursor import retrieve_data

load_dotenv()
logging_config = "logging.ini"
if os.path.exists(logging_config):
    logging.config.fileConfig(logging_config)
else:
   logging.basicConfig(level=logging.INFO)


logger = logging.getLogger(__name__)
os.makedirs("resources", exist_ok=True)

db_usr = os.getenv("db_user")
db_pwd = os.getenv("db_pwd")
db_dsn = os.getenv("db_dsn")
ticker_a = "JEDI.DE"
ticker_b = "NQSE.DE"
sql = f"SELECT TICKER, CLOSE_PRICE, DATA FROM ETF_HISTORY WHERE TICKER in ('{ticker_a}', '{ticker_b}') AND DATA >= '01-JAN-2023' ORDER BY DATA"
connection = get_connected(db_usr, db_pwd, db_dsn)
universo_base = retrieve_data(connection, sql)

def get_ticker_universe(f_ticker:str, univ: list[tuple]):
    ticker_universe = []
    for ticker, close_price, data in univ:
        if ticker.lower() == f_ticker.lower():

            ticker_universe.append((ticker, close_price, data))
    return sorted(ticker_universe, key=lambda x: x[2])


def get_flags(ticker_universe:list[tuple]):
    flags = {}
    for i in range(1, len(ticker_universe)):
        if ticker_universe[i][1] > ticker_universe[i-1][1]:
            flags[ticker_universe[i][2]] = True
        else:
            flags[ticker_universe[i][2]] = False
    return flags


def get_p(flags:dict[datetime:bool]):    
    def get_p():
        counter = 0
        for k, v in flags.items():
            if v == True:
                counter += 1
        return counter
    return get_p() / len(flags)


jedi_universe = get_ticker_universe(ticker_a, universo_base)
nqse_universe = get_ticker_universe(ticker_b, universo_base)
print(len(jedi_universe) == len(nqse_universe))
flags_jedi = get_flags(jedi_universe)
p_jedi = get_p(flags_jedi)


print(f"Probabilita' che {ticker_a} salga domani: {round(p_jedi*100, 2)}%")
