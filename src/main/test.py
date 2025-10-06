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
ticker_a = "EUNL.DE"
ticker_b = "NQSE.DE"
sql = f"SELECT TICKER, CLOSE_PRICE, DATA FROM ETF_HISTORY WHERE TICKER in ('{ticker_a}', '{ticker_b}') AND DATA >= '01-JAN-2023' ORDER BY DATA"
connection = get_connected(db_usr, db_pwd, db_dsn)
universo = retrieve_data(connection, sql)

def get_universo_ticker(universo:list[tuple], f_ticker:str) -> list[tuple]:
    universo_ticker = []
    for ticker, close_price, data in universo:
        if ticker.lower() == f_ticker.lower():
            universo_ticker.append((ticker, close_price, data))
    return sorted(universo_ticker, key=lambda x: x[2])

def get_prob_classica(univ:list[tuple]) -> float:
    def get_casi_favorevoli() -> int:
        counter = 0
        for i in range(1, len(univ)):

            if univ[i][1] > univ[i-1][1]:
                counter +=1 
        return counter
    return get_casi_favorevoli() / (len(univ) - 1)

def get_up_days(univ:list[tuple]):
    up_days = {}
    for i in range(1, len(univ)):
        if univ[i][1] > univ[i-1][1]:
            up_days[univ[i][2]] = True
        else:
            up_days[univ[i][2]] = False
    return up_days
            

universo_eunl = get_universo_ticker(universo, ticker_a)
probabilita_eunl_up = get_prob_classica(universo_eunl)

universo_nqse = get_universo_ticker(universo, ticker_b)
print(len(universo_eunl) == len(universo_nqse))

probabilita_nqse_up = get_prob_classica(universo_nqse)

print(f"Probabilita' che nella prossima chiusura EUNL sia up: {round(probabilita_eunl_up * 100, 2)}%")
print(f"Probabilita' che nella prossima chiusura NQSE sia up: {round(probabilita_nqse_up * 100, 2)}%")

up_down_days_eunl = get_up_days(universo_eunl)
up_down_days_nqse = get_up_days(universo_nqse)

set_eunl_up = {d for d, up in up_down_days_eunl.items() if up}
set_nqse_up = {d for d, up in up_down_days_nqse.items() if up}

date_comuni = set(up_down_days_eunl.keys()).intersection(set(up_down_days_nqse.keys()))

date_congiunte = set_eunl_up.intersection(set_nqse_up)

p_eunl = len(set_eunl_up.intersection(date_comuni)) / len(date_comuni)
p_nqse = len(set_nqse_up.intersection(date_comuni)) / len(date_comuni)
p_eunl_nqse = len(date_congiunte) / len(date_comuni)

print(p_eunl_nqse*100 > probabilita_eunl_up * probabilita_nqse_up)

