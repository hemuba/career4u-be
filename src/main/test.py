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
ticker_a = "IS3N.DE"
ticker_b = "EUNL.DE"
sql = f"SELECT TICKER, CLOSE_PRICE, DATA FROM ETF_HISTORY WHERE TICKER in ('{ticker_a}', '{ticker_b}') AND DATA >= '01-JAN-2025' ORDER BY DATA"
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


def get_congiunta(univ_a, univ_b):
    def get_common_dates():
        common_dates = set(univ_a.keys()) & set(univ_b.keys())
        if not common_dates:
            logger.error("Non ci sono date in comune")
            raise ValueError("Non ci sono date in comune")
        return common_dates
    U = get_common_dates()
    A_true = {d for d in U if univ_a[d]}
    B_true = {d for d in U if univ_b[d]}
    A_and_B = A_true & B_true
    pA = len(A_true) / len(U)
    pB = len(B_true) / len(U)
    pAB = len(A_and_B) / len(U)
    
    return pA, pB, pAB

def check_congiunta(f_pA, f_pB, f_pAB):
    return f_pAB > f_pA * f_pB 

def is_dependent(pA: float, pB: float, pAB: float, tau: float = 1e-3) -> bool:
    return pAB > pA * pB + tau 

def get_condizionata(f_pAB, f_pB):
    if f_pB == 0:
        logger.error("ERR: Divisione per 0")
        raise ValueError("ERR: Divisione per 0")
    return f_pAB / f_pB

jedi_universe = get_ticker_universe(ticker_a, universo_base)
nqse_universe = get_ticker_universe(ticker_b, universo_base)
flags_jedi = get_flags(jedi_universe)
flags_nqse = get_flags(nqse_universe)

pA, pB, pAB = get_congiunta(flags_jedi, flags_nqse)
logger.info(f"Probabilita' che {ticker_a} sia UP: {round(pA*100, 2)}")
logger.info(f"Probabilita' {ticker_b} UP: {round(pB*100, 2)}")
logger.info(f"Probabilita' Congiunta {ticker_a} & {ticker_b}: {round(pAB*100, 2)}")

if is_dependent(pA, pB, pAB, tau=1e-3):
    a_cond_b = get_condizionata(pAB, pB)
    logger.info(f"{ticker_a} sale con il {round(a_cond_b*100, 2)}% quando sale {ticker_b}")
else:
    logger.info("Nessuna dipendenza positiva significativa: salto P(A|B).")
