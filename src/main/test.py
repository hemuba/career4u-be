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
sql = "SELECT TICKER, CLOSE_PRICE, DATA FROM ETF_HISTORY WHERE TICKER = 'EUNL.DE' AND DATA >= '01-JAN-2023' ORDER BY DATA"
connection = get_connected(db_usr, db_pwd, db_dsn)
universo = retrieve_data(connection, sql)




def get_prob(univ:list[tuple]) -> float:
    def get_casi_favorevoli() -> int:
        counter = 0
        for i in range(1, len(univ)):

            if univ[i][1] > univ[i-1][1]:
                counter +=1 
        return counter
    return get_casi_favorevoli() / (len(univ) - 1)



probabilita = get_prob(universo)

print(probabilita*100)