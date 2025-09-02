import logging
import logging.config
from dotenv import load_dotenv
import os
import oracledb as odb

load_dotenv()

im_user = os.getenv("db_user")
im_pwd = os.getenv("db_pwd")
im_dsn = os.getenv("db_dsn")

logging.config.fileConfig("logging.ini")
logger = logging.getLogger(__name__)

sql = "SELECT TICKER, DATA, CLOSE_PRICE FROM ETF_HISTORY WHERE TICKER IN (SELECT TICKER FROM CURRENT_ETF) AND DATA >= '01-JAN-2020' ORDER BY DATA"

with odb.connect(user=im_user, password=im_pwd, dsn=im_dsn) as connection:
    with connection.cursor() as cursor:
        try:
            cursor.execute(sql)
            
            table = cursor.fetchall()
            logger.info(f"fetched {len(table)} rows using {sql}")
            
            for i in range(len(table)):
                ticker = table[i][0]
                data = table[i][1]
                close_price = table[i][2]

            
        except Exception as e:
            print(f"Errore durante l'esecuzione della query: {sql} - {e}")
            logger.error(f"Errore durante l'esecuzione della query: {sql} - {e}")
            
            
