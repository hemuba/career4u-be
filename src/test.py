import logging
import logging.config
from dotenv import load_dotenv
import os
import oracledb as odb
from datetime import datetime

load_dotenv()

im_user = os.getenv("db_user")
im_pwd = os.getenv("db_pwd")
im_dsn = os.getenv("db_dsn")

logging.config.fileConfig("logging.ini")
logger = logging.getLogger(__name__)

sql = "SELECT TICKER, DATA, CLOSE_PRICE FROM ETF_HISTORY WHERE TICKER IN (SELECT TICKER FROM CURRENT_ETF) AND DATA >= '01-JAN-2019' ORDER BY DATA"

eunl_prices = {}
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
                
            
                if ticker == "EUNL.DE":
                    if ticker in eunl_prices:
                        eunl_prices[ticker].append(close_price)
                    else:
                        eunl_prices[ticker] = [close_price]
                        
            drop_t = []
            for ticker, lista in eunl_prices.items():
                max_val = lista[0] 
                for i in range(len(lista)):
                    
                    current_val = lista[i] 
                    
                    if current_val > max_val:
                        max_val = current_val
                    drop_t.append((current_val - max_val) / max_val * 100)
               
            if drop_t:     
                if drop_t[0] != 0 or len(drop_t) != len(eunl_prices["EUNL.DE"]):
                    raise RuntimeError("Error: check your logic, there is an issue calculating the DrawDown")
            
                if any(n > 0 for n in drop_t):
                    raise RuntimeError("Error: check your logic, there is an issue calculating the DrawDown")
            else:
                raise RuntimeError("Error: check your logic, there is an issue calculating the DrawDown")
                
                
            
            max_dd = min(drop_t)
    
            
            print(f"Max DD for EUNL.DE: {round(max_dd, 2)}%")
                    
                               

        
            
        except Exception as e:
            print(f"Errore durante l'esecuzione della query: {sql} - {e}")
            logger.error(f"Errore durante l'esecuzione della query: {sql} - {e}")
            
            
