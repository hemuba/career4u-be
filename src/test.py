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

eunl_prices = []
nqse_prices = []
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
                    eunl_prices.append(close_price)
                elif ticker == "NQSE.DE":
                    nqse_prices.append(close_price)
                        
            drop_t_eunl = []
            eunl_max_val = eunl_prices[0] 
            for i in range(len(eunl_prices)):
                    
                    current_val = eunl_prices[i] 
                    
                    if current_val > eunl_max_val:
                        eunl_max_val = current_val
                    drop_t_eunl.append((current_val - eunl_max_val) / eunl_max_val * 100)
               
            if drop_t_eunl:     
                if drop_t_eunl[0] != 0 or len(drop_t_eunl) != len(eunl_prices):
                    raise RuntimeError("Error: check your logic, there is an issue calculating the DrawDown")
            
                if any(n > 0 for n in drop_t_eunl):
                    raise RuntimeError("Error: check your logic, there is an issue calculating the DrawDown")
            else:
                raise RuntimeError("Error: check your logic, there is an issue calculating the DrawDown")
            
            drop_t_nqse = []
            nqse_max_val = nqse_prices[0] 
            for i in range(len(nqse_prices)):
                    
                    current_val = nqse_prices[i] 
                    
                    if current_val > nqse_max_val:
                        nqse_max_val = current_val
                    drop_t_nqse.append((current_val - nqse_max_val) / nqse_max_val * 100)
               
            if drop_t_nqse:     
                if drop_t_nqse[0] != 0 or len(drop_t_nqse) != len(nqse_prices):
                    raise RuntimeError("Error: check your logic, there is an issue calculating the DrawDown")
            
                if any(n > 0 for n in drop_t_nqse):
                    raise RuntimeError("Error: check your logic, there is an issue calculating the DrawDown")
            else:
                raise RuntimeError("Error: check your logic, there is an issue calculating the DrawDown")
                
                
            
            max_dd_eunl = min(drop_t_eunl)
            max_dd_nqse = min(drop_t_nqse)
            
            print(f"Max DD for EUNL.DE: {round(max_dd_eunl, 2)}%\nMax DD for NQSE.DE: {round(max_dd_nqse, 2)}")
                    
                               

        
            
        except Exception as e:
            print(f"Errore durante l'esecuzione della query: {sql} - {e}")
            logger.error(f"Errore durante l'esecuzione della query: {sql} - {e}")
            
            
