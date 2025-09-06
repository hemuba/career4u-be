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

sql = "SELECT TICKER, DATA, CLOSE_PRICE FROM ETF_HISTORY WHERE TICKER IN (SELECT TICKER FROM CURRENT_ETF) AND DATA >= '01-JAN-2021' ORDER BY DATA"

eunl_prices = []
nqse_prices = []

eunl_percs = []
nqse_percs = []
with odb.connect(user=im_user, password=im_pwd, dsn=im_dsn) as connection:
    with connection.cursor() as cursor:
        try:
            cursor.execute(sql)
            
            table = cursor.fetchall()
            logger.info(f"fetched {len(table)} rows using {sql}")
            try:
                for i in range(len(table)):
                    ticker = table[i][0]
                    data = table[i][1]
                    close_price = table[i][2]
                    
                
                    if ticker == "EUNL.DE":
                        eunl_prices.append(close_price)
                    elif ticker == "NQSE.DE":
                        nqse_prices.append(close_price)
                            
                
                if len(eunl_prices) != len(nqse_prices):
                    raise RuntimeError("Check your code, series must have same lenghts.")
                
                for i in range(1, len(eunl_prices)):
                    eunl_percs.append((eunl_prices[i] / eunl_prices[i-1] -1) * 100)
                    nqse_percs.append((nqse_prices[i] / nqse_prices[i-1] -1) * 100)
                    
                ma_eunl = sum(eunl_percs) / len(eunl_percs)
                ma_nqse = sum(nqse_percs) / len(nqse_percs)
            
                scostamenti_eunl = [n - ma_eunl for n in eunl_percs]
                
                scostamenti_nqse = [n - ma_nqse for n in nqse_percs]

                n = len(scostamenti_eunl)
                somma = 0
                for i in range(len(scostamenti_eunl)):
                    somma += scostamenti_eunl[i] * scostamenti_nqse[i]
                    
                cov_eunl_nqse = somma / (n - 1)
                    

                var_eunl = [n ** 2 for n in scostamenti_eunl]
                var_nqse = [n ** 2 for n in scostamenti_nqse]
                
                std_eunl = (sum(var_eunl) / (len(eunl_percs) - 1)) ** 0.5
                std_nqse = (sum(var_nqse) / (len(nqse_percs) - 1)) ** 0.5
                
                correlazione = cov_eunl_nqse / (std_eunl * std_nqse)
                
                print(f"La correlazione tra EUNL e NQSE e' di: {round(correlazione, 2)}")
                
            except Exception as e:
                print(f"Error: {e}")
            
            
            
            
        except Exception as e:
            print(f"Errore durante l'esecuzione della query: {sql} - {e}")
            logger.error(f"Errore durante l'esecuzione della query: {sql} - {e}")
            
            
