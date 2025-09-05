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

etfs = {}
close_prices_eunl = []
rendimenti_eunl = []
close_prices_nqse = []
rendimenti_nqse = []
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
                
                if ticker in etfs:
                    etfs[ticker].append((data, close_price))
                else:
                    etfs[ticker] = [(data, close_price)]
            
            for ticker, tupla in etfs.items():
                if ticker == "EUNL.DE":
                    for d, p in tupla:
                        close_prices_eunl.append(p)
                elif ticker == "NQSE.DE":
                    for d, p in tupla:
                        close_prices_nqse.append(p)

            for i in range(1, len(close_prices_eunl)):
                rendimenti_eunl.append((close_prices_eunl[i] / close_prices_eunl[i-1] - 1 ) * 100)
                
            for i in range(1, len(close_prices_nqse)):
                rendimenti_nqse.append((close_prices_nqse[i] / close_prices_nqse[i-1] - 1) * 100)
                
                
            if len(rendimenti_eunl) - len(rendimenti_nqse) != 0:
                raise RuntimeError(f"Cannot compare entities. Len entities is not the same.")
            
            if len(rendimenti_eunl) != len(close_prices_eunl) -1:
                raise RuntimeError(f"Rendimenti_eunl is: {len(rendimenti_eunl)}, should be {len(close_prices_eunl) -1}")
            
            den_eunl = len(rendimenti_eunl)
            MA_eunl = sum(rendimenti_eunl) / den_eunl
            
            den_nqse = len(rendimenti_nqse)
            MA_nqse = sum(rendimenti_nqse) / den_nqse
            
            scostamento_quadratico_eunl = [(r -  MA_eunl)**2 for r in rendimenti_eunl]
            volatilita_eunl_daily = (sum(scostamento_quadratico_eunl) / den_eunl) ** 0.5
            
            scostamento_quadratico_nqse = [(r- MA_nqse) **2 for r in rendimenti_nqse]
            volatilita_nqse_daily = (sum(scostamento_quadratico_nqse) / den_nqse) ** 0.5
            
            
            volalita_annualizzata_eunl = volatilita_eunl_daily * (252**0.5)
            print(volalita_annualizzata_eunl)
            
            volalita_annualizzata_nqse = volatilita_nqse_daily * (252**0.5)
            
            print(f"Vol EUNL: {volalita_annualizzata_eunl}")
            print(f"Vol NQSE: {volalita_annualizzata_nqse}")
            
        except Exception as e:
            print(f"Errore durante l'esecuzione della query: {sql} - {e}")
            logger.error(f"Errore durante l'esecuzione della query: {sql} - {e}")
            
            
