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

sql = "SELECT TICKER, DATA, CLOSE_PRICE FROM ETF_HISTORY WHERE TICKER IN (SELECT TICKER FROM CURRENT_ETF) AND DATA >= '24-JUN-2022' ORDER BY DATA"

etfs = {}
etfs_and_prices= {}
etf_and_rendimenti = {}
rendimento_perc_etf = {}
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

            for ticker, lista in etfs.items():
                for d, p in lista:
                    if ticker in etfs_and_prices:
                        etfs_and_prices[ticker].append(p)
                    else:
                        etfs_and_prices[ticker] = [p]
           
                
            for ticker, prices in etfs_and_prices.items():
                rendimento = []
                for i in range(1, len(prices)):
                    r = prices[i] / prices[i-1] -1
                    rendimento.append(r)
                etf_and_rendimenti[ticker] = rendimento
                
            for ticker, lista in etfs.items():
                
                lista.sort(key= lambda x: x[0])
                
                first_price = lista[0][1]
                last_price = lista[-1][1]
                
                rendimento_perc_etf[ticker] = (last_price / first_price - 1) * 100
                
            for k, v in rendimento_perc_etf.items():
                print(k, round(v,2))
                
            rendimenti_effettivi =sum(rendimento_perc_etf.values())
            
            den = len(rendimento_perc_etf.values())
            
            ma = rendimenti_effettivi / den
            
            print(f"Il portafogli {" - ".join(etf_and_rendimenti.keys())} ha una % di guadagno del: {round(ma,2)}% dal {list(etfs.values())[0][0][0].year} ad oggi {datetime.now().year}")
                
                   

        except Exception as e:
            print(f"Errore durante l'esecuzione della query: {sql} - {e}")
            logger.error(f"Errore durante l'esecuzione della query: {sql} - {e}")
            
            
