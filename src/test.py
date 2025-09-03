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

sql = "SELECT TICKER, DATA, CLOSE_PRICE FROM ETF_HISTORY WHERE TICKER IN (SELECT TICKER FROM CURRENT_ETF) AND DATA >= '01-JAN-2020' ORDER BY DATA"
ticker_e_prices = {}
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

                if ticker in ticker_e_prices:
                    ticker_e_prices[ticker].append((data, close_price))
                else:
                    ticker_e_prices[ticker] = [(data, close_price)]
            
            best_prices = {}
            etf_dominance = {}
            totale_per_data = {}
            try:
                for ticker, data_e_prezzo in ticker_e_prices.items():

                    max_data = None
                    max_price = 0.0

                    for data, prezzo in data_e_prezzo:
                        if prezzo > max_price:
                            max_price = prezzo
                            max_data = data
                    best_prices[ticker] = (max_data, max_price)
            
                    
                    
                for ticker, lista in ticker_e_prices.items():
                    for data, prezzo in lista:
                        if data not in totale_per_data:
                            totale_per_data[data] = 0
                        totale_per_data[data] += prezzo
                    
                    
                for ticker, (data, price) in best_prices.items():
                        d_max = data
                        p_max = price
                        denominatore = totale_per_data.get(d_max)
                        if bool(denominatore) == False:
                            logger.warning(f"Nessun totale per data {d_max}")
                            continue
                        perc = p_max / denominatore * 100
                        etf_dominance[ticker] = (d_max, perc)
      
                    
                sorted_etf_dominance = dict(sorted(etf_dominance.items(), key= lambda x:x[1][1], reverse=True))
                print(sorted_etf_dominance)
                for k, v in sorted_etf_dominance.items():
                    data = v[0].strftime("%d-%m-%Y")
                    peso = round(v[1], 2)
                    print(data, k, peso)

            except Exception as e:
                print(f"An error occurred: {e}")
                logger.error(f"An error occurred: {e}")
                
            
        except Exception as e:
            print(f"Errore durante l'esecuzione della query: {sql} - {e}")
            logger.error(f"Errore durante l'esecuzione della query: {sql} - {e}")
            
            
