import logging
import logging.config
from dotenv import load_dotenv
import os
import oracledb as odb
from datetime import datetime
import typing

load_dotenv()

im_user = os.getenv("db_user")
im_pwd = os.getenv("db_pwd")
im_dsn = os.getenv("db_dsn")

logging.config.fileConfig("logging.ini")
logger = logging.getLogger(__name__)

sql = "SELECT TICKER, DATA, CLOSE_PRICE FROM ETF_HISTORY WHERE TICKER IN (SELECT TICKER FROM CURRENT_ETF) AND DATA >= '01-JAN-2021' ORDER BY TICKER, DATA"


def db_retrieve(f_sql:str = sql, f_user:str =im_user, f_password:str =im_pwd, f_dsn:str = im_dsn) -> list[tuple]:
    
    with odb.connect(user=f_user, password=f_password, dsn=f_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f_sql)
            table = cursor.fetchall()
    logger.info(f"fetched {len(table)} rows using {f_sql}")
    return table

def get_prices(i_ticker: str, table: list[tuple]) -> list[float]:
    ticker_prices: list[float] = []
    for i in range(len(table)):
        ticker:str = table[i][0]
        close_price:float = table[i][2]
        
        if ticker.lower() == i_ticker.lower():
            ticker_prices.append(close_price)
        
    return ticker_prices
        
def get_percentage_return(ticker_prices: list[float]) -> list[float]:
    if len(ticker_prices) < 2:
        logger.error(f"Serie has less than two elements, cannot compute percentage return")
        raise ValueError
    ticker_r_percs: list[float] = []
    for i in range(1, len(ticker_prices)):
        if ticker_prices[i-1] == 0:
            logger.error(f"{ticker_prices[i-1]} --> Zero value encountered in {__name__}, fix your series.")
            raise ValueError
        ticker_r_percs.append(ticker_prices[i] / ticker_prices[i-1] - 1)
    return ticker_r_percs

def check_series(a, b) -> None:
    if a is None or b is None or len(a) == 0 or len(b) == 0:
        raise ValueError("None/Empty series")
    if len(a) != len(b):
        logger.error("Series length mismatch")
        raise ValueError("Series length mismatch")
    
    
def get_ma(percentage_returns: list[float]) -> float:
    if bool(percentage_returns) == False:
        logger.error(f"The provided list is empty: {len(percentage_returns)} items.")
        raise ValueError
    return sum(percentage_returns) / len(percentage_returns)
        
    
def get_ma_deviation(series: list[float], callback: typing.Callable[[list[float]], float]) -> list[float]:
    series_ma = callback(series)
    ma_deviations = []
    for n in series:
        ma_deviations.append(n - series_ma)
        
    return ma_deviations
    
def get_covariation(series_a: list[float], series_b: list[float]) -> float:
    check_series(series_a, series_b)
    den = len(series_a) - 1
    deviations_sum = 0
    for i in range(len(series_a)):
        deviations_sum += (series_a[i] * series_b[i])
        
    
    return deviations_sum / den
    
def get_standard_deviation(series_a: list[float], series_b: list[float]) -> tuple[float, float]:
    check_series(series_a, series_b)
    a_pre_std_dev  = [v**2 for v in series_a]
    b_pre_std_dev = [v**2 for v in series_b]
    
    a_after_div = sum(a_pre_std_dev) / (len(series_a) - 1)
    b_after_div = sum(b_pre_std_dev) / (len(series_b) - 1)
    
    std_a = a_after_div ** 0.5
    std_b = b_after_div ** 0.5
    
    return std_a, std_b

def get_correlation(cov, std_a: float, std_b: float) -> float: 
    if std_a * std_b == 0:
        logger.error(f"Division by zero encountered")
        raise ValueError
    return cov/ (std_a * std_b)
    
table = db_retrieve()

ticker_a_prices = get_prices("eunl.de", table)
ticker_a_percs = get_percentage_return(ticker_a_prices)

ticker_b_prices = get_prices("nqse.de", table)
ticker_b_percs = get_percentage_return(ticker_b_prices)


ticker_b_ma_deviation = get_ma_deviation(ticker_b_percs, get_ma)
ticker_a_ma_deviation = get_ma_deviation(ticker_a_percs, get_ma)

cov = get_covariation(ticker_a_ma_deviation, ticker_b_ma_deviation)

standard_d = get_standard_deviation(ticker_a_ma_deviation, ticker_b_ma_deviation)

print(get_correlation(cov, standard_d[0], standard_d[1]))