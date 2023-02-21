import yfinance as yf
import pandas as pd
from tinydb import TinyDB, Query

from datetime import datetime, timedelta
import logging
import pandas_market_calendars as mcal

YahooFinanceLogger = logging.getLogger("YahooFinance")
YahooFinanceLogger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
YahooFinanceLogger.addHandler(ch)


class YahooFinance:
    def getPrice(self, symbol: str, date: datetime):
        nyse = mcal.get_calendar("NYSE")
        cal = nyse.schedule(
            start_date=pd.Timestamp(date, tz="America/New_York"),
            end_date=pd.Timestamp(date, tz="America/New_York"),
        )
        try:
            if not nyse.open_at_time(
                cal,
                pd.Timestamp(
                    f"{date.strftime('%Y-%m-%d')} 09:30", tz="America/New_York"
                ),
            ):
                self.logger.debug("Markget close, return None")
                return None
        except IndexError:
            self.logger.debug("Cannot get open markget, return None")
            return None
        q = Query()
        result = self.db.search(
            (q.Symbol == symbol) & (q.Date == date.strftime("%Y-%m-%d"))
        )
        if len(result):
            self.logger.debug("Find data in db")
            return result
        data = yf.download(
            tickers=symbol,
            start=date,
            group_by="ticker",
            progress=False,
        )
        data.insert(0, "Symbol", symbol)
        data.insert(0, "Date", data.index.strftime("%Y-%m-%d"))
        self.db.insert_multiple(data.to_dict("records"))

    def getPriceRange(self, symbol: str, start: datetime, end: datetime):
        ret = []
        cur = start
        while cur <= end:
            if d := self.getPrice(symbol, cur):
                ret.append(d)
            cur += timedelta(days=1)
        return ret

    def __init__(self, db: TinyDB, logger=YahooFinanceLogger):
        self.db = db
        self.logger = logger


if __name__ == "__main__":

    logger = logging.getLogger("main")
    logger.setLevel(logging.DEBUG)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    db = TinyDB("data.json")
    ydb = YahooFinance(db, logger=logger)
    logger.info(ydb.getPrice("MSFT", datetime.strptime("2022-1-20", "%Y-%m-%d")))
    logger.info(
        ydb.getPriceRange(
            "MSFT",
            datetime.strptime("2021-11-20", "%Y-%m-%d"),
            datetime.strptime("2022-1-30", "%Y-%m-%d"),
        )
    )
