#!/usr/bin/env python3

import logging

from tinydb import TinyDB

SimulateLogger = logging.getLogger("simulator")
SimulateLogger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
SimulateLogger.addHandler(ch)


class Asset:
    def __init__(self):
        self._Cash = 0
        self._Stock = 0

    def setCash(self, amount):
        self._Cash = amount

    def setStock(self, amount):
        self._Stock = amount

    def getCash(self):
        return self._Cash

    def getStock(self):
        return self._Stock


class Simulate:
    def __init__(self, data, logger=SimulateLogger):
        self.data = data
        self.logger = logger
        self.asset = Asset()
        return

    def setCash(self, amount):
        self.asset.setCash(amount)

    def setStock(self, amount):
        self.asset.setStock(amount)

    def _buy(self, volume: int, price: int):
        bvol = min(volume, self.asset.getCash() // price)
        self.asset.setCash(self.asset.getCash() - bvol * price)
        self.asset.setStock(self.getStock() + bvol)

    def _sell(self, volume: int, price: int):
        svol = min(volume, self.asset.getStock())
        self.asset.setCash(self.asset.getCash() + svol * price)
        self.asset.setStock(self.getStock() - svol)

    def run(self, buy, sell, private=None):
        ret = []
        for d in self.data:
            bvol = buy(d["Date"], d["Open"], self.asset.getCash(), self.getStock())
            if bvol > 0:
                self._buy(bvol, d["Open"])
            svol = sell(d["Date"], d["Open"], self.asset.getCash(), self.getStock())
            if svol > 0:
                self._sell(svol, d["Open"])

            totalAsset = self.asset.getCash() + self.asset.getStock() * d["Close"]
            ret.append(
                {
                    "Cash": self.asset.getCash(),
                    "Stock": self.asset.getStock(),
                    "TotalAsset": totalAsset,
                }
            )
        return


def test():
    from YahooFinance import YahooFinance
    from datetime import datetime

    db = TinyDB("data.json")
    ydb = YahooFinance(db)
    data = ydb.getPriceRange(
        "MSFT",
        datetime.strptime("2021-11-20", "%Y-%m-%d"),
        datetime.strptime("2022-1-30", "%Y-%m-%d"),
    )
    sim = Simulate(data)

    def buy(date: str, price):
        return 5


if __name__ == "__main__":
    test()
