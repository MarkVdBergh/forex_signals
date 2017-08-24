# from __future__ import (absolute_import, division, print_function,
#                         unicode_literals)

import datetime  # For datetime objects
import pandas as pd
import backtrader as bt
from database.db_queries import get_all_resampled_data


class SimpleSMAStrategy(bt.Strategy):
    params = (('smaperiod', 5),)

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None
        # Add a MovingAverageSimple indicator
        self.sma = bt.indicators.SimpleMovingAverage(self.datas[0], period=self.params.smaperiod)

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def next(self):
        # Simply log the closing price of the series from the reference
        # self.log('Close, %.4f Position: %.4f' % (self.dataclose[0], self.position.size))

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return
        # Check if we are in the market
        if not self.position:
            # Not yet ... we MIGHT BUY if ...
            if self.dataclose[0] > self.sma[0] * 1.01:
                # BUY, BUY, BUY!!! (with all possible default parameters)
                if self.dataclose >= self.datas[0].open[1]: comp = '>='
                else: comp = '<'
                self.log('BUY CREATE, Close:%.4f %s Next Open:%.4f' % (self.dataclose[0], comp, self.datas[0].open[1]))
                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()
        else:
            if self.dataclose[0] < self.sma[0]:
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log('SELL CREATE, %.4f' % self.dataclose[0])
                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell()

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return  # Buy/Sell order submitted/accepted to/by broker - Nothing to do
        # Check if an order has been completed  Attention: broker could reject order if not enougth cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED, Size: %.4f Price: %.4f, Cost: %.4f, Comm %.4f' % (self.position.size, order.executed.price, order.executed.value, order.executed.comm))
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Size: %.4f Price: %.4f, Cost: %.4f, Comm %.4f' % (self.position.size, order.executed.price, order.executed.value, order.executed.comm))

            self.bar_executed = len(self)  # Todo: can be removed?

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed: return
        self.log('OPERATION PROFIT, GROSS %.4f, NET %.4f' % (trade.pnl, trade.pnlcomm))


if __name__ == '__main__':
    # Create a Data Feed
    usd_open = get_all_resampled_data(currency='EURUSD', frequency='D', begin='1970', end='2020', pricetype='open')
    usd_high = get_all_resampled_data(currency='EURUSD', frequency='D', begin='1970', end='2020', pricetype='high')
    usd_low = get_all_resampled_data(currency='EURUSD', frequency='D', begin='1970', end='2020', pricetype='low')
    usd_close = get_all_resampled_data(currency='EURUSD', frequency='D', begin='1970', end='2020', pricetype='close')
    usd_data = pd.concat([usd_open, usd_high, usd_low, usd_close], axis=1)
    data = bt.feeds.PandasData(dataname=usd_data, fromdate=datetime.datetime(2004, 1, 1), todate=datetime.datetime(2004, 12, 7))

    cerebro = bt.Cerebro()
    cerebro.addstrategy(SimpleSMAStrategy)
    cerebro.adddata(data)
    cerebro.broker.setcash(100.0)
    cerebro.addsizer(bt.sizers.AllInSizer)  # Can't be used with commission, because 100% cash is used
    # Todo: Buy size is cash/losing price but buy executed at opening next bar. If percents=100 => Order rejected if close<open
    cerebro.addsizer(bt.sizers.PercentSizer, percents=100)
    cerebro.broker.setcommission(commission=0.00)
    print('Starting Portfolio Value: %.4f' % cerebro.broker.getvalue())
    cerebro.run()
    print('Final Portfolio Value: %.4f' % cerebro.broker.getvalue())
    cerebro.plot()
