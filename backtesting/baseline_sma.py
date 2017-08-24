import datetime  # For datetime objects
import pandas as pd
import backtrader as bt
from backtrader import analyzers

from database.db_queries import get_all_resampled_data


class BaselineSMAStrategy(bt.Strategy):
    params = (('sma_short_period', 5),
              ('sma_long_period', 14),
              )

    def __init__(self):
        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None
        # Add a MovingAverageSimple indicator
        self.short_sma = bt.indicators.SimpleMovingAverage(self.datas[0], period=self.params.sma_short_period)
        self.long_sma = bt.indicators.SimpleMovingAverage(self.datas[0], period=self.params.sma_long_period)

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def next(self):
        # Simply log the closing price of the series from the reference
        # self.log('Close, %.4f Position: %.4f' % (self.datas[0].close[0], self.position.size))
        if self.order:         # Check if an order is pending ... if yes, we cannot send a 2nd one
            return

        if not self.position:   # Check if we are in the market
            # Not yet ... we MIGHT BUY if ...
            if self.short_sma[0] > self.long_sma[0]:
            # if self.datas[0].close[0]<self.datas[0].close[1]: # Cheating !!!
                if self.datas[0].close >= self.datas[0].open[1]: comp = '>='
                else: comp = '<'
                self.log('BUY CREATE, Close:%.4f %s Next Open:%.4f' % (self.datas[0].close[0], comp, self.datas[0].open[1]))
                # BUY, BUY, BUY!!! (with all possible default parameters)
                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()
        else:
            if self.short_sma[0] <= self.long_sma[0]:
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log('SELL CREATE, %.4f' % self.datas[0].close[0])
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
    data = bt.feeds.PandasData(dataname=usd_data, fromdate=datetime.datetime(2014, 1, 1), todate=datetime.datetime(2020, 12, 7))

    cash = 1000.0
    size = 90  # %
    commission = 0.  # %
    cerebro = bt.Cerebro()
    cerebro.addstrategy(BaselineSMAStrategy, sma_short_period=7, sma_long_period=28)
    cerebro.adddata(data)
    cerebro.broker.setcash(cash)
    cerebro.addsizer(bt.sizers.AllInSizer)  # Can't be used with commission, because 100% cash is used
    # Todo: Buy size is cash/losing price but buy executed at opening next bar. If percents=100 => Order rejected if close<open
    cerebro.addsizer(bt.sizers.PercentSizer, percents=size)
    cerebro.broker.setcommission(commission=commission/100)

    cerebro.addanalyzer(analyzers.Returns, _name='myreturns')
    cerebro.addanalyzer(analyzers.SQN, _name='mysqn')
    cerebro.addanalyzer(analyzers.DrawDown, _name='mydrawdown')

    print('Starting Portfolio Value: %.4f' % cerebro.broker.getvalue())
    strat = cerebro.run()

    print '-' * 200
    print 'Final Portfolio Value: %.2f' % cerebro.broker.getvalue()
    print 'Final Portfolio P/L: %.2f' % (cerebro.broker.getvalue()-cash)
    print 'Total return: {:.2f}% '.format(100 * (cerebro.broker.getvalue() - cash) / cash)
    print 'Anualized return: {:.2f}% '.format(strat[0].analyzers.myreturns.get_analysis()['rnorm100'])
    sqn = strat[0].analyzers.mysqn.get_analysis()['sqn']
    if sqn < 1.6: sqn_txt = 'Very bad'
    elif sqn < 1.9: sqn_txt = 'Below average'
    elif sqn < 2.4: sqn_txt = 'Average'
    elif sqn < 2.9: sqn_txt = 'Good'
    elif sqn < 5.0: sqn_txt = 'Excellent'
    elif sqn < 6.9: sqn_txt = 'Superb'
    else: sqn_txt = 'To good to be true !!!?'
    print 'SQN: {:.2f} => {}'.format(sqn, sqn_txt)
    drawdown = strat[0].analyzers.mydrawdown.get_analysis()
    print 'Draw Down Lenght: {} \nMax Draw Down: {:.2f}% \nMax Money Down: {:.2f}'.format(drawdown['max']['len'], drawdown['max']['drawdown'], drawdown['max']['moneydown'])

    cerebro.plot()
