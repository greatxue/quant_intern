import backtrader as bt
import datetime

class PeriodicInvestmentStrategy(bt.Strategy):
    params = (
        ('investment_amount', 1000),  # 每期投資金額
        ('printlog', True),  # 是否打印交易日志
    )

    def __init__(self):
        self.order = None
        self.add_timer(
            when=bt.Timer.SESSION_START,
            monthdays=[1],  # 每月的第一天
            monthcarry=True,  # 如果第一天不是交易日，則延至下一個交易日
        )

    def notify_timer(self, timer, when, *args, **kwargs):
        self.log('進行定期投資')
        self.order = self.buy(size=self.params.investment_amount)

    def log(self, txt, dt=None):
        ''' 日誌函數 '''
        dt = dt or self.datas[0].datetime.date(0)
        if self.params.printlog:
            print('%s, %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('買入執行, 價格: %.2f, 成本: %.2f, 手續費: %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            elif order.issell():
                self.log('賣出執行, 價格: %.2f, 成本: %.2f, 手續費: %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('訂單 取消/保證金不足/拒絕')

        self.order = None

# 初始化 Cerebro 引擎
cerebro = bt.Cerebro()
cerebro.addstrategy(PeriodicInvestmentStrategy)

# 添加數據
data = bt.feeds.YahooFinanceData(dataname='AAPL',
                                 fromdate=datetime.datetime(2019, 1, 1),
                                 todate=datetime.datetime(2020, 12, 31))
cerebro.adddata(data)

# 設置初始資本
cerebro.broker.setcash(10000.0)

# 設置每筆交易的手續費
cerebro.broker.setcommission(commission=0.001)

# 執行策略
cerebro.run()

# 繪製結果
cerebro.plot()