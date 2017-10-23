from WindPy import *
import numpy as np
import pandas as pd
import datetime
import math

class Strategy:
    global w
    # 设置回测开始时间
    start_date = '20170101'
    # 设置回测结束时间
    end_date = '20170130'
    #每日日终生成的交易信号,包含stock_code, stock_name, amount和direction等信息
    signal = {}
    #策略持仓,包含stock_code, stock_name, amount, cost, trade_date等信息
    position = {}
    #现金(默认初始现金1000万）
    cash = 10000000
    #资产总值
    total_asset = {}
    #组合持仓数限制
    cap_num = 50
    #手续费
    commission = 0.002
    #策略交易记录,包含stock_code, stock_name, amount, price, direction, trade_date等信息
    transaction = []
    #下一个调仓日
    next_signal_date = ''

    def initialize(self):
        self.trade_days = w.tdays(self.start_date, self.end_date, "").Data[0]
        first_signal_date = w.tdaysoffset(-1, self.trade_days[0], "").Data[0]
        self.trade_days = first_signal_date + self.trade_days
        self.last_signal_date = datetime.datetime.strftime(self.trade_days[-2], '%Y%m%d')
        self.next_signal_date = datetime.datetime.strftime(self.trade_days[0], '%Y%m%d')

    def order(self, date):
        if not self.signal:
            return

        stock_codes = list(self.signal.keys())
        trade_status = w.wss(stock_codes, "trade_status", "tradeDate=" + date).Data[0]
        maxupordown = w.wss(stock_codes, "maxupordown", "tradeDate=" + date).Data[0]
        trade_status = pd.Series(trade_status, index = stock_codes)
        maxupordown = pd.Series(maxupordown, index = stock_codes)
        open_prices = w.wss(stock_codes, "open", "tradeDate=" + date + ";priceAdj=U;cycle=D").Data[0]
        open_prices = pd.Series(open_prices, index = stock_codes)
        open_prices_f = w.wss(stock_codes, "open", "tradeDate=" + date + ";priceAdj=F;cycle=D").Data[0]
        open_prices_f = pd.Series(open_prices_f, index = stock_codes)

        #处理卖信号
        for stock_code in list(self.signal.keys()):
            if self.signal[stock_code][-1] == "Buy":
                continue
            else:
                if trade_status[stock_code] == '交易' and maxupordown[stock_code] == 0:
                    s = self.signal[stock_code]
                    stock_name = s[0]
                    amount = s[1]
                    open_price = open_prices[stock_code]
                    self.cash = self.cash + open_price * amount * (1 - self.commission)
                    del self.signal[stock_code]
                    del self.position[stock_code]
                    self.transaction.append(
                        [stock_code, stock_name, amount, open_price, "Sell", date])

        #处理买信号
        for stock_code in list(self.signal.keys()):
            if self.signal[stock_code][-1] == "Buy":
                if trade_status[stock_code] == '交易' and maxupordown[stock_code] == 0:
                    s = self.signal[stock_code]
                    stock_name = s[0]
                    amount = s[1]
                    open_price = open_prices[stock_code]
                    if amount * open_price * (1 + self.commission) > self.cash:
                        amount = math.floor(self.cash / (1 + self.commission) / open_price / 100) * 100
                    if amount > 0:
                        self.cash = self.cash - open_price * amount * (1 + self.commission)
                        self.position[stock_code] = [stock_name, amount, open_prices_f[stock_code], date]
                        self.transaction.append([stock_code, stock_name, amount, open_price, "Buy", date])
                #无论买信号执行与否，删除买信号
                del self.signal[stock_code]

    def generateSignal(self, date):
        #time1 = time.time()
        self.generateBuySignal(date)
        #time2 = time.time()
        self.generateSellSignal(date)
        # time3 = time.time()
        # print("生成买信号耗时：%f" % (time2 - time1))
        # print("生成卖信号耗时：%f" % (time3 - time2))
        self.next_signal_date = datetime.datetime.strftime(w.tdaysoffset(10, self.next_signal_date, "").Data[0][0], '%Y%m%d')

    def generateBuySignal(self, date):
        #提取当日的沪深300成分股
        stock_codes_data = w.wset("sectorconstituent","date=" + date + ";sectorid=a001030201000000;field=wind_code,sec_name")
        stock_codes = stock_codes_data.Data[0]
        stock_names = pd.Series(stock_codes_data.Data[1])

        #提取当日主力净流入额，计算近10日主力净流入额和近90日主力净流入额
        date_pre10 = w.tdaysoffset(-9, date, "")
        date_pre10 = datetime.datetime.strftime(date_pre10.Data[0][0], '%Y%m%d')
        date_pre90 = w.tdaysoffset(-89, date, "")
        date_pre90 = datetime.datetime.strftime(date_pre90.Data[0][0], '%Y%m%d')
        mfd_inflow_m_10 = w.wsd(stock_codes, "mfd_inflow_m", date_pre10, date, "unit=1")
        mfd_inflow_m_90 = w.wsd(stock_codes, "mfd_inflow_m", date_pre90, date, "unit=1")
        mfd_inflow_m_10_mean = pd.Series(np.nan_to_num(np.array(mfd_inflow_m_10.Data)).mean(axis = 1))
        mfd_inflow_m_90_mean = pd.Series(np.nan_to_num(np.array(mfd_inflow_m_90.Data)).mean(axis = 1))
        mfd_inflow_m_today = pd.Series(np.array(mfd_inflow_m_10.Data).T[-1])

        #剔除缺数据，当日主力净流入额为负，近10日主力净流入额均值为负，近90日主力净流入额均值为负的个股
        #选出近10日主力净流入额均值/近90日主力净流入额均值 > 2.5的个股
        stock_codes = pd.Series(stock_codes)
        data = pd.DataFrame({"stock_codes" : stock_codes, "stock_name":stock_names, "mfd_inflow_m_10_mean":mfd_inflow_m_10_mean, "mfd_inflow_m_90_mean":mfd_inflow_m_90_mean, "mfd_inflow_m_today":mfd_inflow_m_today})
        data.set_index("stock_codes", inplace=True)
        data.dropna(axis=0, how='any', inplace = True)
        data = data[(data.mfd_inflow_m_today > 0) & (data.mfd_inflow_m_10_mean > 0) & (data.mfd_inflow_m_90_mean > 0) & (data.mfd_inflow_m_10_mean.values / data.mfd_inflow_m_90_mean.values > 2.5)]
        stock_codes = list(data.index)

        #剔除已在持仓中的股票
        stocks_in_position = set(self.position.keys())
        stock_codes = list(set(stock_codes).difference(stocks_in_position))
        stock_names = data.loc[stock_codes, 'stock_name'].values

        #生成买入信号
        n = len(stock_codes)
        close_prices = w.wsd(list(stock_codes), "close", date, date, "Fill=Previous").Data[0]
        stock_asset = 1.0 * self.total_asset[date] / self.cap_num
        for i in range(n):
            amount = math.floor(stock_asset / close_prices[i] / 100) * 100
            self.signal[stock_codes[i]] = [stock_names[i], amount, "Buy"]


    def generateSellSignal(self, date):
        #无持仓返回
        if not self.position:
            return

        # 卖出当日主力净流入额为负的股票
        stocks_in_position = list(self.position.keys())
        n = len(stocks_in_position)
        mfd_inflow_m = w.wss(stocks_in_position, "mfd_inflow_m", "unit=1;tradeDate=" + date).Data[0]
        idx = [True] * n
        for i in range(n):
            stock_code = stocks_in_position[i]
            if mfd_inflow_m[i] < 0:
                p = self.position[stock_code]
                stock_name = p[0]
                amount = p[1]
                self.signal[stock_code] = [stock_name, amount, "Sell"]
                idx[i] = False
        stocks_in_position = np.array(stocks_in_position)[idx]

        #止盈30%卖出
        n = stocks_in_position.size
        close_prices = w.wsd(list(stocks_in_position), 'close', date, date, "Fill=Previous;PriceAdj=F").Data[0]
        idx = [True] * n
        for i in range(n):
            stock_code = stocks_in_position[i]
            p = self.position[stock_code]
            cost = p[2]
            close_price = close_prices[i]
            if (close_price - cost) / cost >= 0.3:
                stock_name = p[0]
                amount = p[1]
                self.signal[stock_code] = [stock_name, amount, 'Sell']
                idx[i] = False
        stocks_in_position = stocks_in_position[idx]

        #卖出持仓满30个交易日的股票
        n = stocks_in_position.size
        for i in range(n):
            stock_code = stocks_in_position[i]
            p = self.position[stock_code]
            tradedays_in_position = w.tdayscount(p[-1], date, "").Data[0][0]
            if tradedays_in_position >= 30:
                stock_name = p[0]
                amount = p[1]
                self.signal[stock_code] = [stock_name, amount, "Sell"]

    def generateClearSignal(self, date):
        for stock_code, p in self.position.items():
            stock_name = p[0]
            amount = p[1]
            self.signal[stock_code] = [stock_name, amount, "Sell"]


    #按收盘价计算组合资产净值
    def asset_evaluation(self, date):
        stock_value = 0
        stocks_in_position = list(self.position.keys())
        n = len(stocks_in_position)
        if n > 0:
            close_prices = w.wsd(stocks_in_position, 'close', date, date, "Fill=Previous").Data[0]
            for i in range(n):
                stock_code = stocks_in_position[i]
                amount = self.position[stock_code][1]
                close_price = close_prices[i]
                stock_value += close_price * amount

        self.total_asset[date] = stock_value + self.cash






















