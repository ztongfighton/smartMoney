from WindPy import *
import numpy as np
import pandas as pd
import datetime
import time
import math
import strategy_lib as sl
class Strategy:
    global w
    # 设置回测开始时间
    start_date = '20170301'
    # 设置回测结束时间
    end_date = '20170331'
    #回测期间交易日
    trade_days = []
    #生成清盘信号日
    last_signal_date = ''
    #股票每日主力净流入
    mfd_inflow_m = {}
    #每日日终生成的交易信号,包含stock_code, stock_name, amount和direction等信息
    signal = {}
    #策略持仓,包含stock_code, stock_name, amount, cost(前复权）, trade_date等信息
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

    #策略初始化，取最近89个交易日的主力净流入量
    def initialize(self):
        self.trade_days = w.tdays(self.start_date, self.end_date, "").Data[0]
        first_signal_date = w.tdaysoffset(-1, self.trade_days[0], "").Data[0]
        self.trade_days = first_signal_date + self.trade_days
        self.last_signal_date = datetime.datetime.strftime(self.trade_days[-2], '%Y%m%d')
        first_signal_date = datetime.datetime.strftime(self.trade_days[0], '%Y%m%d')


        stock_codes = w.wset("sectorconstituent","date=" + first_signal_date + ";sectorid=a001030201000000;field=wind_code,sec_name").Data[0]
        date_pre90 = w.tdaysoffset(-89, first_signal_date, "")
        date_pre90 = datetime.datetime.strftime(date_pre90.Data[0][0], '%Y%m%d')
        date_pre1 = w.tdaysoffset(-1, first_signal_date, "")
        date_pre1 = datetime.datetime.strftime(date_pre1.Data[0][0], '%Y%m%d')
        mfd_inflow_m_90 = w.wsd(stock_codes, "mfd_inflow_m", date_pre90, date_pre1, "unit=1")
        self.mfd_inflow_m = dict(zip(mfd_inflow_m_90.Codes, mfd_inflow_m_90.Data))


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

    def generateBuySignal(self, date):
        #提取当日的沪深300成分股
        sectorconstituent = w.wset("sectorconstituent","date=" + date + ";sectorid=a001030201000000;field=wind_code,sec_name")
        stock_codes = sectorconstituent.Data[0]
        stock_names = sectorconstituent.Data[1]
        stock_info = dict(zip(stock_codes, stock_names))

        #提取当日主力净流入额
        mfd_inflow_m_data = w.wss(stock_codes, "mfd_inflow_m", "unit=1;tradeDate=" + date)
        mfd_inflow_m_today = dict(zip(mfd_inflow_m_data.Codes, mfd_inflow_m_data.Data[0]))

        #检查是否有成分股调整
        sectorconstituent_old = set(self.mfd_inflow_m.keys())
        sectorconstituent_new = set(mfd_inflow_m_today.keys())
        stocks_adjust_in = list(sectorconstituent_new - sectorconstituent_old)
        stocks_adjust_out = list(sectorconstituent_old - sectorconstituent_new)
        #删除调出的股票
        if stocks_adjust_out:
            for stock_code in stocks_adjust_out:
                del self.mfd_inflow_m[stock_code]
        #补入调出的股票，补齐历史资金流向数据
        if stocks_adjust_in:
            date_pre90 = w.tdaysoffset(-89, date, "")
            date_pre90 = datetime.datetime.strftime(date_pre90.Data[0][0], '%Y%m%d')
            date_pre1 = w.tdaysoffset(-1, date, "")
            date_pre1 = datetime.datetime.strftime(date_pre1.Data[0][0], '%Y%m%d')
            mfd_inflow_m_89 = w.wsd(stock_codes, "mfd_inflow_m", date_pre90, date_pre1, "unit=1")
            mfd_inflow_m_new = dict(zip(mfd_inflow_m_89.Codes, mfd_inflow_m_89.Data))
            self.mfd_inflow_m = dict(self.mfd_inflow_m, **mfd_inflow_m_new)
        #补齐当日主力净流入额数据
        for stock_code in list(mfd_inflow_m_today.keys()):
            self.mfd_inflow_m[stock_code].append(mfd_inflow_m_today[stock_code])

        #生成买入信号
        stocks_in_position = list(self.position.keys())
        stock_asset = 1.0 * self.total_asset[date] / self.cap_num
        for stock_code in list(self.mfd_inflow_m.keys()):
            if math.isnan(mfd_inflow_m_today[stock_code]):
                continue
            elif mfd_inflow_m_today[stock_code] < 0:
                continue
            else:
                mfd_inflow_m_10_mean = np.nan_to_num(np.array(self.mfd_inflow_m[stock_code][-10:])).mean()
                mfd_inflow_m_90_mean = np.nan_to_num(np.array(self.mfd_inflow_m[stock_code][-90:])).mean()
                if mfd_inflow_m_10_mean > 0 and mfd_inflow_m_90_mean > 0 and mfd_inflow_m_10_mean / mfd_inflow_m_90_mean > 2.5 and stock_code not in stocks_in_position:
                    close_price = w.wss(stock_code, "close", "tradeDate=" + date + ";priceAdj=U;cycle=D").Data[0][0]
                    amount = math.floor(stock_asset / close_price / 100) * 100
                    self.signal[stock_code] = [stock_info[stock_code], amount, "Buy"]

    def generateSellSignal(self, date):
        #无持仓返回
        if not self.position:
            return

        stocks_in_position = list(self.position.keys())
        close_price_data = w.wss(stocks_in_position, "close", "tradeDate="+ date + ";priceAdj=F;cycle=D")
        close_prices = dict(zip(close_price_data.Codes, close_price_data.Data[0]))

        #处理当日主力净流入额为负的股票
        n = len(stocks_in_position)
        idx = [True] * n
        for i in range(n):
            stock_code = stocks_in_position[i]
            if self.mfd_inflow_m[stock_code][-1] < 0:
                p = self.position[stock_code]
                cost = p[2]
                close_price = close_prices[stock_code]
                #如果日终浮亏2个点或者连续两日主力净流入为负则卖出
                if (close_price - cost) / cost < -0.02 or self.mfd_inflow_m[stock_code][-2] < 0:
                    stock_name = p[0]
                    amount = p[1]
                    self.signal[stock_code] = [stock_name, amount, "Sell"]
                    idx[i] = False
        stocks_in_position = np.array(stocks_in_position)[idx]

        #止盈30%卖出
        n = stocks_in_position.size
        idx = [True] * n
        for i in range(n):
            stock_code = stocks_in_position[i]
            p = self.position[stock_code]
            cost = p[2]
            close_price = close_prices[stock_code]
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






















