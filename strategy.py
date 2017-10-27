from WindPy import *
import numpy as np
import pandas as pd
import datetime
import math
import cx_Oracle


class Strategy:
    global w
    # 设置回测开始时间
    start_date = '20170101'
    # 设置回测结束时间
    end_date = '20170331'
    # 回测期间交易日
    trade_days = []
    # 生成清盘信号日
    last_signal_date = ''
    # 下一个调仓日
    next_signal_date = ''
    #当日大单净流入数据
    mfd_inflow_today = dict()
    #昨日大单净流入数据
    mfd_inflow_yesterday = dict()
    #每日日终生成的交易信号,包含stock_code, stock_name, amount和direction等信息
    signal = {}
    # 记录生成买入信号的相关信息
    buy_signal_info = []
    #策略持仓,包含stock_code, stock_name, amount, cost, trade_date, days_in_position等信息
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




    def initialize(self):
        self.trade_days = w.tdays(self.start_date, self.end_date, "").Data[0]
        first_signal_date = w.tdaysoffset(-1, self.trade_days[0], "").Data[0]
        self.trade_days = first_signal_date + self.trade_days
        self.last_signal_date = datetime.datetime.strftime(self.trade_days[-2], '%Y%m%d')
        self.next_signal_date = datetime.datetime.strftime(self.trade_days[0], '%Y%m%d')
        # 连接wind数据库
        self.conn = cx_Oracle.connect("wind/wind@WIND_ZJLX")
        self.cur = self.conn.cursor()



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
        other_prices = w.wss(stock_codes, "close, high, low", "tradeDate=" + date + ";priceAdj=U;cycle=D").Data
        other_prices = pd.DataFrame(data = np.matrix(other_prices).T, index = stock_codes, columns = ["close", "high", "low"])

        #处理卖信号
        for stock_code in list(self.signal.keys()):
            if self.signal[stock_code][-1] == "Buy":
                continue
            else:
                open_price = open_prices[stock_code]
                high_price = other_prices.at[stock_code, "high"]
                low_price = other_prices.at[stock_code, "low"]
                if trade_status[stock_code] == '交易' and (maxupordown[stock_code] == 0 or (maxupordown[stock_code] != 0 \
                    and (open_price != low_price or open_price != high_price))):
                    s = self.signal[stock_code]
                    stock_name = s[0]
                    amount = s[1]
                    #open_price = open_prices[stock_code]
                    self.cash = self.cash + open_price * amount * (1 - self.commission)
                    del self.signal[stock_code]
                    del self.position[stock_code]
                    self.transaction.append(
                        [stock_code, stock_name, amount, open_price, "Sell", date])

        #处理买信号
        for stock_code in list(self.signal.keys()):
            if self.signal[stock_code][-1] == "Buy":
                open_price = open_prices[stock_code]
                high_price = other_prices.at[stock_code, "high"]
                low_price = other_prices.at[stock_code, "low"]
                if trade_status[stock_code] == '交易' and (maxupordown[stock_code] == 0 or (maxupordown[stock_code] != 0 \
                    and (open_price != high_price or open_price != low_price))):
                    s = self.signal[stock_code]
                    stock_name = s[0]
                    amount = s[1]
                    #open_price = open_prices[stock_code]
                    if amount * open_price * (1 + self.commission) > self.cash:
                        amount = math.floor(self.cash / (1 + self.commission) / open_price / 100) * 100
                    if amount > 0:
                        self.cash = self.cash - open_price * amount * (1 + self.commission)
                        self.position[stock_code] = [stock_name, amount, open_prices_f[stock_code], date, 0]
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
        self.next_signal_date = datetime.datetime.strftime(w.tdaysoffset(1, self.next_signal_date, "").Data[0][0], '%Y%m%d')
        self.mfd_inflow_yesterday = self.mfd_inflow_today


    def generateBuySignal(self, date):
        #提取当日的沪深300成分股
        sectorconstituent = w.wset("sectorconstituent","date=" + date + ";sectorid=a001030201000000;field=wind_code,sec_name")
        stock_codes = sectorconstituent.Data[0]
        stock_names = dict(zip(stock_codes, sectorconstituent.Data[1]))



        #提取当日主力净流入额，计算近10日主力净流入额和近90日主力净流入额
        date_pre10 = w.tdaysoffset(-9, date, "")
        date_pre10 = datetime.datetime.strftime(date_pre10.Data[0][0], '%Y%m%d')
        date_pre90 = w.tdaysoffset(-89, date, "")
        date_pre90 = datetime.datetime.strftime(date_pre90.Data[0][0], '%Y%m%d')


        #查询个股近10日大单（>100万）净流入量
        sql = "select S_INFO_WINDCODE as stock_code, AVG(VALUE_DIFF_INSTITUTE) as inflow_10_mean  from asharemoneyflow \
               where TRADE_DT <= " + date + " and TRADE_DT >= " + date_pre10 + "group by S_INFO_WINDCODE"
        self.cur.execute(sql)
        mfd_inflow_m_10_mean = self.cur.fetchall()
        mfd_inflow_m_10_mean = dict(mfd_inflow_m_10_mean)
        # 查询个股近90日大单（>100万）净流入量
        sql = "select S_INFO_WINDCODE as stock_code, AVG(VALUE_DIFF_INSTITUTE) as inflow_90_mean  from asharemoneyflow \
               where TRADE_DT <= " + date + " and TRADE_DT >= " + date_pre90 + "group by S_INFO_WINDCODE"
        self.cur.execute(sql)
        mfd_inflow_m_90_mean = self.cur.fetchall()
        mfd_inflow_m_90_mean = dict(mfd_inflow_m_90_mean)

        sql = "select S_INFO_WINDCODE as stock_code, VALUE_DIFF_INSTITUTE as inflow_today from asharemoneyflow where TRADE_DT = " + date
        self.cur.execute(sql)
        mfd_inflow_today = self.cur.fetchall()
        self.mfd_inflow_today = dict(mfd_inflow_today)

        #生成买入信号
        close_prices = w.wss(list(stock_codes), "close", "tradeDate=" + date + ";priceAdj=U;cycle=D").Data[0]
        close_prices = dict(zip(stock_codes, close_prices))
        stock_asset = 1.0 * self.total_asset[date] / self.cap_num

        for stock_code in stock_codes:
            if stock_code in self.position.keys():
                continue
            if stock_code not in mfd_inflow_m_10_mean.keys():
                continue
            if stock_code not in mfd_inflow_m_90_mean.keys():
                continue
            if stock_code not in self.mfd_inflow_today.keys():
                continue

            #近10日大单净流入量比近90日大单净流入量>2.5, 当日大单净流入量大于近10日大单净流入量均值
            if self.mfd_inflow_today[stock_code] > 0 and mfd_inflow_m_10_mean[stock_code] > 0 and mfd_inflow_m_90_mean[stock_code] > 0 and  \
               mfd_inflow_m_10_mean[stock_code] / mfd_inflow_m_90_mean[stock_code] > 2.5 and \
                            self.mfd_inflow_today[stock_code] > mfd_inflow_m_10_mean[stock_code]:
                amount = math.floor(stock_asset / close_prices[stock_code] / 100) * 100
                self.signal[stock_code] = [stock_names[stock_code], amount, "Buy"]
                self.buy_signal_info.append([stock_code, stock_names[stock_code], mfd_inflow_m_10_mean[stock_code], \
                                             mfd_inflow_m_90_mean[stock_code], self.mfd_inflow_today[stock_code], date])


    def generateSellSignal(self, date):
        #无持仓返回
        if not self.position:
            return

        #对于所有持仓股，持仓天数加1
        for stock_code in self.position.keys():
            self.position[stock_code][-1] += 1

        # 卖出当日大单（>100万）净流入额为负的股票
        stocks_in_position = list(self.position.keys())
        close_prices = w.wss(stocks_in_position, "close", "tradeDate=" + date + ";priceAdj=F;cycle=D").Data[0]
        close_prices = dict(zip(stocks_in_position, close_prices))
        yesterday = w.tdaysoffset(-1, date, "").Data[0][0]
        yesterday = datetime.datetime.strftime(yesterday, "%Y%m%d")
        open_prices_yesterday = w.wss(stocks_in_position, "open", "tradeDate=" + yesterday + ";priceAdj=F;cycle=D").Data[0]
        open_prices_yesterday = dict(zip(stocks_in_position, open_prices_yesterday))
        n = len(stocks_in_position)

        idx = [True] * n
        for i in range(n):
            stock_code = stocks_in_position[i]
            if stock_code in self.mfd_inflow_today.keys():
                inflow_today = self.mfd_inflow_today[stock_code]
            else:
                continue

            if stock_code in self.mfd_inflow_yesterday.keys():
                inflow_yesterday = self.mfd_inflow_yesterday[stock_code]
            else:
                continue

            close_price = close_prices[stock_code]
            open_price_yesterday = open_prices_yesterday[stock_code]

            # 如果连续两天大单净流出,当日大单净流出额大于昨日大单净流出额，且净流出的两天跌幅达2%，则卖出
            if inflow_today < 0 and inflow_yesterday < 0 and inflow_today < inflow_yesterday and \
            (close_price - open_price_yesterday) / open_price_yesterday < -0.02:
                p = self.position[stock_code]
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
            tradedays_in_position = p[-1]
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






















