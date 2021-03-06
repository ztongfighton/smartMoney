import numpy as np
import matplotlib as mpl
import matplotlib.dates as dt
import matplotlib.pyplot as plt
import xlrd



#判断股票是否停牌
def isTrading(w, stock_code, date):
    trade_status = w.wss(stock_code, "trade_status", "tradeDate=" + date).Data[0][0]
    if trade_status == "交易":
        return True
    else:
        return False

#判断股票是否开盘涨停或跌停
def isMaxUpOrDown(w, stock_code, date):
    maxupordown = w.wss(stock_code, "maxupordown", "tradeDate=" + date).Data[0][0]
    open_price = w.wsd(stock_code, 'open', date, date, "Fill=Previous").Data[0][0]
    close_price = w.wsd(stock_code, 'close', date, date, "Fill=Previous").Data[0][0]
    low_price = w.wsd(stock_code, 'low', date, date, "Fill=Previous").Data[0][0]
    high_price = w.wsd(stock_code, 'high', date, date, "Fill=Previous").Data[0][0]

    if maxupordown == 0 or (maxupordown != 0 and (open_price != close_price or open_price != high_price or open_price != low_price)):
        return False
    else:
        return True


#组合净值与沪深300的对比图
def plotComparison(w, s):
    #计算沪深300指数收益率
    hs300 = np.array(w.edb("M0020209", s.trade_days[0], s.trade_days[-1]).Data[0])
    hs300 = hs300 / hs300[0]

    #计算组合净值
    total_asset = xlrd.open_workbook(r'回测结果.xls').sheet_by_index(0).col_values(1)
    total_asset = np.array(total_asset[1:])
    total_asset = total_asset / total_asset[0]

    x = dt.date2num(s.trade_days)

    mpl.rcParams["font.sans-serif"] = ["Microsoft YaHei"]  # 用来正常显示中文标签
    fig = plt.figure()
    plt.xlabel('日期')
    plt.ylabel('净值')
    plt.plot_date(x, hs300, fmt='g--', xdate=True, ydate=False, label="沪深300")
    plt.plot_date(x, total_asset, fmt='r-', xdate=True, ydate=False, label="策略")
    plt.legend(loc='upper left')
    fig.autofmt_xdate()
    plt.show()
    plt.hold()









