from WindPy import *
import pandas as pd
import datetime
import strategy

w.start()

s = strategy.Strategy()
s.initialize()


for trade_day in s.trade_days:
    date = datetime.datetime.strftime(trade_day, "%Y%m%d")
    #执行前一交易日生成的买卖信号
    s.order(date)
    s.asset_evaluation(date)
    #日终生成下一日的买卖信号
    if date == s.last_signal_date:
        s.generateClearSignal(date)
    elif date == s.next_signal_date:
        s.generateSignal(date)
    print("Finished process " + date)

#关闭数据库连接
s.cur.close()
s.conn.close()

#生成净值文件
writer = pd.ExcelWriter("净值.xls")
total_asset = pd.DataFrame(s.total_asset, columns = ["日期", "单位净值", "资产规模", "现金"])
total_asset.set_index(["日期"], inplace = True)
total_asset.sort_index(axis = 0, ascending = True, inplace = True)
total_asset.to_excel(writer, "净值")
writer.save()

#生成交易文件
writer = pd.ExcelWriter("交易.xls")
transaction = pd.DataFrame(s.transaction, columns = ["日期", "成交时间", "证券代码", "交易市场代码", "交易方向", "投保", "交易数量", "交易价格"])
transaction.to_excel(writer, "交易", index = False)
writer.save()

#生成买入信号文件
writer = pd.ExcelWriter("买入信号.xls")
buy_signal_info = pd.DataFrame(s.buy_signal_info, columns = ["证券代码", "证券简称", "近10日大单净流入额平均值", "近90日大单净流入额平均值", \
                                                             "大单净流入额", "对A股流通市值的比值", "日期"])
buy_signal_info.to_excel(writer, "买入信号生成信息")
writer.save()

#生成卖出信号文件
writer = pd.ExcelWriter("卖出信号.xls")
sell_signal_info = pd.DataFrame(s.sell_signal_info, columns = ["证券代码", "证券简称", "卖出类型", "卖出信息", "日期"])
sell_signal_info.to_excel(writer, "卖出信号生成信息")
writer.save()

print("Done!")








