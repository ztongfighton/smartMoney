from WindPy import *
import pandas as pd
import datetime
import strategy
import time
import strategy_lib as sl

w.start()
s = strategy.Strategy()

trade_days = w.tdays(s.start_date, s.end_date, "").Data[0]
previous_trade_day = w.tdaysoffset(-1, trade_days[0], "").Data[0]
trade_days = previous_trade_day + trade_days
#初始化生成建仓信号的日期
s.next_signal_date = datetime.datetime.strftime(previous_trade_day[0], '%Y%m%d')
for trade_day in trade_days:
    date = datetime.datetime.strftime(trade_day, '%Y%m%d')
    s.asset_evaluation(date)
    #日终生成下一日的买卖信号
    s.generateSignal(date)
    next_trade_date = w.tdaysoffset(1, trade_day, "").Data[0][0]
    next_trade_date = datetime.datetime.strftime(next_trade_date, '%Y%m%d')
    #下一交易日执行买卖信号
    s.order(next_trade_date)
    print("Finished process " + date)

writer = pd.ExcelWriter('回测结果.xls')
total_asset = pd.DataFrame(list(s.total_asset.items()), columns = ["date", "value"])
total_asset.set_index(["date"], inplace = True)
total_asset.sort_index(axis = 0, ascending = True, inplace = True)
transaction = pd.DataFrame(s.transaction, columns = ["stock_code", "stock_name", "amount", "price", "direction", "trade_date"])
total_asset.to_excel(writer, '组合资产净值')
transaction.to_excel(writer, '调仓记录')
writer.save()
print("Done!")


sl.plotComparison(w, s)








