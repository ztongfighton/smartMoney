from WindPy import *
import pandas as pd
import datetime
import strategy
import strategy_lib as sl

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

writer = pd.ExcelWriter("回测结果.xls")
total_asset = pd.DataFrame(list(s.total_asset.items()), columns = ["date", "value"])
total_asset.set_index(["date"], inplace = True)
total_asset.sort_index(axis = 0, ascending = True, inplace = True)
transaction = pd.DataFrame(s.transaction, columns = ["stock_code", "stock_name", "amount", "price", "direction", "trade_date"])
buy_signal_info = pd.DataFrame(s.buy_signal_info, columns = ["stock_code", "stock_name", "inflow_10", "inflow_90", "inflow_today", "date"])
total_asset.to_excel(writer, "组合资产净值")
transaction.to_excel(writer, "调仓记录")
buy_signal_info.to_excel(writer, "买入信号生成信息")
writer.save()
print("Done!")


sl.plotComparison(w, '20170101', '20170331')








