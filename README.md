# smartMoney
本分支下的版本与master分支的版本不同之处在于：
1. 优化了从万德终端取数的代码，缓存了历史主力净流入额数据，节约万德数据流量
2. 调仓周期由10天调整为1天，每日生成买卖信号
3. 调整卖出信号生成规则，当主力净流入额为负时，如果浮亏2%或者连续2日主力净流入额为负，才卖出
4. 调整了策略每日工作流程，先以开盘价执行前一日的买卖信号，再于日终计算组合资产净值，最后生成下一日的买卖信号
5. 回测区间最后一个交易日以开盘价卖出所有持仓