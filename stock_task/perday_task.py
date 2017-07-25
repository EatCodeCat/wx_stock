import json
import tushare as ts

from mongodao import stockmodel

basic_model = stockmodel.BasicStockModel()

k_model = stockmodel.KStockModel()


# 股票列表
def basic_data():
    basic = ts.get_stock_basics()
    # data = json.loads(basic_data.to_json(orient='records'))
    for i in basic.index:
        item = basic.ix[i].to_dict()
        item['code'] = i
        item['timeToMarket'] = float(item['timeToMarket'])
        item['holders'] = float(item['holders'])
        basic_model.replace_one(item, code=i)





basic_data()
