import json
import tushare as ts
import datetime
from mongodao import stockmodel
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

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


# 日k线
def day_k_stock():
    stock_list = basic_model.find()
    now = datetime.date.today().strftime('%Y-%m-%d')
    yestoday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    for stock in stock_list:
        print(stock['code'])
        k = ts.get_k_data(stock['code'], start='2017-07-11', end=now)
        if len(k) > 0:
            data = json.loads(k.to_json(orient='records'))
            k_model.insert(data)


def per_day_task():
    print('start perday task')
    basic_data()
    day_k_stock()


def done(e):
    print('task done', e)


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(per_day_task, 'cron', hour=5, minute=50)
    scheduler.add_listener(done, events.EVENT_JOB_EXECUTED)
    scheduler.start()
