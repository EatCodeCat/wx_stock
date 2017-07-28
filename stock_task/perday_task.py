import json
import tushare as ts
import datetime
from mongodao import stockmodel
from apscheduler import events
from apscheduler.schedulers.blocking import BlockingScheduler

basic_model = stockmodel.BasicStockModel()
k_model = stockmodel.KStockModel()
sche_config = stockmodel.SchedulerConfig()
k30_model = stockmodel.K30StockModel()

DateFmt = '%Y-%m-%d'


# 股票列表
def basic_data():
    basic = ts.get_stock_basics()
    for i in basic.index:
        item = basic.ix[i].to_dict()
        item['code'] = i
        item['timeToMarket'] = float(item['timeToMarket'])

        item['holders'] = float(item['holders'])
        basic_model.replace_one(item, code=i)


# 日k线
def day_k_stock(cb, ktype='D', start_date='2017-01-01'):
    stock_list = basic_model.find()
    config = sche_config.get_scheduler_config()
    if config is not None and 'start_date' in config:
        start_date = config['start_date']
    fail_stock_list = []
    for stock in stock_list:
        print(stock['code'])
        try:
            k = ts.get_k_data(stock['code'], start=start_date, ktype=ktype)
            if len(k) > 0:
                data = json.loads(k.to_json(orient='records'))
                if cb is None:
                    k_model.insert(data)
                else:
                    cb(data)
        except Exception as e:
            fail_stock_list.append(stock['code'])
    start_date = datetime.datetime.strptime(start_date, DateFmt) + datetime.timedelta(days=1)
    sche_config.setperdaydate(ktype, start_date.strftime(DateFmt))
    if len(fail_stock_list) > 0:
        sche_config.insert_fail_kstock_code(start_date, fail_stock_list)


def per_day_task():
    print('start perday task')
    basic_data()
    day_k_stock()
    day_k_stock(lambda data: k30_model.insert(data), 30, '2017-07-01')


def done(e):
    print('task done', e)


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(per_day_task, 'cron', hour=19, minute=40)
    scheduler.add_listener(done, events.EVENT_JOB_EXECUTED)
    scheduler.start()
