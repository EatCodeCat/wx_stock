from mongodao import mongoclient


class BasicStockModel(mongoclient.MClient):
    def __init__(self):
        super().__init__('stock', 'basic_stock')


class KStockModel(mongoclient.MClient):
    def __init__(self):
        super().__init__('stock', 'k_stock')


class K30StockModel(mongoclient.MClient):
    def __init__(self):
        super().__init__('stock', 'k30_stock')


PERDAYTYPE = 'perdaytask'


class SchedulerConfig(mongoclient.MClient):
    def __init__(self):
        super().__init__('stock', 'scheduler_config')

    def setperdaydate(self, ktype, date):
        self.update({
            'start_date_' + ktype: date
        }, {'type': PERDAYTYPE})

    def get_scheduler_config(self):
        return self.find_one(type=PERDAYTYPE)

    def insert_fail_kstock_code(self, start_date, stock_list):
        self.insert_one({'type': PERDAYTYPE,
                         'start_date': start_date,
                         'stock_list': stock_list,
                         'is_repair': 'no'})
