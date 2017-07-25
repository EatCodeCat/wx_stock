
from mongodao import mongoclient


class BasicStockModel(mongoclient.MClient):
    def __init__(self):
        super().__init__('stock', 'basic_stock')


class KStockModel(mongoclient.MClient):
    def __init__(self):
        super().__init__('stock', 'k_stock')





