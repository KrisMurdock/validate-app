from src.utils.data_provider import DataProvider
import pandas as pd

class DataFactory:
    """
    统一数据源工厂类 (DataFactory)
    
    使用说明:
    1. 实例化 DataFactory，指定数据源类型 ('tushare', 'akshare', 'mysql', 'default')。
    2. 如果使用 tushare，需要提供 token。
    3. 调用 get_provider() 获取具体的 Provider 实例。
    4. 使用 fetch_minute_data() 或 get_latest_bar() 获取数据。
    
    示例:
    factory = DataFactory(source='tushare', tushare_token='YOUR_TOKEN')
    provider = factory.get_provider()
    df = provider.fetch_minute_data('000001.SZ', start_date, end_date)
    """
    
    def __init__(self, source='akshare', tushare_token=None):
        self.source = source
        self.tushare_token = tushare_token
        self.provider = self._create_provider()
        
    def _create_provider(self):
        if self.source == 'tushare':
            from src.utils.tushare_provider import TushareProvider
            if not self.tushare_token:
                print("Warning: Tushare source selected but no token provided.")
            return TushareProvider(token=self.tushare_token)

        elif self.source == 'akshare':
            from src.utils.akshare_provider import AkshareProvider
            return AkshareProvider()
        elif self.source == 'mysql':
            from src.utils.mysql_provider import MysqlProvider
            return MysqlProvider()
        elif self.source == 'postgresql':
            from src.utils.postgres_provider import PostgresProvider
            return PostgresProvider()
        elif self.source == 'duckdb':
            from src.utils.duckdb_provider import DuckDbProvider
            return DuckDbProvider()
        elif self.source == 'tdx':
            from src.utils.tdx_provider import TdxProvider
            return TdxProvider()
        elif self.source == 'eastmoney':
            from src.utils.eastmoney_provider import EastmoneyProvider
            return EastmoneyProvider()

        else:
            return DataProvider()
            
    def get_provider(self):
        return self.provider

# 导出工具类，方便外部直接 import 使用
__all__ = ['DataFactory', 'DataProvider']
