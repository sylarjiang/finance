# -*- coding:utf-8 -*-
from sqlalchemy import create_engine, Column, Integer, String, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
import tushare as ts
import conf


class TuShare(object):
    def __init__(self,type="ts"):

        self.pro = ts.pro_api(conf.TOKEN)
        self.type = type
        self.task_no =conf.TASK_NO

    def Main(self):
        self.SetDB()

        if self.type =="new":
            # 获取新股信息
            self.GetNew()
        elif self.type =="index":
            #获取指数
            self.GetIndex()
        elif self.type =="coin":
            #获取数字货币
            self.GetCoin()
        else:
            #默认爬取历史记录
            self.GetHistory()


    def GetCoin(self):
        self.GetCoinList()
        pass

    def GetIndex(self):
        self.task_no =6
        self.indexs=["MSCI","CSI","SSE","SZSE","CICC","SW","OTH"]
        self.MultiTread("GetIndexDF")

    def GetHistory(self):
        self.indexs = self.GetStockList()
        self.MultiTread("GetHistoryDF")


    def GetNew(self):
        df = self.pro.new_share()
        self.AddDB(df,self.type)




    def SetDB(self):
        """
        根据数据种类创建数据库
        :return:
        """
        MYSQL_PATH = "mysql+mysqlconnector://{}:{}@{}:{}/{}?charset=utf8".format(conf.USERNAME,conf.PASSWORD,conf.DB,conf.PORT,self.type)
        self.engine = create_engine(
            MYSQL_PATH,
            max_overflow=0,  # 超过连接池大小外最多创建的连接
            pool_size=self.task_no,  # 连接池大小
            pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
            pool_recycle=-1  # 多久之后对线程池中的线程进行一次连接的回收（重置）
        )
    def GetCoinList(self):
        exchange = self.pro.coinexchanges(area_code="")
        coinlist = self.pro.coinlist(start_date='', end_date='')
        self.AddDB(exchange,"exchange")
        self.AddDB(coinlist,"coinlist")

    def GetStockList(self):
        data = self.pro.stock_basic(exchange='', list_status='L', fields='symbol,name,delist_date')
        # indexs = [  i[1]["symbol"] for i in data.iterrows() if i[1]["delist_date"] != "None"]
        return  [i for i in data["symbol"].array]

    def MultiTread(self,func):
        i = 0
        tag = True
        from threading import Thread
        while tag:
            tag = False if i > len(self.indexs) else True
            end = i + self.task_no
            for index in self.indexs[i:end]:
                if not isinstance(index,str):
                    index = str(index)
                t = Thread(target=getattr(self,func), args=[index, ])
                t.start()
                t.join()
            i += self.task_no

    def GetHistoryDF(self,index):
        df = ts.get_hist_data(index)
        self.AddDB(df, index)

    def GetIndexDF(self,index):
        df = self.pro.index_basic(market=index)
        self.AddDB(df, index)

    def AddDB(self,df,table=None):
        """
        :param df: 数据
        :param table: 表名
        :return:
        """
        if table:
            table=table.lower()
        if (df is not None):
            Base = declarative_base(self.engine)

            # 定义data对象
            class TableName(Base):
                __tablename__ = table
                id = Column(Integer, primary_key=True, nullable=False, default=1)

            # types =df.dtypes #获取数据类型
            for col in df:
                setattr(TableName, col, (Column(col, DECIMAL(10, 4))))

            df.to_sql(table, self.engine, if_exists='append', dtype={"date": String(length=20)})



if __name__ == '__main__':
    TuShare().Main()


