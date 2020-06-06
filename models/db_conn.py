# -*- coding:utf-8 -*-
import os
import conf
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import Column, Integer, String, or_, create_engine
from sqlalchemy import func
from copy import deepcopy
from sqlalchemy_pagination import paginate

Base = declarative_base()


class CreateDBSess:
    def __init__(self, sqlite_file):
        """
        https://docs.sqlalchemy.org/en/13/orm/tutorial.html#querying
        :param sqlite_file: sqlite文件路径
        """
        self.dbfile = sqlite_file
        self.engine = self.CreateEngine()
        self.session = self.CreateSession()
        if not os.path.isfile(self.dbfile):
            Base.metadata.create_all(self.engine)


    # def __del__(self):
    #     if not self.session.closed():
    #
    #         self.Close()

    def CreateEngine(self):
        """
        获取engine对象
        :return:
        """
        return create_engine(self.dbfile)

    def CreateSession(self):
        """
        设置sess对象
        :return:
        """
        sess = sessionmaker(self.engine)
        return sess()

    def Commit(self):
        self.session.commit()

    def Close(self):
        self.session.close()

    def RollBack(self):
        self.session.rollback()
        self.Commit()
        # self.Close()

    def AddOne(self, data, commit=True):
        """
        添加一条数据
        :param data: CompanyInfo(name = "test",url_addr = "test",content="test",keyword="test")
        :param commit: 默认提交到数据库
        :return:
        """

        self.session.add(data)
        if commit:
            self.Commit()
            res = deepcopy(data.ToDict())
            # self.Close()
            return res

    def Addmany(self, data, commit=True):
        """
        添加多条数据,使用列表
        :param data:[CompanyInfo(name = "test1",url_addr = "test1",content="test1",keyword="test"),
                     CompanyInfo(name = "test2",url_addr = "test2",content="test2"),keyword="test"]
        :param commit: 默认提交到数据库
        :return:
        """
        self.session.add_all(data)
        id_list = []
        if commit:
            self.Commit()
            for item in data:
                id_list.append(item.ToDict())
            # self.Close()
        return id_list

    def Ordby(self, table_name, column):
        """
        sess.Ordby(CompanyInfo,CompanyInfo.name)排序,
        :param table_name: 表名
        :param column: 字段名
        :return:
        """
        res = self.session.query(table_name).order_by(column).all()
        # self.Close()
        return res

    def GetAll(self, table_name,order=None):
        """
        获取表中所有数据
        :param table_name:
        :return:
        """
        if order:
            res = self.session.query(table_name).order_by(order).all()
        else:
            res = self.session.query(table_name).all()
        # self.Close()
        return res

    def GetColumn(self, column):
        """
        获取一个字段下所有的值
        CreateDBSess().GetColumn(ErrorLog.url)
        :param column: 字段名
        :return: 返回表中字段所有的值
        """
        res = self.session.query(column).all()
        # self.Close()
        return res

    def FindFisrt(self, table_name, column, value):
        """
        找到对应值的第一条记录
        :param table_name: 表名
        :param column: 列名
        :param value: 值
        :return:
        """
        res = self.session.query(table_name).filter(column == value).first()
        # self.Close()
        return res

    def Update(self, table_name, data):
        """
        更新数据,传入表和一条字典数据,根据id更新
        :param table_name:
        :param data:
        :return:
        """
        id = data["id"]
        query = self.FindFisrt(table_name, table_name.id, id)
        for item in data:
            if item == "id":
                continue
            else:
                value = data[item]
            setattr(query, item, value)
        try:
            self.Commit()
        except Exception as e:
            print(e)
            return False
            # self.Close()
            # return data

        return data

    def AddOneLoop(self, table_name, data):
        """
        传入字典,保存到数据库

        :param table_name:
        :param data:
        :return:
        """
        a = table_name()
        for item in data:
            setattr(a, item, data[item])
        try:
            self.AddOne(a)
            self.Commit()
        except Exception as e:
            print(e)
            return False
        return True

    def FindAll(self, table_name, column, value):
        """
        找到对应值的所有条记录,执行.count()可以统计条数
        sess.FindAll(CompanyInfo,CompanyInfo.name)
        :param table_name: 表名
        :param column: 列名
        :param value: 值
        :return:
        """
        res = self.session.query(table_name).filter(column == value).all()
        # self.Close()
        return res

    def FindNot(self, table_name, column, value):
        """
        找不符合条件的记录,相当于 select * from * where name != 'test'
        :param table_name: 表名
        :param column: 列名
        :param value: 值
        :return:
        """
        res = self.session.query(table_name).filter(column != value).all()
        # self.Close()
        return res

    def Like(self, table_name, column, value):
        """
        找符合条件的记录,相当于 select * from * where name like '%test%'
        sess.Like(CompanyInfo,CompanyInfo.name,'%test%')
        :param table_name: 表名
        :param column: 列名
        :param value: 值
        :return:
        """

        res = self.session.query(table_name).filter(column.like(value)).all()
        # self.Close()
        return res

    def FindAND(self, table_name, column1, value1, column2, value2):
        """
        sess.FindAND(CompanyInfo,CompanyInfo.name,'test1',CompanyInfo.url_addr,'aaa')
        多条件查询
        :param table_name:
        :param data:
        :return:
        """
        return (
            self.session.query(table_name)
            .filter(column1 == value1, column2 == value2)
            .all()
        )

    def FindOR(self, table_name, column1, value1, column2, value2):
        """
        sess.FindOR(CompanyInfo,CompanyInfo.name,'test1',CompanyInfo.url_addr,'aaa')
        多条件查询
        :param table_name:
        :param data:
        :return:
        """
        return (
            self.session.query(table_name)
            .filter(or_(column1 == value1, column2 == value2))
            .all()
        )

    def ExecSQL(self, SQL, para):
        """
        执行SQL语句
        SQL = "SELECT * FROM companys where name=:name and url_addr=:url_addr"
        sess.ExecSQL(text,{"name":"test1","url_addr":"aaa"})
        :param SQL:
        :param para:
        :return:
        """
        self.session.close()
        sess = scoped_session(sessionmaker(self.engine))
        return sess.execute(SQL, para).all()


    # def DelOne(self, obj):
    #
    #     if obj.id > 0:
    #         # print(obj)
    #         self.session.delete(obj)
    #         self.Commit()
    #         self.Close()
    #         return True
    #     return False

    def DelByValue(self, table_name, column, value):
        self.session.query(table_name).filter(column == value).delete()
        self.Commit()
        # self.Close()
        return True



    def Paging(self, table_name, page_size, page_index, column=None, value=None,orderby=None,reverse=None):
        """
        返回分页数据
        :param table_name: 表名
        :param page_index: 查询的页数
        :param page_size: 每页的条数
        :return:
        """
        query = self.session.query(table_name).filter(column == value)
        if column and value:
            items = paginate(
                query,
                page_index,
                page_size,
            )
        else:
            items = paginate(query, page_index, page_size)
        return items

    def GetColumnMax(self, column):
        return self.session.query(func.max(column)).scalar()

    def Count(self, table_name):
        return self.session.query(table_name).count()

def UpdateDB(sess,query,data):
    """

    :param sess: 数据连接
    :param query:数据中的查询对象
    :param data: 需要更新的数据
    :return:
    """
    for item in data:
        if hasattr(query, item):
            setattr(query, item, data[item])
    sess.Commit()
    return query.ToDict()