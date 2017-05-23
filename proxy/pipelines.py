# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb


class ProxyPipeline(object):
    def __init__(self,mysql_host,mysql_user,mysql_passwd,mysql_db,mysql_port):
        self.mysql_host=mysql_host
        self.mysql_user=mysql_user
        self.mysql_passwd=mysql_passwd
        self.mysql_db=mysql_db
        self.mysql_port=mysql_port
       
    @classmethod
    def from_crawler(cls,crawler):
        return cls(crawler.settings.get('MYSQL_HOST'),crawler.settings.get('MYSQL_USER'),crawler.settings.get('MYSQL_PASSWD'),crawler.settings.get('MYSQL_DB'),crawler.settings.get('MYSQL_PORT'))
    
    def open_spider(self,spider):
        self.connection=MySQLdb.connect(self.mysql_host,self.mysql_user,self.mysql_passwd,self.mysql_db,self.mysql_port)
        self.cursor=self.connection.cursor()
    def close_spider(self,spider):
        self.cursor.close()
        self.connection.close()
    def process_item(self, item, spider):
        f=lambda i:'("'+i+'"),'
        insert_values=''.join(map(f,[value for key,value in item.items()]))[:-1]
        sql='INSERT IGNORE INTO proxy VALUES {}'.format(insert_values)
        print sql
        print self.cursor.execute(sql) #执行SQL
        self.connection.commit()# 写入操作
