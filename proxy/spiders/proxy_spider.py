#coding:utf-8
import scrapy
import re
from itertools import groupby
import MySQLdb

class ProxySpider(scrapy.Spider):
    name='proxy'
    custom_settings={
    'DOWNLOAD_DELAY' : 1,
    'CONCURRENT_REQUESTS_PER_IP' : 1
    }
    def start_requests(self):
        page=10
        url_kuaidaili=[('http://www.kuaidaili.com/proxylist/{}/'.format(i),self.parse_kuaidaili) for i in range(1, page + 1)]
        url_66ip=[('http://m.66ip.cn/mo.php?sxb=&tqsl=100&port=&export=&ktip=&sxa=&submit=%CC%E1++%C8%A1&textarea=',self.parse_66ip)]
        url_youdaili=[('http://www.youdaili.net/Daili/http/',self.parse_youdaili)]
        url_xici = [('http://www.xicidaili.com/nn',self.parse_xici),
                    ('http://www.xicidaili.com/nt',self.parse_xici)]
        url_goubanjia=[('http://www.goubanjia.com/free/gngn/index{}.shtml'.format(i),self.parse_goubanjia) for i in range(1,page+1)]
        urls=dict(url_kuaidaili+url_66ip+url_youdaili+url_xici+url_goubanjia)
        print urls
        for url,method in urls.items():
            yield scrapy.Request(url,method,dont_filter=True)
        

    def parse_kuaidaili(self,response):
        ips=response.xpath('//*[@id="index_free_list"]/table/tbody/tr/td[1]/text()').extract()
        ports=response.xpath('//*[@id="index_free_list"]/table/tbody/tr/td[2]/text()').extract()
        proxy=['http://%s:%s' %(ip,port) for ip,port in zip(ips,ports)]
        if proxy:
            proxy=dict([(i,j) for i,j in enumerate(proxy)])
            print '-----------------'
            print len(proxy)
            print '-----------------'
            yield proxy
        
    
    def parse_66ip(self,response):
        proxy=re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}', response.text)
        proxy=['http://%s' %i for i in proxy]
        if proxy:
            proxy=dict([(i,j) for i,j in enumerate(proxy)])
            print '-----------------'
            print len(proxy)
            print '-----------------'
            yield proxy
            
    def parse_youdaili(self,response):
        url_next=response.xpath('//*[@class="chunlist"]/ul/li/p/a/@href').extract_first()
        if url_next:
            yield scrapy.Request(url_next,self.parse_youdaili_next)
    def parse_youdaili_next(self,response):
        proxy=re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}', response.text)
        proxy=['http://%s' %i for i in proxy]
        if proxy:
            proxy=dict([(i,j) for i,j in enumerate(proxy)])
            print '-----------------'
            print len(proxy)
            print '-----------------'
            yield proxy
    def parse_xici(self,response):
        ips = response.xpath('.//table[@id="ip_list"]//tr/td[2]/text()').extract()
        ports = response.xpath('.//table[@id="ip_list"]//tr/td[3]/text()').extract()
        proxy=['http://%s:%s' %(ip,port) for ip,port in zip(ips,ports)]
        if proxy:
            proxy=dict([(i,j) for i,j in enumerate(proxy)])
            print '-----------------'
            print len(proxy)
            print '-----------------'
            yield proxy
    def parse_goubanjia(self,response):
        class_to_port={'GEGEI':':8081','HBZIE':':8998','GEGE':':808','GEGEA':':8080','GEA':':80','CFACE':':3128','GEZIE':':8123','GEZEE':':8118','HCAAA':':9000'}
        extract_text=response.xpath('//td[@class="ip"]').extract_first()
        #print extract_text
        ip_ports=response.xpath('//td[@class="ip"]').re(r'<div style="display:\s*inline-block;">([\.\d]+)</div>|<span>([\.\d]+)</span>|<span style="display:\s*inline-block;">([\.\d]+)</span>|:<span class="port (\w+)">')
        #print ip_ports
        '''ip_ports格式如下
        [u'2', u'', u'', u'', u'', u'', u'22', u'', u'.', u'', u'', u'', u'', u'', u'95', u'', u'', u'.1', u'', u'', u'', u'', u'8', u'', u'', u'.', u'', u'', u'13', u'', u'', u'', u'', u'', u'7', u'', u'', u'', u'', u'GEGE', u'', u'39', u'', u'', u'', u'', u'.', u'', u'7', u'', u'', u'', u'', u'4', u'', u'', u'', u'', u'.2', u'', u'19', u'', u'', u'', u'.', u'', u'', u'', u'', u'1', u'', u'', u'31', u'', u'', u'', u'', u'', u'', u'HBZIE']
        '''
        f=lambda s,n=[]:len(n)-1 if s.isalpha() and not n.append(0) else len(n)  #用于itertools.groupby把ip_ports分组 每个ip和port一组
        g=lambda x,y:x+class_to_port.get(y,':808') if y.isalpha() else x+y       #用于把字母格式的port替换成对应的数字端口
        real_ip_ports=[reduce(g,li) for li in [list(group) for key,group in groupby(ip_ports,f)]]
        proxy=[u'http://%s' %i for i in real_ip_ports]
        if proxy:
            proxy=dict([(i,j) for i,j in enumerate(proxy)])
            
            print '-----------------'
            print len(proxy)
            print '-----------------'
            yield proxy


class ProxyValidSpider(scrapy.Spider):
    name='valid'
    custom_settings={
    'DOWNLOAD_DELAY' : 0.1,
    'CONCURRENT_REQUESTS' : 32,
    'CONCURRENT_REQUESTS_PER_DOMAIN' : 32
    }
    def start_requests(self):
        self.mysql_host=self.settings.get('MYSQL_HOST')
        self.mysql_user=self.settings.get('MYSQL_USER')
        self.mysql_passwd=self.settings.get('MYSQL_PASSWD')
        self.mysql_db=self.settings.get('MYSQL_DB')
        self.mysql_port=self.settings.get('MYSQL_PORT')
        connection=MySQLdb.connect(self.mysql_host,self.mysql_user,self.mysql_passwd,self.mysql_db,self.mysql_port)
        cursor=connection.cursor()
        sql='SELECT * FROM proxy'
        cursor.execute(sql)
        result_tuple=cursor.fetchall()
        if result_tuple:
            print cursor.execute('DELETE FROM proxy')    
        cursor.close()
        connection.commit()
        connection.close()
        #print result_tuple
        for proxy_tuple in result_tuple:
            yield scrapy.Request(url='https://secure3.hilton.com/en_US/dt/reservation/book.htm?internalDeepLinking=true?inputModule=HOTEL_SEARCH&ctyhocn=WUXXDDI',callback=self.after_baidu,meta={'proxy':proxy_tuple[0],'dont_retry':True,'download_timeout':25},dont_filter=True)
    def after_baidu(self,response):
        
        if response.status==200:
            connection=MySQLdb.connect(self.mysql_host,self.mysql_user,self.mysql_passwd,self.mysql_db,self.mysql_port)
            cursor=connection.cursor()
            sql='INSERT IGNORE INTO validproxy VALUES ("{}")'.format(response.meta['proxy'])
            res=cursor.execute(sql)
            print '-------------------------------------'
            print response.meta['proxy'],response.status
            print res
            print '-------------------------------------'
            cursor.close()
            connection.commit()
            connection.close()
        
        


