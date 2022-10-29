#！/usr/bin/env python
"""

[ author ]: liangqian
[ function ]: Get us dollar exchange rate
[ time ]: 2022/10/29

"""

from tenacity import retry, TryAgain
from fake_useragent import UserAgent
from lxml import html

import time
import json
import urllib3
import requests
import psycopg2

etree = html.etree

# 禁用安全请求警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CountryRate:
    def __init__(self):

        self.url = "https://www.x-rates.com/table/?from=USD&amount=1"

        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'content-type': 'text/html; charset=UTF-8',
            'sec-fetch-dest':'document',
            'x-requested-with': 'XMLHttpRequest',
            'referer': 'https://www.exchangerates.org.uk/',
            'upgrade-insecure-requests': '1',
            'cache-control': 'max-age=0',
        }

        self.PG_SQL_LOCAL = {
            'database': 'data_rantion',
            'user': 'dc_reptile',
            'password': "reptile@rt123",
            'host': 'pgm-j6cng43e627gn554145310.pg.rds.aliyuncs.com',
            'port': 5432
        }

        self.country_to_abbr = {'Argentine Peso':'AR',
                                'Australian Dollar':'AU',
                                'Bahraini Dinar':'BH',
                                'Botswana Pula':'BW',
                                'Brazilian Real':'BR',
                                'British Pound':'GB',
                                'Bruneian Dollar':'BN',
                                'Bulgarian Lev':'BG',
                                'Canadian Dollar':'CA',
                                'Chilean Peso':'CL',
                                'Chinese Yuan Renminbi':'CN',
                                'Colombian Peso':'CO',
                                'Croatian Kuna':'HR',
                                'Czech Koruna':'CZ',
                                'Danish Krone':'DK',
                                'Emirati Dirham':'AE',
                                'Euro':'EU',
                                'Hong Kong Dollar':'HK',
                                'Hungarian Forint':'HU',
                                'Icelandic Krona':'IS',
                                'Indian Rupee':'IN',
                                'Indonesian Rupiah':'ID',
                                'Iranian Rial':'IR',
                                'Israeli Shekel':'IL',
                                'Japanese Yen':'JP',
                                'Kazakhstani Tenge':'KZ',
                                'Kuwaiti Dinar':'KW',
                                'Libyan Dinar':'LY',
                                'Malaysian Ringgit':'MY',
                                'Mauritian Rupee':'MU',
                                'Mexican Peso':'MX',
                                'Nepalese Rupee':'NP',
                                'New Zealand Dollar':'NZ',
                                'Norwegian Krone':'NO',
                                'Omani Rial':'OM',
                                'Pakistani Rupee':'PK',
                                'Philippine Peso':'PH',
                                'Polish Zloty':'PL',
                                'Qatari Riyal':'QA',
                                'Romanian New Leu':'RO',
                                'Russian Ruble':'RU',
                                'Saudi Arabian Riyal':'SA',
                                'Singapore Dollar':'SG',
                                'South African Rand':'ZA',
                                'South Korean Won':'KR',
                                'Sri Lankan Rupee':'LK',
                                'Swedish Krona':'SE',
                                'Swiss Franc':'CH',
                                'Taiwan New Dollar':'TW',
                                'Thai Baht':'TH',
                                'Trinidadian Dollar':'TT',
                                'Turkish Lira':'TR',
                                'Venezuelan Bolivar':'VE'
                                }


    def rate_data(self):
        response = self.rate_request(self.url, self.headers)
        html= etree.HTML(response.content)
        rate_time = html.xpath("//div[@class='col2 pull-right module bottomMargin']/div[@class='moduleContent']/span[@class='ratesTimestamp']/text()")[0]
        if rate_time:
            rate_time = time.strptime(rate_time, "%b %d, %Y %H:%M %Z")
            rate_time = int(time.mktime(rate_time))
            exchange_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(rate_time))
        else:
            exchange_time = -1

        rate_list = html.xpath("//div[@class='col2 pull-right module bottomMargin']/div[@class='moduleContent']/table[@class='tablesorter ratesTable']/tbody/tr")
        for vaule in rate_list:
            if vaule is not None:
                # 连接数据库
                conn = psycopg2.connect(**self.PG_SQL_LOCAL)
                conn.autocommit = True
                cursor = conn.cursor()
                # 国家全称
                country_name = vaule.xpath(".//td/text()")[0]
                if country_name:
                    country_name = country_name
                else:
                    country_name = '-1'
                # 国家缩写
                convert_to = self.country_to_abbr[country_name]
                if convert_to:
                    convert_to = convert_to
                else:
                    convert_to = '-1'
                # 货币缩写
                currency_type = vaule.xpath(".//td[@class='rtRates']/a/@href")[0]
                currency_type = currency_type[-3:]
                if currency_type:
                    currency_type = currency_type
                else:
                    currency_type = '-1'
                # 货币汇率
                currency = vaule.xpath(".//td[@class='rtRates']/a/text()")[1]
                if currency and "INF" not in currency:
                    currency = currency
                else:
                    currency = '-1'

                # 存入数据库
                cursor.execute(f'''INSERT INTO src_dc_clr.country_rate(exchange_time,currency_name,convert_to,currency,currency_type)\
                VALUES('{exchange_time}','{country_name}','{convert_to}',{currency},'{currency_type}')''')
                conn.commit()
                # print(exchange_time , country_name , convert_to , currency , currency_type)
        print('%s :导入成功。'%exchange_time)


    @retry()
    def rate_request(self, url, headers):
        headers["user-agent"] = UserAgent().chrome
        response = requests.get(url, headers=headers, proxies=self.request_proxies(), verify=False, timeout=6)
        if response.status_code == 200:
            return response
        raise TryAgain

    @retry()
    def request_proxies(self):
        # 代理ip内网地址
        response = requests.get(url="http://10.110.32.85:9000/vps_one?region=random&id=000959&pw=yj1314",
                                headers={
                                    "Content-Type": "text/plain; charset=utf-8",
                                }, timeout=2)
        res = json.loads(response.content)
        if res[0]:
            proxies = {
                'http': res[0],
                'https': res[0],
            }
            return proxies
        raise TryAgain

if __name__ == '__main__':
    CountryRate().rate_data()
