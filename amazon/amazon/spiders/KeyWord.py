from scrapy_redis.spiders import RedisSpider
from amazon.items import KeywordItem
from amazon.rank import CountryScreen
from urllib.parse import unquote

import re
import time
import scrapy

class KeywordSpider(RedisSpider):
    name = 'KeyWord'
    redis_key = 'KeyWord:start_urls'

    def parse(self, response):

        item = KeywordItem()
        # 站点
        self.referer_url = ' '.join(re.findall(r'([a-zA-z]+://[0-9a-z\.]+)', response.request.url))
        item['site'] = self.referer_url

        # 关键词
        keyword= response.xpath("//input[@id='twotabsearchtextbox']/@value").extract_first()
        item['keyword'] = keyword

        # 页码
        # goods_page = response.xpath("//span[@class='s-pagination-item s-pagination-selected']/text()").extract_first()
        goods_page = ' '.join(re.findall(r'sr_pg_([.1-3]$)', response.request.url))
        item['page'] = goods_page

        # 普通展位60个商品
        label_xpath = response.xpath("//div[@data-component-type='s-search-result']")
        if label_xpath:
            label_list = []
            for value in label_xpath:
                label_list.append(self.data_extract(value,response))

            goods_xpath = response.xpath("//div[@class='s-main-slot s-result-list s-search-results sg-row']/div[@data-component-type]/@class").extract()

            sp_index_list = []
            ord_index_list = []
            num = 0
            for index, value in enumerate(goods_xpath, start=1):
                if "s-result-item sg-col-0-of-12 sg-col-16-of-20 s-widget sg-col s-flex-geom sg-col-12-of-16 s-widget-spacing-large" == value:
                    goods_index_list = response.xpath(f"//div[@class='s-main-slot s-result-list s-search-results sg-row']/div[@data-component-type][{index}]")
                    sponsored_title = goods_index_list.xpath(".//span[@class='a-size-medium-plus a-color-base']/text()").extract_first()
                    sbv_video_product = goods_index_list.xpath(".//div[@class='sg-col-inner']/div/span/@data-component-type").extract_first()
                    goods_list = []
                    if sponsored_title:
                        goods_value_list = goods_index_list.xpath(".//div[@class='s-border-top-overlap']/div[@data-asin]|.//li[@class='a-carousel-card']/div[@data-asin]")
                        for goods_value in goods_value_list:
                            goods_set = self.data_extract(goods_value,response)
                            goods_set['type'] = sponsored_title
                            goods_set['sp'] = 'sponsored'
                            goods_list.append(goods_set)
                    elif sbv_video_product:
                        goods_value = goods_index_list.xpath(".//div[@class='a-section sbv-product']")
                        goods_set = self.data_extract(goods_value, response)
                        goods_list.append(goods_set)

                    num +=1
                    goods_index = index - num
                    if (goods_index % 4) != 0:
                        goods_index = goods_index +(4 - (goods_index % 4))

                    if sp_index_list:
                        sp_index = goods_index - ord_index_list[0] + sp_index_list[0]
                        label_list[sp_index:sp_index] = goods_list
                        ord_index_list[0] = goods_index
                        sp_index_list[0] =sp_index + len(goods_list)
                    else:
                        label_list[goods_index:goods_index]= goods_list
                        ord_index_list.append(goods_index)
                        sp_index_list.append(goods_index + len(goods_list))

            goods_info = []
            for index, value in enumerate(label_list, start=1):
                value['sales_rank'] = index
                goods_info.append(value)
            item['goods']=goods_info
            yield item
            
        else:
             item['goods']= -1
             yield item

        

    def data_extract(self, value, response):
        data_dict = {}

        # asin
        asin_common = value.xpath("./@data-asin").extract_first()
        asin_sbv =value.xpath(".//div[@class='a-section a-spacing-none s-expand-height']/a[@class='a-link-normal']/@href").extract_first()
        asin_all = value.xpath(".//a[@class='a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal']/@href").extract_first()
        if asin_common:
            asin = asin_common
        elif asin_sbv:
            asin = ' '.join(re.findall(r'dp/([0-9a-zA-z]+)', asin_sbv))
        else:
            asin = unquote(asin_all)
            asin = ' '.join(re.findall(r'dp/([0-9a-zA-z]+)', asin))
        data_dict['asin'] = asin

        # 是否广告
        sponsored = value.xpath(".//span[@class='a-color-secondary']")
        sbv = value.xpath("./@class").extract_first()
        if sponsored:
            sp = 'sponsored'
        elif 'sbv' in sbv:
            sp = 'sbv'
        else:
            sp = '-1'
        data_dict['sp'] = sp

        # 标题
        title_a = value.xpath(".//span[@class='a-size-base-plus a-color-base a-text-normal']/text()").extract_first()
        title_b = value.xpath(".//span[@class='a-size-medium a-color-base a-text-normal']/text()").extract_first()
        title_c = value.xpath(".//span[@class='a-size-base a-color-base a-text-normal']/text()").extract_first()
        if title_a:
            title = title_a
        elif title_b:
            title = title_b
        elif title_c:
            title = title_c
        elif asin:
            title = response.xpath(f"//div[@data-asin='{asin}']//span[@class='a-size-base-plus a-color-base a-text-normal']/text()").extract_first()
        else:
            title = -1
        data_dict['title'] = str(title).replace('\n', '').replace('\u200e', '').replace('\xa0', ' ').strip()

        # 评论数
        globalRatings = value.xpath(".//span[@class='a-size-base s-underline-text']/text()").extract_first()
        if globalRatings:
            globalRatings = "".join(list(filter(str.isdigit, globalRatings)))
        else:
            globalRatings = -1
        data_dict['global_ratings'] = globalRatings

        # 星级
        star_str = value.xpath(".//span[@class='a-icon-alt']/text()").extract_first()
        if star_str:
            star = CountryScreen().overallMerit(star_str)
        else:
            star = -1
        data_dict['star'] = star

        # 价格
        price = value.xpath(".//span[@class='a-price']/span[@class='a-offscreen']/text()").extract_first()
        if price:
            price = CountryScreen().goodsPrice(self.referer_url, price)
        else:
            price = -1
        data_dict['price'] = price

        # 历史/优惠前价格
        listPrice = value.xpath(".//span[@class='a-price a-text-price']/span[@class='a-offscreen']/text()").extract_first()
        if listPrice:
            listPrice = CountryScreen().goodsPrice(self.referer_url, listPrice)
        else:
            listPrice = -1
        data_dict['list_price'] = listPrice

        # 获取时间
        data_dict['time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        return data_dict