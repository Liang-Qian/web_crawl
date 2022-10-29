from scrapy_redis.spiders import RedisSpider
from amazon.items import BestsellersItem

import re
import time
import json


class BestsellersSpider(RedisSpider):
    name = 'BestSellers'
    redis_key = 'BestSellers:start_urls'

    def parse(self, response):
        item = BestsellersItem()

        # 站点
        country = ' '.join(re.findall(r'([a-zA-z]+://[0-9a-z\.]+)', response.request.url))
        item['site'] = country

        # 类别树
        categoryTree_list = response.xpath("//div[not(contains(@role,'group'))]/div[@role='treeitem']/a/text()").extract()
        categoryTree_set = {}

        if categoryTree_list:
            # 删除 Any Department 类别
            del categoryTree_list[0]
            # 把当前页面类别加入列表
            categoryTree = response.xpath("//div[@id='zg-left-col']//span/text()").extract_first()
            categoryTree_list.append(categoryTree)
            # 遍历类别树列表
            for v, i in enumerate(categoryTree_list, start=1):
                categoryTree_set[v] = i
            categoryTree = json.loads(json.dumps(categoryTree_set, ensure_ascii=False))
        else:
            categoryTree = -1
        item['category_tree'] = categoryTree

        # 商品排名和asin
        bestSellers_xpath = response.xpath("//div[@id='zg-right-col']//@data-client-recs-list").extract_first()
        for bestSellers in eval(bestSellers_xpath):
            if bestSellers['id']:
                parentAsin = bestSellers['id']
            else:
                parentAsin = -1

            if bestSellers['metadataMap']['render.zg.rank']:
                salesRank = bestSellers['metadataMap']['render.zg.rank']
            else:
                salesRank = -1
            item['parent_asin'] = parentAsin
            item['sales_rank'] = salesRank

            # 当前类目ID
            categoryId = re.search('(\d+)/ref', response.request.url)
            if categoryId:
                categoryId = categoryId.group(1)
            else:
                categoryId = -1
            item['category_id'] = categoryId

            # 排行获取时间
            item['time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

            yield item

        next_page_url=response.xpath("//ul[@class='a-pagination']/li[@class='a-last']/a/@href").get()
        if next_page_url:
            yield response.follow(next_page_url, callback=self.parse, dont_filter=True)

