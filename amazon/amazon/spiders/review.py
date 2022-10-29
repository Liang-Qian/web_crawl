
from scrapy_redis.spiders import RedisSpider
from amazon.items import ReviewItem
from amazon.rank import CountryScreen
from scrapy.http import Request
from io import BytesIO

# from PIL import Image
from lxml import html


import re
import time
import json
import scrapy

import urllib.request



etree = html.etree

class ReviewSpider(scrapy.Spider):
    name = 'review'
    allowed_domains = ['amazon.com']
    start_urls = ['B07Z8XDXHK']

    def start_requests(self):
        star_5 =  f'https://www.amazon.com/product-reviews/{self.start_urls[0]}/ref=cm_cr_arp_d_viewopt_sr?ie=UTF8&filterByStar=five_star&reviewerType=all_reviews&sortBy=recent#reviews-filter-bar'
        star_4 =  f'https://www.amazon.com/product-reviews/{self.start_urls[0]}/ref=cm_cr_arp_d_viewopt_sr?ie=UTF8&filterByStar=four_star&reviewerType=all_reviews&mediaType=all_contents&sortBy=recent#reviews-filter-bar'
        star_3 = f'https://www.amazon.com/product-reviews/{self.start_urls[0]}/ref=cm_cr_arp_d_viewopt_sr?ie=UTF8&filterByStar=three_star&reviewerType=all_reviews&mediaType=all_contents&sortBy=recent#reviews-filter-bar'
        star_2 = f'https://www.amazon.com/product-reviews/{self.start_urls[0]}/ref=cm_cr_arp_d_viewopt_sr?ie=UTF8&filterByStar=two_star&reviewerType=all_reviews&mediaType=all_contents&sortBy=recent#reviews-filter-bar'
        star_1 = f'https://www.amazon.com/product-reviews/{self.start_urls[0]}/ref=cm_cr_arp_d_viewopt_sr?ie=UTF8&filterByStar=one_star&reviewerType=all_reviews&mediaType=all_contents&sortBy=recent#reviews-filter-bar'

        url_list = [star_1,star_2,star_3,star_4,star_5]
        for url in url_list:
            yield self.make_requests_from_url(url)

    def make_requests_from_url(self, url):
        if 'product-reviews' in url:
            return Request(url=url, dont_filter=True)

    def parse(self, response):
        with open('ceshi.html', 'w', encoding='utf-8') as f:
            f.write(response.text)

        self.referer_url = ' '.join(re.findall(r'([a-zA-z]+://[0-9a-z\.]+/[0-9a-z\.-]+/[0-9a-zA-Z]+)', response.request.url))

        self.asin = ' '.join(re.findall(r'[a-zA-z]+://[0-9a-z\.]+/[0-9a-z\.-]+/([0-9a-zA-Z]+)', response.request.url))

        reviewNum_xpath_1 = response.xpath("//div[@id='filter-info-section']/div[@class='a-row a-spacing-base a-size-base']/span/text()").extract_first()  # 评论数量
        reviewNum_xpath_2 = response.xpath("//div[@id='filter-info-section']/div/text()").extract_first()  # 评论数量

        if reviewNum_xpath_1:
            reviewNum_xpath = reviewNum_xpath_1
        else:
            reviewNum_xpath = reviewNum_xpath_2

        reviewNum_xpath = str(reviewNum_xpath).replace(',', '')
        print(reviewNum_xpath)
        if "global review" in reviewNum_xpath:
            reviewNum = re.search('\| (\d+) global review', reviewNum_xpath).group(1)
        elif "with review" in reviewNum_xpath:
            reviewNum = re.search(' (\d+) with review', reviewNum_xpath).group(1)
        else:
            reviewNum = 0
        print(reviewNum)
        pages = divmod(int(reviewNum), 10)
        if pages[1] != 0:
            page = pages[0] + 1
        else:
            page = pages[0]
        if page > 5:
            page = 5
        for i in range(page):
            if 'five_star' in response.request.url:
                star = 5
                site = f"{self.referer_url}/ref=cm_cr_getr_d_paging_btm_next_{i + 1}?filterByStar=five_star&pageNumber={i + 1}"
                yield scrapy.Request(site, callback=self.review_item, meta={'star': star}, dont_filter=True)
            elif 'four_star' in response.request.url:
                star = 4
                site = f"{self.referer_url}/ref=cm_cr_getr_d_paging_btm_next_{i + 1}?filterByStar=four_star&pageNumber={i + 1}"
                yield scrapy.Request(site, callback=self.review_item, meta={'star': star}, dont_filter=True)
            elif 'three_star' in response.request.url:
                star = 3
                site = f"{self.referer_url}/ref=cm_cr_getr_d_paging_btm_next_{i + 1}?filterByStar=three_star&pageNumber={i + 1}"
                yield scrapy.Request(site, callback=self.review_item, meta={'star': star}, dont_filter=True)
            elif 'two_star' in response.request.url:
                star = 2
                site = f"{self.referer_url}/ref=cm_cr_getr_d_paging_btm_next_{i + 1}?filterByStar=two_star&pageNumber={i + 1}"
                yield scrapy.Request(site, callback=self.review_item, meta={'star': star}, dont_filter=True)
            elif 'one_star' in response.request.url:
                star = 1
                site = f"{self.referer_url}/ref=cm_cr_getr_d_paging_btm_next_{i + 1}?filterByStar=one_star&pageNumber={i + 1}"
                yield scrapy.Request(site, callback=self.review_item, meta={'star': star}, dont_filter=True)
            # else:
            #     star = None
            #     site = None


    def review_item(self, response):
        star = response.meta['star']
        review_xpath = response.xpath("//div[@data-hook='review']")
        item = ReviewItem()
        for review in review_xpath:
            reviewBody_xpath = review.xpath("string(.//span[@data-hook='review-body'])").extract_first()  # 评论内容
            item['reviewBody'] = reviewBody_xpath.replace('\n', '').replace('\xa0', ' ').strip()

            item['reviewTime'] = review.xpath("string(.//span[@data-hook='review-date'])").extract_first().split('on')[1].strip()

            item['star'] = star

            item['asin'] = self.asin

            yield item
