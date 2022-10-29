from scrapy_redis.spiders import RedisSpider
from amazon.items import ProductinfoItem
from amazon.rank import CountryScreen

from scrapy.http import Request

from io import BytesIO
from PIL import Image
from lxml import html

import re
import time
import json
import redis
import scrapy

import urllib.request

etree = html.etree

class ProductinfoSpider(RedisSpider):
    name = 'ProductInfo'
    redis_key = 'ProductInfo:start_urls'

    def parse(self, response):

        self.referer_url = ' '.join(re.findall(r'([a-zA-z]+://[0-9a-z\.]+)', response.request.url))
        self.header = str(response.request.headers).replace('[', '').replace(']', '')

        # 获取商品页面数据
        item = ProductinfoItem()
        # 产品信息
        productDetails_set = {}
        # 产品信息为 Product details
        productDetails = response.xpath("//div[@id='detailBulletsWrapper_feature_div']/h2/text()").extract_first()
        # 产品信息为 Product information
        productInformation = response.xpath("//div[@id='prodDetails']/h2/text()").extract_first()
        # 产品信息为 content-grid-widget
        grid_widget = response.xpath("//div[@id='content-grid-widget-v1.0']")

        # if productDetails or productInformation or grid_widget:
        # 0812-旧的判断层，当前已删掉该判断条件
        if productDetails:
            productDetails_list = response.xpath("//div[@id='detailBullets_feature_div']/ul/li/span")
            for details in productDetails_list:
                details_key_text = details.xpath("./span[@class='a-text-bold']/text()").extract_first()
                details_key = re.sub('[^\w\s]', '', str(details_key_text)).strip()
                details_value = details.xpath("./span[2]/text()").extract_first()
                if details_value:
                    details_value = details_value.replace("\n", "").strip()
                productDetails_set[details_key] = details_value
        elif productInformation:
            # 存在Technical Details标签
            technicalDetails = response.xpath("//div[@class='a-column a-span6']/h1/text()").extract_first()
            if technicalDetails:
                technicalDetails_list = response.xpath("//table[@id='productDetails_techSpec_section_1']/tr")
                for details in technicalDetails_list:
                    details_key = details.xpath("./th/text()").extract_first().strip()
                    details_value_text = details.xpath("./td/text()").extract_first()
                    details_value = re.sub('[^\w\s]', '', str(details_value_text)).strip()
                    if details_key is None:  # Technical Details有时会有空值
                        details_key_text = details.xpath("./th/span/a/text()").extract_first()
                        details_key = re.sub('[^\w\s]', '', str(details_key_text)).strip()
                    productDetails_set[details_key] = details_value
            # 存在 Technical Details, 产品信息会变成 Additional Information, 例子：https://www.amazon.com/AmazonBasics-Multipurpose-Copy-Printer-Paper/dp/B01FV0F8H8
            additional_list = response.xpath("//table[@id='productDetails_detailBullets_sections1']/tr/td[@class='a-size-base prodDetAttrValue']")
            for details in additional_list:
                details_key = details.xpath("./preceding-sibling::th/text()").extract_first().strip()
                details_value = details.xpath("./text()").extract_first().strip()
                productDetails_set[details_key] = details_value

        # 价格下存在商品详情简介，例子：https://www.amazon.com/Donner-DEP-10-Beginner-Weighted-Keyboard/dp/B07XDXVRZT
        spacing_small_xpath = response.xpath("//table[@class='a-normal a-spacing-micro']/tr")
        if spacing_small_xpath:
            for small in spacing_small_xpath:
                additional_key = small.xpath("./td[@class='a-span3']/span/text()").extract_first().strip()
                additional_value = small.xpath("./td[@class='a-span9']/span/text()").extract_first().strip()
                # 排除key,value 名称有差异, 数据重复
                if additional_value not in str(productDetails_set):
                    productDetails_set[additional_key] = additional_value

        # 价格下存在 Product Specifications, 例子：https://www.amazon.com/dp/B07VNSXY31
        product_specifications = response.xpath("//table[@id='product-specification-table']/tr")
        if product_specifications:
            for product in product_specifications:
                product_key = product.xpath("./th/text()").extract_first().strip()
                product_value = product.xpath("./td/text()").extract_first().strip()
                # 排除key 相同,value 有差异, 避免父信息覆盖子信息
                if product_key not in str(productDetails_set):
                    productDetails_set[product_key] = product_value
        # 处理时间格式
        if productDetails_set:
            CountryScreen().dateAdded(productDetails_set)
        item['product_details'] = productDetails_set

        # 获取 BSR 排名
        # Best Sellers Rank 和 Amazon Best Sellers Rank 判断两种不同的解析规则
        salesRank_set = {}
        # 当标签为 Best Sellers Rank 时
        salesRank_a_xpath = response.xpath("//table[@id='productDetails_detailBullets_sections1']/tr/td/span/span")
        salesRank_b_xpath = response.xpath("//div[@id='detailBulletsWrapper_feature_div']/ul//li/span/a")

        if salesRank_a_xpath:
            for v, salesRank in enumerate(salesRank_a_xpath, start=1):
                rank_set = {}
                salesRank_name_xpath = salesRank.xpath("./a/text()").extract_first().strip()
                salesRank_num_xpath = salesRank.xpath("./text()").extract_first()
                salesRank_url_xpath = salesRank.xpath("./a/@href").extract_first()
                salesRank_url = self.referer_url + salesRank_url_xpath

                rank_set["name"] = CountryScreen().salesRank(salesRank_name_xpath)
                rank_set["rank"] = "".join(list(filter(str.isdigit, salesRank_num_xpath)))
                rank_set["id"] = CountryScreen().categoryUrl(salesRank_url)
                rank_set["url"] = salesRank_url
                salesRank_set[v] = rank_set

        elif salesRank_b_xpath:
            for v, salesRank in enumerate(salesRank_b_xpath, start=1):
                rank_set = {}
                salesRank_name_xpath = salesRank.xpath("./text()").extract_first().strip()
                salesRank_url_xpath = salesRank.xpath("./@href").extract_first()    
                salesRank_num_xpath = salesRank.xpath("./parent::*/text()").extract_first().strip()
                if not salesRank_num_xpath:
                    salesRank_num_xpath = response.xpath("//div[@id='detailBulletsWrapper_feature_div']/ul/li/span/text()").extract()
                salesRank_num = ' '.join(salesRank_num_xpath).strip()
                salesRank_url = self.referer_url + salesRank_url_xpath

                rank_set["name"] = CountryScreen().salesRank(salesRank_name_xpath)
                rank_set["rank"] = "".join(list(filter(str.isdigit, salesRank_num)))
                rank_set["id"] = CountryScreen().categoryUrl(salesRank_url)
                rank_set["url"] = salesRank_url
                salesRank_set[v] = rank_set
        else:
            salesRank_set = None
        salesRank = json.loads(json.dumps(salesRank_set, ensure_ascii=False))
        item['sales_rank'] = salesRank

        # 产品所在类别
        category_xpath = response.xpath("//div[@id='wayfinding-breadcrumbs_feature_div']/ul/li/span/a")
        if category_xpath:
            category_set = {}
            for i, v in enumerate(category_xpath, start=1):
                binding = {}
                name = v.xpath("./text()").extract_first()
                url = v.xpath("./@href").extract_first()
                binding['name'] = name.strip()
                binding['id'] = re.search('node=(\d+)', url).group(1)
                category_set[i] = binding
            category = json.loads(json.dumps(category_set, ensure_ascii=False))
        else:
            category = -1
        item['category'] = category

        # 商品跟卖数
        to_sell_str = response.xpath("//div[@class='olp-text-box']/span[1]/text()").extract_first()
        if to_sell_str:
            to_sell = ''.join(list(filter(str.isdigit, to_sell_str)))
        else:
            to_sell = 1
        item['to_sell'] = to_sell

        # 是否支持prime服务
        prime_x = response.xpath("//div[@id='bbop_feature_div']/script/text()").extract()
        if prime_x:
            prime = True
        else:
            prime = False
        item["prime"] = prime

        # 产品类型
        try:
            type_xpath = response.xpath("//script[@language='JavaScript']").extract_first()
            productTypeName = re.search('"productTypeName":"(.*?)",', type_xpath).group(1)
            item["type"] = productTypeName
        except:
            item["type"] = -1

        # 父 Asin
        parentAsin_xpath = response.xpath("//div[@id='imageBlockVariations_feature_div']/script[@type='text/javascript']/text()").extract_first()
        try:
            parentAsin = re.search('"parentAsin":"(.*?)",', parentAsin_xpath).group(1)
            item["parent_asin"] = parentAsin
        except:
            item["parent_asin"] = -1

        # ASIN
        try:
            asin = re.search('"mediaAsin":"(.*?)",', parentAsin_xpath).group(1)
            item["asin"] = asin
        except:
            item["asin"] = -1

        # 商品标题
        title_a = response.xpath("//span[@id='gc-asin-title']/text()").extract_first()
        title_b = response.xpath("//span[@id='productTitle']/text()").extract_first()
        title_c = response.xpath("//span[@id='btAsinTitle']/span/text()").extract_first()
        title_d = response.xpath("//div[@class='DVWebNode-detail-atf-wrapper DVWebNode']//h1/text()").extract_first()
        title_e = response.xpath("//div[@id='dmusicProductTitle_feature_div']/h1/text()").extract_first()
        if title_a:
            title = str(title_a).replace('\n', '').replace('\u200e', '').replace('\xa0', ' ').strip()
        elif title_b:
            title = str(title_b).replace('\n', '').replace('\u200e', '').replace('\xa0', ' ').strip()
        elif title_c:
            title = str(title_c).replace('\n', '').replace('\u200e', '').replace('\xa0', ' ').strip()
        elif title_d:
            title = title_d
        elif title_e:
            title = str(title_e).replace('\n', '').replace('\u200e', '').replace('\xa0', ' ').strip()
        else:
            title = -1
        item["title"] = title

        # 产品价格
        price_a = response.xpath("//span[@id='actualPriceValue']/strong[@class='priceLarge']/text()").extract_first()
        # price_b = response.xpath("string(//div[@id='apex_offerDisplay_desktop']//span[@class='a-offscreen'])").extract_first()
        price_b = response.xpath("//div[@class='a-section a-spacing-none aok-align-center']/span[@data-a-color='base']/span[@class='a-offscreen']/text()").extract_first()
        price_c = response.xpath("//td[@class='a-span12']/span[@data-a-color='price']/span[@class='a-offscreen']/text()").extract_first()
        # 当前价格
        if price_a:
            price = CountryScreen().goodsPrice(self.referer_url, price_a)
        elif price_b:
            price = CountryScreen().goodsPrice(self.referer_url, price_b)
        elif price_c:
            price = CountryScreen().goodsPrice(self.referer_url, price_c)
        else:
            price = -1
        item["price"] = price

        # 历史价格
        historicalPrice_a = response.xpath("string(//td[@class='a-span12 a-color-secondary a-size-base']/span[@class='a-price a-text-price a-size-base']/span[@class='a-offscreen'])").extract_first()
        historicalPrice_b = response.xpath("//div[@class='a-section a-spacing-small aok-align-center']//span[@class='a-price a-text-price']/span[@class='a-offscreen']/text()").extract_first()
        if historicalPrice_a:
            listPrice = CountryScreen().goodsPrice(self.referer_url, historicalPrice_a)
        elif historicalPrice_b:
            listPrice = CountryScreen().goodsPrice(self.referer_url, historicalPrice_b)
        else:
            listPrice = -1
        item["list_price"] = listPrice

        # 出售商
        soldBy_a_xpath = response.xpath("//a[@id='sellerProfileTriggerId']/text()").extract_first()
        soldBy_b_xpath = response.xpath("//div[@class='tabular-buybox-text a-spacing-none']/span/text()").extract_first()
        if soldBy_a_xpath:
            soldBy = soldBy_a_xpath
        elif soldBy_b_xpath:
            soldBy = soldBy_b_xpath
        else:
            soldBy = -1
        item["sold_by"] = soldBy

        # 产品描述
        description_a = response.xpath("string(//div[@id='productDescription'])").extract_first()
        description_b = response.xpath("string(//div[@id='mas-product-description']/div)").extract_first()
        if description_a:
            description = re.sub('[^\w\s]', '', description_a).replace('\xa0', ' ').replace('\n', '').strip()
        elif description_b:
            description = re.sub('[^\w\s]', '', description_b).replace('\xa0', ' ').replace('\n', '').strip()
        else:
            description = -1
        item["description"] = description

        # 产品特性
        features_a_xpath = response.xpath("//div[@id='feature-bullets']/ul[@class='a-unordered-list a-vertical a-spacing-mini']/li[not(@id)]/span/text()").extract()
        features_b_xpath = response.xpath("//div[@id='mas-product-feature']/div/ul/li/span/text()").extract()
        features = {}
        if features_a_xpath:
            for i, v in enumerate(features_a_xpath, start=1):
                features[i] = v.replace('\xa0', ' ').strip()
            bullets = json.loads(json.dumps(features, ensure_ascii=False))
        elif features_b_xpath:
            for i, v in enumerate(features_b_xpath, start=1):
                features[i] = v.replace('\xa0', ' ').strip()
            bullets = json.loads(json.dumps(features, ensure_ascii=False))
        else:
            bullets = -1
        item["bullets"] = bullets

        # 获取 Customer reviews 产品评分
        customerReviews_set = {}
        customerReviews = response.xpath("//div[@id='reviewsMedley']//h2/text()").extract_first()
        if customerReviews:
            # 综合评分
            overall_merit_a = response.xpath("//span[@class='a-size-medium a-color-base']/text()").extract_first()
            overall_merit_b = response.xpath("//span[@id='acrPopover']//a[@class='a-popover-trigger a-declarative']/i/span[@class='a-icon-alt']/text()").extract_first()
            # 评价人数
            global_ratings = response.xpath("//div[@class='a-row a-spacing-medium averageStarRatingNumerical']/span/text()").extract_first()
            # 评分率
            star_xpath = response.xpath("//table[@id='histogramTable']/tr")

            # 综合评分
            if overall_merit_a and re.search(r'\d', overall_merit_a):
                overallMerit = CountryScreen().overallMerit(overall_merit_a)
            elif overall_merit_b and re.search(r'\d', overall_merit_b):
                overallMerit = CountryScreen().overallMerit(overall_merit_b)
            else:
                overallMerit = -1
            customerReviews_set["overall_merit"] = overallMerit

            # 评分人数
            if global_ratings:
                globalRatings = "".join(list(filter(str.isdigit, global_ratings)))
            else:
                globalRatings = -1
            customerReviews_set["global_ratings"] = globalRatings

            # 评分率
            if star_xpath:
                star_set = {}
                for star in star_xpath:
                    star_key_xpath = star.xpath("string(./td[@class='aok-nowrap' or @class='a-nowrap']/span[@class='a-size-base'])").extract_first()
                    star_value = star.xpath("string(./td[@class='a-text-right a-nowrap']/span[@class='a-size-base'])").extract_first().strip()
                    star_key = "".join(list(filter(str.isdigit, star_key_xpath)))
                    star_set[star_key] = star_value
            else:
                star_set = False
            customerReviews_set["star"] = star_set

            # 功能性评分
            # 1.获取ASIN和CSRF参数
            csrf_xpath = response.xpath("//span[@id='cr-state-object']/@data-state").extract_first()
            asin = json.loads(csrf_xpath)["asin"]
            locale = json.loads(csrf_xpath)["locale"]
            lazyWidgetLoaderUrl = json.loads(csrf_xpath)["lazyWidgetLoaderUrl"]
            lazyWidgetCsrfToken = json.loads(csrf_xpath)["lazyWidgetCsrfToken"]

            # 2.请求功能性评分 By feature
            feature_headers = eval(self.header)
            feature_headers['content-type'] = 'application/x-www-form-urlencoded;charset=utf-8'
            feature_headers['cache-control'] = 'no-cache'
            feature_data = {
                'scope':'reviewsAjax2',
                'th':'1'
                }
            feature_url = self.referer_url + f"{lazyWidgetLoaderUrl}?asin={asin}&csrf={lazyWidgetCsrfToken}&language={locale}&lazyWidget=cr-summarization-attributes&lazyWidget=cr-age-recommendation&lazyWidget=cr-solicitation&lazyWidget=cr-summarization-lighthut"
            feature_response = CountryScreen().request_news("post", url=feature_url, headers=feature_headers, data=json.dumps(feature_data))
            response_html = etree.HTML(feature_response.text.replace('\\', ''))
            by_feature = response_html.xpath("//div[@class='a-row a-spacing-medium']/span/text()")

            # By feature 功能性评分
            if by_feature:
                by_feature_xptah = response_html.xpath("//div[@data-hook='cr-summarization-attribute']")
                by_feature_set = {}
                for feature in by_feature_xptah:
                    by_feature_key = " ".join(feature.xpath(".//span[@class='a-size-base a-color-base']/text()"))
                    by_feature_val = " ".join(
                        feature.xpath(".//span[@class='a-size-base a-color-tertiary']/text()"))
                    by_feature_set[by_feature_key] = by_feature_val
            else:
                by_feature_set = False
            customerReviews_set["by_feature"] = by_feature_set
        item["customer_reviews"] = customerReviews_set

        # 竞争商品广告数据
        competitor_set = {}
        # 判断是否存在 sp_detail 标签
        sp_detail_a_xpath = response.xpath("//div[@id='sp_detail']/@data-a-carousel-options").extract_first()
        sp_detail_b_1_xpath = response.xpath("//div[@id='sp_detail2']/@data-a-carousel-options").extract_first()
        sp_detail_b_2_xpath = response.xpath("//div[@id='sp_detail2-prime_theme_for_non_prime_members']/@data-a-carousel-options").extract_first()
        sp_detail_c_xpath = response.xpath("//div[@class='a-section a-spacing-large bucket']/div/div/@data-a-carousel-options").extract_first()
        sp_detail_d_xpath = response.xpath("//div[@id='sp_detail_thematic-highly_rated']/@data-a-carousel-options").extract_first()

        # 标签 sp_detail 广告数据
        if sp_detail_a_xpath:
            spDetail_xpath = response.xpath("//div[@id='sp_detail']//ol/li")
            sp_detail_a_set = self.sp_detail(spDetail_xpath)
            competitor_set["sp_detail"] = json.loads(json.dumps(sp_detail_a_set, ensure_ascii=False))

        # other 相似商品推荐数据
        if sp_detail_c_xpath:
            sp_detail_c_set = {}
            spDetail_xpath = response.xpath("//div[@class='a-section a-spacing-large bucket']//ol/li")
            for v, spDetail in enumerate(spDetail_xpath, start=1):
                sp_detail_set = {}
                spAsin = spDetail.xpath(".//a[@tabindex='-1']/@href").extract_first()
                if spAsin:
                    spAsin = spDetail.xpath(".//a[@tabindex='-1']/@href").extract_first()
                    spTitle = spDetail.xpath(".//span/div/text()").extract_first()
                    spOverallMerit = spDetail.xpath(".//span[@class='a-icon-alt']/text()").extract_first()
                    spGlobalRatings = spDetail.xpath(".//div[@class='a-icon-row']/a/span/text()").extract_first()
                    spPrice = spDetail.xpath("string(.//span[@class='a-offscreen' or @class='p13n-sc-price'])").extract_first()
                    spSalesRank_num = spDetail.xpath(".//div[@class='a-row']/a/i/text()").extract_first()
                    spSalesRank_name = spDetail.xpath(".//span[@class='a-size-small a-color-secondary']/span/text()").extract_first()
                    spSalesRank_url = spDetail.xpath(".//a[@class='a-size-small a-link-normal p13n-best-seller']/@href").extract_first()
                    spSalesRank_set = {}
                    sp_detail_set["sp_title"] = spTitle
                    if spSalesRank_num:
                        spSalesRank_set["name"] = CountryScreen().salesRank(spSalesRank_name)
                        spSalesRank_set["rank"] = "".join(list(filter(str.isdigit, spSalesRank_num)))
                        spSalesRank_set["url"] = self.referer_url + spSalesRank_url
                        sp_detail_set["sp_sales_rank"] = spSalesRank_set
                    spAsin = re.search('dp/(.*?)/', str(spAsin))
                    if spAsin is not None:
                        sp_detail_set["sp_asin"] = spAsin.group(1)
                    else:
                        sp_detail_set["sp_asin"] = spAsin
                    if spOverallMerit:
                        sp_detail_set["sp_overall_merit"] = CountryScreen().overallMerit(spOverallMerit)
                        sp_detail_set["sp_global_ratings"] = spGlobalRatings.replace(',', '').replace('.', '').replace('\xa0', '')
                    else:
                        sp_detail_set["sp_overall_merit"] = -1
                        sp_detail_set["sp_global_ratings"] = -1
                    if spPrice:
                        sp_detail_set["sp_price"] = CountryScreen().goodsPrice(self.referer_url, spPrice)
                    else:
                        sp_detail_set["sp_price"] = -1
                    sp_detail_c_set[v] = sp_detail_set
            competitor_set["other"] = json.loads(json.dumps(sp_detail_c_set, ensure_ascii=False))

        # 标签 highly_rated 广告数据
        if sp_detail_d_xpath:
            spDetail_xpath = response.xpath("//div[@id='sp_detail_thematic-highly_rated']//ol/li")
            sp_detail_b_set = self.sp_detail(spDetail_xpath)
            competitor_set["highly_rated"] = json.loads(json.dumps(sp_detail_b_set, ensure_ascii=False))

        # 标签 sp_detail2 广告数据
        if sp_detail_b_1_xpath:
            spDetail_xpath = response.xpath("//div[@id='sp_detail2']//ol/li")
            sp_detail_b_set = self.sp_detail(spDetail_xpath)
            competitor_set["sp_detail2"] = json.loads(json.dumps(sp_detail_b_set, ensure_ascii=False))
        elif sp_detail_b_2_xpath:
            spDetail_xpath = response.xpath("//div[@id='sp_detail2-prime_theme_for_non_prime_members']//ol/li")
            sp_detail_b_set = self.sp_detail(spDetail_xpath)
            competitor_set["sp_detail2"] = json.loads(json.dumps(sp_detail_b_set, ensure_ascii=False))

        if not competitor_set:
            competitor_set = -1
        # 获取页面广告数据
        item['sp'] = competitor_set

        # 获取主图尺寸
        img_url_a_xpath = response.xpath("//img/@data-old-hires").extract_first()
        img_url_b_xpath = response.xpath("//img[@id='js-masrw-main-image']/@src").extract_first()
        img_url_c_xpath = response.xpath("//div[@id='imgTagWrapperId']/img/@src").extract_first()
        if img_url_a_xpath:
            if len(img_url_a_xpath) == 0:
                img_url_a_xpath = response.xpath("//img[@data-old-hires='']/@src").extract_first()
            imageSize = self.image_size(img_url_a_xpath)
        elif img_url_b_xpath:
            imageSize = self.image_size(img_url_b_xpath)
        elif img_url_c_xpath:
            imageSize = self.image_size(img_url_c_xpath)
        else:
            imageSize = -1
        item['image_size'] = imageSize

        # 获取主图和副图数量
        imageNum = response.xpath("//div[@id='altImages']//li[@class='a-spacing-small item']").extract()
        if not imageNum:
            imageNum = response.xpath("//img[@class='masrw-thumbnail']/@src").extract()
        item['image_num'] = len(imageNum)

        # 判断是否存在视频
        videoProduct_a_xpath = response.xpath("//div[@id='imageBlockVariations_feature_div']/script[@type='text/javascript']/text()").extract_first()
        videoProduct_b_xpath = response.xpath("//a[@id='js-masrw-play-video-button']/span/text()").extract_first()
        if videoProduct_a_xpath:
            videoProduct_str = re.search(r'"videos":\[(.*?)\],', videoProduct_a_xpath).group(1)
            if videoProduct_str:
                videoProduct = True
            else:
                videoProduct = False
        elif videoProduct_b_xpath:
            videoProduct = True
        else:
            videoProduct = -1
        item['video_product'] = videoProduct

        #站点
        item['site'] = self.referer_url
        # 数据获取时间
        item['time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        yield item

        # # 商品参数不存在时,则重新获取页面所有数据
        # else:
        #     yield scrapy.Request(url=response.request.url, callback=self.parse, dont_filter=True)

    # 广告数据提取
    def sp_detail(self, spDetail_xpath):
        spDetail_set = {}
        for v, spDetail in enumerate(spDetail_xpath, start=1):
            sp_detail_set = {}
            spAsin = spDetail.xpath("./div/@data-asin").extract_first()
            spTitle = spDetail.xpath(".//a[@class='a-link-normal']/@title").extract_first()
            spOverallMerit = spDetail.xpath(".//div[@class='a-row']/a/i/@class").extract_first()
            spGlobalRatings = spDetail.xpath(".//a[@class='a-link-normal adReviewLink a-text-normal']/span/text()").extract_first()
            spPrice = spDetail.xpath(".//span[@class='a-size-medium a-color-price']/text()").extract_first()
            spPrime = spDetail.xpath(".//i/@role").extract_first()
            sp_detail_set["sp_asin"] = spAsin
            sp_detail_set["sp_title"] = spTitle.replace('\xa0', ' ')
            if spPrice:
                sp_detail_set["sp_price"] = CountryScreen().goodsPrice(self.referer_url, spPrice)
            else:
                sp_detail_set["sp_price"] = -1
            if spOverallMerit:
                sp_detail_set["sp_overall_merit"] = spOverallMerit.lstrip('a-icon a-icon-star a-star-').replace('-', '.')
                sp_detail_set["sp_global_ratings"] = spGlobalRatings.replace('\u202f', '').replace(',', '').replace('.', '')
            else:
                sp_detail_set["sp_overall_merit"] = -1
                sp_detail_set["sp_global_ratings"] = -1
            if spPrime:
                sp_detail_set["sp_prime"] = True
            else:
                sp_detail_set["sp_prime"] = False

            spDetail_set[v] = sp_detail_set
        return spDetail_set

    # 识别图片尺寸
    def image_size(self, img_url):
        req = urllib.request.Request(url=img_url, headers=eval(self.header))
        file = urllib.request.urlopen(req, timeout=6)
        f = BytesIO(file.read())
        img = Image.open(f)
        imageSize = str(img.width) + '*' + str(img.height)
        return imageSize