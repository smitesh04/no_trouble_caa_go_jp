import os.path
from typing import Iterable

import pandas as pd
import scrapy
from scrapy import Request
from scrapy.cmdline import execute
from no_trouble_caa_go_jp.common_func import headers, page_write, create_md5_hash



class DataSpider(scrapy.Spider):
    name = "data"

    def __init__(self):
        self.page = 1

    def start_requests(self):
        url = 'https://www.no-trouble.caa.go.jp/search/result1.html'

        hashid = create_md5_hash(url)
        pagesave_dir = rf"C:/Users/Actowiz/Desktop/pagesave/no_trouble_caa_go_jp"
        file_name = fr"{pagesave_dir}/{hashid}.html"
        meta = {}
        meta['hashid'] = hashid
        meta['pagesave_dir'] = pagesave_dir
        meta['file_name'] = file_name

        if os.path.exists(file_name):
            yield scrapy.Request('file:///'+file_name, callback=self.parse, cb_kwargs=meta)
        else:
            yield scrapy.Request(url, headers=headers(), callback=self.parse, cb_kwargs=meta)

    def parse(self, response, **kwargs):
        if not os.path.exists(kwargs['file_name']):
            page_write(pagesave_dir=kwargs['pagesave_dir'], file_name=kwargs['file_name'], data=response.text)
        table_heads = response.xpath('//table[@class="c-table"]/thead//th/text()').getall()
        df = pd.DataFrame(columns= table_heads)
        if table_heads:
            table_rows = response.xpath('//table[@class="c-table"]/tbody//tr')
            for rows in table_rows:
                rows_data = rows.xpath('.//td')
                df_dict = dict()
                for row_data, header_data in zip(rows_data, table_heads):
                    data = row_data.xpath('.//text()').getall()
                    data = ' '.join(data)
                    df_dict[header_data] = data
                df = pd.concat([df, pd.DataFrame([df_dict])], ignore_index=True)
            csv_path = rf'C:/Users/Actowiz/Desktop/Smitesh_Docs/Project/no_trouble_caa_go_jp/page{self.page}.csv'
            df.to_csv(csv_path, index=False)
            print(csv_path)
            self.page +=1
            url = f'https://www.no-trouble.caa.go.jp/search/result{self.page}.html'
            hashid = create_md5_hash(url)
            pagesave_dir = rf"C:/Users/Actowiz/Desktop/pagesave/no_trouble_caa_go_jp"
            file_name = fr"{pagesave_dir}/{hashid}.html"
            meta = {}
            meta['hashid'] = hashid
            meta['pagesave_dir'] = pagesave_dir
            meta['file_name'] = file_name

            yield scrapy.Request(url, headers=headers(), callback=self.parse, cb_kwargs=meta)

if __name__ == '__main__':
    execute("scrapy crawl data".split())