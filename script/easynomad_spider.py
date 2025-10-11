import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from datetime import datetime
from base.base_spider import BaseSpider
from base.job_entity import JobEntity


class EasyNomadSpider(BaseSpider):
    """
    专门用于爬取EasyNomad网站远程工作信息的爬虫
    """
    
    def __init__(self, headers: Optional[Dict[str, Any]] = None, cookies: Optional[Dict[str, Any]] = None, timeout: int = 10, retry_times: int = 3, delay: float = 1.0):
        """
        初始化爬虫
        
        Args:
            headers: 自定义请求头
            cookies: Cookie字典
            timeout: 请求超时时间
            retry_times: 请求失败重试次数
            delay: 请求间隔时间
        """
        # 如果没有提供headers和cookies，使用默认值
        default_headers = {
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Referer': 'https://easynomad.cn/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
        }
        
        # 使用提供的值或默认值
        headers = headers or default_headers
        cookies = cookies or {}
        
        super().__init__(headers=headers, cookies=cookies, timeout=timeout, retry_times=retry_times, delay=delay)
        self.source = 'easynomad.cn'
    
    def crawl(self, start_page: Optional[int] = None, max_pages: Optional[int] = None) -> List[JobEntity]:
        """
        实现爬取逻辑，获取整个列表的数据
        
        Args:
            start_page: 起始页标识，默认从第1页开始
            max_pages: 最大爬取页数，None表示不限制
            
        Returns:
            List[JobEntity]: 爬取到的工作实体列表
        """
        print(f"开始爬取EasyNomad网站的远程工作信息...")
        def get_data(page):
            params = (
                ('limit', '100'),
                ('page', page),
                ('jobCategory', ''),
                ('contractType', ''),
            )

            response = requests.get('https://easynomad.cn/api/posts/list', headers=self.headers, params=params,
                                    cookies=self.cookies)
            data = response.json()['data']
            print(data)
            return data

        def get_list():
            page = 1
            all_data = []
            while True:
                data = get_data(page)
                page += 1
                if not data:
                    break
                all_data.extend(data)
            return all_data
        all_data = get_list()
        all_data = [JobEntity(id=item['jobCode'], title=item['jobTitle'], company=item['company'], description=item['descContent'], salary=item['salary'], tags=item['jobType'], url=item['url'], publish_time=item['jobPublishTime'], source=self.source) for item in all_data]
        self.recorded_data = all_data
        return all_data



def main():
    """
    主函数
    """
    print("开始增量爬取EasyNomad网站的远程工作信息...")
    
    # 创建爬虫实例
    spider = EasyNomadSpider(delay=1.0)
    
    # 设置爬取和保存参数
    output_file = '../data/easynomad_jobs'
    save_formats = ['csv', 'json', 'sqlite', 'excel', 'markdown']
    incremental = True
    primary_key = 'id'
    db_table = 'jobs'
    max_pages = 100
    remove_columns = ['descContent', 'source', 'crawl_time']  # 保存Markdown时移除的列
    
    try:
        # 使用抽象类提供的crawl_and_save方法，一步完成爬取和保存
        spider.crawl_and_save(
            output_file=output_file,
            save_formats=save_formats,
            incremental=incremental,
            primary_key=primary_key,
            db_table=db_table,
            remove_columns=remove_columns,
            max_pages=max_pages
        )
        
        print("所有数据已保存")
        
    except Exception as e:
        print(f"程序运行出错: {str(e)}")
    finally:
        # 关闭爬虫会话
        spider.close()


if __name__ == '__main__':
    main()