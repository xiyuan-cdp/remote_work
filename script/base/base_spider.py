from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

import requests
from pydantic import BaseModel

from script.base.job_entity import JobEntity
from script.utils.data_recorder import DataRecorder


class BaseSpider(ABC):
    """
    爬虫抽象基类，提供通用功能和接口定义
    
    子类需要实现以下抽象方法：
    - crawl(start_page, max_pages): 实现具体的爬取逻辑，返回实体列表
    - create_entity(data): 创建实体对象
    
    可选实现的方法：
    - parse_page(response): 解析单页数据
    """

    def __init__(self, headers: Optional[Dict[str, str]] = None, cookies: Optional[Dict[str, str]] = None,
                 timeout: int = 10, retry_times: int = 3, delay: float = 1.0):
        """
        初始化爬虫
        
        Args:
            headers: 请求头字典
            cookies: Cookie字典
            timeout: 请求超时时间（秒）
            retry_times: 请求失败重试次数
            delay: 请求间隔时间（秒），防止过快请求被封
        """
        self.headers = headers or {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.cookies = cookies or {}
        self.timeout = timeout
        self.retry_times = retry_times
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        if self.cookies:
            self.session.cookies.update(self.cookies)
        self.recorded_data: List[JobEntity] = []  # 存储已爬取的数据

    @abstractmethod
    def crawl(self, start_page: Optional[Any] = None, max_pages: Optional[int] = None) -> List[JobEntity]:
        """
        子类实现具体的爬取逻辑，负责获取整个列表的数据
        
        Args:
            start_page: 起始页标识
            max_pages: 最大爬取页数
            
        Returns:
            List[JobEntity]: 爬取到的实体列表
        """
        pass

    def process_entities(self, entities: List[JobEntity]) -> List[JobEntity]:
        """
        处理实体列表，确保每个实体都有ID
        
        Args:
            entities: 实体列表
            
        Returns:
            List[JobEntity]: 处理后的实体列表
        """
        for entity in entities:
            entity.ensure_id()
        return entities

    # 以下方法为辅助方法，子类可以选择重写或使用

    def save_data(self, output_file: str, save_formats: Optional[List[str]] = None,
                  incremental: bool = True, primary_key: str = 'id',
                  db_table: str = 'data', remove_columns: Optional[List[str]] = None) -> None:
        """
        保存爬取的数据到文件
        
        Args:
            output_file: 输出文件路径或文件名前缀
            save_formats: 保存格式列表，如['csv', 'json', 'sqlite', 'excel', 'markdown']
            incremental: 是否增量保存
            primary_key: 主键字段名
            db_table: 数据库表名
            remove_columns: 需要移除的列名列表（适用于Markdown格式）
        """
        if not self.recorded_data:
            print("没有数据需要保存，请先调用crawl方法")
            return

        # 转换实体列表为字典列表
        # 确保正确处理Pydantic实体
        data_dicts = []
        for entity in self.recorded_data:
            if isinstance(entity, BaseModel):
                # 对于Pydantic模型，使用dict()方法
                data_dicts.append(entity.dict(by_alias=False))
            elif hasattr(entity, 'to_dict'):
                # 对于自定义类，使用to_dict方法
                data_dicts.append(entity.to_dict())
            else:
                # 尝试直接转换
                data_dicts.append(entity)

        # 默认保存格式
        if save_formats is None:
            save_formats = ['csv']

        # 保存到不同格式
        for fmt in save_formats:


            # 根据格式设置文件扩展名
            format_extensions = {
                'csv': '.csv',
                'json': '.json',
                'sqlite': '.db',
                'excel': '.xlsx',
                'markdown': '.md'
            }

            if fmt.lower() in format_extensions:
                file_path = f"{output_file}{format_extensions[fmt.lower()]}"

                # 创建记录器并保存数据
                recorder = DataRecorder(file_path, primary_key=primary_key, db_table=db_table)
                recorder.add_data(data_dicts, incremental=incremental)

                # 对于Markdown格式，传递remove_columns参数
                if fmt.lower() == 'markdown' and remove_columns:
                    recorder.record(file_format=fmt, remove_columns=remove_columns)
                else:
                    recorder.record(file_format=fmt)

                # 关闭资源
                recorder.close()
            else:
                print(f"不支持的保存格式: {fmt}")

    def crawl_and_save(self, output_file: str, save_formats: Optional[List[str]] = None,
                       incremental: bool = True, primary_key: str = 'id',
                       db_table: str = 'data', remove_columns: Optional[List[str]] = None,
                       start_page: Optional[Any] = None, max_pages: Optional[int] = None) -> List[JobEntity]:
        """
        爬取数据并保存
        
        Args:
            output_file: 输出文件路径或文件名前缀
            save_formats: 保存格式列表
            incremental: 是否增量保存
            primary_key: 主键字段名
            db_table: 数据库表名
            remove_columns: 需要移除的列名列表
            start_page: 起始页标识
            max_pages: 最大爬取页数
            
        Returns:
            List[JobEntity]: 爬取到的数据实体列表
        """
        # 执行爬取
        data = self.crawl(start_page=start_page, max_pages=max_pages)

        # 保存数据
        self.save_data(output_file, save_formats=save_formats, incremental=incremental,
                       primary_key=primary_key, db_table=db_table, remove_columns=remove_columns)

        return data

    def close(self):
        """
        关闭会话，释放资源
        """
        self.session.close()

    def __enter__(self):
        """
        支持上下文管理器协议，允许使用with语句
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        退出上下文管理器时关闭会话
        """
        self.close()
