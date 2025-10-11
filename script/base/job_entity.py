from .base_entity import BaseEntity
from typing import Optional, List, Any
from datetime import datetime
from pydantic import Field


class JobEntity(BaseEntity):
    """
    基于Pydantic的远程工作实体类，用于规范化远程工作信息的数据结构
    提供类型检查、数据验证和自动ID生成功能
    """
    
    # 定义字段并设置类型提示
    id: Optional[str] = Field(None, alias='jobId')
    title: Optional[str] = Field(None)
    company: Optional[str] = Field(None)
    description: Optional[str] = Field(None)
    salary: Optional[str] = Field(None)
    location: Optional[str] = Field(None)
    tags: Optional[Any] = Field(None)  # 允许字符串或列表
    url: Optional[str] = Field(None)
    publish_time: Optional[str] = Field(None, alias='publishTime')
    crawl_time: str = Field(default_factory=lambda: datetime.now().isoformat())
    source: str = Field(default='unknown')
    
    model_config = {
        # 允许通过别名设置字段
        'populate_by_name': True,
        # 允许额外的字段
        'extra': 'allow'
    }
    
    @property
    def job_id(self) -> Optional[str]:
        """
        获取工作ID
        
        Returns:
            工作ID
        """
        return self.id
    
    def set_source(self, source: str):
        """
        设置数据来源
        
        Args:
            source: 数据来源名称
        """
        self.source = source
    
    def ensure_id(self) -> str:
        """
        确保实体有唯一标识符
        优先使用已有的id，然后是title+company的组合，最后生成哈希
        
        Returns:
            ID值
        """
        # 已有的id会被Pydantic自动处理
        if self.id is not None:
            return self.id
        
        # 尝试使用title和company组合生成ID
        if self.title and self.company:
            combined = f"{self.title}_{self.company}"
            import hashlib
            self.id = hashlib.md5(combined.encode('utf-8')).hexdigest()
            return self.id
        
        # 最后使用基类的生成方法
        return super().ensure_id()
    
    def to_dict(self) -> dict:
        """
        转换为字典格式，确保包含所有必要字段
        
        Returns:
            实体的字典表示
        """
        # 确保ID存在
        self.ensure_id()
        # 使用Pydantic的dict方法
        return self.dict(by_alias=False)
    
    def __str__(self) -> str:
        """
        字符串表示
        
        Returns:
            实体的字符串表示
        """
        return f"JobEntity(title='{self.title}', company='{self.company}', source='{self.source}')"