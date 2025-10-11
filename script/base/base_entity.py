from typing import Dict, Any, Optional, ClassVar
from pydantic import BaseModel, Field
import hashlib


class BaseEntity(BaseModel):
    """
    基于Pydantic的基础数据实体类，提供类型检查和数据验证功能
    """
    
    # 类变量，定义默认ID字段名
    ID_FIELD: ClassVar[str] = 'id'
    
    # 定义可选的ID字段
    id: Optional[str] = Field(None, alias='jobId')
    
    model_config = {
        # 允许通过别名设置字段
        'populate_by_name': True,
        # 允许额外的字段
        'extra': 'allow'
    }
    
    def update(self, **kwargs):
        """
        更新实体属性
        
        Args:
            **kwargs: 要更新的属性键值对
        """
        # 直接更新字典，避免可能的递归
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
    
    def get_attribute(self, name: str, default: Any = None) -> Any:
        """
        安全地获取属性值，避免属性不存在时抛出异常
        
        Args:
            name: 属性名
            default: 默认值
            
        Returns:
            属性值或默认值
        """
        return getattr(self, name, default)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        将实体转换为字典格式
        
        Returns:
            实体的字典表示
        """
        # 使用Pydantic的model_dump方法（v2版本）
        return self.model_dump(by_alias=False)
    
    def generate_id(self) -> str:
        """
        生成一个基于实体属性内容的唯一标识符
        
        Returns:
            生成的MD5哈希ID
        """
        # 使用model_dump方法避免递归
        data_dict = self.model_dump(exclude={'id'}, by_alias=False)
        
        # 过滤掉复杂对象，只保留可序列化的值
        simple_dict = {}
        for key, value in data_dict.items():
            if isinstance(value, (str, int, float, bool, type(None))):
                simple_dict[key] = value
        
        # 将属性排序并转换为字符串，生成哈希
        sorted_items = sorted(simple_dict.items())
        data_str = str(sorted_items)
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()
    
    def ensure_id(self) -> Any:
        """
        确保实体有唯一标识符，如果不存在则生成
        
        Returns:
            ID值
        """
        if self.id is None:
            self.id = self.generate_id()
        return self.id
    
    def has_attribute(self, name: str) -> bool:
        """
        检查实体是否具有指定属性
        
        Args:
            name: 属性名
            
        Returns:
            是否存在该属性
        """
        return hasattr(self, name)
    
    def __eq__(self, other: 'BaseEntity') -> bool:
        """
        比较两个实体是否相等（基于ID）
        
        Args:
            other: 另一个实体
            
        Returns:
            是否相等
        """
        if not isinstance(other, BaseEntity):
            return False
        return self.id == other.id
    
    def __str__(self) -> str:
        """
        更友好的字符串表示
        
        Returns:
            实体的字符串表示
        """
        class_name = self.__class__.__name__
        # 使用安全的属性访问，避免递归
        attrs = []
        for key, value in self.model_dump().items():
            if not key.startswith('_') and isinstance(value, (str, int, float, bool, type(None))):
                attrs.append(f"{key}={value!r}")
        attrs_str = ', '.join(attrs)
        return f"{class_name}({attrs_str})"
    
    def __repr__(self) -> str:
        """
        正式的字符串表示
        
        Returns:
            实体的正式字符串表示
        """
        return self.__str__()