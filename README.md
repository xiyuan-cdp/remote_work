# 多网站爬虫框架

这是一个灵活的多网站爬虫框架，采用面向对象的设计理念，通过抽象类定义统一的接口，使得添加新的网站爬虫变得简单。

## 项目结构

```
remote_work/
├── script/
│   ├── __init__.py
│   ├── base/
│   │   ├── __init__.py
│   │   └── base_spider.py  # 爬虫抽象基类
│   ├── utils/
│   │   ├── __init__.py
│   │   └── data_recorder.py  # 数据记录器
│   ├── easynomad_spider.py  # EasyNomad网站爬虫实现
│   └── remote_work.py  # 原始爬虫脚本
├── requirements.txt
├── data.csv  # 输出的CSV数据
└── data.md   # 输出的Markdown数据
```

## 框架架构

本框架采用面向对象的模块化设计，主要包含以下组件：

1. **BaseSpider（抽象基类）**：所有爬虫的抽象基类，提供通用的爬取逻辑、数据保存功能和实体管理
2. **BaseEntity（实体基类）**：数据实体的抽象基类，提供标准化的数据结构和管理方法
3. **JobEntity（实现类）**：远程工作数据的实体实现类
4. **EasyNomadSpider（实现类）**：特定网站的爬虫实现
5. **DataRecorder**：数据记录器，负责将爬取的数据保存到不同格式的文件中

## 核心功能

1. **面向对象架构**：基于泛型和实体类的设计，提高代码复用性和可维护性
2. **实体规范化**：通过实体类统一数据结构，确保数据一致性
3. **统一数据保存**：抽象类提供标准的数据保存接口，支持多格式导出
4. **增量爬取**：基于主键或哈希值实现增量爬取，避免重复数据
5. **错误处理**：完善的异常处理机制，确保爬虫稳定运行
6. **配置灵活**：支持自定义请求头、延迟时间等参数

## 使用方法

### 1. 运行现有的EasyNomad爬虫

```bash
python script/easynomad_spider.py
```

### 2. 创建新的网站爬虫

要创建新的网站爬虫，只需继承`BaseSpider`类并实现必要的抽象方法：

```python
from script.base.base_spider import BaseSpider

class MyCustomSpider(BaseSpider):
    def get_url(self) -> str:
        # 返回爬虫的基础URL
        return 'https://example.com/api/data'
    
    def parse_page(self, response) -> List[Dict[str, Any]]:
        # 解析页面内容，提取需要的数据
        # 实现具体的解析逻辑
        pass
    
    def get_next_page(self, response) -> Optional[Any]:
        # 获取下一页信息，如果没有下一页则返回None
        # 实现具体的分页逻辑
        pass
```

### 3. 使用数据记录器

```python
from script.utils.data_recorder import DataRecorder

# 创建数据记录器（支持增量爬取）
recorder = DataRecorder('output.db', primary_key='id', db_table='jobs')

# 添加数据（增量模式）
recorder.add_data(data_list, incremental=True)

# 记录数据到SQLite
recorder.record()

# 记录到CSV
csv_recorder = DataRecorder('output.csv', primary_key='id')
csv_recorder.add_data(data_list, incremental=True)
csv_recorder.record()

# 记录到Excel
excel_recorder = DataRecorder('output.xlsx', primary_key='id')
excel_recorder.add_data(data_list, incremental=True)
excel_recorder.record()

# 保存为Markdown，同时移除某些列
md_recorder = DataRecorder('output.md')
md_recorder.add_data(data_list)
md_recorder.record(remove_columns=['unnecessary_column'])

# 关闭数据库连接
recorder.close()
```

## 类说明

### 1. 基础爬虫类 - BaseSpider

BaseSpider是所有爬虫的抽象基类，定义了爬虫的基本接口、通用方法和数据保存功能。采用泛型设计，要求实现类返回特定类型的实体对象。

#### 核心方法

- `get_url()`: 获取初始URL
- `parse_page(response)`: 解析页面响应，返回实体对象列表
- `create_entity(data)`: 创建实体对象
- `get_next_page(response)`: 获取下一页URL
- `crawl(max_pages=None)`: 执行爬取逻辑，返回实体对象列表
- `save_data(output_file, data, save_formats, incremental, primary_key, db_table, remove_columns)`: 保存数据到多种格式
- `crawl_and_save()`: 一步完成爬取和保存操作
- `fetch_page(url)`: 获取页面内容
- `close()`: 关闭爬虫会话

### 2. 实体基类 - BaseEntity

BaseEntity是数据实体的抽象基类，提供标准化的数据结构和管理方法，确保数据的一致性和完整性。

#### 核心方法

- `__init__(**kwargs)`: 初始化实体对象
- `__getitem__(key)`: 支持字典式访问
- `__setitem__(key, value)`: 支持字典式赋值
- `to_dict()`: 转换为字典
- `update(**kwargs)`: 更新属性
- `get_id()`: 获取实体ID
- `set_id(id_value)`: 设置实体ID
- `generate_id()`: 生成唯一ID
- `ensure_id()`: 确保实体有唯一ID
- `__eq__(other)`: 比较两个实体是否相等
- `__hash__()`: 计算实体哈希值

### 3. 工作实体 - JobEntity

JobEntity是远程工作数据的具体实体实现，继承自BaseEntity，包含工作相关的属性和方法。

#### 核心方法

- 继承BaseEntity的所有方法
- 特定的ID生成逻辑，优先使用jobId字段

### 4. 数据记录器 - DataRecorder

DataRecorder负责将爬取的数据保存到不同格式的文件中，支持CSV、JSON、Markdown、SQLite和Excel格式。

#### 核心方法

- `__init__(file_path, primary_key=None, db_table=None)`: 初始化记录器
- `add_data(data, incremental=False)`: 添加数据，支持增量模式
- `record(remove_columns=None)`: 记录数据到文件
- `close()`: 关闭资源（如数据库连接）
- `get_data_count()`: 获取数据条数
- `get_sample_data(limit=5)`: 获取样本数据

#### 使用示例

通过抽象类的统一接口使用：

```python
# 创建爬虫实例
spider = EasyNomadSpider()

# 一步完成爬取和多格式保存
spider.crawl_and_save(
    output_file='easynomad_jobs',
    save_formats=['csv', 'json', 'sqlite', 'excel', 'markdown'],
    incremental=True,
    primary_key='id',
    db_table='jobs',
    remove_columns=['description'],
    max_pages=100
)
```

## 架构设计优势

1. **分离关注点**：爬虫逻辑和数据处理逻辑分离
2. **统一接口**：所有爬虫提供一致的接口
3. **类型安全**：使用泛型和实体类确保类型安全
4. **可扩展性**：容易添加新的爬虫和数据格式
5. **代码复用**：通用功能在抽象类中实现一次，多处复用

## 依赖

- Python 3.6+
- requests 2.32.3
- openpyxl: 用于支持Excel格式（可选，安装命令：`pip install openpyxl`）

## 安装依赖

```bash
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

## 注意事项

1. 请遵守网站的robots.txt规则，不要过度爬取
2. 可以通过调整`delay`参数来控制请求频率，避免被封IP
3. 添加新的爬虫时，请确保实现所有必要的抽象方法
4. 如需更复杂的爬虫功能，可以扩展基类或添加新的工具类
5. 增量爬取依赖于主键或数据哈希值，请确保数据有唯一标识
6. 使用Excel格式时需要安装openpyxl库：`pip install openpyxl`
7. SQLite数据库自动创建表结构，所有字段默认为TEXT类型
8. 数据量大时，SQLite格式比CSV和Excel有更好的查询性能
