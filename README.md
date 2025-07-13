# ReadMe
## Intro
……
## Structure
project-root/  
├── src/          # 主要源代码  
│   ├── models/     # 模型(alphaNet、树模型)  
│   └── utils/      # 工具函数(数据清洗函数、数据制图函数、算子……)  
├── testNotebook/        # 测试代码(ipynb)  
├── data/         # 数据  
│   ├── raw/        # 原始数据  
│   ├── washed/     # 处理后数据(清洗后数据)  
│   └── forecast/   # 工具函数  
└── config/       # 配置文件  
## Branches
### main
Official version, storing executable code. 
### develop
Development version, personal branches are merged into this branch after passing unit tests.
### user
Personal code base, everyone stores the changes they are responsible for in this branch first, and merges them into the develop branch after passing the unit test
