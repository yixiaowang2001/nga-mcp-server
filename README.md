# NGA MCP Server

基于Playwright的NGA游戏论坛MCP Server，专注于数据获取，支持帖子内容爬取、板块帖子获取和板块浏览等操作。

## Demo with Cursor

![demo_with_cursor](demo.gif)

## 工具列表

- **crawl_post**: 爬取帖子完整内容，包括标题、正文和所有回复评论
- **list_posts**: 抓取板块/板面/合集列表页的帖子，获取热门讨论话题
- **get_board_structure**: 获取NGA论坛板块分类结构，了解整体布局
- **get_board_links**: 根据板块名称或类别名称返回板块信息，支持模糊匹配

自动处理多页帖子、跳过中间页面、智能去重排序，确保数据完整性。

## 环境要求

- Python 3.8+
- pip 包管理器

## 使用方法

1. clone 本项目

```bash
git clone <repository-url>
cd nga-mcp-server
```

2. 使用 pip 安装依赖

```bash
pip install -r requirements.txt
playwright install chromium
```

3. 生成或更新板块索引（首次使用）

```bash
python boards_index_tool.py
```

4. 在任意 mcp client 中配置本 Server

```json
{
  "mcpServers": {
    "nga": {
      "command": "python",
      "args": ["/your-project-path/nga-mcp-server/nga_mcp_server.py"]
    }
  }
}
```

5. 在 client 中使用

## 快速测试

使用 MCP Inspector 进行测试：

```bash
# 安装 MCP Inspector
npm install -g @modelcontextprotocol/inspector

# 启动测试
mcp-inspector -- python nga_mcp_server.py
```

## 使用示例

### 获取板块结构
```json
{}
```

### 查找板块
```json
{
  "name": "魔兽世界",
  "topk": 3
}
```

### 获取帖子列表
```json
{
  "url": "https://bbs.nga.cn/thread.php?fid=178",
  "topk": 20
}
```

### 爬取帖子内容
```json
{
  "url": "https://bbs.nga.cn/read.php?tid=12345678",
  "topk": 50
}
```

## 配置Cookies

如果需要访问需要登录的板块，需要配置用户cookies：

```bash
python cookies_tool.py
```

按照提示在浏览器中登录，然后回到终端按回车即可自动保存cookies。

## 环境变量

- `HEADLESS`: 是否无头模式，默认为 `true`，可设为 `false` 观察浏览器行为
- `NGA_INDEX_PATH`: 板块索引文件路径，默认为 `boards_index.json`
- `NGA_COOKIES_PATH`: Cookies文件路径，默认为 `nga_cookies.json`

## 注意事项

- **板块索引**: 首次使用前需运行 `boards_index_tool.py` 生成板块索引
- **Cookies**: 访问需要登录的板块需要有效的用户cookies
- **网络环境**: 确保网络连接稳定，大型帖子可能需要较长处理时间
- **请求频率**: 已添加请求延迟，避免触发反爬机制

## 故障排除

1. **索引文件不存在**: 板块查询失败
   - 解决方案：运行 `python boards_index_tool.py` 生成索引

2. **访问权限不足**: 某些板块需要登录
   - 解决方案：运行 `python cookies_tool.py` 保存登录状态

3. **Playwright错误**: 浏览器启动失败
   - 解决方案：运行 `playwright install chromium` 安装浏览器

4. **帖子抓取超时**: 大型帖子处理时间过长
   - 解决方案：设置环境变量调整超时时间或减少topk数量

## 项目结构

```
nga-mcp-server/
├── nga_mcp_server.py         # MCP服务器主文件
├── nga_client.py             # NGA爬虫客户端
├── boards_index_tool.py      # 板块索引构建工具
├── cookies_tool.py           # Cookies管理工具
├── boards_index.json        # 板块索引文件
├── nga_cookies.json          # 用户cookies配置
├── requirements.txt          # 依赖包
└── README.md                 # 项目说明
```