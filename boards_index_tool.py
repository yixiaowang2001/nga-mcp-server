#!/usr/bin/env python3
"""NGA 板块索引构建与查询工具"""

import asyncio

from nga_client import NGAClient

# 配置
INDEX_PATH = "boards_index.json"
COOKIES_PATH = "nga_cookies.json"
DO_BUILD_INDEX = True
DO_QUERY = False
QUERY_NAME = "炉石"

# 性能参数
CONCURRENCY = 8
MAX_BOARDS = None

# 离线模式（可选）
USE_LOCAL_HTML = False
HTML_PATH = "nga_home.html"
BASE_URL = "https://bbs.nga.cn/"


async def main():
    client = NGAClient(cookies_path=COOKIES_PATH)

    if DO_BUILD_INDEX:
        if USE_LOCAL_HTML:
            try:
                with open(HTML_PATH, "r", encoding="utf-8") as f:
                    html = f.read()
                result = await client.build_boards_index_from_html(html, save_path=INDEX_PATH, base_url=BASE_URL)
            except FileNotFoundError:
                print(f"未找到 HTML 文件: {HTML_PATH}，回退为在线抓取")
                result = await client.build_boards_index(save_path=INDEX_PATH)
        else:
            # 深度抓取：先构建浅层，再抓取子级
            shallow = await client.build_boards_index(save_path=INDEX_PATH, max_boards=MAX_BOARDS)
            boards = shallow.get("boards") or []
            client.CONCURRENCY = CONCURRENCY
            result = await client.build_boards_index_with_boards(boards, save_path=INDEX_PATH)

        # 统计结果
        total = len(result.get("boards") or [])
        categories = {}
        for board in result.get("boards") or []:
            cat = (board.get("category_l1") or "").strip() or "(未分组)"
            categories[cat] = categories.get(cat, 0) + 1

        print(f"索引已生成: {INDEX_PATH}，板块数: {total}")
        print("分类统计：")
        for cat, count in sorted(categories.items(), key=lambda x: (-x[1], x[0])):
            print(f"  - {cat}: {count}")

    if DO_QUERY:
        result = client.query_boards(QUERY_NAME, index_path=INDEX_PATH, topk=3)
        if not result.get("success"):
            print(f"查询失败: {result.get('error')}")
            return

        print(f"查询: {QUERY_NAME}，命中 {result.get('topk')} 个")
        for i, item in enumerate(result.get("results") or [], 1):
            name = item.get("name", "")
            url = item.get("url", "")
            fid = item.get("fid", "")
            forums = item.get("forums") or []
            collections = item.get("collections") or []

            print(f"[{i}] {name} (fid={fid})")
            print(f"    URL: {url}")
            if forums:
                print(f"    板面: {', '.join(f.get('name', '') for f in forums[:5])}")
            if collections:
                print(f"    合集: {', '.join(c.get('name', '') for c in collections[:5])}")


if __name__ == "__main__":
    asyncio.run(main())
