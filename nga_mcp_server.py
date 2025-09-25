#!/usr/bin/env python3
"""
NGA MCP 服务器

这是一个用于NGA论坛的MCP（Model Context Protocol）服务器，提供以下功能：
- 爬取NGA论坛帖子内容和评论
- 获取论坛板块列表和分类结构
- 查询特定板块信息
- 浏览论坛帖子列表

支持的工具：
1. crawl_post: 爬取指定帖子的内容和评论
2. list_posts: 获取板块/板面/合集的帖子列表
3. get_board_structure: 获取论坛板块分类结构
4. get_board_links: 根据名称查询板块信息
"""

import logging
import os
from typing import Any, Dict

from mcp.server.fastmcp import FastMCP

from nga_client import NGAClient


# 全局配置
def _get_script_dir():
    """获取脚本所在目录"""
    return os.path.dirname(os.path.abspath(__file__))


SCRIPT_DIR = _get_script_dir()
DEFAULT_INDEX_PATH = os.environ.get("NGA_INDEX_PATH", os.path.join(SCRIPT_DIR, "boards_index.json"))
DEFAULT_COOKIES_PATH = os.environ.get("NGA_COOKIES_PATH", os.path.join(SCRIPT_DIR, "nga_cookies.json"))

mcp = FastMCP("NGA MCP Server")
logger = logging.getLogger("nga_mcp")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


@mcp.tool()
async def crawl_post(url: str, topk: int = 50) -> Dict[str, Any]:
    """
    爬取 NGA 帖子内容
    
    Args:
        url: 帖子链接（形如 https://bbs.nga.cn/read.php?tid=XXXX）
        topk: 返回评论条数上限，默认50条，0表示获取所有评论
        
    Returns:
        包含帖子标题、正文和评论列表的字典数据
    """
    try:
        logger.info(f"crawl_post: url='{url}', topk={topk}")
        client = NGAClient(cookies_path=DEFAULT_COOKIES_PATH)
        return await client.crawl_post(post_url=url, max_comments=max(0, int(topk)))
    except Exception as e:
        logger.exception("crawl_post 失败")
        return {"success": False, "error": str(e)}


@mcp.tool()
async def list_posts(url: str, topk: int = 30) -> Dict[str, Any]:
    """
    抓取板块/板面/合集列表页的帖子
    
    Args:
        url: 列表页链接（如 https://bbs.nga.cn/thread.php?fid=459 或 stid=43954481）
        topk: 主题条数上限，默认30
        
    Returns:
        包含帖子标题、回复数、发布时间等信息的字典数据
    """
    try:
        logger.info(f"list_posts: url='{url}', topk={topk}")
        client = NGAClient(cookies_path=DEFAULT_COOKIES_PATH)
        return await client.list_posts(list_url=url, topk=max(0, int(topk)))
    except Exception as e:
        logger.exception("list_posts 失败")
        return {"success": False, "error": str(e)}


@mcp.tool()
async def get_board_structure() -> Dict[str, Any]:
    """
    获取NGA论坛板块分类结构
    
    建议首先调用此方法了解论坛整体结构，然后再使用get_board_links查询具体板块信息。
        
    Returns:
        包含按类别组织的板块名称列表的字典数据
    """
    try:
        logger.info(f"get_board_structure: index='{DEFAULT_INDEX_PATH}'")
        client = NGAClient()
        return client.get_board_structure()
    except Exception as e:
        logger.exception("get_board_structure 失败")
        return {"success": False, "error": str(e)}


@mcp.tool()
async def get_board_links(name: str, topk: int = 3) -> Dict[str, Any]:
    """
    根据板块名称或类别名称返回板块信息
    
    支持两种查询模式：
    1. 板块名称查询：返回最接近的TopK板块（模糊匹配，按编辑距离排序）
    2. 类别名称查询：返回该类别下的所有板块
    
    Args:
        name: 板块名称或类别名称（支持模糊匹配）
        topk: 返回数量上限，默认3个（类别查询时返回该类别所有板块）
        
    Returns:
        包含板块详细信息（包含板面与合集）的字典数据
    """
    try:
        logger.info(f"get_board_links: name='{name}', topk={topk}")
        client = NGAClient()
        return client.query_boards(name=name, topk=max(1, int(topk)))
    except Exception as e:
        logger.exception("get_board_links 失败")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    mcp.run()
