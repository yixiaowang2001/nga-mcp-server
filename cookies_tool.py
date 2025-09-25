#!/usr/bin/env python3
"""NGA Cookies 保存工具"""

import json
from pathlib import Path

from playwright.sync_api import sync_playwright

SITE_URL = "https://bbs.nga.cn/"
SITE_DOMAIN = ".nga.178.com"
COOKIES_PATH = Path(__file__).parent / "nga_cookies.json"


def _clean_cookies(raw):
    """清理和标准化cookies"""
    cleaned, seen = [], set()
    for c in raw or []:
        if not isinstance(c, dict):
            continue
        name = str(c.get("name", "")).strip()
        value = str(c.get("value", ""))
        if not name or value == "":
            continue

        key = (c.get("name"), c.get("domain"), c.get("path", "/"))
        if key in seen:
            continue
        seen.add(key)

        c = c.copy()
        # 标准化expires字段
        if "expires" in c:
            try:
                c["expires"] = int(c["expires"]) if c["expires"] and c["expires"] > 0 else None
                if c["expires"] is None:
                    c.pop("expires", None)
            except Exception:
                c.pop("expires", None)

        # 修复安全设置
        if c.get("sameSite") == "None" and not c.get("secure", False):
            c["secure"] = True
        if not c.get("path"):
            c["path"] = "/"
        if not c.get("domain"):
            c["domain"] = SITE_DOMAIN

        cleaned.append(c)
    return cleaned


def save_nga_cookies():
    """启动浏览器并保存NGA登录cookies"""
    print("正在启动浏览器...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        try:
            print(f"打开 NGA: {SITE_URL}")
            page.goto(SITE_URL, timeout=120000)
            page.wait_for_load_state('networkidle')
            print("请在浏览器中完成登录，完成后回到终端按回车...")
            input()

            cookies = _clean_cookies(context.cookies())
            with open(COOKIES_PATH, "w", encoding="utf-8") as f:
                json.dump(cookies, f, indent=2, ensure_ascii=False)
            print(f"NGA cookies 已保存: {COOKIES_PATH}")
        except Exception as e:
            print(f"保存 cookies 失败: {e}")
        finally:
            browser.close()


if __name__ == "__main__":
    print("=== NGA Cookies 保存工具 ===")
    save_nga_cookies()
