#!/usr/bin/env python3
"""NGA 业务客户端"""

import asyncio
import json
import logging
import os
import re
import time
import urllib.parse
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any, Dict, List

logger = logging.getLogger("nga_client")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


def _env_ms(name: str, default_ms: int) -> int:
    """从环境变量获取毫秒值"""
    try:
        v = os.environ.get(name)
        return max(0, int(v)) if v else default_ms
    except Exception:
        return default_ms


def _build_page_url_template(post_url: str) -> str:
    """根据帖子链接构造分页模板"""
    parsed = urllib.parse.urlparse(post_url)
    qs = urllib.parse.parse_qs(parsed.query)

    # 优先使用 tid 构造标准分页 URL
    tid_vals = qs.get("tid") or []
    if tid_vals:
        try:
            tid = int(str(tid_vals[0]).strip())
            path = parsed.path or "/read.php"
            return urllib.parse.urlunparse((
                parsed.scheme or "https",
                parsed.netloc or "bbs.nga.cn",
                path, "", f"tid={tid}&page={{}}", ""
            ))
        except Exception:
            pass

    # 回退：直接替换 page 参数
    qs["page"] = ["{}"]
    new_query = urllib.parse.urlencode({k: v[0] for k, v in qs.items() if v})
    return urllib.parse.urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


class NGAClient:
    """NGA 内容抓取客户端"""

    CONCURRENCY = 5
    MAX_CHAIN_STEPS = 30
    EMPTY_PAGE_STOP = 2

    def __init__(self, cookies_path: str | None = None):
        self.cookies_path = cookies_path

    def _clean_text(self, text: str) -> str:
        """清理文本内容"""
        if not text:
            return ""
        t = text.replace("\u200b", "").strip()
        t = re.sub(r".*?\(undefined\)", "", t)
        t = re.sub(r"显示图片\([^)]*\)", "", t)
        t = " ".join(t.split())
        return "\n".join(line.strip() for line in t.splitlines() if line.strip())

    def _clean_title(self, title: str) -> str:
        """清理标题"""
        if not title:
            return ""
        t = title.strip()
        for suffix in ("NGA玩家社区", "艾泽拉斯国家地理论坛"):
            if t.endswith(suffix):
                t = t[:-len(suffix)].rstrip(" - ")
        return t

    def _infer_cookies_path(self, override_path: str | None = None) -> str | None:
        """推断cookies文件路径"""
        try:
            path = override_path or self.cookies_path or os.environ.get("NGA_COOKIES_PATH") or "nga_cookies.json"
            return path.strip() if path and os.path.exists(path.strip()) else None
        except Exception:
            return None

    def _load_cookies(self, cookies_path: str) -> List[Dict[str, Any]]:
        """加载cookies文件"""
        try:
            with open(cookies_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            cookies = []
            for c in data or []:
                if not isinstance(c, dict):
                    continue
                name = str(c.get("name", "")).strip()
                value = str(c.get("value", ""))
                if not name or value == "":
                    continue

                item = {k: v for k, v in c.items()
                        if k in ("name", "value", "url", "domain", "path", "expires", "httpOnly", "secure", "sameSite")}

                # 规范化字段
                if "path" not in item or not item.get("path"):
                    item["path"] = "/"
                if item.get("sameSite") == "None" and not item.get("secure", False):
                    item["secure"] = True
                if "expires" in item and isinstance(item["expires"], float):
                    try:
                        item["expires"] = int(item["expires"])
                    except Exception:
                        item.pop("expires", None)

                cookies.append(item)
            return cookies
        except Exception:
            logger.exception(f"加载 cookies 失败: {cookies_path}")
            return []

    async def _new_context(self, browser, default_timeout_ms: int, user_agent: str | None = None,
                           cookies_path: str | None = None):
        """创建浏览器上下文"""
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=user_agent
        )
        context.set_default_timeout(default_timeout_ms)

        cp = self._infer_cookies_path(cookies_path)
        if cp:
            cookies = self._load_cookies(cp)
            if cookies:
                try:
                    await context.add_cookies(cookies)
                    logger.info(f"已加载 cookies: {cp} (共 {len(cookies)} 条)")
                except Exception:
                    logger.exception(f"注入 cookies 失败: {cp}")
        return context

    async def _extract_posts(self, page) -> List[Dict[str, Any]]:
        """提取帖子内容"""
        try:
            data = await page.evaluate('''
                () => {
                    const result = [];
                    const rows = Array.from(document.querySelectorAll("tr.postrow"));
                    const pid2floor = {};
                    const rowInfo = new Map();

                    for (const tr of rows) {
                        const tdLeft = tr.querySelector('td.c1');
                        const tdRight = tr.querySelector('td.c2[id^="postcontainer"]');
                        if (!tdRight) continue;

                        let pid = null;
                        const pidA = tdRight.querySelector("a[id^='pid'][id$='Anchor']");
                        if (pidA) {
                            const mPid = (pidA.getAttribute('id') || '').match(/^pid(\\d+)Anchor$/);
                            if (mPid) pid = mPid[1];
                        }

                        let floor = null;
                        const nameAnchor = tdRight.querySelector('a[name^="l"]');
                        if (nameAnchor) {
                            const mL = (nameAnchor.getAttribute('name') || '').match(/^l(\\d+)$/);
                            if (mL) floor = parseInt(mL[1], 10);
                        }
                        if (floor === null && tdLeft) {
                            const leftBtn = tdLeft.querySelector('a.small_colored_text_btn');
                            if (leftBtn) {
                                const txt = (leftBtn.textContent || '').trim();
                                const mHash = txt.match(/#(\\d+)/);
                                if (mHash) floor = parseInt(mHash[1], 10);
                            }
                        }
                        if (floor === null && tdRight) {
                            const id = tdRight.getAttribute('id') || '';
                            const m = id.match(/postcontainer(\\d+)/);
                            if (m) floor = parseInt(m[1], 10);
                        }

                        rowInfo.set(tr, { pid, floor });
                        if (pid && floor !== null) pid2floor[pid] = floor;
                    }

                    for (const tr of rows) {
                        const tdRight = tr.querySelector('td.c2[id^="postcontainer"]');
                        if (!tdRight) continue;
                        const info = rowInfo.get(tr) || {};
                        const pid = info.pid || null;
                        const floor = (info.floor !== undefined) ? info.floor : null;

                        let time = '';
                        if (floor !== null) {
                            const t1 = tdRight.querySelector(`#postdate${floor}`);
                            if (t1) time = (t1.textContent || '').trim();
                        }
                        if (!time) {
                            const t2 = tdRight.querySelector('.postInfo .postdatec');
                            if (t2) time = (t2.textContent || '').trim();
                        }

                        let contentText = '';
                        let quote_floor = null;
                        let contentEl = null;
                        if (floor !== null) {
                            contentEl = tdRight.querySelector(`#postcontent${floor}`) || 
                                       tdRight.querySelector(`#postcontentandsubject${floor}`);
                        }
                        if (!contentEl) {
                            contentEl = tdRight.querySelector("p[id^='postcontent'], span[id^='postcontent']");
                        }
                        if (contentEl) {
                            const clone = contentEl.cloneNode(true);
                            clone.querySelectorAll('div.quote').forEach(q => q.remove());
                            contentText = (clone.textContent || '').trim();

                            const q = contentEl.querySelector('div.quote');
                            if (q) {
                                const link = q.querySelector("a[href*='#pid']") || q.querySelector('a.block_txt');
                                if (link) {
                                    const href = link.getAttribute('href') || '';
                                    const m3 = href.match(/#pid(\\d+)Anchor/);
                                    if (m3 && pid2floor[m3[1]] !== undefined) quote_floor = pid2floor[m3[1]];
                                }
                            }
                        }

                        let likes = 0;
                        const likeEl = tdRight.querySelector('.goodbad .recommendvalue');
                        if (likeEl) {
                            const v = (likeEl.textContent || '').trim();
                            const n = parseInt(v, 10);
                            if (!Number.isNaN(n)) likes = n;
                        }

                        result.push({ pid, floor, time, content: contentText, quote: quote_floor, likes });
                    }
                    return result;
                }
            ''')

            for item in data or []:
                item["content"] = self._clean_text(item.get("content", ""))
            return data or []
        except Exception:
            logger.exception("extract_posts failed")
            return []

    async def _extract_posts_fallback(self, page) -> List[Dict[str, Any]]:
        """兜底解析方法"""
        try:
            data = await page.evaluate('''
                () => {
                    const result = [];
                    const contents = Array.from(document.querySelectorAll("p[id^='postcontent'], span[id^='postcontent']"));
                    for (const el of contents) {
                        const id = el.getAttribute('id') || '';
                        const m = id.match(/^postcontent(?:andsubject)?(\\d+)$/);
                        if (!m) continue;
                        const floor = parseInt(m[1], 10);
                        if (Number.isNaN(floor)) continue;

                        let tdRight = el.closest('td.c2[id^="postcontainer"]');
                        let time = '';
                        if (tdRight) {
                            const t1 = tdRight.querySelector(`#postdate${floor}`);
                            if (t1) time = (t1.textContent || '').trim();
                            if (!time) {
                                const t2 = tdRight.querySelector('.postInfo .postdatec');
                                if (t2) time = (t2.textContent || '').trim();
                            }
                        }

                        let contentText = '';
                        const clone = el.cloneNode(true);
                        clone.querySelectorAll('div.quote').forEach(q => q.remove());
                        contentText = (clone.textContent || '').trim();

                        let likes = 0;
                        if (tdRight) {
                            const likeEl = tdRight.querySelector('.goodbad .recommendvalue');
                            if (likeEl) {
                                const v = (likeEl.textContent || '').trim();
                                const n = parseInt(v, 10);
                                if (!Number.isNaN(n)) likes = n;
                            }
                        }

                        result.push({ pid: null, floor, time, content: contentText, quote: null, likes });
                    }
                    return result;
                }
            ''')

            for item in data or []:
                item["content"] = self._clean_text(item.get("content", ""))
            return data or []
        except Exception:
            return []

    async def _extract_posts_with_fallback(self, page) -> List[Dict[str, Any]]:
        """带兜底的帖子提取"""
        primary = await self._extract_posts(page)
        if len(primary) >= 5:
            return primary
        fallback = await self._extract_posts_fallback(page)
        return fallback if len(fallback) > len(primary) else primary

    async def _maybe_bypass_interstitial(self, page) -> None:
        """绕过中间页"""
        try:
            html = await page.content()
            if "访客不能直接访问" in html:
                try:
                    await page.click("a:has-text('如不能自动跳转')", timeout=3000)
                    await page.wait_for_load_state("networkidle")
                except Exception:
                    pass

            # 尝试点击跳过按钮
            for sel in ["a:has-text('跳过')", "a:has-text('进入')", "a:has-text('继续')", "button:has-text('跳过')"]:
                try:
                    btn = await page.query_selector(sel)
                    if btn:
                        await btn.click()
                        await page.wait_for_load_state("networkidle")
                        break
                except Exception:
                    pass
        except Exception:
            pass

    async def _extract_first_page(self, page) -> Dict[str, Any]:
        """提取首页信息"""
        title = ""
        try:
            tloc = page.locator("h3#postsubject0")
            if await tloc.count() > 0:
                title = (await tloc.inner_text()).strip()
        except Exception:
            pass

        if not title:
            try:
                html_title = await page.title()
                parts = html_title.split(" - ")
                if len(parts) > 2 and "NGA玩家社区" in parts[-1]:
                    title = " - ".join(parts[:-2]).strip()
                elif len(parts) > 1 and ("NGA玩家社区" in parts[-1] or "艾泽拉斯国家地理论坛" in parts[-1]):
                    title = parts[0].strip()
                else:
                    title = html_title.strip()
            except Exception:
                title = ""

        title = self._clean_title(title)

        description = ""
        try:
            iloc = page.locator("p#postcontent0")
            if await iloc.count() > 0:
                description = (await iloc.inner_text()).strip()
        except Exception:
            pass

        posts = await self._extract_posts_with_fallback(page)

        post_time = ""
        for p in posts:
            if p.get("floor") == 0:
                post_time = p.get("time", "")
                break

        return {
            "title": title,
            "description": description,
            "time": post_time,
            "posts": posts,
        }

    async def _infer_total_pages(self, page) -> int:
        """推断总页数"""
        try:
            # 查找"最后页/末页/尾页"按钮
            sel_last = "a[title*='最后页'], a[title*='末页'], a[title*='尾页']"
            locator = page.locator(sel_last)
            if await locator.count() > 0:
                href = await locator.first.get_attribute("href")
                if href:
                    href = href.replace("&amp;", "&")
                    m = re.search(r"page=(\d+)", href)
                    if m:
                        return max(1, int(m.group(1)))

            # 回退：从所有分页链接中解析最大页码
            anchors = await page.eval_on_selector_all(
                "a.uitxt1, a[href*='page=']",
                "els => els.map(e => e.getAttribute('href') || '')"
            )
            max_page = 1
            for h in anchors or []:
                if not h:
                    continue
                try:
                    href = h.replace("&amp;", "&")
                    m = re.search(r"page=(\d+)", href)
                    if m:
                        max_page = max(max_page, int(m.group(1)))
                except Exception:
                    continue
            return max_page
        except Exception:
            return 1

    async def _find_next_url(self, page) -> str:
        """查找下一页链接"""
        try:
            cur_url = page.url

            sel = "a[title*='加载下一页'], a[title*='下一页'], a[title*='后页']"
            el = await page.query_selector(sel)
            if el:
                href = await el.get_attribute("href")
                if href:
                    return urllib.parse.urljoin(cur_url, href)

            anchors = await page.eval_on_selector_all(
                "a.uitxt1",
                "els => els.map(e => e.getAttribute('href') || '')"
            )
            try:
                p = urllib.parse.urlparse(cur_url)
                qs = urllib.parse.parse_qs(p.query)
                cur_page = int((qs.get('page') or ['1'])[0])
            except Exception:
                cur_page = 1

            target_page = cur_page + 1
            for h in anchors or []:
                if not h:
                    continue
                full = urllib.parse.urljoin(cur_url, h)
                try:
                    pp = urllib.parse.urlparse(full)
                    qq = urllib.parse.parse_qs(pp.query)
                    if int((qq.get('page') or ['0'])[0]) == target_page:
                        return full
                except Exception:
                    continue
            return ""
        except Exception:
            return ""

    async def _goto_and_extract(self, context, url: str, default_timeout_ms: int, goto_timeout_ms: int) -> List[
        Dict[str, Any]]:
        """打开页面并提取帖子"""
        page = await context.new_page()
        try:
            page.set_default_timeout(default_timeout_ms)
            await page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
            await self._maybe_bypass_interstitial(page)
            return await self._extract_posts_with_fallback(page)
        except Exception:
            return []
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _fetch_pages_range(self, context, template: str, start_page: int, end_page_inclusive: int,
                                 default_timeout_ms: int, goto_timeout_ms: int, out_of_budget,
                                 max_comments: int, posts: List[Dict[str, Any]]) -> None:
        """按页码范围抓取"""
        consecutive_empty = 0
        for page_num in range(max(2, start_page), max(1, end_page_inclusive) + 1):
            if out_of_budget():
                break
            url = template.format(page_num)
            arr = await self._goto_and_extract(context, url, default_timeout_ms, goto_timeout_ms)
            if not arr:
                consecutive_empty += 1
                if consecutive_empty >= self.EMPTY_PAGE_STOP:
                    break
                continue
            consecutive_empty = 0
            posts.extend(arr)
            if max_comments and max_comments > 0 and len(posts) >= max_comments:
                break

    async def _chain_via_next(self, context, start_url: str, default_timeout_ms: int, goto_timeout_ms: int,
                              out_of_budget, max_comments: int, posts: List[Dict[str, Any]]) -> None:
        """沿"下一页"链接抓取"""
        next_url = start_url
        visited = set()
        steps = 0
        while next_url and steps < self.MAX_CHAIN_STEPS:
            if next_url in visited or out_of_budget():
                break
            visited.add(next_url)
            steps += 1
            arr = await self._goto_and_extract(context, next_url, default_timeout_ms, goto_timeout_ms)
            posts.extend(arr)
            if max_comments and max_comments > 0 and len(posts) >= max_comments:
                break

            # 查询下一页
            try:
                page = await context.new_page()
                page.set_default_timeout(default_timeout_ms)
                await page.goto(next_url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
                await self._maybe_bypass_interstitial(page)
                next_url = await self._find_next_url(page)
            except Exception:
                break
            finally:
                try:
                    await page.close()
                except Exception:
                    pass

    async def crawl_post(self, post_url: str, max_comments: int) -> Dict[str, Any]:
        """爬取帖子内容"""
        from playwright.async_api import async_playwright

        headless = os.environ.get("HEADLESS", "true").lower() in ("1", "true", "yes")
        overall_budget_ms = _env_ms("CRAWL_TIME_BUDGET_MS", 120_000)
        default_timeout_ms = _env_ms("PLAYWRIGHT_DEFAULT_TIMEOUT_MS", 120_000)
        goto_timeout_ms = _env_ms("PLAYWRIGHT_GOTO_TIMEOUT_MS", 90_000)
        crawl_start = time.monotonic()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await self._new_context(browser, default_timeout_ms)

            # 首页
            first_page = await context.new_page()
            first_page.set_default_timeout(default_timeout_ms)
            template = _build_page_url_template(post_url)
            first_url = template.format(1)
            await first_page.goto(first_url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
            await self._maybe_bypass_interstitial(first_page)

            first = await self._extract_first_page(first_page)
            total_pages = await self._infer_total_pages(first_page)
            next_url = await self._find_next_url(first_page)

            try:
                await first_page.close()
            except Exception:
                pass

            posts: List[Dict[str, Any]] = list(first.get("posts") or [])

            # 其他页并发抓取
            sem = asyncio.Semaphore(self.CONCURRENCY)

            async def fetch_page(page_num: int) -> List[Dict[str, Any]]:
                async with sem:
                    if overall_budget_ms > 0 and (time.monotonic() - crawl_start) * 1000 >= overall_budget_ms:
                        return []
                    url = template.format(page_num)
                    return await self._goto_and_extract(context, url, default_timeout_ms, goto_timeout_ms)

            if total_pages > 1:
                max_pages_to_fetch = total_pages
                if max_comments and max_comments > 0:
                    first_count = max(0, len([p for p in posts if isinstance(p.get("floor"), int) and p["floor"] != 0]))
                    approx_pp = max(1, first_count)
                    need_more = max_comments - len(posts)
                    if need_more > 0:
                        extra_pages = (need_more + approx_pp - 1) // approx_pp
                        max_pages_to_fetch = min(total_pages, 1 + extra_pages)

                tasks = [fetch_page(i) for i in range(2, max_pages_to_fetch + 1)]
                if tasks:
                    other_pages = await asyncio.gather(*tasks)
                    for arr in other_pages:
                        posts.extend(arr)

                # 继续扩展抓取
                if (not max_comments) or len(posts) < max_comments:
                    def out_of_budget():
                        return overall_budget_ms > 0 and (time.monotonic() - crawl_start) * 1000 >= overall_budget_ms

                    await self._fetch_pages_range(
                        context, template, start_page=max_pages_to_fetch + 1,
                        end_page_inclusive=max_pages_to_fetch + 30,
                        default_timeout_ms=default_timeout_ms, goto_timeout_ms=goto_timeout_ms,
                        out_of_budget=out_of_budget, max_comments=max_comments, posts=posts
                    )

                # 再次兜底扫描
                if total_pages > 1 and ((not max_comments) or len(posts) < max_comments):
                    await self._fetch_pages_range(
                        context, template, start_page=2, end_page_inclusive=total_pages,
                        default_timeout_ms=default_timeout_ms, goto_timeout_ms=goto_timeout_ms,
                        out_of_budget=out_of_budget, max_comments=max_comments, posts=posts
                    )
            else:
                # 兜底：按"下一页"链接抓取
                def out_of_budget():
                    return overall_budget_ms > 0 and (time.monotonic() - crawl_start) * 1000 >= overall_budget_ms

                await self._chain_via_next(
                    context, start_url=next_url, default_timeout_ms=default_timeout_ms,
                    goto_timeout_ms=goto_timeout_ms, out_of_budget=out_of_budget,
                    max_comments=max_comments, posts=posts
                )

                if (not max_comments or len(posts) < max_comments):
                    await self._fetch_pages_range(
                        context, template, start_page=2, end_page_inclusive=self.MAX_CHAIN_STEPS,
                        default_timeout_ms=default_timeout_ms, goto_timeout_ms=goto_timeout_ms,
                        out_of_budget=out_of_budget, max_comments=max_comments, posts=posts
                    )

            # 关闭资源
            try:
                await context.close()
                await browser.close()
            except Exception:
                pass

            # 去重与排序
            try:
                pid_seen = set()
                dedup: List[Dict[str, Any]] = []
                for p in posts:
                    pid = p.get("pid")
                    if pid and pid in pid_seen:
                        continue
                    if pid:
                        pid_seen.add(pid)
                    dedup.append(p)
                posts = sorted(dedup, key=lambda x: (x.get("floor") if isinstance(x.get("floor"), int) else 10 ** 9))
            except Exception:
                pass

            if max_comments and max_comments > 0:
                posts = posts[:max_comments]

            return {
                "success": True,
                "title": first.get("title", ""),
                "description": first.get("description", ""),
                "time": first.get("time", ""),
                "total_comments": len(posts),
                "comments": posts,
                "source": post_url,
            }

    async def list_posts(self, list_url: str, topk: int) -> Dict[str, Any]:
        """抓取板块/板面/合集列表页的帖子 TopK"""
        from playwright.async_api import async_playwright

        headless = os.environ.get("HEADLESS", "true").lower() in ("1", "true", "yes")
        default_timeout_ms = _env_ms("PLAYWRIGHT_DEFAULT_TIMEOUT_MS", 120_000)
        goto_timeout_ms = _env_ms("PLAYWRIGHT_GOTO_TIMEOUT_MS", 90_000)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await self._new_context(browser, default_timeout_ms)
            page = await context.new_page()
            page.set_default_timeout(default_timeout_ms)

            try:
                await page.goto(list_url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
                await self._maybe_bypass_interstitial(page)

                data = await page.evaluate('''
                    () => {
                        const rows = Array.from(document.querySelectorAll('table#topicrows tr.topicrow'));
                        const topics = [];
                        for (const tr of rows) {
                            const td2 = tr.querySelector('td.c2');
                            if (!td2) continue;

                            let aTopic = td2.querySelector('a.topic');
                            let chosen = null;
                            if (aTopic) {
                                const href = (aTopic.getAttribute('href') || '');
                                const full = href ? new URL(href, location.href).href : '';
                                if (/read\\.php\\?tid=\\d+/.test(full)) {
                                    chosen = aTopic;
                                }
                            }
                            
                            if (!chosen) {
                                const candidates = Array.from(td2.querySelectorAll('a[href*="read.php?tid="]'));
                                for (const a of candidates) {
                                    const txt = (a.innerText || '').trim();
                                    const cls = a.getAttribute('class') || '';
                                    if (txt === '版面' || txt === '合集') continue;
                                    if (/\\bvertmod\\b/.test(cls)) continue;
                                    chosen = a; break;
                                }
                            }
                            if (!chosen) continue;

                            const href = chosen.getAttribute('href') || '';
                            const url = href ? new URL(href, location.href).href : '';
                            if (!/read\\.php\\?tid=\\d+/.test(url)) continue;

                            const rawTitle = (chosen.innerText || '').trim();
                            if (rawTitle === '版面' || rawTitle === '合集') continue;

                            let replies = 0;
                            const td1 = tr.querySelector('td.c1');
                            if (td1) {
                                const ra = td1.querySelector('a.replies');
                                if (ra) {
                                    const t = (ra.textContent || '').trim();
                                    const n = parseInt(t, 10);
                                    if (!Number.isNaN(n)) replies = n;
                                }
                            }

                            let postDate = '';
                            const td3 = tr.querySelector('td.c3 .postdate');
                            if (td3) {
                                postDate = (td3.getAttribute('title') || td3.textContent || '').trim();
                            }

                            let lastReplyTime = '';
                            const td4 = tr.querySelector('td.c4 .replydate');
                            if (td4) {
                                lastReplyTime = (td4.getAttribute('title') || td4.textContent || '').trim();
                            }

                            topics.push({ title: rawTitle, replies, post_date: postDate, last_reply_time: lastReplyTime, url });
                        }
                        return topics;
                    }
                ''')

                posts: List[Dict[str, Any]] = []
                for t in data or []:
                    title = self._clean_title(str(t.get("title", "")))
                    posts.append({
                        "title": title,
                        "replies": int(t.get("replies") or 0),
                        "post_date": str(t.get("post_date") or ""),
                        "last_reply_time": str(t.get("last_reply_time") or ""),
                        "url": str(t.get("url") or ""),
                    })

                if topk and topk > 0:
                    posts = posts[:topk]

                return {
                    "success": True,
                    "total": len(posts),
                    "posts": posts,
                    "source": list_url,
                }
            except Exception:
                logger.exception("list_posts 解析失败")
                return {"success": False, "error": "failed_to_parse", "source": list_url}
            finally:
                try:
                    await context.close()
                    await browser.close()
                except Exception:
                    pass

    async def _load_home_sections(self, context) -> List[Dict[str, Any]]:
        """抓取论坛首页的所有板块入口"""
        page = await context.new_page()
        try:
            await page.goto("https://bbs.nga.cn/", wait_until="networkidle")
            await self._maybe_bypass_interstitial(page)
            try:
                await page.wait_for_selector("a[href*='thread.php?fid=']", timeout=5000)
            except Exception:
                pass

            data = await page.evaluate('''
                () => {
                    function normalizeTitle(t) {
                        t = (t || '').trim();
                        return t.replace(/^:+\\s*/, '').replace(/\\s*:+$/, '').trim();
                    }

                    const items = [];
                    const seen = new Set();

                    const blocks = Array.from(document.querySelectorAll('.indexblock'));
                    for (const block of blocks) {
                        const cate1El = block.querySelector('h2.catetitle');
                        if (!cate1El) continue;
                        const cate1Raw = (cate1El ? (cate1El.textContent || cate1El.innerText) : '') || '';
                        const cate1 = normalizeTitle(cate1Raw);
                        if (!cate1 || /^(undefined|收藏版面)$/i.test(cate1)) continue;

                        let currentH3 = '';
                        const walker = document.createNodeIterator(block, NodeFilter.SHOW_ELEMENT);
                        let node;
                        while ((node = walker.nextNode())) {
                            const el = node;
                            if (el.matches && el.matches('h3.catetitle')) {
                                currentH3 = normalizeTitle((el.textContent || el.innerText || ''));
                                continue;
                            }
                            if (!(el.matches && el.matches("a[href*='thread.php?fid=']"))) continue;

                            const href = el.getAttribute('href') || '';
                            const url = href ? new URL(href, location.href).href : '';
                            const m = url.match(/\\bfid=(-?\\d+)\\b/);
                            const name = (el.innerText || '').trim();
                            if (!url || !m || !name) continue;

                            let desc = '';
                            try {
                                const box = el.closest('div.b') || el.closest('div.a') || el.parentElement;
                                const p = box ? box.querySelector('p') : null;
                                if (p) desc = (p.innerText || p.textContent || '').trim();
                            } catch (e) {}

                            const fid = m[1];
                            const key = `${fid}|${name}`;
                            if (seen.has(key)) continue;
                            seen.add(key);
                            if (name === '版面' || name === '合集') continue;

                            items.push({ name, url, type: 'board', fid, desc, cate1, cate2: currentH3 });
                        }
                    }

                    return items;
                }
            ''')

            items = [
                {
                    "name": str(it.get("name", "")),
                    "url": str(it.get("url", "")),
                    "type": "board",
                    "fid": str(it.get("fid", "")),
                    "description": str(it.get("desc", "")),
                    "category_l1": str(it.get("cate1", "")),
                    "category_l2": str(it.get("cate2", "")),
                }
                for it in (data or [])
                if it and it.get("url")
            ]

            if items:
                return items

            # 回退至站点地图
            try:
                return await self._load_site_map_sections(context)
            except Exception:
                return []
        except Exception:
            logger.exception("load_home_sections failed")
            return []
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _load_site_map_sections(self, context) -> List[Dict[str, Any]]:
        """回退：抓取全部版面站点地图"""
        page = await context.new_page()
        try:
            await page.goto("https://bbs.nga.cn/forum.php", wait_until="networkidle")
            await self._maybe_bypass_interstitial(page)
            try:
                await page.wait_for_selector("a[href*='thread.php?fid=']", timeout=5000)
            except Exception:
                pass

            data = await page.evaluate('''
                () => {
                    const anchors = Array.from(document.querySelectorAll("a[href*='thread.php?fid=']"));
                    const items = [];
                    const seen = new Set();
                    for (const a of anchors) {
                        const href = a.getAttribute('href') || '';
                        const url = href ? new URL(href, location.href).href : '';
                        const m = url.match(/\\bfid=(-?\\d+)\\b/);
                        const name = (a.innerText || '').trim();
                        let desc = '';
                        try {
                            const box = a.closest('div.b') || a.closest('div.a') || a.parentElement;
                            const p = box ? box.querySelector('p') : null;
                            if (p) desc = (p.innerText || p.textContent || '').trim();
                        } catch (e) {}
                        if (!url || !m || !name) continue;
                        const fid = m[1];
                        const key = `${fid}|${name}`;
                        if (seen.has(key)) continue;
                        seen.add(key);
                        if (name === '版面' || name === '合集') continue;
                        items.push({ name, url, type: 'board', fid, desc });
                    }
                    return items;
                }
            ''')

            return [
                {
                    "name": str(it.get("name", "")),
                    "url": str(it.get("url", "")),
                    "type": "board",
                    "fid": str(it.get("fid", "")),
                    "description": str(it.get("desc", "")),
                }
                for it in (data or [])
                if it and it.get("url")
            ]
        except Exception:
            logger.exception("load_site_map_sections failed")
            return []
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _load_board_children(self, context, board_url: str) -> Dict[str, Any]:
        """抓取单个板块页的子级板面与合集链接"""
        page = await context.new_page()
        try:
            await page.goto(board_url, wait_until="networkidle")
            await self._maybe_bypass_interstitial(page)
            try:
                await page.wait_for_selector("#sub_forums, a[href*='thread.php?fid='], a[href*='thread.php?stid=']",
                                             timeout=5000)
            except Exception:
                pass

            data = await page.evaluate('''
                () => {
                    const result = { forums: [], collections: [] };
                    const seenF = new Set();
                    const seenC = new Set();
                    const box = document.querySelector('#sub_forums') || 
                               document.querySelector('div.catenew#sub_forums') || 
                               document.querySelector('div#sub_forums.catenew');
                    const anchors = Array.from((box ? box : document).querySelectorAll("a[href*='thread.php?']"));
                    for (const a of anchors) {
                        const href = a.getAttribute('href') || '';
                        const url = href ? new URL(href, location.href).href : '';
                        const name = (a.textContent || '').trim();
                        if (!url || !name) continue;
                        const mf = url.match(/\\bfid=(-?\\d+)\\b/);
                        const mc = url.match(/\\bstid=(\\d+)\\b/);
                        if (mf) {
                            const fid = String(mf[1]);
                            if (!seenF.has(fid)) {
                                seenF.add(fid);
                                result.forums.push({ name, url, fid });
                            }
                            continue;
                        }
                        if (mc) {
                            const stid = String(mc[1]);
                            if (!seenC.has(stid)) {
                                seenC.add(stid);
                                result.collections.push({ name, url, stid });
                            }
                            continue;
                        }
                    }
                    return result;
                }
            ''')

            forums = [
                {"name": str(it.get("name", "")), "url": str(it.get("url", "")), "fid": str(it.get("fid", ""))}
                for it in (data or {}).get("forums", [])
                if it and it.get("url") and it.get("fid")
            ]
            collections = [
                {"name": str(it.get("name", "")), "url": str(it.get("url", "")), "stid": str(it.get("stid", ""))}
                for it in (data or {}).get("collections", [])
                if it and it.get("url") and it.get("stid")
            ]
            return {"forums": forums, "collections": collections}
        except Exception:
            logger.exception(f"load_board_children failed: {board_url}")
            return {"forums": [], "collections": []}
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def build_boards_index(self, save_path: str = "boards_index.json", max_boards: int | None = None) -> Dict[
        str, Any]:
        """抓取首页并保存板块链接索引"""
        from playwright.async_api import async_playwright

        headless = os.environ.get("HEADLESS", "true").lower() in ("1", "true", "yes")
        default_timeout_ms = _env_ms("PLAYWRIGHT_DEFAULT_TIMEOUT_MS", 120_000)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await self._new_context(
                browser, default_timeout_ms,
                user_agent=os.environ.get("PLAYWRIGHT_USER_AGENT") or
                           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            )

            boards: List[Dict[str, Any]] = []
            try:
                home_boards = await self._load_home_sections(context)
                logger.info(f"发现首页板块 {len(home_boards)} 个")

                # 去重按 fid
                seen_fid = set()
                unique_boards: List[Dict[str, Any]] = []
                for b in home_boards:
                    fid = b.get("fid")
                    if not fid or fid in seen_fid:
                        continue
                    seen_fid.add(fid)
                    unique_boards.append(b)

                if max_boards and max_boards > 0:
                    unique_boards = unique_boards[:max_boards]

                boards = [
                    {
                        "name": b["name"],
                        "url": b["url"],
                        "fid": b["fid"],
                        "description": b.get("description", ""),
                        "category_l1": b.get("category_l1", ""),
                        "category_l2": b.get("category_l2", ""),
                        "forums": [],
                        "collections": [],
                    }
                    for b in unique_boards
                ]
            finally:
                try:
                    await context.close()
                    await browser.close()
                except Exception:
                    pass

        result = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "boards": boards,
        }

        try:
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            logger.info(f"板块索引已保存: {save_path}")
        except Exception:
            logger.exception(f"保存板块索引失败: {save_path}")
        return result

    async def build_boards_index_with_boards(self, boards: List[Dict[str, Any]],
                                             save_path: str = "boards_index.json") -> Dict[str, Any]:
        """基于已给出的板块列表，抓取其子级并保存索引"""
        from playwright.async_api import async_playwright

        headless = os.environ.get("HEADLESS", "true").lower() in ("1", "true", "yes")
        default_timeout_ms = _env_ms("PLAYWRIGHT_DEFAULT_TIMEOUT_MS", 120_000)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await self._new_context(
                browser, default_timeout_ms,
                user_agent=os.environ.get("PLAYWRIGHT_USER_AGENT") or
                           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            )

            sem = asyncio.Semaphore(self.CONCURRENCY)

            async def load_one(b):
                async with sem:
                    try:
                        children = await self._load_board_children(context, b["url"])
                    except Exception:
                        children = {"forums": [], "collections": []}
                    return {
                        "name": b.get("name", ""),
                        "url": b.get("url", ""),
                        "fid": b.get("fid", ""),
                        "description": b.get("description", ""),
                        "category_l1": b.get("category_l1", ""),
                        "category_l2": b.get("category_l2", ""),
                        "forums": children.get("forums", []),
                        "collections": children.get("collections", []),
                    }

            try:
                tasks = [load_one(b) for b in (boards or []) if b and b.get("url")]
                total = len(tasks)
                results = []

                if total == 0:
                    results = []
                else:
                    done = 0
                    bar_width = 28
                    start_ts = time.monotonic()
                    durations: list[float] = []

                    def render_bar(done_count: int, total_count: int, elapsed_s: float, eta_s: float | None) -> None:
                        filled = int(bar_width * done_count / max(1, total_count))
                        bar = "█" * filled + "-" * (bar_width - filled)

                        def fmt(sec: float) -> str:
                            sec = max(0.0, sec)
                            if sec >= 3600:
                                h = int(sec // 3600)
                                m = int((sec % 3600) // 60)
                                return f"{h}h{m:02d}m"
                            if sec >= 60:
                                m = int(sec // 60)
                                s = int(sec % 60)
                                return f"{m}m{s:02d}s"
                            return f"{int(sec)}s"

                        elapsed_str = fmt(elapsed_s)
                        eta_str = fmt(eta_s) if (eta_s is not None) else "--"
                        print(f"\r抓取子级进度 [{bar}] {done_count}/{total_count} 已用: {elapsed_str} 预估: {eta_str}",
                              end='', flush=True)

                    try:
                        for fut in asyncio.as_completed(tasks):
                            one_start = time.monotonic()
                            res = await fut
                            one_cost = time.monotonic() - one_start
                            durations.append(one_cost)
                            results.append(res)
                            done += 1
                            elapsed = time.monotonic() - start_ts
                            avg = (sum(durations) / len(durations)) if durations else 0.0
                            remaining = max(0, total - done)
                            eta = remaining * avg if avg > 0 else None
                            render_bar(done, total, elapsed, eta)
                    finally:
                        if total > 0:
                            print()  # 换行
            finally:
                try:
                    await context.close()
                    await browser.close()
                except Exception:
                    pass

        result = {"generated_at": datetime.now(timezone.utc).isoformat(), "boards": results}
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            logger.info(f"板块索引已保存: {save_path}")
        except Exception:
            logger.exception(f"保存板块索引失败: {save_path}")
        return result

    async def build_boards_index_from_html(self, html: str, save_path: str = "boards_index.json",
                                           base_url: str = "https://bbs.nga.cn/") -> Dict[str, Any]:
        """从外部提供的主页 HTML 中提取板块列表并生成索引"""
        boards = self.extract_sections_from_html(html, base_url=base_url)
        # 按 fid 去重
        uniq = []
        seen = set()
        for b in boards:
            fid = b.get("fid")
            if not fid or fid in seen:
                continue
            seen.add(fid)
            uniq.append({
                "name": b.get("name", ""),
                "url": b.get("url", ""),
                "fid": b.get("fid", ""),
                "description": b.get("description", ""),
                "forums": [],
                "collections": [],
            })

        result = {"generated_at": datetime.now(timezone.utc).isoformat(), "boards": uniq}
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            logger.info(f"板块索引已保存: {save_path}")
        except Exception:
            logger.exception(f"保存板块索引失败: {save_path}")
        return result

    class _BoardAnchorParser(HTMLParser):
        """HTML解析器，用于从HTML中提取板块链接"""

        def __init__(self, base_url: str) -> None:
            super().__init__()
            self.base_url = base_url or "https://bbs.nga.cn/"
            self.in_target_a = False
            self.current_href = ""
            self.current_text_parts: List[str] = []
            self.items: List[Dict[str, str]] = []
            self.seen = set()

        def handle_starttag(self, tag, attrs):
            if tag.lower() != 'a':
                return
            href = None
            for k, v in attrs:
                if k.lower() == 'href':
                    href = v or ''
                    break
            if not href or 'thread.php?fid=' not in href:
                return
            self.in_target_a = True
            self.current_href = href
            self.current_text_parts = []

        def handle_data(self, data):
            if self.in_target_a and data:
                self.current_text_parts.append(data)

        def handle_endtag(self, tag):
            if tag.lower() != 'a':
                return
            if not self.in_target_a:
                return
            name = (''.join(self.current_text_parts) or '').strip()
            url = self.current_href
            self.in_target_a = False
            self.current_href = ''
            self.current_text_parts = []
            if not name or name in ('版面', '合集'):
                return
            try:
                full = urllib.parse.urljoin(self.base_url, url)
                m = urllib.parse.parse_qs(urllib.parse.urlparse(full).query)
                fid_vals = m.get('fid') or []
                fid = str(fid_vals[0]) if fid_vals else ''
            except Exception:
                full = url
                fid = ''
            key = f"{fid}|{name}"
            if not fid or key in self.seen:
                return
            self.seen.add(key)
            self.items.append({"name": name, "url": full, "type": "board", "fid": fid})

    def extract_sections_from_html(self, html: str, base_url: str = "https://bbs.nga.cn/") -> List[Dict[str, Any]]:
        """从HTML中解析板块入口"""
        parser = self._BoardAnchorParser(base_url)
        try:
            parser.feed(html or '')
            return parser.items
        except Exception:
            logger.exception("extract_sections_from_html failed")
            return []

    def _levenshtein(self, a: str, b: str) -> int:
        """计算编辑距离"""
        a = a or ""
        b = b or ""
        la, lb = len(a), len(b)
        if la == 0:
            return lb
        if lb == 0:
            return la
        dp = list(range(lb + 1))
        for i in range(1, la + 1):
            prev = dp[0]
            dp[0] = i
            for j in range(1, lb + 1):
                cur = dp[j]
                cost = 0 if a[i - 1] == b[j - 1] else 1
                dp[j] = min(dp[j] + 1, dp[j - 1] + 1, prev + cost)
                prev = cur
        return dp[lb]

    def _score_board(self, board: Dict[str, Any], needle: str) -> int:
        """计算板块相关性得分"""
        candidates: List[str] = []
        nm = str(board.get("name", "")).strip().lower()
        if nm:
            candidates.append(nm)
        for f in board.get("forums", []) or []:
            fn = str(f.get("name", "")).strip().lower()
            if fn:
                candidates.append(fn)
        for c in board.get("collections", []) or []:
            cn = str(c.get("name", "")).strip().lower()
            if cn:
                candidates.append(cn)
        if not candidates:
            return 10 ** 9
        return min(self._levenshtein(needle, x) for x in candidates)

    def get_board_structure(self, index_path: str | None = None) -> Dict[str, Any]:
        """获取板块分类结构"""
        if not index_path:
            # 尝试多个可能的路径
            script_dir = os.path.dirname(os.path.abspath(__file__))
            possible_paths = [
                os.path.join(script_dir, "boards_index.json"),
                "boards_index.json",
                os.path.join(os.getcwd(), "boards_index.json")
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    index_path = path
                    break
            if not index_path:
                return {"success": False, "error": "index_not_found_or_invalid"}

        try:
            with open(index_path, "r", encoding="utf-8") as f:
                index = json.load(f)
        except Exception:
            return {"success": False, "error": "index_not_found_or_invalid"}

        boards = list((index or {}).get("boards", []) or [])
        structure = {}

        for board in boards:
            category_l1 = str(board.get("category_l1", "")).strip() or "(未分组)"
            category_l2 = str(board.get("category_l2", "")).strip()
            board_name = str(board.get("name", "")).strip()

            if category_l1 not in structure:
                structure[category_l1] = {}

            if category_l2:
                if category_l2 not in structure[category_l1]:
                    structure[category_l1][category_l2] = []
                structure[category_l1][category_l2].append({
                    "name": board_name,
                    "fid": board.get("fid", ""),
                    "description": board.get("description", "")
                })
            else:
                if "_direct_boards" not in structure[category_l1]:
                    structure[category_l1]["_direct_boards"] = []
                structure[category_l1]["_direct_boards"].append({
                    "name": board_name,
                    "fid": board.get("fid", ""),
                    "description": board.get("description", "")
                })

        return {
            "success": True,
            "structure": structure,
            "total_categories": len(structure),
            "generated_at": index.get("generated_at", "")
        }

    def query_boards(self, name: str, index_path: str | None = None, topk: int = 3) -> Dict[str, Any]:
        """查询板块，支持按类别名称或板块名称查询"""
        if not index_path:
            # 尝试多个可能的路径
            script_dir = os.path.dirname(os.path.abspath(__file__))
            possible_paths = [
                os.path.join(script_dir, "boards_index.json"),
                "boards_index.json",
                os.path.join(os.getcwd(), "boards_index.json")
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    index_path = path
                    break
            if not index_path:
                return {"success": False, "error": "index_not_found_or_invalid", "query": name}

        try:
            with open(index_path, "r", encoding="utf-8") as f:
                index = json.load(f)
        except Exception:
            return {"success": False, "error": "index_not_found_or_invalid", "query": name}

        boards = list((index or {}).get("boards", []) or [])
        needle = (name or "").strip().lower()

        # 首先检查是否是类别名称查询
        category_results = self._query_by_category(boards, needle)
        if category_results:
            return {
                "success": True,
                "query": name,
                "query_type": "category",
                "topk": len(category_results),
                "results": category_results
            }

        # 模糊匹配查询
        scored = [(self._score_board(b, needle), b) for b in boards]
        scored.sort(key=lambda x: (x[0], str(x[1].get("name", ""))))
        results = [b for _, b in scored[:max(1, topk)]]
        return {
            "success": True,
            "query": name,
            "query_type": "fuzzy",
            "topk": len(results),
            "results": results
        }

    def _query_by_category(self, boards: List[Dict[str, Any]], needle: str) -> List[Dict[str, Any]]:
        """按类别名称查询"""
        category_matches = []

        for board in boards:
            category_l1 = str(board.get("category_l1", "")).strip().lower()
            category_l2 = str(board.get("category_l2", "")).strip().lower()

            if needle in category_l1 or category_l1 in needle:
                category_matches.append(board)
            elif category_l2 and (needle in category_l2 or category_l2 in needle):
                category_matches.append(board)

        return category_matches
