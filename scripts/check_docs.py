#!/usr/bin/env python3
"""文档与代码的对齐检查。

只做**可判定**的断言：文档里出现标识符、路径、常量、结构字段时，要求它在代码里真实存在。
不做语义判断——行为顺序、并发语义、性能承诺这类只能靠人工审阅。

退出码非 0 表示发现漂移，CI 直接失败。
"""
import io
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKIP_DIRS = {"target", "node_modules", "tmp", ".git", "dist", "archive", "build"}

problems = []


def fail(rule, where, detail):
    problems.append((rule, where, detail))


def read(path):
    with io.open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def walk_md():
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if name.endswith(".md"):
                yield os.path.join(dirpath, name)


def src_text():
    out = []
    for dirpath, dirnames, filenames in os.walk(os.path.join(ROOT, "src")):
        for name in filenames:
            if name.endswith(".rs"):
                out.append(read(os.path.join(dirpath, name)))
    return "\n".join(out)


def rel(path):
    return os.path.relpath(path, ROOT)


# ── 参考集：从代码里抽出来的"真实存在的东西" ────────────────────────────────
SRC = src_text()
MAIN_RS = read(os.path.join(ROOT, "src", "main.rs"))

SDK_PY = read(os.path.join(ROOT, "sdk", "python", "atelier_sdk", "__init__.py"))
SDK_JS = read(os.path.join(ROOT, "sdk", "nodejs", "index.js"))

sdk_members = set(re.findall(r"^\s*(?:async )?def ([A-Za-z_]\w*)", SDK_PY, re.M))
sdk_members |= set(re.findall(r"^\s*@property\s*\n\s*def ([A-Za-z_]\w*)", SDK_PY, re.M))
sdk_members |= set(re.findall(r"self\.([a-z_]\w*) =", SDK_PY))
sdk_members |= set(re.findall(r"^\s+(?:async\s+)?(?:get\s+)?([A-Za-z_]\w*)\s*\(", SDK_JS, re.M))
sdk_members |= set(re.findall(r"this\.([A-Za-z_]\w*) =", SDK_JS))
sdk_members |= {"mjs", "js", "py", "json"}

kernel_targets = set(re.findall(r'"((?:kernel|web_ui)\.[a-z_.]+)"', SRC))

env_names = set(re.findall(r'"(ATELIER_[A-Z0-9_]+)"', SRC))

cli_flags = set(re.findall(r'"(--[a-z][a-z-]*)"', MAIN_RS))

# ── 规则 1：相对链接必须能解析 ─────────────────────────────────────────────
LINK = re.compile(r"\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
# 疑似写坏的链接语法：](( 、 ]( 后紧跟括号
SUSPECT = re.compile(r"\]\(\(")

for path in walk_md():
    text = read(path)
    for lineno, line in enumerate(text.splitlines(), 1):
        for target in LINK.findall(line):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            target = target.split("#", 1)[0]
            if not target:
                continue
            resolved = os.path.normpath(os.path.join(os.path.dirname(path), target))
            if not os.path.exists(resolved):
                fail("link", "%s:%d" % (rel(path), lineno), target)
        if SUSPECT.search(line):
            fail("link-syntax", "%s:%d" % (rel(path), lineno), line.strip()[:80])

# ── 规则 2：文档里的 domain.<member> 必须在某个 SDK 里存在 ─────────────────
MENTION = re.compile(r"\bdomain\.([A-Za-z_]\w*)")
NEGATIVE = ("没有", "已移除", "旧版", "不再", "does not", "已删除", "不是", "无 ")
for path in walk_md():
    for lineno, line in enumerate(read(path).splitlines(), 1):
        if any(k in line for k in NEGATIVE):
            continue
        for name in MENTION.findall(line):
            if name not in sdk_members:
                fail("sdk-member", "%s:%d" % (rel(path), lineno), "domain." + name)

# ── 规则 3：kernel.<target> 必须是协议里定义的内置目标 ─────────────────────
# 排除文件名（kernel.svg / kernel.exe 之类）与路径片段。
FILE_EXT = {"svg", "exe", "py", "js", "mjs", "json", "md", "db", "rs", "tgz", "whl",
            "toml", "lock", "pid", "html", "sh", "yml", "yaml"}
KTARGET = re.compile(r"\b((?:kernel|web_ui)\.[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*)\b")
for path in walk_md():
    for lineno, line in enumerate(read(path).splitlines(), 1):
        if any(k in line for k in NEGATIVE):
            continue
        for name in KTARGET.findall(line):
            if name.rsplit(".", 1)[-1] in FILE_EXT:
                continue
            if name not in kernel_targets:
                fail("kernel-target", "%s:%d" % (rel(path), lineno), name)

# ── 规则 4：ATELIER_* 环境变量必须是内核真正使用的名字 ─────────────────────
ENVREF = re.compile(r"\b(ATELIER_[A-Z0-9_]+)\b")
for path in walk_md():
    for lineno, line in enumerate(read(path).splitlines(), 1):
        if any(k in line for k in NEGATIVE):
            continue
        for name in ENVREF.findall(line):
            if name not in env_names:
                fail("env-var", "%s:%d" % (rel(path), lineno), name)

# ── 规则 5：提到的 src/ 路径必须存在 ───────────────────────────────────────
SRCPATH = re.compile(r"`(src/[A-Za-z0-9_/]+\.rs)`")
for path in walk_md():
    for lineno, line in enumerate(read(path).splitlines(), 1):
        for target in SRCPATH.findall(line):
            if not os.path.exists(os.path.join(ROOT, target)):
                fail("src-path", "%s:%d" % (rel(path), lineno), target)

# ── 规则 6：命令行参数必须是真实的 flag ────────────────────────────────────
FLAGREF = re.compile(r"`(--[a-z][a-z-]*)")
for path in walk_md():
    for lineno, line in enumerate(read(path).splitlines(), 1):
        for flag in FLAGREF.findall(line):
            if flag not in cli_flags:
                fail("cli-flag", "%s:%d" % (rel(path), lineno), flag)

# ── 规则 7：两语言 SDK 能力表必须逐项对应 ──────────────────────────────────
def capability_rows(path):
    rows = []
    for line in read(path).splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) >= 3 and cells[0].isdigit():
            rows.append(cells[1])
    return rows


py_rows = capability_rows(os.path.join(ROOT, "docs", "sdk", "python.md"))
js_rows = capability_rows(os.path.join(ROOT, "docs", "sdk", "nodejs.md"))
if py_rows != js_rows:
    fail("sdk-table", "docs/sdk/{python,nodejs}.md",
         "capability tables differ: %s vs %s" % (py_rows, js_rows))

# ── 规则 8：VARIANT_GUIDE 的发布包目录树必须与本地打包脚本一致 ─────────────
GUIDE = os.path.join(ROOT, "docs", "VARIANT_GUIDE.md")
SCRIPT = os.path.join(ROOT, "scripts", "build_dist.sh")
if os.path.exists(GUIDE) and os.path.exists(SCRIPT):
    guide_text = read(GUIDE)
    blocks, current, in_block = [], [], False
    for line in guide_text.splitlines():
        if line.strip().startswith("```"):
            if in_block:
                blocks.append(current)
                current = []
            in_block = not in_block
            continue
        if in_block:
            current.append(line)
    # 只取"发布包结构"那一块，不要 --init 生成的变体目录树
    bundle = next((b for b in blocks if any("atelier-kernel-runtime" in l for l in b)), None)
    tree = []
    if bundle:
        for line in bundle:
            m = re.search(r"[├└]──\s+([A-Za-z0-9_.-]+/?)\s", line)
            if m:
                tree.append(m.group(1).rstrip("/"))
    script_text = read(SCRIPT)
    produced = set(re.findall(r'\$\{RUNTIME_OUT\}/([A-Za-z0-9_.*-]+)', script_text))
    produced |= {"atelier-kernel", "kernel_boot.template.json", "README.md"}
    produced = {p for p in produced}
    for entry in tree:
        if entry == "sdk" or entry.endswith((".whl", ".tgz")):
            if not any(p.startswith("sdk") for p in produced):
                fail("release-layout", rel(GUIDE), "sdk/ 未由打包脚本产出")
            continue
        if entry not in produced:
            fail("release-layout", rel(GUIDE), entry)

# ── 输出 ──────────────────────────────────────────────────────────────────
if problems:
    print("文档漂移 %d 处：" % len(problems))
    for rule, where, detail in problems:
        print("  [%s] %s  ->  %s" % (rule, where, detail))
    sys.exit(1)

print("文档与代码对齐：链接、SDK 成员、kernel 目标、环境变量、源码路径、CLI 参数、能力表全部匹配。")
