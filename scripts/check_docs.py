#!/usr/bin/env python3
"""文档与代码的对齐检查。

只做**可判定**的断言：文档里出现标识符、路径、常量、结构字段时，要求它在代码里真实存在。
不做语义判断——行为顺序、并发语义、性能承诺这类只能靠人工审阅。

每条规则都会统计"实际检查了多少处"，输出里一并打印：绿不等于全对，至少要知道看了什么。

环境变量：
  DOCS_CHECK_INCLUDE_ARCHIVE=1  把 docs/archive/ 也纳入（用来验证检查器是否灵敏；
                                归档里本来就允许出现已废弃的名字，正常门禁不开）
"""
import io
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKIP_DIRS = {"target", "node_modules", "tmp", ".git", "dist", "build"}
INCLUDE_ARCHIVE = os.environ.get("DOCS_CHECK_INCLUDE_ARCHIVE") == "1"
if not INCLUDE_ARCHIVE:
    SKIP_DIRS.add("archive")

problems = []
checked = {}


def fail(rule, where, detail):
    problems.append((rule, where, detail))


def count(rule, n=1):
    checked[rule] = checked.get(rule, 0) + n


def read(path):
    with io.open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def walk_md():
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for name in filenames:
            if name.endswith(".md"):
                yield os.path.join(dirpath, name)


def rel(path):
    return os.path.relpath(path, ROOT)


def src_text():
    out = []
    for dirpath, _dirnames, filenames in os.walk(os.path.join(ROOT, "src")):
        for name in filenames:
            if name.endswith(".rs"):
                out.append(read(os.path.join(dirpath, name)))
    return "\n".join(out)


# ── 参考集：从代码里抽出来的"真实存在的东西" ─────────────────────────────────
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

NEGATIVE = ("没有", "已移除", "旧版", "不再", "does not", "已删除", "不是", "无 ")

# ── 规则 1：相对链接必须能解析；并识别写坏的链接语法 ─────────────────────────
LINK = re.compile(r"\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
SUSPECT = re.compile(r"\]\(\(")

# ── 规则 2：文档里的 domain.<member> 必须在某个 SDK 里存在 ───────────────────
MENTION = re.compile(r"\bdomain\.([A-Za-z_]\w*)")

# ── 规则 3：kernel.<target> 必须是协议里定义的内置目标 ───────────────────────
FILE_EXT = {"svg", "exe", "py", "js", "mjs", "json", "md", "db", "rs", "tgz", "whl",
            "toml", "lock", "pid", "html", "sh", "yml", "yaml"}
KTARGET = re.compile(r"\b((?:kernel|web_ui)\.[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*)\b")

# ── 规则 4 / 5 / 6 的正则 ────────────────────────────────────────────────────
ENVREF = re.compile(r"\b(ATELIER_[A-Z0-9_]+)\b")
SRCPATH = re.compile(r"`(src/[A-Za-z0-9_/]+\.rs)`")
FLAGREF = re.compile(r"`(--[a-z][a-z-]*)")

for path in walk_md():
    for lineno, line in enumerate(read(path).splitlines(), 1):
        where = "%s:%d" % (rel(path), lineno)

        for target in LINK.findall(line):
            count("link")
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            target = target.split("#", 1)[0]
            if not target:
                continue
            resolved = os.path.normpath(os.path.join(os.path.dirname(path), target))
            if not os.path.exists(resolved):
                fail("link", where, target)

        if SUSPECT.search(line):
            count("link-syntax")
            fail("link-syntax", where, line.strip()[:80])

        for target in SRCPATH.findall(line):
            count("src-path")
            if not os.path.exists(os.path.join(ROOT, target)):
                fail("src-path", where, target)

        for flag in FLAGREF.findall(line):
            count("cli-flag")
            if flag not in cli_flags:
                fail("cli-flag", where, flag)

        # 否定句跳过：这些行是在说"没有 X / 已移除 X"，不该判成漂移
        if any(k in line for k in NEGATIVE):
            count("skipped-negative-lines")
            continue

        for name in MENTION.findall(line):
            count("sdk-member")
            if name not in sdk_members:
                fail("sdk-member", where, "domain." + name)

        for name in KTARGET.findall(line):
            if name.rsplit(".", 1)[-1] in FILE_EXT:
                continue
            count("kernel-target")
            if name not in kernel_targets:
                fail("kernel-target", where, name)

        for name in ENVREF.findall(line):
            count("env-var")
            if name not in env_names:
                fail("env-var", where, name)

# ── 规则 7：两语言 SDK 能力表必须逐项对应 ────────────────────────────────────
def capability_rows(path):
    rows = []
    for line in read(path).splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) >= 3 and cells[0].isdigit():
            rows.append(cells[1])
    return rows


PY_DOC = os.path.join(ROOT, "docs", "sdk", "python.md")
JS_DOC = os.path.join(ROOT, "docs", "sdk", "nodejs.md")
if os.path.exists(PY_DOC) and os.path.exists(JS_DOC):
    py_rows, js_rows = capability_rows(PY_DOC), capability_rows(JS_DOC)
    count("sdk-table", len(py_rows))
    if py_rows != js_rows:
        fail("sdk-table", "docs/sdk/{python,nodejs}.md",
             "%s vs %s" % (py_rows, js_rows))

# ── 规则 8：发布包目录树必须与本地打包脚本一致 ──────────────────────────────
GUIDE = os.path.join(ROOT, "docs", "VARIANT_GUIDE.md")
SCRIPT = os.path.join(ROOT, "scripts", "build_dist.sh")
if os.path.exists(GUIDE) and os.path.exists(SCRIPT):
    blocks, current, in_block = [], [], False
    for line in read(GUIDE).splitlines():
        if line.strip().startswith("```"):
            if in_block:
                blocks.append(current)
                current = []
            in_block = not in_block
            continue
        if in_block:
            current.append(line)
    bundle = next((b for b in blocks if any("atelier-kernel-runtime" in l for l in b)), None)
    script_text = read(SCRIPT)
    produced = set(re.findall(r'\$\{RUNTIME_OUT\}/([A-Za-z0-9_.*-]+)', script_text))
    produced |= {"atelier-kernel", "kernel_boot.template.json", "README.md"}
    if bundle:
        for line in bundle:
            # 行尾可能没有注释、直接换行（如 `└── sdk/`），所以不能用 `\s` 收尾。
            m = re.search(r"^(\s*)[├└]──\s+([A-Za-z0-9_.<>-]+/?)(?=\s|$)", line)
            if not m:
                continue
            indent = len(m.group(1))
            entry = m.group(2).rstrip("/")
            count("release-layout")
            if indent > 0:
                # 嵌套项（sdk/ 下的 whl / tgz）只要求父级是 sdk，名字不参与比对
                continue
            if entry == "sdk" or entry.endswith((".whl", ".tgz")):
                if not any(p.startswith("sdk") for p in produced):
                    fail("release-layout", rel(GUIDE), "sdk/ 未由打包脚本产出")
            elif entry not in produced:
                fail("release-layout", rel(GUIDE), entry)

# ── 输出 ────────────────────────────────────────────────────────────────────
ORDER = ("link", "link-syntax", "sdk-member", "kernel-target", "env-var", "src-path",
         "cli-flag", "sdk-table", "release-layout", "skipped-negative-lines")

if problems:
    print("文档漂移 %d 处：" % len(problems))
    for rule, where, detail in problems[:80]:
        print("  [%s] %s  ->  %s" % (rule, where, detail))
    if len(problems) > 80:
        print("  ... 还有 %d 处" % (len(problems) - 80))
else:
    print("文档与代码对齐：以下断言全部通过。")

print("")
print("扫描 %d 个 markdown 文件；各类断言检查次数：" % len(list(walk_md())))
for rule in ORDER:
    print("  %-24s %d" % (rule, checked.get(rule, 0)))

sys.exit(1 if problems else 0)
