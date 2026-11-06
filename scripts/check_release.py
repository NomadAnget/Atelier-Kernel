#!/usr/bin/env python3
"""发版一致性检查：版本号单一来源、发版说明存在且不是归档件。

在打 tag 之前就应该绿，否则发出去的包版本号与说明会对不上。
"""
import io
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
problems = []


def read(path):
    with io.open(os.path.join(ROOT, path), "r", encoding="utf-8") as f:
        return f.read()


def check(condition, message):
    if not condition:
        problems.append(message)


cargo = re.search(r'^version = "([^"]+)"', read("Cargo.toml"), re.M).group(1)
lock = re.search(r'name = "atelier_kernel"\nversion = "([^"]+)"', read("Cargo.lock")).group(1)
node = re.search(r'"version": "([^"]+)"', read("sdk/nodejs/package.json")).group(1)
py = re.search(r'^__version__ = "([^"]+)"', read("sdk/python/atelier_sdk/__init__.py"), re.M).group(1)

for name, value in (("Cargo.lock", lock), ("sdk/nodejs/package.json", node),
                    ("sdk/python/atelier_sdk/__init__.py", py)):
    check(value == cargo, "版本不一致：Cargo.toml=%s 但 %s=%s" % (cargo, name, value))

# Python 打包脚本不得再硬编码版本号，否则发出去的 wheel 与实际代码脱节。
pyproject = read("sdk/python/pyproject.toml")
check('dynamic = ["version"]' in pyproject and "atelier_sdk.__version__" in pyproject,
      "pyproject.toml 必须从 atelier_sdk.__version__ 动态取版本")
check(re.search(r'^version = "', pyproject, re.M) is None,
      "pyproject.toml 不应再写死 version")

for script in ("sdk/python/build_wheel.py", "sdk/python/setup.py"):
    body = read(script)
    check('__version__' in body, "%s 必须从 __init__.py 读取版本号" % script)

notes = "docs/releases/RELEASE_%s.md" % cargo
if not os.path.exists(os.path.join(ROOT, notes)):
    problems.append("缺少当前版本的发布说明：%s" % notes)
else:
    head = read(notes).lstrip().splitlines()[0]
    check("归档" not in head and "不代表当前契约" not in head,
          "%s 首行带归档横幅，会让 GitHub Release 正文以「不代表当前契约」开头" % notes)

if problems:
    print("发版检查未通过 %d 项：" % len(problems))
    for p in problems:
        print("  - " + p)
    sys.exit(1)

print("发版检查通过：版本号 %s 在 4 处声明一致，打包脚本动态取版本，发版说明就位。" % cargo)
