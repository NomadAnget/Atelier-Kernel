#!/usr/bin/env python3
"""SDK 死状态检查：私有属性被赋值却从不被读。

判据只有两条，不需要维护名单：
  1. 只看下划线开头的属性（PEP 8 / 仓库既有约定：公有名是 API，类外可能被读）；
  2. 读的判定覆盖 `self._x`、`this._x`、`getattr(self, "_x")` 三种形态。

已知取舍（都是有意的）：
  - 公有名即使内部零读取也不报 —— 它可能就是对外 API。
  - 万一有人把公开 API 命名成 `_x`，会被误报；改个名即可，不需要加白名单。
"""
import io
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGETS = [
    ("python", os.path.join(ROOT, "sdk", "python", "atelier_sdk", "__init__.py"), "self"),
    ("nodejs", os.path.join(ROOT, "sdk", "nodejs", "index.js"), "this"),
]

problems = []
checked = 0

for label, path, this in TARGETS:
    if not os.path.exists(path):
        continue
    with io.open(path, "r", encoding="utf-8", errors="replace") as f:
        src = f.read()

    writes = {}
    for m in re.finditer(r"\b%s\.(_[A-Za-z0-9_$]+)\s*=(?!=)" % this, src):
        writes[m.group(1)] = writes.get(m.group(1), 0) + 1

    for name, write_count in sorted(writes.items()):
        checked += 1
        escaped = re.escape(name)
        reads = len(re.findall(r"\b%s\.%s\b(?!\s*=[^=])" % (this, escaped), src))
        # getattr(self, "_x") / self["_x"] / obj["_x"] 这类间接读也算
        reads += len(re.findall(r"""getattr\(\s*%s\s*,\s*["']%s["']""" % (this, escaped), src))
        if reads == 0:
            problems.append((label, name, write_count))

if problems:
    print("SDK 死状态 %d 处（私有属性被赋值但从不读取）：" % len(problems))
    for label, name, writes in problems:
        print("  [%s] %s  writes=%d reads=0" % (label, name, writes))
    sys.exit(1)

print("SDK 私有状态：%d 个私有属性全部至少被读取一次。" % checked)
