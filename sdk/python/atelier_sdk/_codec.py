"""C4 消息文件对象编解码。

发送：消息里的原生文件对象 → `{"__atelier_file__": object_id}`。
接收：`{"__atelier_file__": object_id}` → 原生文件对象（由 runtime 注入的解析器完成）。

业务域只在消息里看到文件对象，不接触 `object_id` / `physical_path` / `StorageObjectGet`。
"""

import io

FILE_KEY = "__atelier_file__"


def _namespace():
    return globals().get("_NAMESPACE")


def set_namespace(namespace):
    globals()["_NAMESPACE"] = namespace


def _is_file_object(value):
    if isinstance(value, io.IOBase):
        return True
    return hasattr(value, "read") and isinstance(getattr(value, "name", None), str)


def encode(value, namespace=None):
    ns = namespace or _namespace()
    if ns is not None and _is_file_object(value):
        object_id = ns.object_id_for_path(getattr(value, "name", ""))
        if object_id:
            return {FILE_KEY: object_id}
    if isinstance(value, dict):
        return {key: encode(item, ns) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [encode(item, ns) for item in value]
    return value


async def decode(value, resolver):
    """`resolver(object_id)` 返回一个已打开的原生文件对象（由 runtime 实现）。"""
    if isinstance(value, dict):
        if len(value) == 1 and isinstance(value.get(FILE_KEY), str):
            return await resolver(value[FILE_KEY])
        return {key: await decode(item, resolver) for key, item in value.items()}
    if isinstance(value, list):
        return [await decode(item, resolver) for item in value]
    return value
