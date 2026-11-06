//! C4 存储枢纽：虚拟路径索引、跨域只读、重启解析、消息内文件对象。

#[path = "cases/c4/objects.rs"]
mod objects;
#[path = "cases/c4/multi_process.rs"]
mod multi_process;
#[path = "cases/c4/file_object.rs"]
mod file_object;
#[path = "cases/c4/storage_file_object.rs"]
mod storage_file_object;
