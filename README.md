# EasyDB

“开箱即用”，使用 `SQL` 查询 `CSV`、`JSON`、`Parquet` 文件，底层采用了由 Rust 编写的高性能可扩展查询引擎 `DataFusion`。如果你熟悉
`CDH`，我们的前端界面将让你倍感亲切，因为作者认为 `Cloudera Hue` 的界面非常优秀。还在为无法直接对 CSV
文件进行交互操作而烦恼吗？还在为必须将数据导入数据库才能进行 Join 查询而头疼吗？查看 `JSON` 日志还需要使用 `cat、grep、awk` 或
`Kibana` 吗？`easyDB` 为解决这类烦恼此而生。

## 📖 Features

- SQL 访问 CSV、JSON 文件

## 🔮 Roadmap

- [ ] 优化异常提示
- [ ] 根据路径自动识别表
- [ ] 自动生成 table schema
- [ ] 支持输出更多数据类型
- [ ] 支持 `select * from '/path/example.csv'` 直接访问本地文件，不需要提前 `create table`
- [ ] 支持 s3

## 👍 依赖库

这些开源库用于创建本项目。

- [apache/datafusion](https://github.com/apache/datafusion)

## 📝 许可证

A short snippet describing the license (MIT)

MIT © Cangsheng Shen