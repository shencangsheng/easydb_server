# EasyDB

"Out of the box," use `SQL` to drive `CSV`, `JSON`, `Parquet` files, powered by the high-performance and scalable query engine `DataFusion` written in Rust.

## 📖 Features

- SQL access to CSV, JSON files

## 🔮 Roadmap

- [ ] Optimize error messages
- [ ] Automatically recognize tables based on paths
- [ ] Automatically generate table schema
- [ ] Support outputting more data types
- [x] Support `select * from '/path/example.csv'` to directly access local files without needing to `create table` in advance
- [ ] Support remote files on s3
- [ ] Support multiple paths

## 👍 Dependencies

These open-source libraries are used to create this project.

- [apache/datafusion](https://github.com/apache/datafusion)

## 📝 License

A short snippet describing the license (MIT)

MIT © Cangsheng Shen
