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

## 🚀 Quick Start

```bash
git clone https://github.com/shencangsheng/easy_db.git
docker compose up -d
# http://127.0.0.1:8088
```

### Examples

```sql
select * from '/var/lib/easydb/example/order*.csv';
```

```sql
create table user () location 'example/user.csv';
```

```sql
create table log () location 'example/2025*.log';
```

```sql
select *
from user as t1
inner join log as t2 on (t1.id id = t2.user_id)
```

## 👍 Dependencies

These open-source libraries are used to create this project.

- [apache/datafusion](https://github.com/apache/datafusion)

## 📝 License

A short snippet describing the license (MIT)

MIT © Cangsheng Shen
