# EasyDB

[![Docker Pulls](https://img.shields.io/docker/pulls/shencangsheng/easydb-backend.svg)](https://hub.docker.com/r/shencangsheng/easydb-backend)

EasyDB is a SQL assistant written in Rust, designed to enhance the querying capabilities of text files. With EasyDB, users can treat multiple arbitrary files as a single database and perform SQL queries. It supports various file formats, including CSV, JSON, and Parquet files, and works out of the box without the need for file conversion. 

powered by the high-performance and scalable query engine `DataFusion` written in Rust.

## 📖 Features

- Use standard SQL statements to query file data

## 🔮 Roadmap

- [ ] Optimize error messages
- [ ] Automatically recognize tables based on paths
- [ ] Automatically generate table schema
- [ ] Support outputting more data types
- [x] Support `select * from '/path/example.csv'` to directly access local files without needing to `create table` in advance
- [ ] Support remote files on s3
- [ ] Support multiple paths
- [ ] Support MySQL
- [ ] Support Parquet files

## 🚀 Quick Start

```bash
git clone https://github.com/shencangsheng/easy_db.git
docker compose up -d
# http://127.0.0.1:8088
```

![demo.gif](assets/demo.gif)

### Examples

```sql
select * from '/var/lib/easydb/example/order*.csv'
```

```sql
create table user () location 'example/user.csv'
```

```sql
create table log () location 'example/2025*.log'
```

```sql
select *
from user as t1
inner join log as t2 on (t1.id = t2.user_id)
```

## 👍 Dependencies

These open-source libraries are used to create this project.

- [apache/datafusion](https://github.com/apache/datafusion)

## 📝 License

A short snippet describing the license (MIT)

MIT © Cangsheng Shen
