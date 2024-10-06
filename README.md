# Mission Monitor 后端（Python）

Mission Monitor，《深岩银河》游戏数据分析一站式解决方案。

[主项目地址](https://github.com/saitewasreset/DRG_MissionMonitor)

**本项目已转向使用[Rust 版本后端](https://github.com/saitewasreset/mission-backend-rs)**

## API 文档

[参见](./api.md)

## 部署

### 一键部署

建议使用[主项目](https://github.com/saitewasreset/DRG_MissionMonitor)，利用 Docker compose 进行一键部署。

### 单独构建

本项目已经包含 Dockerfile，可使用 Docker 进行构建。

数据库：mariadb

需要的表结构参见：https://github.com/saitewasreset/DRG_MissionMonitor/blob/main/db_init/init.sql

环境变量：
| 名称 | 含义 |
| ---- | ---- |
| DB_HOST |要连接的数据库主机 |
| DB_DATABASE | 要连接的数据库名称 |
| DB_USER |数据库用户名 |
| DB_PASSWORD | 数据库密码 |
| ADMIN_PREFIX | 管理功能 URL 前缀 |
