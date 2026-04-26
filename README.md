# TG Text Game Assistant

这是一个面向 Telegram 文字交互游戏的项目骨架，当前已经完成第二层：模块划分、SQLite 数据层、Web 管理页、表单提交和基础 Telegram 运行入口。

## 当前模块

- 基础功能
- 宗门
- 战斗
- 物品管理
- 市集
- 股市
- 副本

## 启动 Web

```bash
pip install -r requirements.txt
copy .env.example .env
python run_web.py
```

启动后访问 `http://127.0.0.1:8000`。

## 启动 Telegram Runtime

```bash
copy .env.example .env
python run_telegram.py
```

项目运行时会从仓库根目录的本地 `.env` 读取配置；真实 `.env` 不应提交到仓库，仓库只保留 `.env.example` 示例文件。
群绑定里的 `TG_GAME_BOUND_CHAT_ID`、`TG_GAME_BOUND_THREAD_ID`、`TG_GAME_BOUND_BOT_ID` 已改为代码内固定值，不再从 `.env` 读取。
凡人修仙自动闭关 / 宗门自动任务的默认指令、间隔和时间点也已固定在代码逻辑里，不再通过 `.env` 配置。

需要 `.env` 中至少存在：

- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `AUTHORIZED_USER_ID`（如果你要启用管理员统一写入 / 审计入口）

## 当前已实现

- 角色档案管理
- 固定聊天绑定
- 模块配置管理
- 模块详情页
- 干净的 Telegram runtime 骨架

## 保留与清理

- 已删除旧的 `main.py` 和 `suotou.py` 单文件杂项机器人入口
- `fanren_game.py` 与 `sect_game.py` 仍是当前自动任务链路的活代码依赖，不应按“历史参考文件”处理
