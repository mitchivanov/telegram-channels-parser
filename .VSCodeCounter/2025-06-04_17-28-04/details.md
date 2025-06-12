# Details

Date : 2025-06-04 17:28:04

Directory c:\\Users\\micha\\Desktop\\Projects\\telegram-channels-parser

Total : 76 files,  10144 codes, 436 comments, 731 blanks, all 11311 lines

[Summary](results.md) / Details / [Diff Summary](diff.md) / [Diff Details](diff-details.md)

## Files
| filename | language | code | comment | blank | total |
| :--- | :--- | ---: | ---: | ---: | ---: |
| [README.md](/README.md) | Markdown | 1 | 0 | 0 | 1 |
| [channels-bot/Dockerfile](/channels-bot/Dockerfile) | Docker | 7 | 0 | 0 | 7 |
| [channels-bot/cleanup\_worker.py](/channels-bot/cleanup_worker.py) | Python | 108 | 5 | 16 | 129 |
| [channels-bot/main.py](/channels-bot/main.py) | Python | 317 | 11 | 15 | 343 |
| [channels-bot/requirements.txt](/channels-bot/requirements.txt) | pip requirements | 5 | 0 | 0 | 5 |
| [docker-compose.yml](/docker-compose.yml) | YAML | 240 | 0 | 18 | 258 |
| [filter-bot/Dockerfile](/filter-bot/Dockerfile) | Docker | 6 | 0 | 0 | 6 |
| [filter-bot/main.py](/filter-bot/main.py) | Python | 301 | 16 | 20 | 337 |
| [filter-bot/requirements.txt](/filter-bot/requirements.txt) | pip requirements | 4 | 0 | 0 | 4 |
| [filter/Dockerfile](/filter/Dockerfile) | Docker | 6 | 0 | 0 | 6 |
| [filter/db.py](/filter/db.py) | Python | 7 | 0 | 2 | 9 |
| [filter/frontend/Dockerfile](/filter/frontend/Dockerfile) | Docker | 14 | 2 | 1 | 17 |
| [filter/frontend/README.md](/filter/frontend/README.md) | Markdown | 45 | 0 | 10 | 55 |
| [filter/frontend/eslint.config.js](/filter/frontend/eslint.config.js) | JavaScript | 27 | 0 | 2 | 29 |
| [filter/frontend/index.html](/filter/frontend/index.html) | HTML | 13 | 0 | 1 | 14 |
| [filter/frontend/nginx.conf](/filter/frontend/nginx.conf) | Properties | 27 | 0 | 7 | 34 |
| [filter/frontend/package-lock.json](/filter/frontend/package-lock.json) | JSON | 4,244 | 0 | 1 | 4,245 |
| [filter/frontend/package.json](/filter/frontend/package.json) | JSON | 35 | 0 | 1 | 36 |
| [filter/frontend/public/vite.svg](/filter/frontend/public/vite.svg) | XML | 1 | 0 | 0 | 1 |
| [filter/frontend/src/App.css](/filter/frontend/src/App.css) | CSS | 37 | 0 | 6 | 43 |
| [filter/frontend/src/App.tsx](/filter/frontend/src/App.tsx) | TypeScript JSX | 30 | 0 | 4 | 34 |
| [filter/frontend/src/api/filters.ts](/filter/frontend/src/api/filters.ts) | TypeScript | 56 | 1 | 14 | 71 |
| [filter/frontend/src/assets/react.svg](/filter/frontend/src/assets/react.svg) | XML | 1 | 0 | 0 | 1 |
| [filter/frontend/src/channelNames.ts](/filter/frontend/src/channelNames.ts) | TypeScript | 6 | 1 | 1 | 8 |
| [filter/frontend/src/components/ChannelFilters.tsx](/filter/frontend/src/components/ChannelFilters.tsx) | TypeScript JSX | 97 | 0 | 8 | 105 |
| [filter/frontend/src/components/FilterForm.tsx](/filter/frontend/src/components/FilterForm.tsx) | TypeScript JSX | 102 | 0 | 7 | 109 |
| [filter/frontend/src/components/LoginForm.tsx](/filter/frontend/src/components/LoginForm.tsx) | TypeScript JSX | 44 | 0 | 6 | 50 |
| [filter/frontend/src/components/ParserControl.tsx](/filter/frontend/src/components/ParserControl.tsx) | TypeScript JSX | 127 | 2 | 13 | 142 |
| [filter/frontend/src/index.css](/filter/frontend/src/index.css) | CSS | 61 | 0 | 8 | 69 |
| [filter/frontend/src/main.tsx](/filter/frontend/src/main.tsx) | TypeScript JSX | 9 | 0 | 2 | 11 |
| [filter/frontend/src/pages/LoginPage.tsx](/filter/frontend/src/pages/LoginPage.tsx) | TypeScript JSX | 8 | 0 | 2 | 10 |
| [filter/frontend/src/pages/MainPage.tsx](/filter/frontend/src/pages/MainPage.tsx) | TypeScript JSX | 100 | 0 | 12 | 112 |
| [filter/frontend/src/vite-env.d.ts](/filter/frontend/src/vite-env.d.ts) | TypeScript | 0 | 1 | 1 | 2 |
| [filter/frontend/tsconfig.app.json](/filter/frontend/tsconfig.app.json) | JSON | 22 | 2 | 3 | 27 |
| [filter/frontend/tsconfig.json](/filter/frontend/tsconfig.json) | JSON with Comments | 7 | 0 | 1 | 8 |
| [filter/frontend/tsconfig.node.json](/filter/frontend/tsconfig.node.json) | JSON | 20 | 2 | 3 | 25 |
| [filter/frontend/vite.config.ts](/filter/frontend/vite.config.ts) | TypeScript | 9 | 1 | 2 | 12 |
| [filter/kafka\_worker.py](/filter/kafka_worker.py) | Python | 304 | 32 | 18 | 354 |
| [filter/main.py](/filter/main.py) | Python | 61 | 6 | 11 | 78 |
| [filter/models.py](/filter/models.py) | Python | 26 | 0 | 3 | 29 |
| [filter/requirements.txt](/filter/requirements.txt) | pip requirements | 9 | 0 | 0 | 9 |
| [filter/routes/\_\_init\_\_.py](/filter/routes/__init__.py) | Python | 0 | 0 | 1 | 1 |
| [filter/routes/filters.py](/filter/routes/filters.py) | Python | 146 | 10 | 25 | 181 |
| [filter/routes/health.py](/filter/routes/health.py) | Python | 5 | 0 | 2 | 7 |
| [filter/routes/media.py](/filter/routes/media.py) | Python | 26 | 0 | 4 | 30 |
| [filter/routes/moderation.py](/filter/routes/moderation.py) | Python | 37 | 0 | 5 | 42 |
| [filter/schemas.py](/filter/schemas.py) | Python | 28 | 0 | 7 | 35 |
| [filter/utils.py](/filter/utils.py) | Python | 58 | 6 | 2 | 66 |
| [loki-config.yaml](/loki-config.yaml) | YAML | 35 | 0 | 0 | 35 |
| [notifications-bot/Dockerfile](/notifications-bot/Dockerfile) | Docker | 6 | 0 | 0 | 6 |
| [notifications-bot/main.py](/notifications-bot/main.py) | Python | 120 | 4 | 12 | 136 |
| [notifications-bot/requirements.txt](/notifications-bot/requirements.txt) | pip requirements | 3 | 0 | 0 | 3 |
| [parser.py](/parser.py) | Python | 1,200 | 239 | 275 | 1,714 |
| [parser/Dockerfile](/parser/Dockerfile) | Docker | 11 | 3 | 7 | 21 |
| [parser/\[DEPRECATED\]state.py](/parser/%5BDEPRECATED%5Dstate.py) | Python | 83 | 5 | 12 | 100 |
| [parser/activity\_monitor.py](/parser/activity_monitor.py) | Python | 43 | 0 | 5 | 48 |
| [parser/config\_loader.py](/parser/config_loader.py) | Python | 19 | 0 | 3 | 22 |
| [parser/db\_models.py](/parser/db_models.py) | Python | 15 | 0 | 3 | 18 |
| [parser/entities.csv](/parser/entities.csv) | CSV | 176 | 0 | 1 | 177 |
| [parser/entrypoint.sh](/parser/entrypoint.sh) | Shell Script | 8 | 1 | 2 | 11 |
| [parser/export\_entities\_to\_csv.py](/parser/export_entities_to_csv.py) | Python | 45 | 0 | 7 | 52 |
| [parser/floodwait\_manager.py](/parser/floodwait_manager.py) | Python | 24 | 0 | 3 | 27 |
| [parser/import\_entities\_from\_csv.py](/parser/import_entities_from_csv.py) | Python | 44 | 1 | 4 | 49 |
| [parser/kafka\_producer.py](/parser/kafka_producer.py) | Python | 64 | 1 | 8 | 73 |
| [parser/main.py](/parser/main.py) | Python | 902 | 66 | 74 | 1,042 |
| [parser/postgres\_state.py](/parser/postgres_state.py) | Python | 76 | 0 | 9 | 85 |
| [parser/rate\_limiter.py](/parser/rate_limiter.py) | Python | 23 | 0 | 3 | 26 |
| [parser/requirements.txt](/parser/requirements.txt) | pip requirements | 7 | 0 | 1 | 8 |
| [parser/task\_queue\_manager.py](/parser/task_queue_manager.py) | Python | 104 | 8 | 10 | 122 |
| [parser/test\_telethon\_download.py](/parser/test_telethon_download.py) | Python | 56 | 1 | 3 | 60 |
| [parser/utils.py](/parser/utils.py) | Python | 95 | 8 | 12 | 115 |
| [prometheus.yml](/prometheus.yml) | YAML | 9 | 0 | 2 | 11 |
| [promtail-config.yaml](/promtail-config.yaml) | YAML | 16 | 0 | 3 | 19 |
| [rainbow-text.html](/rainbow-text.html) | HTML | 61 | 0 | 1 | 62 |
| [solution.py](/solution.py) | Python | 0 | 0 | 1 | 1 |
| [test\_telethon\_download.py](/test_telethon_download.py) | Python | 55 | 1 | 5 | 61 |

[Summary](results.md) / Details / [Diff Summary](diff.md) / [Diff Details](diff-details.md)