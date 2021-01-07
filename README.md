# Memcache Loader
## Что делает
Скрипт парсит файлы из заданной папки и заливает данные в memcache.

### Опции
- log - logfile, default = "",
- dry - тестовый прогон, default = True
- pattern - регулярка для поиска файлов, default = /data/appsinstalled/[^.]*.tsv.gz

- idfa, default = 127.0.0.1:33013
- idfv, default = 127.0.0.1:33014
- adid, default = 127.0.0.1:33015
- gaid, default = 127.0.0.1:33016

- workers_count, default = runtime.NumCPU()
- timeout, default = 500ms
