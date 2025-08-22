# Finmodel 2.0

Python-утилиты для импорта и анализа финансовых данных с маркетплейса Wildberries.

> **Примечание.** Документация проекта ведётся на русском языке; новые материалы также
> следует оформлять на русском.

## Возможности
- Импорт рекламных кампаний, заказов, остатков, тарифов и других данных с помощью
  отдельных скриптов.
- Загрузка финансового отчёта по периодам через `finotchet_import` с сохранением
  данных в единую таблицу `FinOtchet`. Скрипт поддерживает инкрементальную
  дозагрузку: перед обращением к API для каждой организации ищется максимальный
  `rrd_id` в таблице и дальнейшие запросы выполняются начиная с него. После
  каждой порции `rrdid` обновляется и запросы продолжаются, пока сервис не
  вернёт пустой ответ.
- Сохранение загруженных записей в `finmodel.db` для последующего анализа.

## Структура
- `devtools/` — вспомогательные скрипты для настройки окружения.
- Рабочие скрипты лежат в пакете `finmodel` в подкаталоге `finmodel/scripts` и
  запускаются через единый CLI `finmodel`.

## Установка
Запустите скрипт для автоматической настройки, он установит зависимости и создаст базу
данных `finmodel.db`:

- Linux/macOS:
  ```bash
  devtools/setup.sh
  ```
- Windows:
  ```powershell
  devtools\setup.ps1
  ```

Либо выполните следующие шаги вручную:
1. Создайте и активируйте виртуальное окружение:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
3. Создайте пустую базу данных из файла `schema.sql`:
   ```bash
   sqlite3 finmodel.db < schema.sql
   # либо через общий CLI
   finmodel create_db
   # или укажите пути явно
   finmodel create_db --db custom.db --schema custom_schema.sql

   ```
Чтобы выгрузить схему из существующей базы данных:

```bash
finmodel dump_schema --db finmodel.db --output schema.sql
```

### Миграция `katalog`

Чтобы добавить колонку `snapshot_date` и новый составной первичный ключ в существующую таблицу `katalog`, выполните SQL-скрипт миграции. Предварительно сделайте резервную копию базы данных.

```bash
# для SQLite
sqlite3 finmodel.db < migrations/20240706_add_snapshot_date_to_katalog.sql

# для PostgreSQL
psql -d finmodel -f migrations/20240706_add_snapshot_date_to_katalog.sql
```

Скрипт создаёт таблицу `katalog_new`, переносит в неё текущие строки с датой снимка `snapshot_date` равной текущей, а затем переименовывает таблицу обратно в `katalog`.

4. Скопируйте `config.example.yml` в `config.yml` и заполните диапазоны дат.
   Файл `Настройки.xlsm` с колонками `id`, `Организация` и `Token_WB` должен
   находиться в корне проекта рядом с базой данных `finmodel.db`. Переменные
   окружения с такими же ключами переопределяют значения файла конфигурации.
   Путь к корню проекта можно задать через `FINMODEL_PROJECT_ROOT`, а путь к
   базе данных — через `FINMODEL_DB_PATH`.
5. Запускайте нужный скрипт через единый CLI:
   ```bash
   finmodel saleswb_import_flat
   # или явно через модуль
   python -m finmodel.cli saleswb_import_flat
   # полная перезагрузка (удаляет существующие продажи)
   finmodel saleswb_import_flat --full-reload
   ```
   Старый формат `python -m finmodel.scripts.*` по-прежнему работает для совместимости.


  Скрипт `wb_goods_prices_import_flat` загружает текущие цены товаров по списку
  `nmID` или весь каталог продавца, если список не указан, и сохраняет данные
  в таблицу `WBGoodsPricesFlat` в `finmodel.db`. При желании можно указать
  дополнительные выводы (CSV, сторонний SQLite, ODBC). Токен можно передать

  через `--api-key` или опустить — тогда скрипт возьмёт все токены из файла
  `Настройки.xlsm` и обработает каждую организацию по очереди:


   ```bash
   python -m finmodel.scripts.wb_goods_prices_import_flat \
     --txt nmids.txt \
     --api-key "$WB_TOKEN" \
     --out-sqlite prices.db
   ```

## Конфигурация

`config.yml` хранит технические параметры, которые редко меняются и удобно
версионировать: период выгрузки, имена листов с настройками и другие глобальные
опции. Таблица `Настройки.xlsm` предназначена для оперативных данных — списка
организаций и их токенов, а также, при необходимости, дат периода. Excel легко
редактировать без участия разработчика, поэтому оба источника дополняют друг
друга: YAML задаёт значения по умолчанию и попадает в систему контроля версий,
а Excel позволяет бухгалтерам или менеджерам править параметры без изменения
кода. Лист с организациями по умолчанию называется `"НастройкиОрганизаций"` и
задаётся ключом `ORG_SHEET`, а лист с периодом — `"Настройки"` и задаётся
ключом `SETTINGS_SHEET`. Переменные окружения переопределяют значения YAML при
запуске.

**Пример.** В файле `config.yml` укажите:

```yaml
settings:
  ПериодНачало: "2023-01-01"
  ПериодКонец: "2023-01-31"
  ORG_SHEET: "НастройкиОрганизаций"
  SETTINGS_SHEET: "Настройки"
```

Чтобы изменить те же параметры через окружение:

```bash
export ПериодНачало=2023-02-01
export ПериодКонец=2023-02-28
export ORG_SHEET=НастройкиОрганизаций
export SETTINGS_SHEET=Настройки
```

Скрипты сначала читают переменные окружения, затем раздел `settings` из
`config.yml` и при необходимости могут загрузить период из листа,
указанного через `SETTINGS_SHEET`, в `Настройки.xlsm`.

### Дополнительные параметры

Лист, заданный ключом `SETTINGS_SHEET`, может содержать произвольные пары
`Параметр`/`Значение`. Получить их в коде можно так:

```python
from finmodel.utils.settings import load_global_settings

params = load_global_settings()
api_url = params.get("api_url")
```

Это позволяет передавать в скрипты дополнительные настройки без правки `config.yml`.

## Использование CLI

Проект предоставляет единую команду `finmodel`, которая даёт доступ ко всем поддерживаемым скриптам. При запуске без параметров открывается интерактивное меню.

Выведите список доступных подкоманд:

```bash
finmodel --help
```

Запустите любую из них:

```bash
finmodel saleswb_import_flat
```

Старый формат `python -m finmodel.scripts.*` по-прежнему работает для совместимости,
но теперь в нём нет необходимости.

### Интерактивное меню

Запустите программу без аргументов:

```bash
finmodel
```

Команда `finmodel menu` также доступна для явного вызова меню.

Пример начального вывода:

```text
=========== Finmodel 2.0 ===========
1. orderswb_import_flat
2. saleswb_import_flat
3. katalog
0. Exit
====================================
Select an option:
```

Список формируется автоматически из зарегистрированных команд. Введите номер пункта
и нажмите `Enter`. Параметры команд можно вводить интерактивно, когда скрипт их
запрашивает. Для выхода из меню выберите `0`.

> **Note**
> `katalog` and other data-import scripts expect the workbook `Настройки.xlsm` to
> contain the columns `id`, `Организация` and `Token_WB`.
> It also reads from `/content/v2/get/cards/trash` to include archived cards in the
> resulting `katalog` table.

### finotchet_import

`finotchet_import` загружает детализацию финансового отчёта через API
`/api/v5/supplier/reportDetailByPeriod` и сохраняет данные в таблицу `FinOtchet`.
Таблица не очищается между запусками: скрипт дописывает новые строки или
обновляет существующие по первичному ключу (`org_id`, `rrd_id`), что обеспечивает
идемпотентность импорта. При повторном запуске скрипт сначала определяет
максимальный `rrd_id` для каждой организации в таблице и начинает запросы с
него, тем самым дозагружая только новые записи.

Если API не возвращает строк, скрипт выводит предупреждение и таблица остаётся
пустой.

Для работы скрипта в `Настройки.xlsm` должны быть указаны `Token_WB` для
организаций, а также значения `ПериодНачало` и `ПериодКонец` на листе,
заданном `SETTINGS_SHEET`.

Пример запуска:

```bash
finmodel finotchet_import
```

После каждой порции данных `rrdid` обновляется и запросы продолжаются до тех
пор, пока API не вернёт пустой список.

### wb_goods_prices_import_flat

Скрипт `wb_goods_prices_import_flat` запрашивает цены и скидки товаров и

сохраняет результат в таблицу `WBGoodsPricesFlat` внутри `finmodel.db`.
Дополнительно можно указать вывод в CSV, сторонний SQLite или через ODBC.
Начиная с этой версии, в выгрузку добавляется колонка `vendorCode`.
Можно передать список `nmID` из CSV, TXT или SQLite, либо не указывать
список — тогда для каждой организации из базы `finmodel.db` будет
использован её собственный список из таблицы `katalog`. Для каждого товара
сохраняются цены по всем размерам (`sizeID`). Токен можно указать вручную

через `--api-key` или загрузить автоматически из файла
`Настройки.xlsm` (лист `НастройкиОрганизаций`).


Пример запуска со списком:

```bash
python -m finmodel.scripts.wb_goods_prices_import_flat \
  --txt nmids.txt \
  --api-key "$WB_TOKEN" \
  --out-sqlite prices.db


# автоматический режим по всем организациям, запись в finmodel.db
python -m finmodel.scripts.wb_goods_prices_import_flat

# дополнительные выводы

python -m finmodel.scripts.wb_goods_prices_import_flat \
  --out-odbc "DSN=FinModel"
```

## Docker
Docker-образ позволяет запускать любой скрипт импорта в изолированном окружении.

Соберите образ:

```bash
docker build -t finmodel .
```

Запустите импорт, смонтировав файл конфигурации в контейнер:

```bash
docker run --rm \
  -v $(pwd)/config.yml:/app/config.yml \
  -v $(pwd)/Настройки.xlsm:/app/Настройки.xlsm \
  -v $(pwd)/finmodel.db:/app/finmodel.db \
  finmodel
```

Замените скрипт с помощью переменной `FINMODEL_SCRIPT`:

```bash
docker run --rm -e FINMODEL_SCRIPT=finmodel.scripts.orderswb_import_flat \
  -v $(pwd)/config.yml:/app/config.yml \
  -v $(pwd)/Настройки.xlsm:/app/Настройки.xlsm \
  -v $(pwd)/finmodel.db:/app/finmodel.db \
  finmodel
```

Флаг `--full-reload` перезагружает данные заново, начиная с `ПериодНачало`:

```bash
docker run --rm -e FINMODEL_SCRIPT=finmodel.scripts.orderswb_import_flat \
  -v $(pwd)/config.yml:/app/config.yml \
  -v $(pwd)/Настройки.xlsm:/app/Настройки.xlsm \
  -v $(pwd)/finmodel.db:/app/finmodel.db \
  finmodel -- --full-reload
```

Чтобы использовать PostgreSQL вместо SQLite, поднимите приложение через `docker-compose`:

```yaml
version: "3.8"
services:
  app:
    build: .
    volumes:
      - ./config.yml:/app/config.yml:ro
      - ./Настройки.xlsm:/app/Настройки.xlsm:ro
      - ./finmodel.db:/app/finmodel.db
    environment:
      - FINMODEL_SCRIPT=finmodel.scripts.saleswb_import_flat
    depends_on:
      - db
  db:
    image: postgres:15
    environment:
      POSTGRES_USER: finmodel
      POSTGRES_PASSWORD: finmodel
      POSTGRES_DB: finmodel
    volumes:
      - pgdata:/var/lib/postgresql/data
volumes:
  pgdata:
```

Запустите стек:

```bash
docker-compose up --build
```

## Планирование
Для регулярного импорта данных запланируйте выполнение контейнера.

### Linux (cron)
Выполните `crontab -e` и добавьте строку:

```
0 3 * * * docker run --rm \
  -v /path/to/config.yml:/app/config.yml \
  -v /path/to/Настройки.xlsm:/app/Настройки.xlsm \
  -v /path/to/finmodel.db:/app/finmodel.db \
  finmodel
```

Этот пример запускает импорт каждый день в 03:00.

### Windows (Планировщик задач)


Для автоматического запуска используйте скрипт:

```powershell
devtools\setup_scheduler.ps1

```

Скрипт регистрирует ежедневные, еженедельные и повторяющиеся задания через `schtasks`. Время запуска и учётные данные можно передать параметрами. Чтобы удалить задачи, выполните:

```powershell
devtools\setup_scheduler.ps1 -Cleanup
```


Для автоматической регистрации задачи используйте скрипт `devtools\setup_scheduler.ps1`:

```powershell
# ежедневный запуск в 03:00 под текущей учётной записью
devtools\setup_scheduler.ps1 -DailyTime 03:00

# запуск от имени другого пользователя
devtools\setup_scheduler.ps1 -DailyTime 06:30 -Username "DOMAIN\user" -Password "Secret"
```

Параметр `-DailyTime` задаёт время запуска (часы:минуты). Через `-Username` и `-Password`
можно указать учётную запись. По умолчанию задача создаётся под текущим пользователем
с именем `FinmodelImport`.

Удалить задачу можно командами:

```powershell
# через скрипт
devtools\setup_scheduler.ps1 -Remove

# напрямую через Планировщик задач
schtasks /Delete /TN "FinmodelImport" /F
```


## Разработка
- Следуйте инструкциям в `AGENTS.md` по стандартам кодирования и тестированию
  (русская версия доступна в `AGENTS.ru.md`).
- Убедитесь, что новые скрипты содержат информативные docstring и защищённую точку входа `main`.
- Перед коммитом выполните `python -m compileall -q .` для проверки синтаксиса.
- Тесты в каталоге `tests/` повторяют структуру пакета `finmodel` (например, `tests/utils/`).

## Логи
Все скрипты пишут логи в файл `log/finmodel.log`. Создайте каталог `log/`, если его
нет, чтобы сохранять собранные логи. База данных `finmodel.db` и каталог `log/`
создаются во время работы и не должны коммититься в репозиторий.

## Лицензия
Укажите лицензию проекта при необходимости.

