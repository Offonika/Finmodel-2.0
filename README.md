# Finmodel 2.0

Python-утилиты для импорта и анализа финансовых данных с маркетплейса Wildberries.

## Возможности
- Импорт рекламных кампаний, заказов, остатков, тарифов и других данных с помощью отдельных скриптов.
- Сохранение загруженных записей в `finmodel.db` для последующего анализа.

## Установка
Запустите скрипт для автоматической настройки, он установит зависимости и создаст базу данных `finmodel.db`:

- Linux/macOS:
  ```bash
  scripts/setup.sh
  ```
- Windows:
  ```powershell
  scripts\setup.ps1
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
3. Скопируйте `config.example.yml` в `config.yml` и заполните путь к базе данных, диапазоны дат и токены. Переменные окружения с такими же ключами переопределяют значения файла.
4. Запускайте нужный скрипт через модуль пакета или установленную консольную команду, например:
   ```bash
   python -m finmodel.scripts.saleswb_import_flat
   # либо после установки пакета:
   saleswb_import_flat
   ```

## Docker
Docker-образ позволяет запускать любой скрипт импорта в изолированном окружении.

Соберите образ:

```bash
docker build -t finmodel .
```

Запустите импорт, смонтировав файл конфигурации в контейнер:

```bash
docker run --rm -v $(pwd)/config.yml:/app/config.yml finmodel
```

Замените скрипт с помощью переменной `FINMODEL_SCRIPT`:

```bash
docker run --rm -e FINMODEL_SCRIPT=finmodel.scripts.orderswb_import_flat \
  -v $(pwd)/config.yml:/app/config.yml finmodel
```

Чтобы использовать PostgreSQL вместо SQLite, поднимите приложение через `docker-compose`:

```yaml
version: "3.8"
services:
  app:
    build: .
    volumes:
      - ./config.yml:/app/config.yml:ro
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
0 3 * * * docker run --rm -v /path/to/config.yml:/app/config.yml finmodel
```

Этот пример запускает импорт каждый день в 03:00.

### Windows (Планировщик задач)
Создайте простую задачу, которая выполняет:

```
docker run --rm -v C:\path\to\config.yml:/app/config.yml finmodel
```

Настройте триггер с нужным интервалом.

## Разработка
- Следуйте инструкциям в `AGENTS.md` по стандартам кодирования и тестированию.
- Убедитесь, что новые скрипты содержат информативные docstring и защищённую точку входа `main`.
- Перед коммитом выполните `python -m compileall -q .` для проверки синтаксиса.

## Логи
Все скрипты пишут логи в файл `log/finmodel.log`. Создайте каталог `log/`, если его нет, чтобы сохранять собранные логи.

## Лицензия
Укажите лицензию проекта при необходимости.

