# -*- coding: utf-8 -*-
import random
import sqlite3
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from finmodel.logger import get_logger, setup_logging
from finmodel.utils.paths import get_db_path
from finmodel.utils.settings import find_setting, load_organizations


def main() -> None:
    setup_logging()
    logger = get_logger(__name__)

    # ---------- Paths ----------
    db_path = get_db_path()
    logger.info("DB: %s", db_path)

    org_sheet = find_setting("ORG_SHEET", default="НастройкиОрганизаций")
    settings_sheet = find_setting("SETTINGS_SHEET", default="Настройки")
    logger.info("Using organizations sheet %s", org_sheet)
    logger.info("Using settings sheet %s", settings_sheet)

    # ---------- Period (last 7 days via interval) ----------
    today = datetime.now().date()
    begin = (today - timedelta(days=6)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    logger.info("Интервал: %s .. %s", begin, end)

    # ---------- WB endpoints & constants ----------
    URL_COUNT = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
    URL_FULLSTATS = "https://advert-api.wildberries.ru/adv/v2/fullstats"
    HEADERS_BASE = {"Content-Type": "application/json"}

    # Статусы: -1 удаляется, 4 готова, 7 завершено, 8 отказался, 9 активно, 11 пауза
    ALLOWED_STATUS = {"7", "9", "11"}
    # Типы: 8 — авто, 9 — аукцион (4/5/6/7 устаревшие)
    ALLOWED_TYPES = {"8", "9"}

    RECENT_CHANGE_DAYS = 7  # ужесточил с 14 до 7, чтобы меньше 400
    REQ_INTERVAL_SEC = 65  # минимум между ЛЮБЫМИ POST к fullstats
    TABLE = "AdvCampaignsFullStats"

    # ---------- Helpers ----------
    def normalize_day(s: str) -> str:
        try:
            return pd.to_datetime(s).strftime("%Y-%m-%d")
        except Exception:
            return str(s)

    def prepare_request_body_interval(ids, begin, end):
        return [{"id": int(cid), "interval": {"begin": begin, "end": end}} for cid in ids]

    def chunked(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    def interval_overlap(a_begin: str, a_end: str, b_begin: str, b_end: str) -> bool:
        try:
            ab = pd.to_datetime(a_begin).date()
            ae = pd.to_datetime(a_end).date()
            bb = pd.to_datetime(b_begin).date()
            be = pd.to_datetime(b_end).date()
            return not (ae < bb or be < ab)
        except Exception:
            return True

    # простой троттлер для всех POST
    _last_post_ts = 0.0

    def throttle_fullstats():
        nonlocal _last_post_ts
        now = time.monotonic()
        wait = _last_post_ts + REQ_INTERVAL_SEC - now
        if wait > 0:
            time.sleep(wait + random.uniform(0, 3))
            now = time.monotonic()
        _last_post_ts = now

    # ---------- Orgs/tokens ----------
    df_orgs = load_organizations(sheet=org_sheet)
    if df_orgs.empty:
        logger.error("Настройки.xlsm не содержит организаций с токенами.")
        raise SystemExit(1)

    # ---------- DB & target table ----------
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        org_id TEXT,
        Организация TEXT,
        advertId TEXT,
        date TEXT,
        appType TEXT,
        nmId TEXT,
        nmName TEXT,
        views TEXT,
        clicks TEXT,
        ctr TEXT,
        cpc TEXT,
        sum TEXT,
        atbs TEXT,
        orders TEXT,
        cr TEXT,
        shks TEXT,
        sum_price TEXT,
        avg_position TEXT,
        LoadDate TEXT,
        PRIMARY KEY (org_id, advertId, date, appType, nmId)
    );
    """
    )
    conn.commit()

    cur.execute(
        """CREATE INDEX IF NOT EXISTS idx_AdvCampDet_org_ad ON AdvCampaignsDetailsFlat(org_id, advertId)"""
    )
    conn.commit()

    # ---------- Local filter (no IN (...)) ----------
    def get_local_eligible_ids(conn, org_id: str, ids_from_api, begin: str, end: str):
        """
        Берём из локальной таблицы AdvCampaignsDetailsFlat:
          status ∈ ALLOWED_STATUS, type ∈ ALLOWED_TYPES
          и (start..end пересекается с окном) ИЛИ (changeTime за RECENT_CHANGE_DAYS)
        Пересекаем с ids_from_api. Если по org_id нет строк — возвращаем пусто.
        """
        if not ids_from_api:
            return []

        q = """
            SELECT advertId, status, type, startTime, endTime, changeTime
            FROM AdvCampaignsDetailsFlat
            WHERE org_id = ?
        """
        try:
            rows = conn.execute(q, (org_id,)).fetchall()
        except Exception:
            rows = []

        ids_api_set = set(int(x) for x in ids_from_api)
        cutoff = datetime.now().date() - timedelta(days=RECENT_CHANGE_DAYS)

        eligible = set()
        for advertId, status, typ, startTime, endTime, changeTime in rows:
            try:
                cid = int(advertId)
            except Exception:
                continue
            if cid not in ids_api_set:
                continue
            if str(status) not in ALLOWED_STATUS:
                continue
            if str(typ) not in ALLOWED_TYPES:
                continue

            keep = False
            st = normalize_day(startTime) if startTime else None
            en = normalize_day(endTime) if endTime else None
            if st and en and interval_overlap(st, en, begin, end):
                keep = True
            if not keep and changeTime:
                try:
                    if pd.to_datetime(changeTime).date() >= cutoff:
                        keep = True
                except Exception:
                    pass

            if keep:
                eligible.add(cid)

        return sorted(eligible)

    # ---------- API list ----------
    def get_campaign_ids_from_api(token: str):
        headers = HEADERS_BASE.copy()
        headers["Authorization"] = token
        try:
            r = requests.get(URL_COUNT, headers=headers, timeout=60)
            if r.status_code != 200:
                logger.warning("  [count] HTTP %s: %s", r.status_code, r.text[:300])
                return []
            data = r.json() or {}
        except Exception as e:
            logger.warning("  [count] error: %s", e)
            return []

        ids = []
        for grp in data.get("adverts", []) or []:
            for it in grp.get("advert_list", []) or []:
                cid = it.get("advertId")
                if cid is not None:
                    ids.append(int(cid))
        return sorted(set(ids))

    # ---------- fullstats with global throttle & split ----------
    def request_fullstats_batch(headers, ids_batch, begin, end, preview=False):
        payload_all = []

        def _post(ids_sub):
            throttle_fullstats()  # <= гарантируем 1 POST/≈минуту на продавца
            body = prepare_request_body_interval(ids_sub, begin, end)
            if preview:
                logger.info("    POST preview: %s (+%s ids)", body[:1], max(0, len(body) - 1))
            return requests.post(URL_FULLSTATS, headers=headers, json=body, timeout=120)

        def _handle(ids_sub):
            nonlocal payload_all
            if not ids_sub:
                return
            resp = _post(ids_sub)

            if resp.status_code == 429:
                # глобальный лимитер WB — подождём подольше и повторим один раз
                sleep_s = REQ_INTERVAL_SEC + 10
                logger.warning("    429 Too Many Requests. Жду %s сек…", sleep_s)
                time.sleep(sleep_s)
                resp = _post(ids_sub)

            if resp.status_code == 200:
                data = resp.json() or []
                if isinstance(data, list):
                    payload_all.extend(data)
                else:
                    logger.warning("    Неожиданный формат ответа; пропущено.")
                return

            if resp.status_code == 400:
                # дробим, но это тоже пойдёт через глобальный троттлер (по одному POST в ~минуту)
                if len(ids_sub) == 1:
                    logger.warning("    400 на кампанию %s — пропускаю.", ids_sub[0])
                    return
                mid = max(1, len(ids_sub) // 2)
                _handle(ids_sub[:mid])
                _handle(ids_sub[mid:])
                return

            if resp.status_code == 401:
                logger.error("    401 Unauthorized — токен/права. Пропускаю организацию.")
                raise PermissionError

            logger.warning("    HTTP %s: %s (пропущено)", resp.status_code, resp.text[:500])

        _handle(ids_batch)
        return payload_all

    # ---------- Main ----------
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_rows = 0

    for _, org in df_orgs.iterrows():
        org_id = str(org["id"])
        org_name = str(org["Организация"])
        token = str(org["Token_WB"]).strip()

        logger.info("→ Организация: %s (ID=%s)", org_name, org_id)

        ids_api = get_campaign_ids_from_api(token)
        logger.info("  Всего кампаний с API: %s", len(ids_api))

        ids_eligible = get_local_eligible_ids(conn, org_id, ids_api, begin, end)
        logger.info(
            "  После локального фильтра (status∈%s, type∈%s, даты/изменения): %s",
            ALLOWED_STATUS,
            ALLOWED_TYPES,
            len(ids_eligible),
        )

        if not ids_eligible:
            logger.warning("  Нет подходящих кампаний для запроса fullstats.")
            continue

        headers = HEADERS_BASE.copy()
        headers["Authorization"] = token

        for batch_num, ids_batch in enumerate(chunked(ids_eligible, 100), start=1):
            logger.info(
                "  ▶ fullstats: батч %s (ids=%s), глобальный лимит ~1 POST/мин…",
                batch_num,
                len(ids_batch),
            )
            try:
                payload = request_fullstats_batch(
                    headers, ids_batch, begin, end, preview=(batch_num == 1)
                )

                rows = []
                for camp in payload:
                    advertId = str(camp.get("advertId", ""))
                    days = camp.get("days", []) or []
                    booster = camp.get("boosterStats", []) or []

                    booster_idx = {}
                    for b in booster:
                        try:
                            b_date = normalize_day(b.get("date", ""))
                            b_nm = str(b.get("nm", ""))
                            booster_idx[(b_date, b_nm)] = str(b.get("avg_position", ""))
                        except Exception:
                            pass

                    for d in days:
                        d_date = normalize_day(d.get("date", ""))
                        # daily
                        rows.append(
                            [
                                org_id,
                                org_name,
                                advertId,
                                d_date,
                                "",
                                "",
                                "",
                                str(d.get("views", "")),
                                str(d.get("clicks", "")),
                                str(d.get("ctr", "")),
                                str(d.get("cpc", "")),
                                str(d.get("sum", "")),
                                str(d.get("atbs", "")),
                                str(d.get("orders", "")),
                                str(d.get("cr", "")),
                                str(d.get("shks", "")),
                                str(d.get("sum_price", "")),
                                "",
                                now_str,
                            ]
                        )
                        # apps
                        for app in d.get("apps", []) or []:
                            appType = str(app.get("appType", ""))
                            rows.append(
                                [
                                    org_id,
                                    org_name,
                                    advertId,
                                    d_date,
                                    appType,
                                    "",
                                    "",
                                    str(app.get("views", "")),
                                    str(app.get("clicks", "")),
                                    str(app.get("ctr", "")),
                                    str(app.get("cpc", "")),
                                    str(app.get("sum", "")),
                                    str(app.get("atbs", "")),
                                    str(app.get("orders", "")),
                                    str(app.get("cr", "")),
                                    str(app.get("shks", "")),
                                    str(app.get("sum_price", "")),
                                    "",
                                    now_str,
                                ]
                            )
                            for nm in app.get("nm", []) or []:
                                nmId = str(nm.get("nmId", ""))
                                nmName = str(nm.get("name", ""))
                                avg_pos = booster_idx.get((d_date, nmId), "")
                                rows.append(
                                    [
                                        org_id,
                                        org_name,
                                        advertId,
                                        d_date,
                                        appType,
                                        nmId,
                                        nmName,
                                        str(nm.get("views", "")),
                                        str(nm.get("clicks", "")),
                                        str(nm.get("ctr", "")),
                                        str(nm.get("cpc", "")),
                                        str(nm.get("sum", "")),
                                        str(nm.get("atbs", "")),
                                        str(nm.get("orders", "")),
                                        str(nm.get("cr", "")),
                                        str(nm.get("shks", "")),
                                        str(nm.get("sum_price", "")),
                                        avg_pos,
                                        now_str,
                                    ]
                                )

                if rows:
                    # проверка длины
                    bad = next((i for i, r in enumerate(rows) if len(r) != 19), None)
                    if bad is not None:
                        raise RuntimeError(
                            f"pack error: row#{bad} len={len(rows[bad])}, expected 19"
                        )

                    cols = (
                        "org_id, Организация, advertId, date, appType, nmId, nmName, "
                        "views, clicks, ctr, cpc, sum, atbs, orders, cr, shks, sum_price, avg_position, LoadDate"
                    )
                    ph = ",".join(["?"] * 19)
                    cur.executemany(f"INSERT OR REPLACE INTO {TABLE} ({cols}) VALUES ({ph})", rows)
                    conn.commit()
                    total_rows += len(rows)
                    logger.info("    ✅ вставлено %s строк (итого: %s)", len(rows), total_rows)
                else:
                    logger.warning("    пустой набор данных для батча.")

            except PermissionError:
                break
            except Exception as e:
                logger.warning("    Ошибка запроса/вставки: %s", e)

    logger.info("✅ Готово. Добавлено/обновлено строк: %s в %s", total_rows, TABLE)
    conn.close()


if __name__ == "__main__":
    main()
