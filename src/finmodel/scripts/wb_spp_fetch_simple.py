def main():
    #!/usr/bin/env python
    # coding: utf-8
    """
    Получаем priceU / salePriceU / sale (≈СПП) по списку nmId
    Сохраняем в CSV или выводим на экран.
    """

    import argparse
    import csv
    import sys
    import time
    from pathlib import Path

    import requests

    from finmodel.logger import get_logger

    API = "https://card.wb.ru/cards/v4/detail"
    DEST = -1257786  # универсальный регион РФ
    SPP = 30  # персональное СПП (если не нужен — поставьте 0)
    CHUNK = 100  # сколько nmId в одном запросе
    SLEEP = 0.5  # пауза между запросами

    def fetch(ids):
        ids = ";".join(ids)
        params = {"appType": 1, "curr": "rub", "dest": DEST, "nm": ids, "spp": SPP}
        r = requests.get(API, params=params, timeout=15)
        r.raise_for_status()
        return r.json().get("data", {}).get("products", [])

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    def main():
        logger = get_logger(__name__)
        ap = argparse.ArgumentParser()
        ap.add_argument(
            "nmids",
            nargs="+",
            help="список nmId (через пробел) или путь к .txt/.csv c колонкой nmId",
        )
        ap.add_argument("--out", help="путь к .csv для сохранения")
        args = ap.parse_args()

        # читаем nmId: либо переданы числа, либо указан файл
        if len(args.nmids) == 1 and Path(args.nmids[0]).is_file():
            with open(args.nmids[0], encoding="utf-8") as f:
                nmids = [line.strip().split(";")[0] for line in f if line.strip()]
        else:
            nmids = args.nmids

        rows = []
        for part in chunks(nmids, CHUNK):
            for p in fetch(part):
                rows.append(
                    {
                        "nmId": p["id"],
                        "priceU": p["priceU"],
                        "salePriceU": p["salePriceU"],
                        "sale(%)": p["sale"],
                        "price_rub": round(p["priceU"] / 100, 2),
                        "salePrice_rub": round(p["salePriceU"] / 100, 2),
                    }
                )
            time.sleep(SLEEP)

        if args.out:
            keys = (
                rows[0].keys()
                if rows
                else ["nmId", "priceU", "salePriceU", "sale(%)", "price_rub", "salePrice_rub"]
            )
            with open(args.out, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=keys)
                w.writeheader()
                w.writerows(rows)
            logger.info("Сохранено %s строк → %s", len(rows), args.out)
        else:
            for r in rows:  # печать в консоль
                logger.info("%s", r)

    if __name__ == "__main__":
        main()


if __name__ == "__main__":
    main()
