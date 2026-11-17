"""Generate a small synthetic dataset with the *same schema* as the real Kaggle data.

Used for CI, unit tests and quick local debugging (NOT for reporting results).
Latent "themes" give the data real structure (co-purchase groups, similar users) so that
FP-Growth, LSH, PageRank and ALS have something to find.

    python tools/make_synthetic.py --out data/sample --users 3000
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

THEMES = ["electronics.smartphone", "electronics.audio", "appliances.kitchen", "computers.notebook",
          "apparel.shoes", "furniture.living_room", "kids.toys", "auto.accessories"]


def make_rees46(out: Path, n_users: int, n_products: int, seed: int) -> None:
    rng = np.random.default_rng(seed)
    theme_of = rng.integers(0, len(THEMES), n_products)
    pop = 1.0 / np.arange(1, n_products + 1) ** 0.8
    price = np.round(rng.lognormal(4.5, 0.8, n_products), 2)
    brands = [f"brand{b}" for b in range(40)]
    brand_of = rng.integers(0, len(brands), n_products)
    by_theme = {t: np.where(theme_of == t)[0] for t in range(len(THEMES))}
    t_probs = {t: pop[idx] / pop[idx].sum() for t, idx in by_theme.items()}
    start = datetime(2019, 10, 1)
    rows = {"2019-Oct.csv": [], "2019-Nov.csv": []}
    sess_counter = 0
    for u in range(n_users):
        uid = 500_000_000 + u
        favs = rng.choice(len(THEMES), size=rng.integers(1, 3), replace=False)
        for _ in range(rng.integers(2, 9)):
            sess_counter += 1
            sid = f"{sess_counter:08x}-sess-{sess_counter % 997:03x}"
            th = int(rng.choice(favs)) if rng.random() < 0.85 else int(rng.integers(len(THEMES)))
            t0 = start + timedelta(seconds=int(rng.integers(0, 61 * 86400)))
            n_view = int(rng.integers(3, 13))
            prods = rng.choice(by_theme[th], size=n_view, p=t_probs[th])
            t = t0
            carted = []
            for p in prods:
                t += timedelta(seconds=int(rng.integers(5, 120)))
                rows_key = "2019-Oct.csv" if t.month == 10 else "2019-Nov.csv"
                rows[rows_key].append((t, "view", p, th, uid, sid))
                if rng.random() < 0.3:
                    t += timedelta(seconds=10)
                    rows[rows_key].append((t, "cart", p, th, uid, sid))
                    carted.append(p)
            for p in carted:
                if rng.random() < 0.5:
                    t += timedelta(seconds=30)
                    rows_key = "2019-Oct.csv" if t.month == 10 else "2019-Nov.csv"
                    if t.month in (10, 11):
                        rows[rows_key].append((t, "purchase", p, th, uid, sid))
    out.mkdir(parents=True, exist_ok=True)
    for fname, recs in rows.items():
        recs.sort(key=lambda r: r[0])
        with open(out / fname, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["event_time", "event_type", "product_id", "category_id", "category_code",
                        "brand", "price", "user_id", "user_session"])
            for t, et, p, th, uid, sid in recs:
                if t.month not in (10, 11):
                    continue
                cat_code = THEMES[th] if rng.random() > 0.1 else ""
                brand = brands[brand_of[p]] if rng.random() > 0.15 else ""
                w.writerow([t.strftime("%Y-%m-%d %H:%M:%S UTC"), et, 1_000_000 + int(p),
                            2_000_000_000 + th, cat_code, brand, price[p], uid, sid])


def make_instacart(out: Path, n_users: int, n_products: int, seed: int) -> None:
    rng = np.random.default_rng(seed + 1)
    n_aisles = 20
    aisle_of = rng.integers(1, n_aisles + 1, n_products)
    by_aisle = {a: np.where(aisle_of == a)[0] + 1 for a in range(1, n_aisles + 1)}
    out.mkdir(parents=True, exist_ok=True)

    def wr(name, header, rows):
        with open(out / name, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(rows)

    wr("aisles.csv", ["aisle_id", "aisle"], [(a, f"aisle {a}") for a in range(1, n_aisles + 1)])
    wr("departments.csv", ["department_id", "department"], [(d, f"dept {d}") for d in range(1, 5)])
    wr("products.csv", ["product_id", "product_name", "aisle_id", "department_id"],
       [(i + 1, f"product {i + 1}", int(aisle_of[i]), int(aisle_of[i]) % 4 + 1) for i in range(n_products)])
    orders, prior, train, oid = [], [], [], 0
    for u in range(1, n_users + 1):
        favs = rng.choice(np.arange(1, n_aisles + 1), size=3, replace=False)
        n_orders = int(rng.integers(4, 12))
        for k in range(1, n_orders + 1):
            oid += 1
            is_last = k == n_orders
            orders.append((oid, u, "train" if is_last else "prior", k, int(rng.integers(0, 7)),
                           int(rng.integers(6, 22)), "" if k == 1 else int(rng.integers(1, 30))))
            basket = set()
            for a in favs:
                if rng.random() < 0.7:
                    basket.update(rng.choice(by_aisle[int(a)], size=min(3, len(by_aisle[int(a)])), replace=False))
            target = train if is_last else prior
            for pos, p in enumerate(sorted(basket), start=1):
                target.append((oid, int(p), pos, int(rng.random() < 0.5)))
    wr("orders.csv", ["order_id", "user_id", "eval_set", "order_number", "order_dow",
                      "order_hour_of_day", "days_since_prior_order"], orders)
    wr("order_products__prior.csv", ["order_id", "product_id", "add_to_cart_order", "reordered"], prior)
    wr("order_products__train.csv", ["order_id", "product_id", "add_to_cart_order", "reordered"], train)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/sample")
    ap.add_argument("--users", type=int, default=3000)
    ap.add_argument("--products", type=int, default=800)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    make_rees46(Path(a.out), a.users, a.products, a.seed)
    make_instacart(Path(a.out), a.users // 2, a.products // 2, a.seed)
    print("synthetic data written to", a.out)
