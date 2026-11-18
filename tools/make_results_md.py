"""Generate docs/RESULTS.md from results/*.json (no number is typed by hand).

Every value in the table is read from a JSON key and the key is printed next to it, so the report can cite
it. A missing file or key shows as "chưa chạy". Run after merging new Kaggle results:
    python tools/make_results_md.py
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / "results"
MISSING = "chưa chạy"


def load(name: str) -> dict | None:
    p = RES / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else None


def get(d, path: str):
    cur = d
    for part in path.split("."):
        if cur is None:
            return None
        if isinstance(cur, list):
            cur = cur[int(part)] if part.lstrip("-").isdigit() and -len(cur) <= int(part) < len(cur) else None
        elif isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def fmt(v) -> str:
    if v is None:
        return MISSING
    if isinstance(v, bool):
        return "có" if v else "không"
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, float):
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        return f"{v:.4g}"
    if isinstance(v, list):
        return "[" + ", ".join(fmt(x) for x in v) + "]"
    return str(v)


# (section, file, [(label, json path)])
ROWS = [
    (
        "Dữ liệu (REES46)",
        "01_ingest_eda",
        [
            ("Sự kiện", "n_events"),
            ("Người dùng (chính xác)", "overview.n_users"),
            ("Sản phẩm", "overview.n_products"),
            ("Phiên", "overview.n_sessions"),
            ("view / cart / purchase", None),
            ("Thiếu category_code", "overview.null_category_code"),
            ("Thiếu brand", "overview.null_brand"),
            ("CSV -> Parquet (s)", "timings_s.csv_to_parquet"),
            ("Tỷ lệ phiên có mua", "session_stats.session_conversion"),
        ],
    ),
    (
        "RQ1 - Sketch luồng",
        "02_streaming_sketches",
        [
            ("CMS top-k overlap", "cms.top_k_overlap"),
            ("CMS trong biên eps*N", "cms.within_bound_fraction"),
            ("CMS bộ nhớ (MB)", "cms.memory_mb"),
            ("CMS phân tán = tuần tự", "cms.distributed_equals_sequential"),
            ("Bloom FPR thực nghiệm (mục tiêu 0.01)", "bloom.empirical_fpr"),
            ("Bloom bộ nhớ (MB)", "bloom.memory_mb"),
            ("DGIM sai số tối đa (lý thuyết <= 0.5)", "dgim.max_rel_err"),
            ("DGIM sai số TB", "dgim.mean_rel_err"),
            ("HLL rsd=0.01: sai số", "hll.2.rel_err"),
            ("Distinct users chính xác", "exact_distinct_users"),
        ],
    ),
    (
        "RQ2 - Tập phổ biến (Instacart)",
        "03_frequent_itemsets",
        [
            ("Số giỏ", "instacart.n_baskets"),
            ("Support 0.001: cặp phổ biến", "instacart.benchmark.-1.frequent_pairs"),
            ("Support 0.001: ứng viên A-Priori", "instacart.benchmark.-1.apriori_candidates"),
            ("Support 0.001: ứng viên PCY", "instacart.benchmark.-1.pcy_candidates"),
            ("Support 0.001: PCY giảm ứng viên", "instacart.benchmark.-1.pcy_candidate_reduction"),
            ("A-Priori = PCY", "instacart.benchmark.-1.same_result"),
            ("Thời gian A-Priori / PCY / FP-Growth (s)", None),
            ("FP-Growth: số itemset", "instacart.n_itemsets"),
            ("Luật lift > 2", "instacart.n_rules_lift_gt_2"),
            ("REES46 giỏ phiên / luật", None),
        ],
    ),
    (
        "RQ3 - LSH",
        "04_lsh_similarity",
        [
            ("Số tập sản phẩm", "n_product_sets"),
            ("Tập đánh giá", "eval_subset"),
            ("Cặp đúng (Jaccard >= ngưỡng)", "n_true_pairs_eval"),
            ("Ngưỡng", "threshold"),
            ("LSH 128 hash: recall / precision", None),
            ("MLlib MinHashLSH: recall", "lsh_vs_naive.-1.recall"),
            ("Mũ thực nghiệm naive", "naive_exponent"),
            ("LSH toàn bộ: số cặp", "lsh_all.pairs"),
            ("LSH toàn bộ: thời gian (s)", "lsh_all.seconds"),
            ("Naive ngoại suy (giờ)", "lsh_all.naive_extrapolated_hours"),
        ],
    ),
    (
        "RQ4 - PageRank / HITS",
        "05_link_analysis",
        [
            ("Nút / cạnh", None),
            ("Spearman(PR, views)", "pagerank.spearman.rank.views"),
            ("Spearman(PR, purchases)", "pagerank.spearman.rank.purchases"),
            ("Top-1000 trùng views", "pagerank.top1000_overlap_with_views"),
            ("Topic mass: toàn cục -> cá nhân hóa", None),
            ("PageRank thời gian (s)", "pagerank.seconds"),
            ("HITS thời gian (s)", "hits_seconds"),
        ],
    ),
    (
        "RQ5 - Hệ gợi ý",
        "06_recommender",
        [
            ("Cutoff / validation cutoff", None),
            ("User đánh giá / user chung", None),
            ("NDCG@10 popularity", "models.popularity.ndcg@10"),
            ("NDCG@10 item-kNN", "models.item_knn.ndcg@10"),
            ("NDCG@10 ALS", "models.als_best.ndcg@10"),
            ("Recall@10 ALS", "models.als_best.recall@10"),
            ("ALS chọn (rank, alpha)", None),
            ("ALS - popularity: diff [CI95]", None),
            ("ALS - item-kNN: diff [CI95]", None),
            ("item-kNN - popularity: diff [CI95]", None),
        ],
    ),
    (
        "Phụ - Phân khúc và phễu",
        "07_segmentation_funnel",
        [
            ("Người mua", "rfm.n_buyers"),
            ("k tốt nhất", "rfm.best_k"),
            ("Tỷ lệ mua lại Instacart", "instacart.reorder_rate"),
        ],
    ),
    (
        "RQ6 - Khả năng mở rộng",
        "08_scalability_benchmark",
        [
            ("Base fraction", "base_fraction"),
            ("Mũ: aggregate", "exponents.t_aggregate"),
            ("Mũ: FP-Growth", "exponents.t_fpgrowth"),
            ("Mũ: PageRank 5 vòng", "exponents.t_pagerank5"),
            ("Mũ: ALS 3 vòng", "exponents.t_als3"),
            ("Speed-up 4 lõi: PageRank", "speedup.-1.speedup_pagerank5"),
            ("Speed-up 4 lõi: ALS", "speedup.-1.speedup_als3"),
            ("Speed-up 4 lõi: aggregate", "speedup.-1.speedup_aggregate"),
        ],
    ),
]


def composite(name: str, label: str, d: dict) -> tuple[str, str]:
    """Values that combine several keys; returns (value, source keys)."""

    def ci(k):
        s = get(d, f"significance_ndcg@10_paired_bootstrap.{k}")
        if not s:
            return MISSING
        return f"{s['diff']:+.4f} [{s['ci95'][0]:+.4f}, {s['ci95'][1]:+.4f}], p={s['p_le_0']:.3g}"

    table = {
        "view / cart / purchase": (
            lambda: " / ".join(fmt(get(d, f"overview.event_types.{t}")) for t in ("view", "cart", "purchase")),
            "overview.event_types",
        ),
        "Thời gian A-Priori / PCY / FP-Growth (s)": (
            lambda: " / ".join(
                fmt(get(d, f"instacart.benchmark.-1.{k}")) for k in ("t_apriori_s", "t_pcy_s", "t_fpgrowth_s")
            ),
            "instacart.benchmark[-1].t_*",
        ),
        "REES46 giỏ phiên / luật": (
            lambda: f"{fmt(get(d, 'rees46_sessions.n_baskets'))} / {fmt(get(d, 'rees46_sessions.n_rules'))}",
            "rees46_sessions",
        ),
        "LSH 128 hash: recall / precision": (
            lambda: f"{fmt(get(d, 'lsh_vs_naive.1.recall'))} / {fmt(get(d, 'lsh_vs_naive.1.precision'))}",
            "lsh_vs_naive[1]",
        ),
        "Nút / cạnh": (lambda: f"{fmt(get(d, 'graph.nodes'))} / {fmt(get(d, 'graph.edges'))}", "graph"),
        "Topic mass: toàn cục -> cá nhân hóa": (
            lambda: (
                f"{fmt(get(d, 'topic_sensitive.mass_global'))} -> "
                f"{fmt(get(d, 'topic_sensitive.mass_personalised'))} "
                f"({get(d, 'topic_sensitive.topic')})"
            ),
            "topic_sensitive",
        ),
        "Cutoff / validation cutoff": (lambda: f"{get(d, 'cutoff')} / {get(d, 'val_cutoff')}", "cutoff, val_cutoff"),
        "User đánh giá / user chung": (
            lambda: f"{fmt(get(d, 'n_eval_users'))} / {fmt(get(d, 'n_common_users'))}",
            "n_eval_users, n_common_users",
        ),
        "ALS chọn (rank, alpha)": (
            lambda: f"({fmt(get(d, 'models.als_best.rank'))}, {fmt(get(d, 'models.als_best.alpha'))})",
            "models.als_best.rank/alpha",
        ),
        "ALS - popularity: diff [CI95]": (
            lambda: ci("als_best_vs_popularity"),
            "significance_ndcg@10_paired_bootstrap",
        ),
        "ALS - item-kNN: diff [CI95]": (lambda: ci("als_best_vs_item_knn"), "significance_ndcg@10_paired_bootstrap"),
        "item-kNN - popularity: diff [CI95]": (
            lambda: ci("item_knn_vs_popularity"),
            "significance_ndcg@10_paired_bootstrap",
        ),
    }
    fn, src = table[label]
    try:
        return fn(), src
    except (TypeError, KeyError):
        return MISSING, src


def main() -> int:
    lines = [
        "# Kết quả",
        "",
        "> Sinh tự động bằng `python tools/make_results_md.py` từ `results/*.json` (lần chạy Kaggle thật).",
        "> Không sửa tay. Cột *Nguồn* là khóa JSON để trích dẫn trong báo cáo. `FRACTION` = tỷ lệ người dùng.",
        "",
    ]
    for section, name, rows in ROWS:
        d = load(name)
        lines.append(f"## {section} - `results/{name}.json`")
        if d is None:
            lines += ["", f"_{MISSING}_", ""]
            continue
        if name == "08_scalability_benchmark" and "base_fraction" not in d:
            lines += [
                "",
                "> **Cảnh báo:** file này từ lần chạy trước bản sửa PR #48: phần theo kích thước dữ liệu "
                "(mũ) **không hợp lệ**; chỉ phần speed-up theo lõi dùng được.",
            ]
        lines += [
            "",
            f"FRACTION = **{fmt(d.get('fraction', d.get('base_fraction')))}**, chạy lúc `{d.get('created_utc', '?')}`",
            "",
            "| Chỉ số | Giá trị | Nguồn |",
            "|---|---|---|",
        ]
        for label, path in rows:
            if path is None:
                val, src = composite(name, label, d)
            else:
                val, src = fmt(get(d, path)), path
            lines.append(f"| {label} | {val} | `{src}` |")
        lines.append("")
    hv = RES / "hadoop" / "hadoop_vs_pipe.json"
    lines.append("## Hadoop 3.3.6 thật - `results/hadoop/hadoop_vs_pipe.json`")
    if hv.exists():
        h = json.loads(hv.read_text())
        lines += [
            "",
            f"Dữ liệu: **{h.get('data')}** ({h.get('users')} user) - kiểm chứng cách chạy, không phải số "
            "liệu dữ liệu thật.",
            "",
            "| Job | Khóa Hadoop / pipe | Tổng Hadoop / pipe | Trùng khớp |",
            "|---|---|---|---|",
        ]
        for job in ("event_counts", "pair_counts"):
            j = h[job]
            lines.append(
                f"| {job} | {fmt(j['keys_hadoop'])} / {fmt(j['keys_pipe'])} | "
                f"{fmt(j['total_hadoop'])} / {fmt(j['total_pipe'])} | {fmt(j['identical'])} |"
            )
    else:
        lines += ["", f"_{MISSING}_"]
    (ROOT / "docs" / "RESULTS.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("wrote docs/RESULTS.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
