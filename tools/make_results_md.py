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
        return f"{v:,}" if abs(v) >= 10_000 else str(v)
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
            ("Mô hình được chọn (luật đặt trước, theo validation)", "selected"),
            ("Seed", "seeds"),
        ],
    ),
    (
        "Phụ - Phân khúc và phễu",
        "07_segmentation_funnel",
        [
            ("Người mua", "rfm.n_buyers"),
            ("Thuật toán được chọn", "rfm.selected_algo"),
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


ROWS.append(
    (
        "Hadoop MapReduce trên dữ liệu thật",
        "09_hadoop_real",
        [
            ("Engine", "engine"),
            ("Dữ liệu vào", "inputs"),
            ("Kết quả Hadoop = Spark", "identical"),
            ("Đếm sự kiện: số khóa / khóa lệch", None),
            ("Đếm cặp: số khóa / khóa lệch", None),
            ("Thời gian Hadoop: đếm / giỏ+cặp (s)", None),
            ("Thời gian Spark: đếm / cặp (s)", None),
        ],
    )
)

ROWS.append(
    (
        "Gợi ý: mở rộng lưới history và ALS (ADR-0006)",
        "10_recsys_grid_extension",
        [
            ("Cổng tái tạo qua", "gate.passed"),
            ("Mô hình được chọn (luật đặt trước, theo validation)", "selected"),
            ("Cấu hình ALS tốt nhất", "best_config.als"),
            ("Half-life history tốt nhất (ngày)", "best_config.history.half_life_days"),
            ("Vẫn ở biên lưới", "at_grid_edge"),
            ("User chung (history và ALS)", "n_common_users"),
            ("Verdict: history_als / history / ALS", "verdicts.key_pairs"),
        ],
    )
)

# extra tables rendered from lists of records: (file, json path, title, columns)
TABLES = [
    (
        "02_streaming_sketches",
        "cms_seeds",
        "Count-Min theo seed",
        ["seed", "never_under", "within_bound", "mean_abs_err"],
    ),
    (
        "02_streaming_sketches",
        "hll_salted",
        "HyperLogLog: phân bố sai số qua các salt",
        ["rsd", "mean_rel_err", "sd_rel_err", "mean_abs_rel_err", "within_2rsd"],
    ),
    (
        "04_lsh_similarity",
        "lsh_eval_summary",
        "LSH: recall theo tập đánh giá, ngưỡng, phương pháp (qua các seed)",
        ["subset", "threshold", "method", "n_true", "recall_mean", "recall_min", "precision_min"],
    ),
    (
        "06_recommender",
        "validation_grid",
        "Validation: mọi cấu hình (NDCG@10, popularity bù user thiếu)",
        ["family", "params", "val_ndcg@10", "val_recall@10", "fit_s"],
    ),
    (
        "06_recommender",
        "test_by_model_seed",
        "Test (một lần) trên user chung, theo mô hình và seed",
        ["model", "seed", "ndcg@10", "recall@10", "precision@10", "map@10", "n_users"],
    ),
    (
        "06_recommender",
        "significance_selected_vs_others",
        "Mô hình được chọn so với từng mô hình khác (bootstrap ghép cặp)",
        ["selected", "other", "seed", "diff", "ci95", "verdict"],
    ),
    (
        "10_recsys_grid_extension",
        "gate.rows",
        "Cổng tái tạo: cấu hình thắng cũ trên cùng validation",
        ["family", "params", "now", "run06", "diff", "ok"],
    ),
    (
        "10_recsys_grid_extension",
        "validation_grid",
        "Validation: lưới mở rộng",
        ["family", "params", "val_ndcg@10", "val_recall@10", "fit_s"],
    ),
    (
        "10_recsys_grid_extension",
        "significance_key_pairs",
        "Câu hỏi chính: bản trộn còn thắng ALS và history đã chỉnh kỹ hơn không",
        ["a", "b", "seed", "diff", "ci95", "verdict"],
    ),
    (
        "07_segmentation_funnel",
        "rfm.comparison",
        "Phân cụm: thuật toán x k (qua các seed)",
        ["algo", "k", "sil_mean", "sil_std", "degenerate_fits", "ari_between_seeds"],
    ),
]


def render_table(d, path, cols) -> list[str]:
    recs = get(d, path)
    if not isinstance(recs, list) or not recs:
        return []
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in recs:
        out.append(
            "| "
            + " | ".join(
                fmt(r.get(c))
                if not isinstance(r.get(c), dict)
                else ", ".join(f"{k}={fmt(v)}" for k, v in r.get(c).items())
                for c in cols
            )
            + " |"
        )
    return out


def composite(name: str, label: str, d: dict) -> tuple[str, str]:
    """Values that combine several keys; returns (value, source keys)."""

    def ci(k):
        s = get(d, f"significance_ndcg@10_paired_bootstrap.{k}")
        if not s:
            return MISSING
        return f"{s['diff']:+.4f} [{s['ci95'][0]:+.4f}, {s['ci95'][1]:+.4f}], p={s['p_le_0']:.3g}"

    table = {
        "Đếm sự kiện: số khóa / khóa lệch": (
            lambda: (
                f"{fmt(get(d, 'compare_event_counts.keys_hadoop'))} / "
                f"{fmt(get(d, 'compare_event_counts.mismatched_keys'))}"
            ),
            "compare_event_counts",
        ),
        "Đếm cặp: số khóa / khóa lệch": (
            lambda: (
                f"{fmt(get(d, 'compare_pair_counts.keys_hadoop'))} / "
                f"{fmt(get(d, 'compare_pair_counts.mismatched_keys'))}"
            ),
            "compare_pair_counts",
        ),
        "Thời gian Hadoop: đếm / giỏ+cặp (s)": (
            lambda: (
                f"{fmt(get(d, 'jobs.counts.seconds'))} / "
                f"{fmt((get(d, 'jobs.baskets_to_pairs.seconds') or 0) + (get(d, 'jobs.pair_counts.seconds') or 0))}"
            ),
            "jobs.*.seconds",
        ),
        "Thời gian Spark: đếm / cặp (s)": (
            lambda: f"{fmt(get(d, 'spark.counts_seconds'))} / {fmt(get(d, 'spark.pairs_seconds'))}",
            "spark",
        ),
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
        "Phân tầng theo danh mục: số tập / cặp đúng": (
            lambda: (
                f"{fmt(get(d, 'eval_category_stratified.n_sets'))} / "
                f"{fmt(get(d, 'eval_category_stratified.n_true_pairs'))}"
            ),
            "eval_category_stratified",
        ),
        "Phân tầng: LSH 128 hash recall [CI95]": (
            lambda: (
                f"{fmt(get(d, 'eval_category_stratified.lsh_vs_naive.1.recall'))} "
                f"{fmt(get(d, 'eval_category_stratified.lsh_vs_naive.1.recall_ci95'))}"
            ),
            "eval_category_stratified.lsh_vs_naive[1]",
        ),
        "Phân tầng: MLlib recall [CI95]": (
            lambda: (
                f"{fmt(get(d, 'eval_category_stratified.lsh_vs_naive.-1.recall'))} "
                f"{fmt(get(d, 'eval_category_stratified.lsh_vs_naive.-1.recall_ci95'))}"
            ),
            "eval_category_stratified.lsh_vs_naive[-1]",
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
        for tname, tpath, title, cols in TABLES:
            if tname == name:
                tab = render_table(d, tpath, cols)
                if tab:
                    lines += [f"**{title}** (`{tpath}`)", ""] + tab + [""]
    hv = RES / "hadoop" / "hadoop_vs_pipe.json"
    lines.append("## Hadoop 3.3.6 thật trên dữ liệu synthetic - `results/hadoop/hadoop_vs_pipe.json`")
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
