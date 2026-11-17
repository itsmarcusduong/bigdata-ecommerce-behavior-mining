# %% [markdown]
# # 05 - Link analysis on the product navigation graph
# **Graph.** Directed edge `a -> b` (weight = #sessions) whenever a user views product `b` right
# after product `a` in the same session: the random surfer is a *shopper browsing the catalogue*.
#
# * **PageRank** (power iteration with dead-end handling, damping sweep, convergence)
# * **Topic-sensitive PageRank** personalised to one category
# * **HITS** hubs/authorities
# * Is rank just popularity?  -> Spearman correlation with views and purchases.
#
# Implementations are Spark-DataFrame joins (see `bdm/graph.py`), unit-tested against `networkx`.

# %%
import time

from pyspark.sql import functions as F

from bdm import config, eda, io
from bdm.graph import hits, pagerank, transition_edges
from bdm.results import save_fig, save_result

spark = config.get_spark("05-links", driver_memory=SPARK_MEM)
events = io.load_events(spark, FRACTION)
out = {"fraction": FRACTION}

# %% [markdown]
# ## 1. Build the graph

# %%
t0 = time.perf_counter()
edges = transition_edges(events, min_weight=2).cache()
n_edges = edges.count()
n_nodes = edges.select("src").union(edges.select("dst")).distinct().count()
t_graph = time.perf_counter() - t0
print(f"graph: {n_nodes:,} nodes, {n_edges:,} edges, built in {t_graph:.1f}s")
meta = eda.product_meta(events).cache()

# %% [markdown]
# ## 2. PageRank (beta = 0.85) and convergence

# %%
t0 = time.perf_counter()
pr, deltas = pagerank(edges, beta=0.85, max_iter=40, tol=1e-9)
pr.cache(); pr.count(); t_pr = time.perf_counter() - t0
print(f"{len(deltas)} iterations in {t_pr:.1f}s; final L1 delta {deltas[-1]:.2e}; sum of ranks = {pr.agg(F.sum('rank')).first()[0]:.6f}")
fig, ax = plt.subplots(figsize=(5.5, 3.5)); ax.semilogy(deltas, marker="o"); ax.set_xlabel("iteration"); ax.set_ylabel("L1 change")
ax.set_title("PageRank convergence"); save_fig(fig, "05_pr_convergence"); plt.show()

top_pr = (pr.join(meta, pr.node == meta.product_id).orderBy(F.desc("rank")).limit(15)
          .select("node", "rank", "category_code", "brand", "avg_price").toPandas())
top_pr

# %% [markdown]
# ## 3. Rank vs popularity (is PageRank adding information?)

# %%
cnt = (events.groupBy("product_id").agg(F.sum((F.col("event_type") == "view").cast("int")).alias("views"),
                                        F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchases")))
cmp_ = pr.join(cnt, pr.node == cnt.product_id).select("node", "rank", "views", "purchases").toPandas()
corr = cmp_[["rank", "views", "purchases"]].corr(method="spearman")
print(corr.round(3))
cmp_["pr_rank"] = cmp_["rank"].rank(ascending=False); cmp_["view_rank"] = cmp_["views"].rank(ascending=False)
top_k = 1000
a, b = set(cmp_.nsmallest(top_k, "pr_rank")["node"]), set(cmp_.nsmallest(top_k, "view_rank")["node"])
overlap = len(a & b) / top_k
print(f"top-{top_k} overlap between PageRank and view-count rankings: {overlap:.2f}")
fig, ax = plt.subplots(figsize=(5.5, 5)); ax.loglog(cmp_["views"] + 1, cmp_["rank"], ".", alpha=.2)
ax.set_xlabel("views"); ax.set_ylabel("PageRank"); ax.set_title("PageRank vs popularity")
save_fig(fig, "05_pr_vs_views"); plt.show()

# %% [markdown]
# ## 4. Damping sweep (teleport probability)

# %%
rows = []
base = {r["node"]: r["rank"] for r in pr.orderBy(F.desc("rank")).limit(top_k).collect()}
for beta in (0.5, 0.7, 0.85, 0.95):
    t0 = time.perf_counter(); p, d = pagerank(edges, beta=beta, max_iter=40, tol=1e-9)
    top = {r["node"] for r in p.orderBy(F.desc("rank")).limit(top_k).collect()}
    rows.append({"beta": beta, "iterations": len(d), "seconds": round(time.perf_counter() - t0, 1),
                 "top1000_overlap_with_0.85": len(top & set(base)) / top_k})
damp = pd.DataFrame(rows); damp

# %% [markdown]
# ## 5. Topic-sensitive PageRank (personalised to one category)
# Teleport only to products of the chosen top-level category -> ranks "important products *for that topic*".

# %%
cats = (meta.withColumn("top", F.split("category_code", r"\.").getItem(0)).filter(F.col("top").isNotNull())
        .groupBy("top").count().orderBy(F.desc("count")).limit(3).toPandas())
topic = cats["top"].iloc[0]
seeds = (meta.filter(F.split("category_code", r"\.").getItem(0) == topic).select(F.col("product_id").alias("node"), F.lit(1.0).alias("p")))
ppr, _ = pagerank(edges, beta=0.85, max_iter=40, tol=1e-9, personalization=seeds)
tops = (ppr.join(meta, ppr.node == meta.product_id).orderBy(F.desc("rank")).limit(10)
        .select("node", "rank", "category_code", "brand").toPandas())
share = (ppr.join(meta, ppr.node == meta.product_id).filter(F.split("category_code", r"\.").getItem(0) == topic)
         .agg(F.sum("rank")).first()[0])
base_share = (pr.join(meta, pr.node == meta.product_id).filter(F.split("category_code", r"\.").getItem(0) == topic)
              .agg(F.sum("rank")).first()[0])
print(f"topic = {topic}: rank mass in topic {share:.3f} (personalised) vs {base_share:.3f} (global PageRank)")
tops

# %% [markdown]
# ## 6. HITS (hubs & authorities)

# %%
t0 = time.perf_counter(); h = hits(edges, max_iter=30, tol=1e-8).cache(); h.count(); t_h = time.perf_counter() - t0
top_auth = h.join(meta, h.node == meta.product_id).orderBy(F.desc("auth")).limit(10).select("node", "auth", "hub", "category_code").toPandas()
top_hub = h.join(meta, h.node == meta.product_id).orderBy(F.desc("hub")).limit(10).select("node", "hub", "auth", "category_code").toPandas()
print(f"HITS in {t_h:.1f}s"); top_auth

# %%
out.update({"graph": {"nodes": n_nodes, "edges": n_edges, "build_seconds": t_graph},
            "pagerank": {"iterations": len(deltas), "seconds": t_pr, "final_delta": deltas[-1],
                         "spearman": corr.to_dict(), f"top{top_k}_overlap_with_views": overlap},
            "damping_sweep": damp.to_dict("records"),
            "topic_sensitive": {"topic": topic, "mass_personalised": share, "mass_global": base_share},
            "hits_seconds": t_h})
print("saved", save_result("05_link_analysis", out))
