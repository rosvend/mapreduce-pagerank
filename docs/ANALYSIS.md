# ANALYSIS — cost and correctness

- **Author:** Roy Sandoval
- **Deliverable:** Entrega B, section 6.2
- **Design:** [`Design/DESIGN_ROYSANDOVAL.md`](Design/DESIGN_ROYSANDOVAL.md)
- **Official dataset:** `data/web_graph_large.txt` — N = 10,000, E = 63,195, 300 dangling

All figures below are measured on the official dataset, not estimated.

| Quantity | Value |
| --- | --- |
| Pairs emitted per pass | `N + E` = **73,195** (10,000 STRUCT + 63,195 RANK) |
| Iterations to converge (`epsilon = 1e-6`) | **16** |
| Pairs shuffled over the whole run | **1,171,120** |
| Wall time per pass | 0.085 – 0.129 s (mean 0.111 s, representative run) |
| Sum of ranks | `1.000000000000000` after every pass |
| In-degree: mean / median / p99 / max | 6.32 / 4 / 31 / 60 |

---

## 1. How many pairs does the mapper emit, and what is the shuffle volume?

For one input item `(u, (rank, adjacency))` the mapper emits:

- exactly **one** `STRUCT` message, always, dangling or not;
- exactly **`outdeg(u)`** `RANK` messages, one per out-link — and **zero** if `u` is dangling.

Summing over every node, per pass:

```
STRUCT messages = N                       = 10,000
RANK   messages = sum of outdeg(u) = E    = 63,195
                                    total = N + E = 73,195
```

Because this framework has no combiner, **every emitted pair crosses the shuffle**: the
shuffle volume equals the mapper's output, `N + E`. Peak memory is the same order — the
`mapped` list and the `shuffled` dict each hold `N + E` entries, all in RAM.

Over the run: `16 × 73,195 =` **1,171,120** pairs. At the 30 iterations the assignment
asks us to consider, `30 × 73,195 =` **2,195,850**.

**A subtlety worth stating: message *count* and message *weight* are not the same thing.**
STRUCT is only 14% of the messages but roughly half the bytes. A RANK message carries one
float; a STRUCT message carries an entire adjacency list, and the adjacency lists together
hold exactly `E = 63,195` node ids. So by payload, the structural traffic is about as
expensive as the rank traffic — and unlike the rank traffic, **it is identical on every
pass**. That observation is the whole argument for Spark in section 4 below.

Note what the design deliberately avoids. Redistributing dangling rank by having each of
the 300 dangling nodes emit a RANK message to all 10,000 nodes would be *correct*, but it
would add `D × N = 3,000,000` pairs per pass — 41× the entire real workload. Handling the
dangling mass as one scalar in the outer loop costs `O(N)` arithmetic and **zero** messages.

---

## 2. Where would a combiner go, and what would it pre-aggregate?

**Placement:** between MAP and SHUFFLE, running inside each map partition, on that
partition's emitted pairs only.

**What it aggregates:** `RANK` messages, and only those. The reducer's use of RANK is a
pure sum, and addition is associative and commutative, so partial sums computed locally are
interchangeable with the full sum. A combiner would turn the many `("RANK", x)` pairs that
one partition produces for the same destination into a single `("RANK", partial_sum)`.

**What it cannot aggregate:** `STRUCT`. It is not a reduction — it carries an adjacency
list that must arrive intact, and there is exactly one per key in the whole job anyway, so
there is nothing to fold. STRUCT traffic stays at `N` no matter what.

**Why it shrinks the shuffle:** RANK traffic drops from `E` (one per edge) toward the number
of *distinct destinations per partition*, bounded by `min(E, P × N)`. Measured on the
official graph, splitting the node list into `P` contiguous partitions:

| Partitions | RANK pairs crossing the shuffle | vs. `E` |
| --- | --- | --- |
| none (this framework) | 63,195 | 100% |
| 1 | 8,629 | 14% |
| 2 | 15,142 | 24% |
| 4 | 24,409 | 39% |
| 8 | 35,183 | 56% |
| 16 | 45,005 | 71% |

With 4 partitions the shuffle drops from 73,195 to 34,409 pairs — a **53% cut overall**, and
a 61% cut in the RANK half. The gain shrinks as partitions multiply, because each partition
re-emits its own partial sum for a popular destination: with `P` partitions a hub receives up
to `P` messages instead of one.

`mapreduce_framework.py` has three phases — MAP, SHUFFLE, REDUCE — and no hook between the
first two, so none of this is available here. The full `E` always crosses.

---

## 3. Data skew: what happens with a huge in-degree?

The graphs were built with preferential attachment, so in-degree is heavily right-skewed
while out-degree is nearly uniform:

| | in-degree | out-degree |
| --- | --- | --- |
| mean | 6.32 | 6.32 |
| median | 4 | 6 |
| p99 | 31 | 11 |
| max | **60** (P06485, P04814) | 12 |
| zero | 1,371 nodes | 300 nodes (dangling) |

**The bottleneck is the reducer for the highest in-degree key.** `reducer(P, values)` does
`O(indeg(P) + 1)` work, so the hub's call processes a 61-element list while the median call
processes 5 and 1,371 calls process exactly 1. The reduce phase finishes when its *slowest*
key finishes, so wall-clock is governed by `max_P indeg(P)`, not by the average. Here that is
**9.5× the mean reducer**.

Two consequences worth naming:

1. **Total work is still `O(N + E)`.** Skew does not change the sum, it changes the
   *distribution*. On one machine that is invisible — Python loops over the same 73,195
   values either way. It only becomes a real cost when reducers run in parallel, where the
   hub key is the straggler that holds the barrier and leaves other workers idle.
2. **You cannot partition your way out of a single hot key.** Hash or range partitioning
   moves keys between reducers; it cannot split one key, because every value for that key
   must meet in one place to be summed. The only real mitigation is **pre-aggregation** — a
   combiner (section 2), which cuts the hub's input list from 60 to at most one entry per
   partition.

The skew here is mild: 60 versus a mean of 6.3. On the real web the ratio is millions to
one, which is why production PageRank implementations treat hot keys as a first-class
problem rather than an edge case.

---

## 4. Connection with Clase 5: why Spark would be faster

**How many times are the data re-read?** Once per iteration, in full. At 30 iterations the
entire graph is read 30 times and written 30 times. Nothing carries over between passes,
because `mapreduce()` is a pure function: it takes a list, returns a dict, and keeps no state.

In a real Hadoop deployment each of those 30 passes is a full HDFS round trip — read
`N + E`, shuffle it to disk, write `N + E` back, with replication on every write. That is
where the cost lives, and it is paid 30 times over.

**Be precise about this framework, though.** `mapreduce_framework.py` is pure in-memory
Python; it never touches disk. What it *does* reproduce faithfully is the structural
property that causes the disk cost in Hadoop: **every pass rebuilds all 73,195 pairs from
nothing and caches none of them.** The measurement that shows this is the per-iteration
timing:

```
16 passes:  min 0.085 s   max 0.129 s   mean 0.111 s
```

The per-pass time shows **no downward trend as the algorithm converges** — the 1.5x spread
between fastest and slowest pass is system noise, not a pattern. Pass 16, which moves the
ranks by only `9.75e-07`, costs essentially the same as pass 1, which moves them by
`6.85e-01`. The algorithm gets closer to
the answer while the cost per step stays constant, because the same graph is re-shipped every
time regardless of how little is left to learn.

**Why Spark wins.** The adjacency lists are immutable — they are identical on pass 1 and pass
16 — and they are half the payload (section 1). Spark caches that partition in memory once
with `.cache()`, and each iteration joins the small, changing rank vector against it. The
graph is read from storage **once instead of 30 times**; only the rank vector, `O(N)` floats,
moves between iterations. The `N` STRUCT messages per pass — the mechanism this design needs
purely to smuggle the graph from one pass to the next — simply stop existing, because in
Spark the graph never leaves memory in the first place.

That is the exact lesson: MapReduce was built for one pass over data, and PageRank needs many.
Everything awkward in this implementation — re-emitting the adjacency, rebuilding the item
list, the flat cost curve — is a symptom of forcing an iterative algorithm through a
single-pass interface.

**Measured projection.** Cost per pass is linear in `N + E`; fitting the three provided graphs
gives `seconds_per_pass = 1.429e-06 × (N + E) - 1.095e-03`, so a full 16-pass run scales as:

| Scale | N | E | Full run |
| --- | --- | --- | --- |
| 1× | 10,000 | 63,195 | 1.7 s |
| 10× | 100,000 | 631,950 | 16.7 s |
| 100× | 1,000,000 | 6,319,500 | 167 s (2.8 min) |

Linear, as the `O(I × (N + E))` analysis predicts. The real-world curve would be *worse* than
linear once the working set stops fitting in RAM — which is precisely the point at which
Hadoop's disk round trips, and Spark's advantage over them, start to dominate.

---

## 5. Results: top-15 by PageRank versus in-degree

| # | node | pagerank | in-degree | | # | node | pagerank | in-degree |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | P01443 | 0.00116188 | 57 | | 9 | P03428 | 0.00079943 | **15** |
| 2 | P03367 | 0.00103648 | 59 | | 10 | P07315 | 0.00078926 | 55 |
| 3 | P06210 | 0.00090696 | 32 | | 11 | P05650 | 0.00078824 | **22** |
| 4 | P09065 | 0.00089299 | 55 | | 12 | P08541 | 0.00074021 | 29 |
| 5 | P04814 | 0.00086030 | **60** | | 13 | P00751 | 0.00073128 | **20** |
| 6 | P00894 | 0.00082475 | 55 | | 14 | P07382 | 0.00072919 | 48 |
| 7 | P01977 | 0.00081174 | 49 | | 15 | P01632 | 0.00072150 | 41 |
| 8 | P07750 | 0.00080828 | 44 | | | | | |

**They correlate, but not perfectly: Spearman rank correlation = 0.9145.**

The hubs do emerge at the top, as expected. But the ranking is not simply in-degree sorted:

- **P06485 has the highest in-degree in the entire graph (60) and ranks only #25.**
- P04814 also has 60 in-links and ranks #5, while **P01443 wins with only 57**.
- **P03428 reaches #9 with just 15 in-links** — a quarter of the hub's.

The reason is the formula itself. A node's rank is `sum over Q linking to P of
rank(Q)/outdeg(Q)`, so an incoming link is worth two things in-degree cannot see:

1. **Who is voting.** A link from a high-rank page carries more mass than a link from an
   obscure one. In-degree counts all voters equally; PageRank weights them by their own
   importance — and recursively so, which is why it needs iteration at all.
2. **How much the voter is splitting.** A page with 12 out-links gives each target
   `rank/12`; a page with 2 gives each `rank/2`. The same link is worth 6× more from the
   second page. In-degree cannot see the *sender's* out-degree at all.

P03428's 15 in-links must therefore come from a few well-ranked, low-out-degree pages, while
P06485's 60 come from diluted or unimportant ones. If the correlation *were* 1.0, PageRank
would be an expensive way to compute `in-degree`, and Google would not have needed it.

---

## 6. Correctness evidence

**The sum invariant holds exactly.** `tests/test_pagerank.py` asserts
`|sum(ranks) - 1.0| < 1e-9` after **every** pass — not just at the end — on all six graphs
(three hand-made, three provided). Measured on the large graph the sum is
`1.000000000000000` at every one of the 16 passes. This is the assignment's built-in
tripwire for dangling-node handling, and it is the strongest single check in the suite.

**Iteration count: 16, not the ~24 quoted in the assignment.** This is not a bug; the
assignment is internally inconsistent. Its section 4 specifies `epsilon = 1e-6`, and its
section 9 quotes ~24 iterations. Those are different criteria:

| epsilon | 1e-4 | **1e-6** | 1e-8 | 1e-9 | 1e-10 |
| --- | --- | --- | --- | --- | --- |
| iterations | 11 | **16** | 22 | 24 | 27 |

The quoted ~24 corresponds to `epsilon = 1e-9`. The implementation follows the specified
`1e-6`. The companion figure in the same paragraph does match exactly: 10,000 + 63,195 =
**73,195** pairs per pass.

**Cross-validated twice, independently.**

1. Against a separately written textbook power iteration built on an explicit **in-link**
   index — a different data layout and a different code path — agreeing to
   `max|difference| = 1.5e-9` on the large graph.
2. Against **Neo4j's production GDS PageRank** (`src/neo4j_gds.py`), which gives a Spearman
   rank correlation of **exactly 1.000000** on all three graphs, identical top-15 sets and
   an identical #1, with a maximum score difference of `1.7e-08`.

Two independent implementations, one of them a production graph engine, produce the same
ranking. See [`AI_LOG.md`](AI_LOG.md) for how the 16-vs-24 discrepancy was investigated
rather than assumed away.
