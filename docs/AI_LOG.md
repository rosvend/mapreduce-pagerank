# AI_LOG — record of AI assistance

- **Author:** Roy Sandoval
- **Assistant used:** Claude (Claude Code), for the implementation phase
- **Deliverable:** Entrega B, section 7

---

## 1. How I worked

The design came first and the code second, because the assignment gates it that way:
`DESIGN.md` was submitted and approved as Entrega A **before** any code existed. So the AI
was never asked "write me PageRank." It was asked to implement a design I had already
written and defended — the key–value schema, the two message types, the dangling-node
strategy and the iteration loop were all fixed in
[`Design/DESIGN_ROYSANDOVAL.md`](Design/DESIGN_ROYSANDOVAL.md) before the first line of
`pagerank.py`.

I think this is the honest reason the implementation went relatively smoothly: the hard
thinking had already been done, on paper, by me. The places where things *did* go wrong are
listed below, and most of them are outside the core algorithm — which is itself informative
about where AI help is and is not reliable.

---

## 2. What I asked for

1. **The main implementation prompt.** I gave it the task spec and my design doc, and
   required it to read both in full and confirm, in its own words, the key–value schema,
   how adjacency survives between iterations, and how dangling nodes are handled — *before*
   writing anything. I explicitly instructed it to **stop and ask** if my design was
   ambiguous or contradicted the spec, rather than silently resolving it.
2. **Constraints I imposed:** do not modify `mapreduce_framework.py`; standard library only;
   test-driven, running the suite after each component; KISS — "readable by a
   non-Python-developer, no abstractions beyond what the task strictly needs"; and stick to
   my design unless it can be proven to fail.
3. **A separate follow-up** for the Neo4j/GDS demo, the convergence animation and the
   scaling chart — deliberately kept out of the implementation context so it could not
   influence `pagerank.py`.
4. **Documentation**: this log, `ANALYSIS.md`, and a private study guide for the defense.

You can find the whole prompt in `docs/prompt.xml` 

---

## 3. What was wrong, incomplete, or needed correcting

### 3.1 My own design contained a contradiction, which the AI caught

This is the most valuable thing that came out of the session, and it was a flaw in **my**
document, not in the generated code.

Section 3 of my design argues at length that the `STRUCT` message is what preserves the
graph between passes, and section 3.4 claims that removing it collapses the ranking to a
uniform vector. But the loop sketch in my section 5.1 rebuilds each iteration's input from
the static `graph` dict:

```python
items = [(u, (ranks[u], graph[u])) for u in graph]   # every pass
```

If the loop reads adjacency from a process-local dict every pass, then `STRUCT` is emitted,
shuffled and returned — and **ignored**. My section 3.4 claim would have been false in my
own code: deleting the `STRUCT` emit would not have broken anything. My design even
half-admits this in a footnote, which I had not thought through when I wrote it.

**What I decided:** the loop consumes the adjacency that came back through `mapreduce()`:

```python
items = list(result.items())   # adjacency arrives via STRUCT
```

`graph` is now used only to build iteration 0 and to read `N`. This makes the pipeline
honest *as a MapReduce job* — a real distributed run could never rely on a local dict — and
it makes my section 3.4 claim literally verifiable, which matters because "show me where you
preserve adjacency and what happens if you remove it" is the obvious defense question. I
asked for a test that pins it: with the `STRUCT` emit removed, node `E` disappears on pass 1
and pass 2 returns `{}`.

### 3.2 The AI's first `load_graph` leaked a file handle

The first version used `for line in open(path)` with no `with` block. It passed every test,
but running the suite with `-W error::ResourceWarning` surfaced an unclosed-file warning. It
is a small thing, but it is exactly the kind of defect that passes tests and still counts as
sloppy. Fixed with a `with` block.

### 3.3 A naming bug that would have broken the demo — and it was my fault

I had created `src/neo4j.py` as an empty stub. That filename **shadows the `neo4j` driver
package**: with `src/` on `sys.path`, `from neo4j import GraphDatabase` imports my empty
file instead of the driver. Verified directly:

```
imported neo4j from: .../src/neo4j.py
has GraphDatabase: False
```

Any demo code placed in `src/` would have failed on import with a confusing error. The file
was deleted and the demo lives in `src/neo4j_gds.py`.

### 3.4 The AI made a git mistake and had to rebuild the history

When committing, a previously staged file deletion was picked up by an unrelated commit
(`git commit` commits everything staged, not just what was named in the preceding
`git add`), and a chained command failure silently skipped an entire commit. The result was
a history where the "ignore build artifacts" commit also deleted a source file, and the demo
commit did not exist. Since nothing had been pushed, the branch was reset and the four
commits rebuilt with explicit staging, then verified file-by-file before pushing.

Worth recording because it is a failure mode I would not have noticed from the summary
alone — the command *reported* success for three of the four commits.

### 3.5 A visualisation bug that was misdiagnosed on the first attempt

In the animation, node labels rendered as `0073` instead of `P00073`. The first diagnosis
was "the resolution is too low to read the leading character," and the fix applied was to
raise the DPI. That was wrong. Re-rendering at higher resolution showed the real cause: the
labels are **white text**, and any part of a label that overflows its dark node circle lands
on a white background and becomes invisible. The actual fix was to shorten the labels and
add a dark outline. I am logging this because the first explanation was plausible, confident
and incorrect — and only checking the re-rendered image revealed it.

### 3.6 A claim in my own prompt was wrong, and got corrected

When I asked for the scaling chart, I wrote that it should "demonstrate that Spark is faster
since it lives in memory while MapReduce reads the entire disk each iteration." That is true
of **Hadoop**, but not of `mapreduce_framework.py`, which is pure in-memory Python and never
touches the disk at all. If I had presented my own chart as evidence of disk I/O, that is
exactly the kind of claim that falls apart under one question in a defense.

The corrected framing, which is what `ANALYSIS.md` now argues: the measurement demonstrates
that **every pass re-materialises all 73,195 pairs and caches none of them**, which is the
structural property that *causes* the disk cost in Hadoop. The flat per-iteration timing
curve is the evidence, and it is honest.

### 3.7 The assignment's own iteration count did not match

My implementation converges in **16** iterations; the assignment says ~24. Rather than
adjusting anything to match, this was investigated. Sweeping the tolerance shows the
assignment's section 4 (`epsilon = 1e-6`) and its section 9 (~24 iterations) describe
different criteria — ~24 corresponds to `epsilon = 1e-9`:

| epsilon | 1e-4 | **1e-6** | 1e-8 | 1e-9 | 1e-10 |
| --- | --- | --- | --- | --- | --- |
| iterations | 11 | **16** | 22 | 24 | 27 |

To be sure the difference was in the stopping rule and not in my arithmetic, the ranks were
cross-checked against a separately written power iteration over an explicit in-link index
(`max|difference| = 1.5e-9`) and, later, against Neo4j's GDS PageRank (Spearman correlation
exactly 1.000000). The implementation is right; the two numbers in the assignment are
inconsistent with each other.

---

## 4. What I checked myself, independently of the AI

- Every rank value asserted in the tests is one I can derive by hand: the chain case is
  `13/90` and `77/180`, and the dangling case reproduces the four-node trace table from my
  own design document (`0.303125 / 0.196875 / 0.409375 / 0.090625`).
- The sum invariant is asserted after **every** pass on six graphs, not merely at the end —
  this is the check the assignment names as the tripwire for dangling-node handling.
- `git diff` on `src/mapreduce_framework.py` across the entire branch is empty.
- The only imports in `pagerank.py` are `os`, `sys`, `time`, `collections` and the provided
  framework.

---

## 5. Honest assessment

The core `mapper` and `make_reducer` were close to correct on the first attempt. I do not
think that means the task was trivial — it means the design document had already resolved
the three hard questions (two message types, adjacency preservation, dangling mass as an
outer-loop scalar), so the code was largely transcription. When I compare that with the
places things *did* break — a contradiction in my own design, a filename that shadowed a
library, a git history that silently lost a commit, a confidently wrong first diagnosis of a
rendering bug, and a false premise in my own prompt — the pattern is fairly clear: the AI was
reliable at writing code against a specification I had already thought through, and
unreliable exactly where nobody had thought carefully yet.

The single most useful thing it did was refuse to silently resolve the ambiguity in section
3.1 and make me decide instead. Had it quietly picked the version from my own section 5.1
sketch, my design's central claim about `STRUCT` would have been false in my own
implementation, and I would have discovered that during the defense rather than before it.
