"""Animates PageRank converging, one MapReduce pass per frame.

Supplementary demo. Produces, for each graph it is given:
  results/<name>_convergence.gif   -- matplotlib/networkx animation
  results/<name>_convergence.html  -- self-contained page with play + scrub

NOT part of the graded MapReduce deliverable.
Run: uv run --extra demo python src/visualization.py
"""

import json
import os

import matplotlib

matplotlib.use("Agg")

import matplotlib.patheffects as path_effects
import matplotlib.pyplot as plt
import networkx as nx
from matplotlib.animation import FuncAnimation, PillowWriter

from mapreduce_framework import mapreduce
from pagerank import load_graph, make_reducer, mapper, run_pagerank

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
RESULTS = os.path.join(HERE, "..", "results")


def rank_history(graph, d=0.85, max_iter=50, epsilon=1e-6):
    """Runs the MapReduce loop but keeps every intermediate rank vector."""
    N = len(graph)
    ranks = {node: 1.0 / N for node in graph}
    items = [(node, (ranks[node], graph[node])) for node in graph]
    frames = [dict(ranks)]
    l1s = [None]

    for _iteration in range(max_iter):
        dangling_sum = sum(rank for _node, (rank, adjacency) in items if not adjacency)
        result = mapreduce(items, mapper, make_reducer(d, N, dangling_sum))

        new_ranks = {node: rank for node, (rank, _adjacency) in result.items()}
        l1 = sum(abs(new_ranks[node] - ranks[node]) for node in ranks)
        ranks = new_ranks
        items = list(result.items())

        frames.append(dict(ranks))
        l1s.append(l1)
        if l1 < epsilon:
            break
    return frames, l1s


def hub_neighbourhood(graph, count):
    """Induced subgraph around the top hub and the pages pointing at it.

    The top-ranked nodes barely link to each other, so an induced subgraph on them
    comes out almost edgeless. A hub plus its in-links actually shows rank flowing.
    """
    ranks, _iterations, _converged = run_pagerank(graph)
    hub = max(ranks, key=lambda node: ranks[node])

    keep = {hub}
    for node in sorted(graph, key=lambda n: -ranks[n]):
        if len(keep) >= count:
            break
        if hub in graph[node]:
            keep.add(node)
    for neighbour in graph[hub]:
        if len(keep) >= count:
            break
        keep.add(neighbour)
    return {node: [v for v in graph[node] if v in keep] for node in keep}


def short_label(node):
    """P00954 -> 954, so the text still fits inside a small node."""
    trimmed = node.lstrip("P").lstrip("0")
    return trimmed if trimmed else node


def layout(graph):
    """Fixed-seed spring layout so the GIF and the HTML page agree."""
    digraph = nx.DiGraph((u, v) for u, adjacency in graph.items() for v in adjacency)
    digraph.add_nodes_from(graph)
    return digraph, nx.spring_layout(digraph, seed=7, k=1.4)


def write_gif(graph, frames, l1s, name):
    digraph, positions = layout(graph)
    peak = max(max(frame.values()) for frame in frames)
    nodes = list(graph)

    figure, axis = plt.subplots(figsize=(8, 6.5), dpi=140)
    destination = os.path.join(RESULTS, "%s_convergence.gif" % name)

    def draw(index):
        axis.clear()
        axis.set_axis_off()
        axis.margins(0.14)
        ranks = frames[index]
        sizes = [200 + 5200 * ranks[node] / peak for node in nodes]
        colours = [ranks[node] / peak for node in nodes]

        nx.draw_networkx_edges(digraph, positions, ax=axis, edge_color="#b9c2cc",
                               arrowsize=11, width=1.1, node_size=sizes)
        nx.draw_networkx_nodes(digraph, positions, ax=axis, nodelist=nodes, node_size=sizes,
                               node_color=colours, cmap=plt.cm.plasma, vmin=0, vmax=1,
                               edgecolors="#2b2b2b", linewidths=0.7)
        texts = nx.draw_networkx_labels(digraph, positions, ax=axis, font_size=8, font_color="white",
                                        labels={node: short_label(node) for node in nodes})
        # White labels overflow the smallest nodes, so outline them to stay legible.
        for text in texts.values():
            text.set_path_effects([path_effects.withStroke(linewidth=2.0, foreground="#1b1b1b")])

        movement = "initial 1/N" if l1s[index] is None else "L1 = %.2e" % l1s[index]
        axis.set_title("%s -- pass %d of %d   (%s)   sum = %.6f"
                       % (name, index, len(frames) - 1, movement, sum(ranks.values())),
                       fontsize=11)

    animation = FuncAnimation(figure, draw, frames=len(frames), interval=700)
    animation.save(destination, writer=PillowWriter(fps=2))
    plt.close(figure)
    return destination


def write_html(graph, frames, l1s, name):
    """Self-contained SVG page: no CDN, so it works offline during the defense."""
    digraph, positions = layout(graph)
    peak = max(max(frame.values()) for frame in frames)
    scale = 520

    payload = {
        "name": name,
        "peak": peak,
        "nodes": [{"id": node, "label": short_label(node),
                   "x": positions[node][0] * scale, "y": -positions[node][1] * scale}
                  for node in graph],
        "edges": [{"source": u, "target": v} for u, adjacency in graph.items() for v in adjacency],
        "frames": [dict(frame) for frame in frames],
        "l1": l1s,
    }

    destination = os.path.join(RESULTS, "%s_convergence.html" % name)
    with open(destination, "w") as page:
        page.write(HTML_TEMPLATE.replace("__PAYLOAD__", json.dumps(payload)))
    return destination


HTML_TEMPLATE = """<!doctype html>
<html><head><meta charset="utf-8"><title>PageRank convergence</title>
<style>
 body{font:14px system-ui,sans-serif;margin:0;padding:20px;background:#11151a;color:#e8edf2}
 h1{font-size:16px;font-weight:600;margin:0 0 4px}
 p.sub{margin:0 0 14px;color:#93a1b0;font-size:12px}
 #controls{display:flex;align-items:center;gap:12px;margin-bottom:10px}
 button{background:#2a6df4;color:#fff;border:0;border-radius:6px;padding:7px 16px;font-size:13px;cursor:pointer}
 input[type=range]{flex:1;max-width:420px}
 #readout{font-variant-numeric:tabular-nums;color:#93a1b0;font-size:12px}
 svg{background:#161b22;border-radius:10px;width:100%;height:auto}
 text{font:9px system-ui,sans-serif;fill:#fff;pointer-events:none;paint-order:stroke;stroke:#0d1117;stroke-width:2.5px}
</style></head><body>
<h1>PageRank convergence &mdash; one frame per MapReduce pass</h1>
<p class="sub">Node area and colour track the rank. Rank sums to 1.0 in every frame.</p>
<div id="controls">
  <button id="play">Play</button>
  <input type="range" id="slider" min="0" value="0">
  <span id="readout"></span>
</div>
<svg id="canvas" viewBox="-620 -420 1240 840"><defs>
<marker id="arrow" viewBox="0 -5 10 10" refX="22" refY="0" markerWidth="5" markerHeight="5" orient="auto">
<path d="M0,-4L9,0L0,4" fill="#4a5765"></path></marker></defs>
<g id="edges"></g><g id="nodes"></g></svg>
<script>
const data = __PAYLOAD__;
const svgns = "http://www.w3.org/2000/svg";
const position = {};
data.nodes.forEach(n => position[n.id] = n);
const edgeLayer = document.getElementById("edges");
const nodeLayer = document.getElementById("nodes");
const slider = document.getElementById("slider");
const readout = document.getElementById("readout");
const playButton = document.getElementById("play");
slider.max = data.frames.length - 1;

data.edges.forEach(e => {
  const line = document.createElementNS(svgns, "line");
  line.setAttribute("x1", position[e.source].x); line.setAttribute("y1", position[e.source].y);
  line.setAttribute("x2", position[e.target].x); line.setAttribute("y2", position[e.target].y);
  line.setAttribute("stroke", "#3a4553"); line.setAttribute("stroke-width", 1);
  line.setAttribute("marker-end", "url(#arrow)");
  edgeLayer.appendChild(line);
});

const circles = {}, labels = {};
data.nodes.forEach(n => {
  const c = document.createElementNS(svgns, "circle");
  c.setAttribute("cx", n.x); c.setAttribute("cy", n.y);
  c.setAttribute("stroke", "#0d1117"); c.setAttribute("stroke-width", 1.2);
  nodeLayer.appendChild(c); circles[n.id] = c;
  const t = document.createElementNS(svgns, "text");
  t.setAttribute("x", n.x); t.setAttribute("y", n.y + 3);
  t.setAttribute("text-anchor", "middle"); t.textContent = n.label;
  nodeLayer.appendChild(t); labels[n.id] = t;
});

function colour(v){
  const stops = [[13,8,135],[126,3,168],[204,71,120],[248,149,64],[240,249,33]];
  const x = Math.max(0, Math.min(0.999, v)) * (stops.length - 1);
  const i = Math.floor(x), f = x - i;
  const a = stops[i], b = stops[i + 1];
  return `rgb(${Math.round(a[0]+(b[0]-a[0])*f)},${Math.round(a[1]+(b[1]-a[1])*f)},${Math.round(a[2]+(b[2]-a[2])*f)})`;
}

function render(index){
  const ranks = data.frames[index];
  let total = 0;
  data.nodes.forEach(n => {
    const r = ranks[n.id]; total += r;
    const share = r / data.peak;
    circles[n.id].setAttribute("r", 7 + 26 * share);
    circles[n.id].setAttribute("fill", colour(share));
  });
  const movement = data.l1[index] === null ? "initial 1/N" : "L1 = " + data.l1[index].toExponential(2);
  readout.textContent = `pass ${index} / ${data.frames.length - 1}   ${movement}   sum = ${total.toFixed(6)}`;
  slider.value = index;
}

let timer = null;
function stop(){ clearInterval(timer); timer = null; playButton.textContent = "Play"; }
playButton.onclick = () => {
  if (timer) return stop();
  playButton.textContent = "Pause";
  timer = setInterval(() => {
    let next = +slider.value + 1;
    if (next >= data.frames.length) { stop(); return; }
    render(next);
  }, 650);
};
slider.oninput = () => { stop(); render(+slider.value); };
render(0);
</script></body></html>
"""


if __name__ == "__main__":
    os.makedirs(RESULTS, exist_ok=True)

    sample = load_graph(os.path.join(DATA, "web_graph_sample.txt"))
    medium = load_graph(os.path.join(DATA, "web_graph_medium.txt"))
    subgraph = hub_neighbourhood(medium, 30)

    for name, graph in (("sample_8_nodes", sample), ("medium_hub", subgraph)):
        frames, l1s = rank_history(graph)
        gif = write_gif(graph, frames, l1s, name)
        page = write_html(graph, frames, l1s, name)
        print("%-16s %d nodes, %d passes" % (name, len(graph), len(frames) - 1))
        print("   %s" % os.path.normpath(gif))
        print("   %s" % os.path.normpath(page))
