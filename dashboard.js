const dom = {
  cards: document.getElementById("cards"),
  byValidation: document.getElementById("byValidation"),
  byStrategy: document.getElementById("byStrategy"),
  topLint: document.getElementById("topLint"),
  runOutcome: document.getElementById("runOutcome"),
  recentRows: document.getElementById("recentRows"),
  refresh: document.getElementById("refresh"),
  repairFunnel: document.getElementById("repairFunnel"),
  score7: document.getElementById("score7"),
  score30: document.getElementById("score30"),
  intentClusters: document.getElementById("intentClusters"),
  heatmap: document.getElementById("heatmap"),
  exemplarImpact: document.getElementById("exemplarImpact"),
  driftAlerts: document.getElementById("driftAlerts"),
  exemplarRows: document.getElementById("exemplarRows"),
  pinnedRows: document.getElementById("pinnedRows"),
  replayPrompt: document.getElementById("replayPrompt"),
  runReplay: document.getElementById("runReplay"),
  replayJudge: document.getElementById("replayJudge"),
  replayResult: document.getElementById("replayResult"),
};

const state = {
  detail: null,
  pinned: [],
};

function escapeHtml(text) {
  return String(text || "").replace(/[<>&"]/g, (m) => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;", '"': "&quot;" })[m]);
}

function sum(obj, key) {
  return Number(obj?.[key] || 0);
}

function setList(el, items) {
  el.innerHTML = "";
  for (const text of items) {
    const li = document.createElement("li");
    li.textContent = text;
    el.appendChild(li);
  }
}

function renderCards(detail) {
  const byValidation = detail.by_validation || {};
  const total = Number(detail.totals?.generations || 0);
  const accepted = sum(byValidation, "accepted");
  const repaired = sum(byValidation, "repaired");
  const fallback = sum(byValidation, "fallback");
  const errors = sum(byValidation, "error");
  const successRate = total > 0 ? (((accepted + repaired) / total) * 100).toFixed(1) : "0.0";

  const cards = [
    { label: "Generations", value: total, klass: "" },
    { label: "Success Rate", value: `${successRate}%`, klass: "ok" },
    { label: "Accepted", value: accepted, klass: "ok" },
    { label: "Repaired", value: repaired, klass: "warn" },
    { label: "Fallback", value: fallback, klass: "warn" },
    { label: "Errors", value: errors, klass: "bad" },
  ];

  dom.cards.innerHTML = cards
    .map((c) => `<article class="card"><div class="label">${c.label}</div><div class="value ${c.klass}">${c.value}</div></article>`)
    .join("");
}

function formatScorecard(x) {
  return [
    `Total: ${x.total}`,
    `Success rate: ${x.success_rate}%`,
    `First-pass accept: ${x.first_pass_accept_rate}%`,
    `Repair success: ${x.repair_success_rate}%`,
    `Fallback rate: ${x.fallback_rate}%`,
    `Avg lint (init -> final): ${x.avg_lint_initial} -> ${x.avg_lint_final}`,
  ];
}

function renderRecentRows(rows) {
  dom.recentRows.innerHTML = rows
    .map((r) => {
      const dt = r.ts ? new Date(r.ts).toLocaleTimeString() : "-";
      return `<tr>
        <td class="mono">${dt}</td>
        <td>${escapeHtml(String(r.prompt || ""))}</td>
        <td>${escapeHtml(r.validation || "-")}</td>
        <td>${escapeHtml(r.strategy || "-")}</td>
        <td class="mono">${r.lint_initial_count || 0} -> ${r.lint_final_count || 0}</td>
        <td>${r.normalized_changed ? "yes" : "no"}</td>
        <td>${r.exemplars_used || 0}</td>
      </tr>`;
    })
    .join("");
}

function isPinned(family, prompt) {
  return state.pinned.some((x) => x.family === family && x.prompt === prompt);
}

async function setPinned(entry, pinned) {
  await fetch("/api/exemplars/pinned", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      family: entry.family,
      prompt: entry.prompt,
      program: entry.program,
      pinned,
    }),
  });
  await refresh();
}

function renderExemplarRows(rows) {
  dom.exemplarRows.innerHTML = rows
    .slice(0, 80)
    .map((r, idx) => {
      const dt = r.ts ? new Date(r.ts).toLocaleTimeString() : "-";
      const pin = isPinned(r.family, r.prompt);
      return `<tr>
        <td class="mono">${dt}</td>
        <td><span class="badge">${escapeHtml(r.family || "general:generic")}</span></td>
        <td>${escapeHtml(r.prompt || "")}</td>
        <td>${escapeHtml(r.validation || "-")} / ${escapeHtml(r.strategy || "-")}</td>
        <td class="mono">${r.lint_initial_count || 0} -> ${r.lint_final_count || 0}</td>
        <td><button data-pin-row="${idx}">${pin ? "Unpin" : "Pin Seed"}</button></td>
      </tr>`;
    })
    .join("");

  const buttons = dom.exemplarRows.querySelectorAll("button[data-pin-row]");
  buttons.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const idx = Number(btn.getAttribute("data-pin-row"));
      const entry = rows[idx];
      if (!entry) return;
      await setPinned(entry, !isPinned(entry.family, entry.prompt));
    });
  });
}

function renderPinnedRows(rows) {
  dom.pinnedRows.innerHTML = rows
    .map((r, idx) => {
      const dt = r.pinned_at ? new Date(r.pinned_at).toLocaleString() : "-";
      return `<tr>
        <td><span class="badge">${escapeHtml(r.family)}</span><div class="small muted">${escapeHtml(dt)}</div></td>
        <td>${escapeHtml(r.prompt)}</td>
        <td><button data-unpin="${idx}">Remove</button></td>
      </tr>`;
    })
    .join("");

  dom.pinnedRows.querySelectorAll("button[data-unpin]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const idx = Number(btn.getAttribute("data-unpin"));
      const entry = rows[idx];
      if (!entry) return;
      await setPinned(entry, false);
    });
  });
}

function renderAll(detail) {
  renderCards(detail);
  setList(dom.byValidation, Object.entries(detail.by_validation || {}).map(([k, v]) => `${k}: ${v}`));
  setList(dom.byStrategy, Object.entries(detail.by_strategy || {}).map(([k, v]) => `${k}: ${v}`));
  setList(dom.runOutcome, Object.entries(detail.run_outcome || {}).map(([k, v]) => `${k}: ${v}`));
  setList(dom.topLint, (detail.top_lint_findings || []).map((x) => `${x.count}x - ${x.finding}`));
  setList(
    dom.repairFunnel,
    Object.entries(detail.repair_funnel || {}).map(([k, v]) => `${k.replaceAll("_", " ")}: ${v}`)
  );
  setList(dom.score7, formatScorecard(detail.scorecards?.days_7 || {}));
  setList(dom.score30, formatScorecard(detail.scorecards?.days_30 || {}));
  setList(
    dom.intentClusters,
    (detail.intent_clusters || []).map((x) => `${x.intent}: total ${x.total}, success ${x.success_rate}%`)
  );
  setList(dom.heatmap, (detail.validator_heatmap || []).map((x) => `${x.statement}: ${x.count}`));

  const exImpact = detail.exemplar_impact || {};
  setList(dom.exemplarImpact, [
    `With exemplars: success ${exImpact.with_exemplars?.success_rate || 0}% / fallback ${exImpact.with_exemplars?.fallback_rate || 0}% (n=${exImpact.with_exemplars?.total || 0})`,
    `Without exemplars: success ${exImpact.without_exemplars?.success_rate || 0}% / fallback ${exImpact.without_exemplars?.fallback_rate || 0}% (n=${exImpact.without_exemplars?.total || 0})`,
  ]);

  const drift = detail.drift || {};
  const driftItems = [
    `Recent fallback rate: ${drift.recent_fallback_rate || 0}%`,
    `Baseline fallback rate: ${drift.baseline_fallback_rate || 0}%`,
    ...((drift.alerts || []).length ? drift.alerts : ["No drift alerts"]),
  ];
  setList(dom.driftAlerts, driftItems);

  renderRecentRows(detail.recent_generations || []);
  renderExemplarRows(detail.exemplar_library || []);
}

async function refresh() {
  const [detailRes, pinnedRes] = await Promise.all([fetch("/api/metrics/detail"), fetch("/api/exemplars/pinned")]);
  const detail = await detailRes.json();
  const pinned = await pinnedRes.json();
  if (!detailRes.ok) throw new Error(detail.error || "metrics unavailable");
  if (!pinnedRes.ok) throw new Error(pinned.error || "pinned exemplars unavailable");

  state.detail = detail;
  state.pinned = Array.isArray(pinned.exemplars) ? pinned.exemplars : [];
  renderAll(detail);
  renderPinnedRows(state.pinned);
}

dom.refresh.addEventListener("click", () => {
  void refresh();
});

dom.runReplay.addEventListener("click", async () => {
  const prompt = String(dom.replayPrompt.value || "").trim();
  if (!prompt) return;
  dom.replayResult.textContent = "Running replay...";
  try {
    const response = await fetch("/api/replay", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        prompt,
        code: "",
        judge: Boolean(dom.replayJudge.checked),
      }),
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "Replay failed.");
    const report = payload.validator_report || {};
    const judge = payload.judge || null;
    dom.replayResult.textContent = [
      `status: ${report.status || "unknown"}`,
      `strategy: ${report.strategy || "unknown"}`,
      `judge: ${judge ? `${judge.verdict} (${judge.score})` : "off"}`,
      "",
      String(payload.rationale || ""),
    ].join("\n");
    await refresh();
  } catch (error) {
    dom.replayResult.textContent = `Replay failed: ${error.message}`;
  }
});

void refresh();
setInterval(() => {
  void refresh();
}, 15000);
