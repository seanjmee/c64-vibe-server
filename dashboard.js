const dom = {
  cards: document.getElementById("cards"),
  byValidation: document.getElementById("byValidation"),
  byStrategy: document.getElementById("byStrategy"),
  topLint: document.getElementById("topLint"),
  runOutcome: document.getElementById("runOutcome"),
  recentRows: document.getElementById("recentRows"),
  refresh: document.getElementById("refresh"),
};

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
    .map(
      (c) =>
        `<article class="card"><div class="label">${c.label}</div><div class="value ${c.klass}">${c.value}</div></article>`
    )
    .join("");
}

function renderRows(rows) {
  dom.recentRows.innerHTML = rows
    .map((r) => {
      const dt = r.ts ? new Date(r.ts).toLocaleTimeString() : "-";
      return `<tr>
        <td class="mono">${dt}</td>
        <td>${String(r.prompt || "").replace(/[<>&]/g, "")}</td>
        <td>${r.validation || "-"}</td>
        <td>${r.strategy || "-"}</td>
        <td class="mono">${r.lint_initial_count || 0} -> ${r.lint_final_count || 0}</td>
        <td>${r.normalized_changed ? "yes" : "no"}</td>
        <td>${r.exemplars_used || 0}</td>
      </tr>`;
    })
    .join("");
}

async function refresh() {
  const response = await fetch("/api/metrics/detail");
  const detail = await response.json();
  if (!response.ok) throw new Error(detail.error || "metrics unavailable");

  renderCards(detail);

  const byValidation = Object.entries(detail.by_validation || {}).map(([k, v]) => `${k}: ${v}`);
  const byStrategy = Object.entries(detail.by_strategy || {}).map(([k, v]) => `${k}: ${v}`);
  const runOutcome = Object.entries(detail.run_outcome || {}).map(([k, v]) => `${k}: ${v}`);
  const topLint = (detail.top_lint_findings || []).map((x) => `${x.count}x - ${x.finding}`);

  setList(dom.byValidation, byValidation.length ? byValidation : ["No data"]);
  setList(dom.byStrategy, byStrategy.length ? byStrategy : ["No data"]);
  setList(dom.runOutcome, runOutcome.length ? runOutcome : ["No data"]);
  setList(dom.topLint, topLint.length ? topLint : ["No lint findings"]);
  renderRows(detail.recent_generations || []);
}

dom.refresh.addEventListener("click", () => {
  void refresh();
});

void refresh();
setInterval(() => {
  void refresh();
}, 15000);
