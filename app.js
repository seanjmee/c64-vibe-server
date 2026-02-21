import { executeBasic, generateProgramFromPrompt } from "./basic-engine.js";
import { buildPrgBinary } from "./prg.js";
import { applyOperations, requestAIPatch } from "./llm-client.js";

const dom = {
  chatFeed: document.getElementById("chatFeed"),
  chatForm: document.getElementById("chatForm"),
  chatInput: document.getElementById("chatInput"),
  codeEditor: document.getElementById("codeEditor"),
  monitor: document.getElementById("monitor"),
  runtimeLog: document.getElementById("runtimeLog"),
  bubbleTemplate: document.getElementById("chatBubbleTemplate"),
  applyPatch: document.getElementById("applyPatch"),
  runProgram: document.getElementById("runProgram"),
  resetMachine: document.getElementById("resetMachine"),
  exportPrg: document.getElementById("exportPrg"),
  newSession: document.getElementById("newSession"),
  toggleRuntime: document.getElementById("toggleRuntime"),
  emulatorFrame: document.getElementById("emulatorFrame"),
  emulatorStatus: document.getElementById("emulatorStatus"),
  modelMetrics: document.getElementById("modelMetrics"),
  validatorReport: document.getElementById("validatorReport"),
};

const STORAGE_KEY = "c64-vibe-mvp-state";
const EMBEDDED_EMULATOR_URL = "/embedded-emulator.html";
const LEGACY_EMULATOR_URL_KEY = "c64-vibe-emulator-url";

const state = {
  lastSuggestion: "",
  chat: [],
  useRealEmulator: false,
  validatorReport: null,
};

let metricsTimer = null;

function getApiHeaders() {
  const key = localStorage.getItem("c64-vibe-api-key") || "";
  return key ? { "x-c64-api-key": key } : {};
}

function nowText() {
  return new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function persist() {
  const payload = {
    code: dom.codeEditor.value,
    chat: state.chat,
    lastSuggestion: state.lastSuggestion,
    useRealEmulator: state.useRealEmulator,
    validatorReport: state.validatorReport,
  };
  localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
}

function load() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return false;
    const parsed = JSON.parse(raw);
    dom.codeEditor.value = parsed.code || "";
    state.lastSuggestion = parsed.lastSuggestion || "";
    state.chat = Array.isArray(parsed.chat) ? parsed.chat : [];
    state.useRealEmulator = Boolean(parsed.useRealEmulator);
    state.validatorReport =
      parsed.validatorReport && typeof parsed.validatorReport === "object" ? parsed.validatorReport : null;
    return true;
  } catch {
    return false;
  }
}

function appendBubble(role, message) {
  const frag = dom.bubbleTemplate.content.cloneNode(true);
  const root = frag.querySelector(".bubble");
  root.classList.add(role);
  frag.querySelector(".role").textContent = role.toUpperCase();
  frag.querySelector(".timestamp").textContent = nowText();
  frag.querySelector(".message").textContent = message;
  dom.chatFeed.appendChild(frag);
  dom.chatFeed.scrollTop = dom.chatFeed.scrollHeight;
}

function replayChat() {
  dom.chatFeed.innerHTML = "";
  for (const item of state.chat) {
    appendBubble(item.role, item.message);
  }
}

function addMessage(role, message) {
  state.chat.push({ role, message });
  appendBubble(role, message);
  persist();
}

function renderMonitor(execResult) {
  const header = `**** COMMODORE 64 BASIC V2 ****\n64K RAM SYSTEM 38911 BASIC BYTES FREE\n`;
  const color = `\nBORDER:${execResult.border.toString().padStart(2, "0")} BG:${execResult.background
    .toString()
    .padStart(2, "0")}`;
  dom.monitor.textContent = `${header}${execResult.screen.join("\n")}${color}`;
  dom.runtimeLog.textContent = execResult.logs.length ? execResult.logs.join("\n") : "READY.";
}

function setEmulatorStatus(text, tone = "") {
  if (!dom.emulatorStatus) return;
  dom.emulatorStatus.textContent = text;
  dom.emulatorStatus.classList.remove("booting", "ready", "error");
  if (tone) dom.emulatorStatus.classList.add(tone);
}

function runCurrentProgram() {
  const result = executeBasic(dom.codeEditor.value);
  if (!state.useRealEmulator) {
    renderMonitor(result);
    setEmulatorStatus("simulated runtime", "ready");
  }
  persist();
}

function setMetricsLabel(text) {
  if (!dom.modelMetrics) return;
  dom.modelMetrics.textContent = text;
}

function renderValidatorReport(report) {
  if (!dom.validatorReport) return;
  if (!report || typeof report !== "object") {
    dom.validatorReport.textContent = "No generation yet.";
    return;
  }

  const lines = [];
  lines.push(`Status: ${String(report.status || "unknown")}`);
  lines.push(`Strategy: ${String(report.strategy || "unknown")}`);
  lines.push(`Exemplars used: ${Number(report.exemplars_used || 0)}`);
  lines.push(`Typos normalized: ${report.normalized_changed ? "yes" : "no"}`);

  const initial = Array.isArray(report.initial_issues) ? report.initial_issues : [];
  const final = Array.isArray(report.final_issues) ? report.final_issues : [];
  lines.push(`Initial issues: ${initial.length}`);
  for (const issue of initial.slice(0, 8)) lines.push(`- ${issue}`);
  if (initial.length > 8) lines.push(`- ... ${initial.length - 8} more`);

  lines.push(`Final issues: ${final.length}`);
  for (const issue of final.slice(0, 8)) lines.push(`- ${issue}`);
  if (final.length > 8) lines.push(`- ... ${final.length - 8} more`);

  if (report.fallback_reason) lines.push(`Fallback reason: ${String(report.fallback_reason)}`);
  dom.validatorReport.textContent = lines.join("\n");
}

async function refreshMetrics() {
  try {
    const response = await fetch("/api/metrics");
    const metrics = await response.json();
    if (!response.ok) throw new Error(metrics.error || "metrics unavailable");
    const total = Number(metrics.generation_total || 0);
    const good = Number(metrics.generation_accepted || 0) + Number(metrics.generation_repaired || 0);
    const rate = total > 0 ? Math.round((good / total) * 100) : 0;
    setMetricsLabel(`gen ok ${rate}% (${good}/${total})`);
  } catch {
    setMetricsLabel("gen: n/a");
  }
}

async function postRunEvent(event, outcome, detail = "") {
  try {
    const lastUserPrompt = state.chat.filter((m) => m.role === "user").slice(-1)[0]?.message || "";
    await fetch("/api/run-event", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...getApiHeaders() },
      body: JSON.stringify({
        event,
        outcome,
        detail,
        prompt: lastUserPrompt,
      }),
    });
  } catch {
    // best effort only
  }
}

function setEmulatorMode(enabled) {
  state.useRealEmulator = enabled;
  dom.toggleRuntime.textContent = enabled ? "Use Simulated Runtime" : "Use Real Emulator";
  dom.monitor.classList.toggle("hidden", enabled);
  dom.emulatorFrame.classList.toggle("hidden", !enabled);

  if (enabled) {
    dom.emulatorFrame.src = EMBEDDED_EMULATOR_URL;
    setEmulatorStatus("booting emulator", "booting");
    dom.runtimeLog.textContent = "Embedded jsc64 emulator loaded.";
  } else {
    runCurrentProgram();
  }
  persist();
}

function bytesToBase64(bytes) {
  let bin = "";
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    const chunk = bytes.subarray(i, i + chunkSize);
    bin += String.fromCharCode(...chunk);
  }
  return btoa(bin);
}

async function syncEmbeddedEmulator() {
  if (!state.useRealEmulator || !dom.emulatorFrame?.contentWindow) return;

  try {
    const prgBytes = buildPrgBinary(dom.codeEditor.value);
    const response = await fetch("/api/prg", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...getApiHeaders() },
      body: JSON.stringify({
        filename: "active.prg",
        data: bytesToBase64(prgBytes),
      }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "PRG upload failed");
    }
    dom.emulatorFrame.contentWindow.postMessage({ type: "loadPrg", url: payload.url }, window.location.origin);
    dom.runtimeLog.textContent = `Embedded emulator synced (${payload.size} bytes).`;
    setEmulatorStatus("prg loaded", "ready");
    void postRunEvent("embedded_sync", "ok", `${payload.size} bytes`);
  } catch (error) {
    dom.runtimeLog.textContent = `Embedded sync failed: ${error.message}`;
    setEmulatorStatus("sync failed", "error");
    void postRunEvent("embedded_sync", "error", String(error.message || "sync failed"));
  }
}

function bootstrap() {
  localStorage.removeItem(LEGACY_EMULATOR_URL_KEY);
  const hadState = load();
  if (!hadState || !dom.codeEditor.value.trim()) {
    const starter = `10 PRINT "C64 VIBE CODER READY"
20 PRINT "TYPE A PROMPT ON THE LEFT"
30 END`;
    dom.codeEditor.value = starter;
    state.lastSuggestion = starter;
    state.chat = [];
    addMessage(
      "assistant",
      "Describe what to build in BASIC. I will call the backend model for a structured patch, then you can apply and run."
    );
  } else {
    replayChat();
  }

  setEmulatorMode(state.useRealEmulator);
  runCurrentProgram();
  renderValidatorReport(state.validatorReport);
  void refreshMetrics();
  if (metricsTimer) clearInterval(metricsTimer);
  metricsTimer = setInterval(() => {
    void refreshMetrics();
  }, 15000);
}

function buildFallbackResponse(prompt) {
  const generated = generateProgramFromPrompt(prompt);
  state.lastSuggestion = generated.code;
  state.validatorReport = {
    status: "fallback",
    strategy: "client_fallback",
    exemplars_used: 0,
    normalized_changed: false,
    initial_issues: ["Backend unavailable; used local fallback template."],
    final_issues: [],
    fallback_reason: "backend_unavailable",
  };
  renderValidatorReport(state.validatorReport);
  persist();
  return [
    `Plan: ${generated.rationale}`,
    "Patch prepared for `program.bas` (fallback mode):",
    generated.code,
    "Click 'Apply Last Patch' then Run.",
  ].join("\n\n");
}

function looksLikeBasicV2(program) {
  const lines = program
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) return false;
  if (lines.some((line) => !/^\d+\s+/.test(line))) return false;

  const upper = ` ${program.toUpperCase()} `;
  const banned = [" WHILE ", " WEND ", " TRUE", " FALSE", " DO ", " LOOP ", " ELSEIF ", " ENDIF", " ELSE "];
  return !banned.some((token) => upper.includes(token));
}

function normalizeCommonTypos(program) {
  return String(program || "")
    .replace(/\bGTO\b/gi, "GOTO")
    .replace(/\bGOSB\b/gi, "GOSUB")
    .replace(/\bPRNIT\b/gi, "PRINT")
    .replace(/\bINPT\b/gi, "INPUT")
    .replace(/\bRETUNR?\b/gi, "RETURN");
}

async function buildAssistantResponse(prompt) {
  try {
    const aiPatch = await requestAIPatch({
      prompt,
      code: dom.codeEditor.value,
      chatHistory: state.chat.slice(-8),
    });

    const candidateRaw = applyOperations(dom.codeEditor.value, aiPatch.operations);
    const candidate = normalizeCommonTypos(candidateRaw);
    if (!looksLikeBasicV2(candidate)) {
      throw new Error("Model returned non-C64 BASIC V2 output.");
    }
    state.lastSuggestion = candidate;
    state.validatorReport =
      aiPatch.validatorReport && typeof aiPatch.validatorReport === "object" ? aiPatch.validatorReport : null;
    renderValidatorReport(state.validatorReport);
    persist();

    return [
      `Plan: ${aiPatch.rationale}`,
      "Patch prepared for `program.bas` (model mode):",
      candidate,
      "Click 'Apply Last Patch' then Run.",
    ].join("\n\n");
  } catch (error) {
    const fallback = buildFallbackResponse(prompt);
    return `${fallback}\n\nModel backend unavailable, used fallback: ${error.message}`;
  }
}

bootstrap();

// Auto-run on manual edits so runtime stays in sync with coding changes.
dom.codeEditor.addEventListener("input", () => {
  runCurrentProgram();
});

dom.chatForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const prompt = dom.chatInput.value.trim();
  if (!prompt) return;

  addMessage("user", prompt);
  dom.chatInput.value = "";
  const response = await buildAssistantResponse(prompt);
  addMessage("assistant", response);
});

dom.applyPatch.addEventListener("click", () => {
  if (!state.lastSuggestion) return;
  dom.codeEditor.value = state.lastSuggestion;
  addMessage("assistant", "Applied latest patch to program.bas.");
  runCurrentProgram();
  void syncEmbeddedEmulator();
});

dom.runProgram.addEventListener("click", () => {
  runCurrentProgram();
  void syncEmbeddedEmulator();
  void postRunEvent("run_clicked", "launched", state.useRealEmulator ? "real" : "simulated");
  addMessage("assistant", "Program executed in the active runtime panel.");
});

dom.resetMachine.addEventListener("click", () => {
  dom.runtimeLog.textContent = "Machine reset.";
  runCurrentProgram();
  if (state.useRealEmulator) {
    dom.emulatorFrame.src = EMBEDDED_EMULATOR_URL;
    setEmulatorStatus("booting emulator", "booting");
  }
});

dom.exportPrg.addEventListener("click", () => {
  try {
    const source = dom.codeEditor.value;
    const prgBytes = buildPrgBinary(dom.codeEditor.value);
    if (/CHR\$/i.test(source)) {
      const hasChrCallToken = prgBytes.some(
        (value, i) => i + 1 < prgBytes.length && value === 0xc7 && prgBytes[i + 1] === 0x28
      );
      if (!hasChrCallToken) {
        throw new Error("Tokenizer sanity check failed for CHR$ call (expected C7 28 bytes).");
      }
    }
    const blob = new Blob([prgBytes], { type: "application/octet-stream" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = "program.prg";
    link.click();
    URL.revokeObjectURL(link.href);
    addMessage("assistant", `Exported tokenized binary PRG (${prgBytes.length} bytes).`);
    void syncEmbeddedEmulator();
  } catch (error) {
    addMessage("assistant", `PRG export failed: ${error.message}`);
  }
});

dom.newSession.addEventListener("click", () => {
  localStorage.removeItem(STORAGE_KEY);
  state.chat = [];
  state.lastSuggestion = "";
  state.validatorReport = null;
  dom.chatFeed.innerHTML = "";
  dom.codeEditor.value = `10 PRINT "NEW SESSION"
20 END`;
  renderValidatorReport(null);
  addMessage("assistant", "Session reset. Ready for a new prompt.");
  runCurrentProgram();
});

dom.toggleRuntime.addEventListener("click", () => {
  setEmulatorMode(!state.useRealEmulator);
});

window.addEventListener("message", (event) => {
  if (event.origin !== window.location.origin) return;
  const data = event.data;
  if (!data || typeof data !== "object") return;
  if (data.type !== "emulatorStatus") return;

  const statusText = String(data.status || "").toLowerCase();
  if (statusText.includes("error")) {
    setEmulatorStatus(String(data.status), "error");
    return;
  }
  if (statusText.includes("ready") || statusText.includes("loaded")) {
    setEmulatorStatus(String(data.status), "ready");
    return;
  }
  setEmulatorStatus(String(data.status), "booting");
});
