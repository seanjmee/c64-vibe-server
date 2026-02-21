function getApiHeaders() {
  const key = localStorage.getItem("c64-vibe-api-key") || "";
  return key ? { "x-c64-api-key": key } : {};
}

function normalizePatchPayload(payload) {
  if (!payload || typeof payload !== "object") {
    throw new Error("AI response was empty or invalid JSON.");
  }

  if (!Array.isArray(payload.operations) || !payload.operations.length) {
    throw new Error("AI response missing 'operations'.");
  }

  return {
    rationale: String(payload.rationale || "Generated update."),
    operations: payload.operations,
    validatorReport:
      payload.validator_report && typeof payload.validator_report === "object"
        ? payload.validator_report
        : null,
  };
}

export function applyOperations(currentCode, operations) {
  let code = currentCode;

  for (const op of operations) {
    if (!op || typeof op !== "object") continue;

    if (op.op === "replace_file") {
      code = String(op.content || "");
      continue;
    }

    if (op.op === "replace_line_range") {
      const start = Number(op.startLine || 1);
      const end = Number(op.endLine || start);
      const replacement = String(op.content || "");
      const lines = code.split(/\r?\n/);
      const prefix = lines.slice(0, Math.max(0, start - 1));
      const suffix = lines.slice(Math.max(start, end));
      code = [...prefix, ...replacement.split(/\r?\n/), ...suffix].join("\n");
      continue;
    }

    if (op.op === "append_lines") {
      const content = String(op.content || "");
      code = `${code.trimEnd()}\n${content}`;
    }
  }

  return code;
}

export async function requestAIPatch({ prompt, code, chatHistory }) {
  const response = await fetch("/api/generate", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...getApiHeaders(),
    },
    body: JSON.stringify({ prompt, code, chatHistory }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `AI request failed with status ${response.status}`);
  }

  const payload = await response.json();
  return normalizePatchPayload(payload);
}
