const WIDTH = 40;
const HEIGHT = 25;
const MAX_STEPS = 2200;

function blankScreen() {
  return Array.from({ length: HEIGHT }, () => " ".repeat(WIDTH));
}

function parseProgram(source) {
  const lines = source
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((raw) => {
      const match = raw.match(/^(\d+)\s+(.*)$/);
      if (!match) {
        return null;
      }
      return { number: Number(match[1]), code: match[2].trim() };
    })
    .filter(Boolean)
    .sort((a, b) => a.number - b.number);

  const indexByLine = new Map(lines.map((line, index) => [line.number, index]));
  return { lines, indexByLine };
}

function setLine(screen, y, text) {
  if (y < 0 || y >= HEIGHT) return;
  screen[y] = (text + " ".repeat(WIDTH)).slice(0, WIDTH);
}

function scroll(screen) {
  screen.shift();
  screen.push(" ".repeat(WIDTH));
}

function resolveAtom(atom, vars) {
  const trimmed = atom.trim();
  if (/^[A-Z]$/.test(trimmed)) {
    return vars[trimmed] ?? 0;
  }
  const n = Number(trimmed);
  return Number.isFinite(n) ? n : 0;
}

function evalExpression(token, vars) {
  const expr = token.trim();
  if (!expr) return 0;

  const expanded = expr.replace(/\b([A-Z])\b/g, (_m, v) => String(vars[v] ?? 0));
  if (!/^[0-9+\-*/().\s]+$/.test(expanded)) {
    return resolveAtom(expr, vars);
  }
  try {
    const value = evalArithmeticExpression(expanded);
    return Number.isFinite(value) ? value : 0;
  } catch {
    return 0;
  }
}

function evalArithmeticExpression(source) {
  const s = String(source || "").replace(/\s+/g, "");
  const values = [];
  const ops = [];
  let i = 0;
  const prec = { "+": 1, "-": 1, "*": 2, "/": 2 };

  const applyTop = () => {
    const op = ops.pop();
    const b = Number(values.pop() ?? 0);
    const a = Number(values.pop() ?? 0);
    if (op === "+") values.push(a + b);
    else if (op === "-") values.push(a - b);
    else if (op === "*") values.push(a * b);
    else if (op === "/") values.push(b === 0 ? 0 : a / b);
  };

  while (i < s.length) {
    const ch = s[i];
    if (ch === "(") {
      ops.push(ch);
      i += 1;
      continue;
    }
    if (ch === ")") {
      while (ops.length && ops[ops.length - 1] !== "(") applyTop();
      if (ops[ops.length - 1] === "(") ops.pop();
      i += 1;
      continue;
    }
    if (/[+\-*/]/.test(ch)) {
      // unary minus: treat as 0 - expr
      if (ch === "-" && (i === 0 || s[i - 1] === "(" || /[+\-*/]/.test(s[i - 1]))) {
        values.push(0);
      }
      while (ops.length && ops[ops.length - 1] !== "(" && prec[ops[ops.length - 1]] >= prec[ch]) applyTop();
      ops.push(ch);
      i += 1;
      continue;
    }

    const m = s.slice(i).match(/^\d+(\.\d+)?/);
    if (!m) throw new Error("invalid arithmetic expression");
    values.push(Number(m[0]));
    i += m[0].length;
  }

  while (ops.length) applyTop();
  return Number(values.pop() ?? 0);
}

function evalCondition(expr, vars) {
  const m = expr.match(/^(.+?)(=|<|>|<=|>=|<>)(.+)$/);
  if (!m) return false;
  const left = evalExpression(m[1], vars);
  const right = evalExpression(m[3], vars);
  const op = m[2];
  if (op === "=") return left === right;
  if (op === "<") return left < right;
  if (op === ">") return left > right;
  if (op === "<=") return left <= right;
  if (op === ">=") return left >= right;
  if (op === "<>") return left !== right;
  return false;
}

function splitStatements(code, options = {}) {
  const { preserveIfThenColon = true } = options;
  const parts = [];
  let current = "";
  let inQuote = false;

  for (const ch of code) {
    if (ch === '"') inQuote = !inQuote;

    if (ch === ":" && !inQuote) {
      const probe = current.trim().toUpperCase();
      const isIfThen = preserveIfThenColon && probe.startsWith("IF ") && /\bTHEN\b/.test(probe);
      if (isIfThen) {
        current += ch;
        continue;
      }
      if (current.trim()) parts.push(current.trim());
      current = "";
      continue;
    }
    current += ch;
  }

  if (current.trim()) parts.push(current.trim());
  return parts;
}

function splitPrintParts(content) {
  const parts = [];
  let current = "";
  let inQuote = false;
  for (const ch of content) {
    if (ch === '"') inQuote = !inQuote;
    if (!inQuote && (ch === ";" || ch === ",")) {
      parts.push({ text: current.trim(), sep: ch });
      current = "";
      continue;
    }
    current += ch;
  }
  if (current.trim() || parts.length) {
    parts.push({ text: current.trim(), sep: "" });
  }
  return parts;
}

export function executeBasic(source, options = {}) {
  const inputProvider = typeof options.inputProvider === "function" ? options.inputProvider : null;
  const { lines, indexByLine } = parseProgram(source);
  const logs = [];
  const screen = blankScreen();
  const vars = {};

  let cursorY = 0;
  let pc = 0;
  let steps = 0;
  let running = true;
  let border = 6;
  let background = 14;

  if (lines.length === 0) {
    return {
      screen,
      logs: ["No runnable numbered BASIC lines were found."],
      border,
      background,
      halted: true,
    };
  }

  const runStatement = (stmt, number) => {
    const upper = stmt.toUpperCase();

    if (upper.startsWith("REM")) return;

    if (upper === "END" || upper === "STOP") {
      logs.push(`Line ${number}: program halted.`);
      running = false;
      return;
    }

    if (upper === "CLS" || upper === "PRINT CHR$(147)") {
      for (let i = 0; i < HEIGHT; i += 1) {
        setLine(screen, i, "");
      }
      cursorY = 0;
      return;
    }

    if (upper.startsWith("POKE")) {
      const pm = upper.match(/^POKE\s+(\d+)\s*,\s*(.+)$/);
      if (!pm) {
        logs.push(`Line ${number}: invalid POKE syntax.`);
        return;
      }
      const addr = Number(pm[1]);
      const value = Math.trunc(evalExpression(pm[2], vars));
      if (addr === 53280) border = value;
      if (addr === 53281) background = value;
      logs.push(`Line ${number}: poke ${addr},${value}`);
      return;
    }

    if (upper.startsWith("INPUT")) {
      const im = stmt.match(/^INPUT\s+(?:"[^"]*"\s*;)?\s*([A-Z])$/i);
      if (!im) {
        logs.push(`Line ${number}: unsupported INPUT syntax.`);
        return;
      }
      const varName = im[1].toUpperCase();
      const provided = inputProvider ? inputProvider({ varName, line: number, vars: { ...vars } }) : "0";
      const parsed = Number(provided);
      vars[varName] = Number.isFinite(parsed) ? parsed : 0;
      return;
    }

    if (upper.startsWith("LET ")) {
      const lm = stmt.match(/^LET\s+([A-Z])\s*=\s*(.+)$/i);
      if (lm) {
        vars[lm[1].toUpperCase()] = evalExpression(lm[2], vars);
      }
      return;
    }

    if (/^[A-Z]\s*=/.test(upper)) {
      const am = stmt.match(/^([A-Z])\s*=\s*(.+)$/i);
      if (am) {
        vars[am[1].toUpperCase()] = evalExpression(am[2], vars);
      }
      return;
    }

    if (upper.startsWith("PRINT")) {
      const content = stmt.slice(5).trim();
      const parts = splitPrintParts(content);
      let out = "";
      for (const part of parts) {
        const t = part.text;
        const rendered =
          t.startsWith('"') && t.endsWith('"') ? t.slice(1, -1) : String(evalExpression(t, vars));
        out += rendered;
        if (part.sep === ",") out += " ";
      }
      setLine(screen, cursorY, out);
      cursorY += 1;
      if (cursorY >= HEIGHT) {
        scroll(screen);
        cursorY = HEIGHT - 1;
      }
      return;
    }

    if (upper.startsWith("GOTO")) {
      const target = Number(upper.replace("GOTO", "").trim());
      if (indexByLine.has(target)) {
        pc = indexByLine.get(target);
        return "jump";
      }
      logs.push(`Line ${number}: missing target line ${target}.`);
      running = false;
      return;
    }

    if (upper.startsWith("IF ")) {
      const im = stmt.match(/^IF\s+(.+)\s+THEN\s+(.+)$/i);
      if (!im) {
        logs.push(`Line ${number}: invalid IF syntax.`);
        return;
      }
      const ok = evalCondition(im[1], vars);
      if (!ok) return;
      const thenPart = im[2].trim();
      if (/^\d+$/.test(thenPart)) {
        const target = Number(thenPart);
        if (indexByLine.has(target)) {
          pc = indexByLine.get(target);
          return "jump";
        }
        logs.push(`Line ${number}: missing IF target line ${target}.`);
        running = false;
        return;
      }
      for (const nested of splitStatements(thenPart, { preserveIfThenColon: false })) {
        const res = runStatement(nested, number);
        if (res === "jump") return "jump";
        if (!running) return;
      }
      return;
    }

    logs.push(`Line ${number}: unsupported statement '${stmt}'.`);
  };

  while (running && pc >= 0 && pc < lines.length && steps < MAX_STEPS) {
    const { number, code } = lines[pc];
    steps += 1;

    let jumped = false;
    for (const stmt of splitStatements(code)) {
      const res = runStatement(stmt, number);
      if (res === "jump") {
        jumped = true;
        break;
      }
      if (!running) break;
    }

    if (!running) break;
    if (!jumped) pc += 1;
  }

  if (steps >= MAX_STEPS) {
    logs.push("Execution halted after too many steps. Possible infinite loop.");
  }

  return {
    screen,
    logs,
    border,
    background,
    halted: !running || steps >= MAX_STEPS || pc >= lines.length,
  };
}

function hasAny(prompt, keywords) {
  return keywords.some((k) => prompt.includes(k));
}

function templateHello() {
  return `10 PRINT "*** C64 VIBE CODER ***"
20 PRINT "HELLO FROM BASIC V2"
30 END`;
}

function templateRainbow() {
  return `10 POKE 53280,2
20 POKE 53281,6
30 PRINT "BORDER COLOR: RED"
40 END`;
}

function templateLoop() {
  return `10 LET A=1
20 PRINT "FRAME" : PRINT A
30 A=A+1
40 IF A<8 THEN 20
50 PRINT "DONE"
60 END`;
}

function templateScroller() {
  return `10 PRINT "WELCOME TO THE C64 LAB"
20 PRINT "MAKE SOMETHING FUN"
30 GOTO 10`;
}

function templateBouncingBall() {
  return `10 POKE 53280,0:POKE 53281,0
20 Y=0:D=1
30 POKE 1024+40*Y+20,81
40 FOR T=1 TO 220:NEXT T
50 POKE 1024+40*Y+20,32
60 Y=Y+D
70 IF Y=0 THEN D=1
80 IF Y=23 THEN D=-1
90 GOTO 30`;
}

export function generateProgramFromPrompt(promptText) {
  const prompt = promptText.toLowerCase();

  if (hasAny(prompt, ["bounce", "bouncing", "ball"])) {
    return {
      rationale: "Built a true bouncing ball animation using screen-memory POKE and velocity flip.",
      code: templateBouncingBall(),
    };
  }

  if (hasAny(prompt, ["scroll", "scroller", "loop forever", "infinite"])) {
    return {
      rationale: "Built a simple scrolling loop with a deliberate GOTO for repeated output.",
      code: templateScroller(),
    };
  }

  if (hasAny(prompt, ["rainbow", "border", "color", "poke"])) {
    return {
      rationale: "Used POKE to set classic C64 border/background colors.",
      code: templateRainbow(),
    };
  }

  if (hasAny(prompt, ["count", "counter", "loop", "frame"])) {
    return {
      rationale: "Added a finite loop with IF/THEN and a simple counter.",
      code: templateLoop(),
    };
  }

  return {
    rationale: "Started with a clean hello-world program as a safe baseline.",
    code: templateHello(),
  };
}
