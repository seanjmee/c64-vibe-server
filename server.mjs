import { createServer } from "node:http";
import { promises as fs } from "node:fs";
import path from "node:path";

const ROOT = process.cwd();
const PORT = Number(process.env.PORT || 8787);
const HOST = process.env.HOST || "127.0.0.1";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";
const OPENAI_TIMEOUT_MS = Number(process.env.OPENAI_TIMEOUT_MS || 30000);
const API_SHARED_SECRET = process.env.API_SHARED_SECRET || "";
const API_KEY_HEADER = "x-c64-api-key";
const RUN_LOG_ROTATE_BYTES = Number(process.env.RUN_LOG_ROTATE_BYTES || 5 * 1024 * 1024);
const RATE_LIMIT_WINDOW_MS = Number(process.env.RATE_LIMIT_WINDOW_MS || 60_000);
const RATE_LIMIT_MAX_MUTATIONS = Number(process.env.RATE_LIMIT_MAX_MUTATIONS || 60);
const RUNTIME_DIR = path.join(ROOT, "runtime");
const RUN_LOG_PATH = path.join(RUNTIME_DIR, "runs.jsonl");
const RUN_LOG_BACKUP_PATH = path.join(RUNTIME_DIR, "runs.1.jsonl");
const PINNED_EXEMPLARS_PATH = path.join(RUNTIME_DIR, "pinned-exemplars.json");

const CONTENT_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
};

const runLogCache = {
  data: [],
  mtimeMs: -1,
  size: -1,
};

const rateWindowByIp = new Map();

function securityHeaders(contentType = "text/plain; charset=utf-8") {
  return {
    "Content-Type": contentType,
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "X-Frame-Options": "SAMEORIGIN",
    "Content-Security-Policy":
      "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; frame-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'self'",
  };
}

function getRequestIp(req) {
  const xff = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim();
  if (xff) return xff;
  return req.socket?.remoteAddress || "unknown";
}

function isRateLimited(req) {
  const ip = getRequestIp(req);
  const now = Date.now();
  const windowStart = now - RATE_LIMIT_WINDOW_MS;
  const recent = (rateWindowByIp.get(ip) || []).filter((t) => t >= windowStart);
  recent.push(now);
  rateWindowByIp.set(ip, recent);
  return recent.length > RATE_LIMIT_MAX_MUTATIONS;
}

function isAuthorized(req) {
  if (!API_SHARED_SECRET) return true;
  const provided = String(req.headers[API_KEY_HEADER] || "");
  return provided && provided === API_SHARED_SECRET;
}

function bouncingBallProgram() {
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

function buildTwoWayLinearConverterProgram(spec) {
  const mul = spec.mul;
  const add = spec.add || 0;
  const reverseMul = 1 / mul;
  const header = `${spec.fromLabel} TO ${spec.toLabel} CONVERTER`;

  const forward = add === 0 ? `R=V*${mul}` : `R=V*${mul}+${add}`;
  const reverse = add === 0 ? `R=V*${reverseMul}` : `R=(V-${add})*${reverseMul}`;

  return `10 PRINT "${header}"
20 PRINT "1=${spec.fromAbbr}->${spec.toAbbr}, 2=${spec.toAbbr}->${spec.fromAbbr}, -999=EXIT"
30 INPUT C
40 IF C=-999 THEN END
50 IF C=1 THEN GOTO 80
60 IF C=2 THEN GOTO 110
70 PRINT "INVALID OPTION": GOTO 20
80 PRINT "ENTER ${spec.fromLabel}"
90 INPUT V
100 IF V=-999 THEN END
105 ${forward}: PRINT V;" ${spec.fromAbbr} = ";R;" ${spec.toAbbr}": GOTO 20
110 PRINT "ENTER ${spec.toLabel}"
120 INPUT V
130 IF V=-999 THEN END
140 ${reverse}: PRINT V;" ${spec.toAbbr} = ";R;" ${spec.fromAbbr}"
150 GOTO 20`;
}

function detectConverterSpec(prompt) {
  const p = ` ${String(prompt || "").toLowerCase()} `;
  const hasConversionIntent =
    /\bconvert|converter|conversion|to\b|\bfrom\b/.test(p) ||
    /\bhow many\b|\binto\b/.test(p);
  if (!hasConversionIntent) return null;

  const unitMatches = [
    {
      key: "temp_cf",
      tokens: [" celsius ", " centigrade ", " fahrenheit "],
      spec: {
        fromLabel: "CELSIUS",
        toLabel: "FAHRENHEIT",
        fromAbbr: "C",
        toAbbr: "F",
        mul: 1.8,
        add: 32,
      },
    },
    {
      key: "dist_km_mi",
      tokens: [" kilometer ", " kilometers ", " kilometre ", " kilometres ", " km ", " mile ", " miles ", " mi "],
      spec: {
        fromLabel: "KILOMETERS",
        toLabel: "MILES",
        fromAbbr: "KM",
        toAbbr: "MI",
        mul: 0.62137,
        add: 0,
      },
    },
    {
      key: "mass_kg_lb",
      tokens: [" kilogram ", " kilograms ", " kg ", " pound ", " pounds ", " lb ", " lbs "],
      spec: {
        fromLabel: "KILOGRAMS",
        toLabel: "POUNDS",
        fromAbbr: "KG",
        toAbbr: "LB",
        mul: 2.20462,
        add: 0,
      },
    },
    {
      key: "len_cm_in",
      tokens: [" centimeter ", " centimeters ", " cm ", " inch ", " inches ", " in "],
      spec: {
        fromLabel: "CENTIMETERS",
        toLabel: "INCHES",
        fromAbbr: "CM",
        toAbbr: "IN",
        mul: 0.393701,
        add: 0,
      },
    },
  ];

  for (const candidate of unitMatches) {
    const hitCount = candidate.tokens.reduce((sum, token) => sum + (p.includes(token) ? 1 : 0), 0);
    if (hitCount >= 2) return candidate.spec;
  }
  return null;
}

function extractProgramFromOperations(operations) {
  if (!Array.isArray(operations)) return "";
  for (const op of operations) {
    if (op?.op === "replace_file" && op?.path === "program.bas" && typeof op.content === "string") {
      return op.content;
    }
  }
  return "";
}

function looksLikeBasicV2(program) {
  const lines = program
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) return false;

  for (const line of lines) {
    if (!/^\d+\s+/.test(line)) return false;
  }

  const upper = ` ${String(program).toUpperCase()} `;
  const banned = [" WHILE ", " WEND ", " TRUE", " FALSE", " DO ", " LOOP ", " ELSEIF ", " ENDIF", " ELSE "];
  for (const token of banned) {
    if (upper.includes(token)) return false;
  }
  return true;
}

function splitStatements(lineCode) {
  const parts = [];
  let current = "";
  let inQuote = false;
  for (const ch of lineCode) {
    if (ch === '"') inQuote = !inQuote;
    if (ch === ":" && !inQuote) {
      parts.push(current.trim());
      current = "";
      continue;
    }
    current += ch;
  }
  if (current.trim()) parts.push(current.trim());
  return parts;
}

function normalizeCommonTypos(program) {
  let normalized = String(program || "");
  normalized = normalized
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[\u201c\u201d]/gi, '"')
    .replace(/[\u2013\u2014]/g, "-")
    .replace(/\u00a0/g, " ");
  normalized = normalized.replace(/\bGTO\b/gi, "GOTO");
  normalized = normalized.replace(/\bGOSB\b/gi, "GOSUB");
  normalized = normalized.replace(/\bPRNIT\b/gi, "PRINT");
  normalized = normalized.replace(/\bINPT\b/gi, "INPUT");
  normalized = normalized.replace(/\bRETUNR?\b/gi, "RETURN");
  return normalized;
}

function chooseLoopVar(stmtUpper) {
  const candidates = ["ZZ", "QZ", "J9", "K9"];
  for (const candidate of candidates) {
    if (!new RegExp(`\\b${candidate}\\b`).test(stmtUpper)) return candidate;
  }
  return "ZZ";
}

function rewriteUnsupportedBuiltins(program) {
  const lines = String(program || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const notes = [];
  const rewrittenLines = lines.map((rawLine) => {
    const lineMatch = rawLine.match(/^(\d+)\s+(.*)$/);
    if (!lineMatch) return rawLine;

    const lineNo = lineMatch[1];
    const code = lineMatch[2];
    const statements = splitStatements(code);
    const rewrittenStatements = statements.map((stmt) => {
      let updated = stmt;

      // C64 BASIC uses SPC() in PRINT context, not SPACE$/SPACES$ string functions.
      const spacePattern = /\bSPACES?\$\s*\(\s*([^)]+)\)/gi;
      if (spacePattern.test(updated)) {
        updated = updated.replace(spacePattern, "SPC($1)");
        notes.push(`Line ${lineNo}: rewrote SPACE$/SPACES$ to SPC().`);
      }

      // Rewrite common PRINT ... STRING$(N,"X") pattern into C64-safe looped output.
      const printMatch = updated.match(/^PRINT\s+(.+)$/i);
      if (printMatch && /\bSTRING\$\s*\(/i.test(printMatch[1])) {
        const rendered = printMatch[1].match(
          /^(.*?)(?:;\s*)?STRING\$\(\s*([^,]+)\s*,\s*"([^"]*)"\s*\)\s*(;?)\s*$/i
        );
        if (rendered) {
          const prefix = rendered[1].trim();
          const countExpr = rendered[2].trim();
          const repeatText = rendered[3].length > 0 ? rendered[3] : " ";
          const repeatChar = repeatText[0] || " ";
          const trailingSemi = rendered[4] === ";";
          const loopVar = chooseLoopVar(updated.toUpperCase());
          const prefixPart = prefix ? `PRINT ${prefix};:` : "";
          const loopPart = `FOR ${loopVar}=1 TO ${countExpr}:PRINT "${repeatChar}";:NEXT ${loopVar}`;
          const tailPart = trailingSemi ? "" : ":PRINT";
          updated = `${prefixPart}${loopPart}${tailPart}`;
          notes.push(`Line ${lineNo}: rewrote STRING$() to looped PRINT output.`);
        }
      }

      return updated;
    });

    return `${lineNo} ${rewrittenStatements.join(":")}`;
  });

  return {
    program: rewrittenLines.join("\n"),
    notes,
  };
}

function autoDimensionArrays(program) {
  const parsed = String(program || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((raw) => {
      const m = raw.match(/^(\d+)\s+(.*)$/);
      if (!m) return null;
      return { lineNo: Number(m[1]), code: m[2] };
    })
    .filter(Boolean);

  if (!parsed.length) return { program: String(program || ""), notes: [] };

  const dimmed = new Set();
  const loopTos = new Map();
  const arrayUses = [];

  for (const line of parsed) {
    const codeNoStrings = line.code.replace(/"[^"]*"/g, "");
    const dimMatch = codeNoStrings.match(/\bDIM\b\s+(.+)$/i);
    if (dimMatch) {
      for (const part of dimMatch[1].split(",")) {
        const arr = part.trim().match(/^([A-Z][A-Z0-9]*)\s*\(/i)?.[1];
        if (arr) dimmed.add(arr.toUpperCase());
      }
    }

    const forMatch = codeNoStrings.match(/\bFOR\s+([A-Z][A-Z0-9]*)\s*=\s*.+?\bTO\b\s*([0-9]+)/i);
    if (forMatch) {
      const key = forMatch[1].toUpperCase();
      const n = Number(forMatch[2]);
      if (Number.isFinite(n)) loopTos.set(key, Math.max(loopTos.get(key) || 0, n));
    }

    const useRx = /\b([A-Z][A-Z0-9]*)\s*\(\s*([A-Z][A-Z0-9]*|\d+)/gi;
    let u;
    while ((u = useRx.exec(codeNoStrings))) {
      const arr = u[1].toUpperCase();
      const idx = u[2].toUpperCase();
      if (arr === "FN") continue;
      arrayUses.push({ arr, idx });
    }
  }

  const inferred = new Map();
  for (const use of arrayUses) {
    if (dimmed.has(use.arr)) continue;
    let size = 40;
    if (/^\d+$/.test(use.idx)) size = Math.max(12, Number(use.idx) + 2);
    if (loopTos.has(use.idx)) size = Math.max(12, Number(loopTos.get(use.idx)) + 2);
    inferred.set(use.arr, Math.max(inferred.get(use.arr) || 0, size));
  }

  if (!inferred.size) return { program: String(program || ""), notes: [] };

  const usedLineNos = new Set(parsed.map((x) => x.lineNo));
  let insertLine = parsed[0].lineNo + 1;
  while (usedLineNos.has(insertLine)) insertLine += 1;

  const dimParts = Array.from(inferred.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([name, n]) => `${name}(${n})`);

  const merged = [...parsed, { lineNo: insertLine, code: `DIM ${dimParts.join(",")}` }].sort(
    (a, b) => a.lineNo - b.lineNo
  );
  return {
    program: merged.map((x) => `${x.lineNo} ${x.code}`).join("\n"),
    notes: [`Inserted ${insertLine} DIM ${dimParts.join(",")} for inferred array usage.`],
  };
}

function withProgramContent(operations, program) {
  if (!Array.isArray(operations)) return operations;
  return operations.map((op) => {
    if (op?.op === "replace_file" && op?.path === "program.bas") {
      return { ...op, content: program };
    }
    return op;
  });
}

function lintBasicV2(program) {
  const findings = [];
  const lines = program
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  let previousLine = -1;
  const lineNumbers = new Set();
  const lineBodies = [];
  const dimmedArrays = new Set();
  const usedArrays = new Map();
  for (const rawLine of lines) {
    const lineMatch = rawLine.match(/^(\d+)\s+(.*)$/);
    if (!lineMatch) {
      findings.push(`Line format invalid: '${rawLine}'`);
      continue;
    }

    const lineNo = Number(lineMatch[1]);
    const code = lineMatch[2];
    lineNumbers.add(lineNo);
    lineBodies.push({ lineNo, code });
    const codeNoStrings = code.replace(/"[^"]*"/g, "");
    const dimMatch = codeNoStrings.match(/\bDIM\b\s+(.+)$/i);
    if (dimMatch) {
      for (const part of dimMatch[1].split(",")) {
        const arr = part.trim().match(/^([A-Z][A-Z0-9]*)\s*\(/i)?.[1];
        if (arr) dimmedArrays.add(arr.toUpperCase());
      }
    }

    if (lineNo <= previousLine) {
      findings.push(`Line numbers must increase (saw ${lineNo} after ${previousLine}).`);
    }
    previousLine = lineNo;

    const quoteCount = (code.match(/"/g) || []).length;
    if (quoteCount % 2 !== 0) {
      findings.push(`Line ${lineNo}: unmatched quote.`);
    }

    const openParens = (codeNoStrings.match(/\(/g) || []).length;
    const closeParens = (codeNoStrings.match(/\)/g) || []).length;
    if (openParens !== closeParens) {
      findings.push(`Line ${lineNo}: unbalanced parentheses.`);
    }

    for (const stmt of splitStatements(code)) {
      const upper = stmt.toUpperCase();
      const stmtNoStrings = stmt.replace(/"[^"]*"/g, "");
      const arrRx = /\b([A-Z][A-Z0-9]*)\s*\(/gi;
      let arrMatch;
      while ((arrMatch = arrRx.exec(stmtNoStrings))) {
        const arrName = arrMatch[1].toUpperCase();
        if (arrName !== "FN" && !usedArrays.has(arrName)) {
          usedArrays.set(arrName, lineNo);
        }
      }

      if (upper.includes("==")) {
        findings.push(`Line ${lineNo}: use '=' (not '==').`);
      }
      if (/\bWHILE\b|\bWEND\b|\bTRUE\b|\bFALSE\b|\bDO\b|\bLOOP\b|\bELSE\b/.test(upper)) {
        findings.push(`Line ${lineNo}: non-BASIC-V2 control syntax detected.`);
      }
      if (/\bSTRING\$\s*\(/.test(upper)) {
        findings.push(`Line ${lineNo}: STRING$ is not available in C64 BASIC V2.`);
      }
      if (/\bSPACE\$?\s*\(/.test(upper) || /\bSPACES\$\s*\(/.test(upper)) {
        findings.push(`Line ${lineNo}: use SPC(n) in PRINT, not SPACE$/SPACES$.`);
      }
      if (/^\s*IF\b/.test(upper) && !/\bTHEN\b/.test(upper)) {
        findings.push(`Line ${lineNo}: IF without THEN.`);
      }
      if (/^\s*FOR\b/.test(upper) && !/\bTO\b/.test(upper)) {
        findings.push(`Line ${lineNo}: FOR without TO.`);
      }

      const isKeywordStatement = /^\s*(FOR|IF|PRINT|POKE|NEXT|GOTO|GOSUB|DATA|READ|REM|INPUT|ON|RETURN|STOP|END)\b/i.test(
        stmtNoStrings
      );
      if (!isKeywordStatement) {
        const assign = stmtNoStrings.match(/^\s*(?:LET\s+)?([^=]+?)\s*=\s*.+$/i);
        if (assign) {
          const lhs = assign[1].trim();
          if (!/^[A-Z][A-Z0-9$]*(\([^)]*\))?$/i.test(lhs)) {
            findings.push(`Line ${lineNo}: invalid assignment target '${lhs}'.`);
          }
        } else {
          const token = stmtNoStrings.trim().match(/^([A-Z][A-Z0-9$]*)/i)?.[1];
          if (token) {
            findings.push(`Line ${lineNo}: unknown or unsupported statement '${token}'.`);
          }
        }
      }
    }
  }

  for (const { lineNo, code } of lineBodies) {
    for (const stmt of splitStatements(code)) {
      const upper = stmt.toUpperCase();

      const goto = upper.match(/^\s*GOTO\s+(\d+)\s*$/);
      if (goto && !lineNumbers.has(Number(goto[1]))) {
        findings.push(`Line ${lineNo}: GOTO target ${goto[1]} not found.`);
      }

      const gosub = upper.match(/^\s*GOSUB\s+(\d+)\s*$/);
      if (gosub && !lineNumbers.has(Number(gosub[1]))) {
        findings.push(`Line ${lineNo}: GOSUB target ${gosub[1]} not found.`);
      }

      const ifThenLine = upper.match(/^\s*IF\s+.+\s+THEN\s+(\d+)\s*$/);
      if (ifThenLine && !lineNumbers.has(Number(ifThenLine[1]))) {
        findings.push(`Line ${lineNo}: IF THEN target ${ifThenLine[1]} not found.`);
      }
    }
  }

  for (const [arrName, firstLine] of usedArrays.entries()) {
    if (!dimmedArrays.has(arrName)) {
      findings.push(`Line ${firstLine}: array ${arrName}() used without DIM.`);
    }
  }

  return findings;
}

function fallbackProgramForPrompt(prompt) {
  const requested = String(prompt || "").toLowerCase();
  const dynamicConverterSpec = detectConverterSpec(prompt);
  const needsAnimationFallback = requested.includes("bounce") || requested.includes("bouncing") || requested.includes("ball");
  const needsConverterFallback = Boolean(dynamicConverterSpec) || requested.includes("convert");
  const needsStarfieldFallback = requested.includes("starfield") || requested.includes("stars");

  if (needsAnimationFallback) {
    return {
      rationale:
        "Model output was not valid C64 BASIC V2, so a validated bouncing-ball program was substituted.",
      operations: [
        {
          op: "replace_file",
          path: "program.bas",
          content: bouncingBallProgram(),
        },
      ],
    };
  }

  if (needsConverterFallback) {
    if (dynamicConverterSpec) {
      return {
        rationale:
          "Model output was not valid C64 BASIC V2, so a validated converter was generated from the requested units.",
        operations: [
          {
            op: "replace_file",
            path: "program.bas",
            content: buildTwoWayLinearConverterProgram(dynamicConverterSpec),
          },
        ],
      };
    }
    return {
      rationale:
        "Model output was not valid C64 BASIC V2, so a validated celsius/fahrenheit converter was substituted.",
      operations: [
        {
          op: "replace_file",
          path: "program.bas",
          content: buildTwoWayLinearConverterProgram({
            fromLabel: "CELSIUS",
            toLabel: "FAHRENHEIT",
            fromAbbr: "C",
            toAbbr: "F",
            mul: 1.8,
            add: 32,
          }),
        },
      ],
    };
  }

  if (needsStarfieldFallback) {
    return {
      rationale: "Model output was not valid C64 BASIC V2, so a validated starfield demo was substituted.",
      operations: [
        {
          op: "replace_file",
          path: "program.bas",
          content: `10 POKE 53280,0:POKE 53281,0
20 DIM X(22),Y(22)
25 FOR I=1 TO 20:X(I)=INT(RND(1)*40):Y(I)=INT(RND(1)*24):NEXT I
30 FOR I=1 TO 20:POKE 1024+Y(I)*40+X(I),46:NEXT I
40 FOR T=1 TO 120:NEXT T
50 FOR I=1 TO 20:POKE 1024+Y(I)*40+X(I),32:Y(I)=Y(I)+1:IF Y(I)>23 THEN Y(I)=0:X(I)=INT(RND(1)*40)
60 NEXT I
70 GOTO 30`,
        },
      ],
    };
  }

  return {
    rationale: "Model output was not valid C64 BASIC V2, so a validated baseline program was substituted.",
    operations: [
      {
        op: "replace_file",
        path: "program.bas",
        content: `10 PRINT "C64 BASIC V2 READY"
20 END`,
      },
    ],
  };
}

function tokenizePrompt(text) {
  return new Set(
    String(text || "")
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((word) => word.length > 2)
  );
}

function detectPromptIntent(text) {
  const p = ` ${String(text || "").toLowerCase()} `;
  if (/\bbounce|ball|animate|animation|move|sprite|starfield|scroll\b/.test(p)) return "animation";
  if (/\bconvert|converter|celsius|fahrenheit|km|miles|pounds|kg|inch|cm\b/.test(p)) return "converter";
  if (/\bgame|snake|pong|maze|shoot|play\b/.test(p)) return "game";
  if (/\bprint|text|banner|title|menu|pyramid|pattern\b/.test(p)) return "text_ui";
  if (/\bmath|random|calc|equation|formula\b/.test(p)) return "math";
  return "general";
}

function detectPromptFamily(text) {
  const raw = String(text || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((w) => w.length >= 4);
  const stop = new Set([
    "create",
    "build",
    "make",
    "program",
    "basic",
    "commodore",
    "vibe",
    "coder",
    "please",
    "with",
    "that",
    "this",
    "tool",
  ]);
  const keyword = raw.find((w) => !stop.has(w)) || "generic";
  return `${detectPromptIntent(text)}:${keyword}`;
}

function scorePromptSimilarity(a, b) {
  const sa = tokenizePrompt(a);
  const sb = tokenizePrompt(b);
  if (!sa.size || !sb.size) return 0;
  let overlap = 0;
  for (const token of sa) {
    if (sb.has(token)) overlap += 1;
  }
  return overlap / Math.max(sa.size, sb.size);
}

async function ensureRuntimeDir() {
  await fs.mkdir(RUNTIME_DIR, { recursive: true });
}

async function rotateRunLogIfNeeded() {
  try {
    const stat = await fs.stat(RUN_LOG_PATH);
    if (stat.size < RUN_LOG_ROTATE_BYTES) return;
    await fs.rename(RUN_LOG_PATH, RUN_LOG_BACKUP_PATH).catch(async () => {
      await fs.rm(RUN_LOG_BACKUP_PATH, { force: true });
      await fs.rename(RUN_LOG_PATH, RUN_LOG_BACKUP_PATH);
    });
    runLogCache.data = [];
    runLogCache.mtimeMs = -1;
    runLogCache.size = -1;
  } catch {
    // ignore if log does not exist yet
  }
}

async function appendRunLog(event) {
  await ensureRuntimeDir();
  await rotateRunLogIfNeeded();
  const payload = {
    ...event,
    ts: new Date().toISOString(),
  };
  await fs.appendFile(RUN_LOG_PATH, `${JSON.stringify(payload)}\n`, "utf-8");
  runLogCache.data.push(payload);
  if (runLogCache.data.length > 5000) runLogCache.data = runLogCache.data.slice(-5000);
  runLogCache.mtimeMs = Date.now();
  runLogCache.size = runLogCache.size > 0 ? runLogCache.size + JSON.stringify(payload).length + 1 : runLogCache.size;
}

async function readRunLog(limit = 500) {
  try {
    const stat = await fs.stat(RUN_LOG_PATH);
    if (runLogCache.mtimeMs !== stat.mtimeMs || runLogCache.size !== stat.size || !runLogCache.data.length) {
      const content = await fs.readFile(RUN_LOG_PATH, "utf-8");
      const lines = content.trim().split("\n").filter(Boolean);
      runLogCache.data = lines
        .map((line) => {
          try {
            return JSON.parse(line);
          } catch {
            return null;
          }
        })
        .filter(Boolean);
      runLogCache.mtimeMs = stat.mtimeMs;
      runLogCache.size = stat.size;
    }
    return runLogCache.data.slice(Math.max(0, runLogCache.data.length - limit));
  } catch {
    return [];
  }
}

async function readPinnedExemplars() {
  try {
    const raw = await fs.readFile(PINNED_EXEMPLARS_PATH, "utf-8");
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter((entry) => entry && typeof entry === "object")
      .filter(
        (entry) =>
          typeof entry.family === "string" &&
          typeof entry.prompt === "string" &&
          typeof entry.program === "string" &&
          entry.family.trim() &&
          entry.program.trim()
      )
      .map((entry) => ({
        family: entry.family.trim(),
        prompt: entry.prompt.trim(),
        program: entry.program,
        pinned_at: String(entry.pinned_at || ""),
      }));
  } catch {
    return [];
  }
}

async function writePinnedExemplars(entries) {
  await ensureRuntimeDir();
  await fs.writeFile(PINNED_EXEMPLARS_PATH, `${JSON.stringify(entries, null, 2)}\n`, "utf-8");
}

async function selectExemplars(prompt, max = 3) {
  const family = detectPromptFamily(prompt);
  const pinned = await readPinnedExemplars();
  const logs = await readRunLog(800);
  const candidates = logs
    .filter((entry) => entry.type === "generate_result")
    .filter((entry) => entry.validation === "accepted" || entry.validation === "repaired")
    .filter((entry) => typeof entry.program === "string" && entry.program.trim().length > 0)
    .map((entry) => ({
      prompt: entry.prompt,
      program: entry.program,
      score: scorePromptSimilarity(prompt, entry.prompt),
      source: "history",
    }))
    .filter((entry) => entry.score > 0)
    .sort((a, b) => b.score - a.score);

  const pinnedCandidates = pinned
    .filter((entry) => entry.family === family)
    .map((entry) => ({
      prompt: entry.prompt,
      program: entry.program,
      score: 1.1,
      source: "pinned",
    }));

  const seen = new Set();
  const picked = [];
  for (const item of [...pinnedCandidates, ...candidates]) {
    if (seen.has(item.program)) continue;
    seen.add(item.program);
    picked.push(item);
    if (picked.length >= max) break;
  }
  return picked;
}

async function readBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf-8");
}

function json(res, code, payload) {
  res.writeHead(code, securityHeaders("application/json; charset=utf-8"));
  res.end(JSON.stringify(payload));
}

function structuredPrompt(userPrompt, currentCode, exemplars = []) {
  const exemplarSection =
    exemplars.length > 0
      ? [
          "Relevant successful exemplars from prior runs:",
          ...exemplars.map(
            (entry, idx) =>
              `Example ${idx + 1} (score ${entry.score.toFixed(2)}), prompt: ${entry.prompt}\n${entry.program}`
          ),
        ].join("\n")
      : "No prior exemplars matched.";

  return [
    "You are a Commodore 64 BASIC V2 coding assistant.",
    "Return JSON only with keys: rationale (string), operations (array).",
    "Allowed operations:",
    "- {\"op\":\"replace_file\",\"path\":\"program.bas\",\"content\":\"...\",\"startLine\":0,\"endLine\":0}",
    "- {\"op\":\"replace_line_range\",\"path\":\"program.bas\",\"content\":\"...\",\"startLine\":1,\"endLine\":2}",
    "Prefer replace_file for now.",
    "When user asks for animation (bounce/ball/move), avoid static TAB+PRINT redraw loops.",
    "For moving objects, use C64 screen memory POKE at 1024 + row*40 + col and erase with 32.",
    "Strict C64 BASIC V2 only. Do NOT use ELSE/ELSEIF/ENDIF/WHILE/WEND/DO/LOOP/TRUE/FALSE.",
    "Do NOT use STRING$, SPACE$, or SPACES$ (not in C64 BASIC V2).",
    "If using arrays like A(I), include DIM A(n) before first use.",
    "For spaces, use PRINT SPC(n); and for repeated chars use FOR...NEXT with PRINT \"x\";.",
    "Use IF ... THEN with either a line number or inline statement(s) separated by ':'.",
    "Use ASCII quotes only (\").",
    "Provide runnable numbered BASIC only with ascending line numbers.",
    "Before answering, internally self-check: no unsupported keywords, balanced quotes/parentheses, valid GOTO/GOSUB targets.",
    `User request: ${userPrompt}`,
    exemplarSection,
    "Current program.bas:",
    currentCode,
  ].join("\n");
}

function repairPrompt(userPrompt, brokenProgram, currentCode, lintFindings = []) {
  const lintSection = lintFindings.length
    ? ["Lint findings to fix:", ...lintFindings.map((f) => `- ${f}`)].join("\n")
    : "No lint findings were provided.";
  return [
    "You are repairing broken code into valid Commodore 64 BASIC V2.",
    "Return JSON only with keys: rationale (string), operations (array).",
    "Use a single replace_file op for path program.bas.",
    "For replace_file, always include startLine:0 and endLine:0.",
    "Requirements:",
    "- strict C64 BASIC V2 only",
    "- numbered lines only",
    "- no ELSE/ELSEIF/ENDIF/WHILE/WEND/TRUE/FALSE/DO/LOOP",
    "- no STRING$/SPACE$/SPACES$",
    "- include DIM for arrays before use",
    "- use SPC() and FOR...NEXT for repeat rendering",
    "- keep intent from the user prompt",
    "- keep it runnable and concise",
    `User prompt: ${userPrompt}`,
    lintSection,
    "Broken generated program.bas:",
    brokenProgram,
    "Previous program.bas context:",
    currentCode,
  ].join("\n");
}

async function callOpenAI(prompt) {
  if (!OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY is missing.");
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort("openai_timeout"), OPENAI_TIMEOUT_MS);
  let response;
  try {
    response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: MODEL,
        input: prompt,
        text: {
          format: {
            type: "json_schema",
            name: "c64_patch",
            strict: true,
            schema: {
              type: "object",
              additionalProperties: false,
              properties: {
                rationale: { type: "string" },
                operations: {
                  type: "array",
                  minItems: 1,
                  items: {
                    type: "object",
                    additionalProperties: false,
                    properties: {
                      op: { type: "string" },
                      path: { type: "string" },
                      content: { type: "string" },
                      startLine: { type: "integer" },
                      endLine: { type: "integer" },
                    },
                    required: ["op", "path", "content", "startLine", "endLine"],
                  },
                },
              },
              required: ["rationale", "operations"],
            },
          },
        },
      }),
    });
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`OpenAI API error ${response.status}: ${errorText}`);
  }

  const data = await response.json();
  // Responses API can place structured output in different fields depending on model/version.
  if (data.output_parsed && typeof data.output_parsed === "object") {
    return data.output_parsed;
  }

  if (typeof data.output_text === "string" && data.output_text.trim()) {
    return JSON.parse(data.output_text);
  }

  if (Array.isArray(data.output)) {
    for (const item of data.output) {
      if (!item || !Array.isArray(item.content)) continue;
      for (const part of item.content) {
        if (part?.parsed && typeof part.parsed === "object") {
          return part.parsed;
        }
        if (typeof part?.text === "string" && part.text.trim()) {
          return JSON.parse(part.text);
        }
      }
    }
  }

  throw new Error(`Model returned no parsable structured output. Response id: ${data.id || "unknown"}`);
}

function buildValidatorReport({
  status,
  strategy,
  exemplarsUsed = 0,
  normalizedChanged = false,
  initialIssues = [],
  finalIssues = [],
  fallbackReason = "",
} = {}) {
  return {
    status: String(status || "unknown"),
    strategy: String(strategy || "unknown"),
    exemplars_used: Number(exemplarsUsed || 0),
    normalized_changed: Boolean(normalizedChanged),
    initial_issues: Array.isArray(initialIssues) ? initialIssues : [],
    final_issues: Array.isArray(finalIssues) ? finalIssues : [],
    fallback_reason: String(fallbackReason || ""),
  };
}

async function runGenerationPipeline(prompt, code, { writeLogs = true } = {}) {
  const intent = detectPromptIntent(prompt);
  const family = detectPromptFamily(prompt);
  const exemplars = await selectExemplars(prompt, 3);
  const exemplarSources = exemplars.map((e) => String(e.source || "history"));

  const initial = await callOpenAI(structuredPrompt(prompt, code, exemplars));
  const initialProgram = extractProgramFromOperations(initial?.operations);
  const typoNormalizedInitialProgram = normalizeCommonTypos(initialProgram);
  const rewrittenInitial = rewriteUnsupportedBuiltins(typoNormalizedInitialProgram);
  const autoDimInitial = autoDimensionArrays(rewrittenInitial.program);
  const normalizedInitialProgram = autoDimInitial.program;
  const initialLint = initialProgram ? lintBasicV2(initialProgram) : ["No replace_file program found."];
  const normalizedInitialLint = normalizedInitialProgram
    ? lintBasicV2(normalizedInitialProgram)
    : ["No replace_file program found."];

  if (normalizedInitialProgram && looksLikeBasicV2(normalizedInitialProgram) && normalizedInitialLint.length === 0) {
    const payload = {
      rationale:
        normalizedInitialProgram !== initialProgram
          ? `${initial.rationale} (auto-corrected BASIC syntax and unsupported built-ins)`
          : initial.rationale,
      operations: withProgramContent(initial.operations, normalizedInitialProgram),
      validator_report: buildValidatorReport({
        status: "accepted",
        strategy: "model",
        exemplarsUsed: exemplars.length,
        normalizedChanged: normalizedInitialProgram !== initialProgram,
        initialIssues: [...initialLint, ...rewrittenInitial.notes, ...autoDimInitial.notes],
        finalIssues: normalizedInitialLint,
      }),
    };
    if (writeLogs) {
      await appendRunLog({
        type: "generate_result",
        prompt,
        intent,
        family,
        strategy: "model",
        validation: "accepted",
        program: normalizedInitialProgram,
        initial_issues: [...initialLint, ...rewrittenInitial.notes, ...autoDimInitial.notes],
        final_issues: normalizedInitialLint,
        lint_initial_count: initialLint.length,
        lint_final_count: normalizedInitialLint.length,
        normalized_changed: normalizedInitialProgram !== initialProgram,
        exemplars_used: exemplars.length,
        exemplar_sources: exemplarSources,
      });
    }
    return { statusCode: 200, payload };
  }

  if (initialProgram) {
    try {
      const repaired = await callOpenAI(repairPrompt(prompt, initialProgram, code, initialLint));
      const repairedProgram = extractProgramFromOperations(repaired?.operations);
      const typoNormalizedRepairedProgram = normalizeCommonTypos(repairedProgram);
      const rewrittenRepaired = rewriteUnsupportedBuiltins(typoNormalizedRepairedProgram);
      const autoDimRepaired = autoDimensionArrays(rewrittenRepaired.program);
      const normalizedRepairedProgram = autoDimRepaired.program;
      const repairedLint = repairedProgram ? lintBasicV2(repairedProgram) : ["No replace_file program found."];
      const normalizedRepairedLint = normalizedRepairedProgram
        ? lintBasicV2(normalizedRepairedProgram)
        : ["No replace_file program found."];
      if (normalizedRepairedProgram && looksLikeBasicV2(normalizedRepairedProgram) && normalizedRepairedLint.length === 0) {
        const payload = {
          rationale: `${repaired.rationale} (auto-repaired to valid C64 BASIC V2)`,
          operations: withProgramContent(repaired.operations, normalizedRepairedProgram),
          validator_report: buildValidatorReport({
            status: "repaired",
            strategy: "repair",
            exemplarsUsed: exemplars.length,
            normalizedChanged: normalizedRepairedProgram !== repairedProgram,
            initialIssues: [...initialLint, ...rewrittenRepaired.notes, ...autoDimRepaired.notes],
            finalIssues: normalizedRepairedLint,
          }),
        };
        if (writeLogs) {
          await appendRunLog({
            type: "generate_result",
            prompt,
            intent,
            family,
            strategy: "repair",
            validation: "repaired",
            program: normalizedRepairedProgram,
            initial_issues: [...initialLint, ...rewrittenRepaired.notes, ...autoDimRepaired.notes],
            final_issues: normalizedRepairedLint,
            lint_initial_count: repairedLint.length,
            lint_final_count: normalizedRepairedLint.length,
            normalized_changed: normalizedRepairedProgram !== repairedProgram,
            exemplars_used: exemplars.length,
            exemplar_sources: exemplarSources,
          });
        }
        return { statusCode: 200, payload };
      }
    } catch {
      // fall through to deterministic fallback
    }
  }

  const fallback = fallbackProgramForPrompt(prompt);
  if (writeLogs) {
    await appendRunLog({
      type: "generate_result",
      prompt,
      intent,
      family,
      strategy: "fallback",
      validation: "fallback",
      program: extractProgramFromOperations(fallback.operations),
      lint: initialLint,
      initial_issues: initialLint,
      final_issues: [],
      lint_initial_count: initialLint.length,
      lint_final_count: 0,
      normalized_changed: false,
      exemplars_used: exemplars.length,
      exemplar_sources: exemplarSources,
    });
  }
  return {
    statusCode: 200,
    payload: {
      ...fallback,
      validator_report: buildValidatorReport({
        status: "fallback",
        strategy: "fallback",
        exemplarsUsed: exemplars.length,
        normalizedChanged: false,
        initialIssues: initialLint,
        finalIssues: [],
        fallbackReason: "model_or_repair_failed_validation",
      }),
    },
  };
}

function heuristicJudge(prompt, program) {
  const p = tokenizePrompt(prompt);
  const code = String(program || "").toLowerCase();
  let overlap = 0;
  for (const token of p) {
    if (code.includes(token)) overlap += 1;
  }
  const lint = lintBasicV2(program || "");
  const score = Math.max(0, Math.min(1, (overlap / Math.max(1, p.size)) * 0.6 + (lint.length === 0 ? 0.4 : 0.1)));
  return {
    verdict: score >= 0.65 ? "pass" : "needs_review",
    score: Number(score.toFixed(2)),
    notes: lint.length ? [`lint: ${lint.slice(0, 3).join(" | ")}`] : ["Heuristic pass: prompt-token overlap + clean lint."],
    mode: "heuristic",
  };
}

async function handleGenerate(req, res) {
  try {
    const body = JSON.parse(await readBody(req));
    const prompt = String(body.prompt || "").trim();
    const code = String(body.code || "");
    if (!prompt) {
      json(res, 400, { error: "Prompt is required." });
      return;
    }
    const result = await runGenerationPipeline(prompt, code, { writeLogs: true });
    json(res, result.statusCode, result.payload);
  } catch (error) {
    await appendRunLog({
      type: "generate_result",
      prompt: "unknown",
      intent: "unknown",
      family: "unknown",
      strategy: "error",
      validation: "error",
      error: error.message || "Model generation failed.",
    });
    json(res, 500, { error: error.message || "Model generation failed." });
  }
}

async function handlePrgUpload(req, res) {
  try {
    const body = JSON.parse(await readBody(req));
    const encoded = String(body.data || "");
    const filename = String(body.filename || "active.prg").replace(/[^a-zA-Z0-9._-]/g, "_");
    if (!encoded) {
      json(res, 400, { error: "Missing base64 data." });
      return;
    }

    const bytes = Buffer.from(encoded, "base64");
    if (!bytes.length) {
      json(res, 400, { error: "Decoded PRG is empty." });
      return;
    }

    await ensureRuntimeDir();
    const targetPath = path.join(RUNTIME_DIR, filename);
    await fs.writeFile(targetPath, bytes);

    json(res, 200, { url: `/runtime/${filename}`, size: bytes.length });
  } catch (error) {
    json(res, 500, { error: error.message || "Failed to store PRG." });
  }
}

async function handleRunEvent(req, res) {
  try {
    const body = JSON.parse(await readBody(req));
    await appendRunLog({
      type: "run_event",
      event: String(body.event || "unknown"),
      outcome: String(body.outcome || "unknown"),
      prompt: String(body.prompt || ""),
      detail: String(body.detail || ""),
    });
    json(res, 200, { ok: true });
  } catch (error) {
    json(res, 500, { error: error.message || "Failed to log run event." });
  }
}

async function handleGetPinnedExemplars(_req, res) {
  const pinned = await readPinnedExemplars();
  json(res, 200, { exemplars: pinned });
}

async function handleSetPinnedExemplar(req, res) {
  try {
    const body = JSON.parse(await readBody(req));
    const family = String(body.family || "").trim() || "general:generic";
    const prompt = String(body.prompt || "").trim();
    const program = String(body.program || "");
    const pinned = Boolean(body.pinned);

    let all = await readPinnedExemplars();
    all = all.filter((entry) => !(entry.family === family && entry.prompt === prompt));

    if (pinned && prompt && program.trim()) {
      all.unshift({
        family,
        prompt,
        program,
        pinned_at: new Date().toISOString(),
      });
    }

    if (all.length > 200) all = all.slice(0, 200);
    await writePinnedExemplars(all);
    json(res, 200, { ok: true, exemplars: all });
  } catch (error) {
    json(res, 500, { error: error.message || "Failed to update pinned exemplars." });
  }
}

async function handleReplay(req, res) {
  try {
    const body = JSON.parse(await readBody(req));
    const prompt = String(body.prompt || "").trim();
    const code = String(body.code || "");
    const judge = Boolean(body.judge);
    if (!prompt) {
      json(res, 400, { error: "Prompt is required." });
      return;
    }

    const result = await runGenerationPipeline(prompt, code, { writeLogs: false });
    const generatedProgram = extractProgramFromOperations(result.payload?.operations || []);
    const judgement = judge ? heuristicJudge(prompt, generatedProgram) : null;

    await appendRunLog({
      type: "replay_event",
      prompt,
      outcome: "ok",
      validation: result.payload?.validator_report?.status || "unknown",
      judged: Boolean(judge),
      judge_verdict: judgement?.verdict || "",
      judge_score: judgement?.score || 0,
    });

    json(res, 200, {
      ...result.payload,
      replay: true,
      judge: judgement,
    });
  } catch (error) {
    await appendRunLog({
      type: "replay_event",
      prompt: "unknown",
      outcome: "error",
      error: error.message || "replay failed",
    });
    json(res, 500, { error: error.message || "Replay failed." });
  }
}

async function handleMetrics(_req, res) {
  const logs = await readRunLog(2000);
  const gen = logs.filter((e) => e.type === "generate_result");
  const runs = logs.filter((e) => e.type === "run_event");
  const accepted = gen.filter((e) => e.validation === "accepted").length;
  const repaired = gen.filter((e) => e.validation === "repaired").length;
  const fallback = gen.filter((e) => e.validation === "fallback").length;
  const errors = gen.filter((e) => e.validation === "error").length;

  json(res, 200, {
    generation_total: gen.length,
    generation_accepted: accepted,
    generation_repaired: repaired,
    generation_fallback: fallback,
    generation_error: errors,
    generation_success_rate:
      gen.length > 0 ? Number((((accepted + repaired) / gen.length) * 100).toFixed(2)) : 0,
    run_events: runs.length,
  });
}

async function handleMetricsDetail(_req, res) {
  const logs = await readRunLog(4000);
  const gen = logs.filter((e) => e.type === "generate_result");
  const runs = logs.filter((e) => e.type === "run_event");
  const replays = logs.filter((e) => e.type === "replay_event");
  const pinned = await readPinnedExemplars();

  const byValidation = {};
  const byStrategy = {};
  const lintHistogram = new Map();
  const statementHeatmap = new Map();
  const intentStats = {};

  const statementBuckets = [
    ["IF", /\bIF\b/],
    ["FOR", /\bFOR\b/],
    ["PRINT", /\bPRINT\b/],
    ["INPUT", /\bINPUT\b/],
    ["GOTO", /\bGOTO\b/],
    ["GOSUB", /\bGOSUB\b/],
    ["POKE", /\bPOKE\b/],
    ["STRING$", /\bSTRING\$/],
    ["SPACE$", /\bSPACE\$|\bSPACES\$/],
  ];

  const scoreWindow = (days) => {
    const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
    const rows = gen.filter((row) => Date.parse(row.ts || "") >= cutoff);
    const total = rows.length;
    const accepted = rows.filter((r) => r.validation === "accepted").length;
    const repaired = rows.filter((r) => r.validation === "repaired").length;
    const fallback = rows.filter((r) => r.validation === "fallback").length;
    return {
      total,
      first_pass_accept_rate: total ? Number(((accepted / total) * 100).toFixed(2)) : 0,
      repair_success_rate: total ? Number(((repaired / total) * 100).toFixed(2)) : 0,
      fallback_rate: total ? Number(((fallback / total) * 100).toFixed(2)) : 0,
      success_rate: total ? Number((((accepted + repaired) / total) * 100).toFixed(2)) : 0,
      avg_lint_initial: total
        ? Number((rows.reduce((sum, r) => sum + Number(r.lint_initial_count || 0), 0) / total).toFixed(2))
        : 0,
      avg_lint_final: total
        ? Number((rows.reduce((sum, r) => sum + Number(r.lint_final_count || 0), 0) / total).toFixed(2))
        : 0,
    };
  };

  for (const item of gen) {
    byValidation[item.validation || "unknown"] = (byValidation[item.validation || "unknown"] || 0) + 1;
    byStrategy[item.strategy || "unknown"] = (byStrategy[item.strategy || "unknown"] || 0) + 1;
    const lintList = Array.isArray(item.initial_issues)
      ? item.initial_issues
      : Array.isArray(item.lint)
        ? item.lint
        : [];

    const intent = String(item.intent || detectPromptIntent(item.prompt || ""));
    if (!intentStats[intent]) {
      intentStats[intent] = { total: 0, success: 0, fallback: 0 };
    }
    intentStats[intent].total += 1;
    if (item.validation === "accepted" || item.validation === "repaired") intentStats[intent].success += 1;
    if (item.validation === "fallback") intentStats[intent].fallback += 1;

    for (const finding of lintList) {
      lintHistogram.set(finding, (lintHistogram.get(finding) || 0) + 1);
      const upper = String(finding || "").toUpperCase();
      let bucketHit = false;
      for (const [label, rx] of statementBuckets) {
        if (rx.test(upper)) {
          statementHeatmap.set(label, (statementHeatmap.get(label) || 0) + 1);
          bucketHit = true;
          break;
        }
      }
      if (!bucketHit) {
        statementHeatmap.set("OTHER", (statementHeatmap.get("OTHER") || 0) + 1);
      }
    }
  }

  const recentGenerations = gen.slice(-40).reverse().map((item) => ({
    ts: item.ts,
    prompt: String(item.prompt || "").slice(0, 200),
    strategy: item.strategy || "unknown",
    validation: item.validation || "unknown",
    lint_initial_count: Number(item.lint_initial_count || 0),
    lint_final_count: Number(item.lint_final_count || 0),
    normalized_changed: Boolean(item.normalized_changed),
    exemplars_used: Number(item.exemplars_used || 0),
    intent: String(item.intent || detectPromptIntent(item.prompt || "")),
    family: String(item.family || detectPromptFamily(item.prompt || "")),
  }));

  const topLintFindings = Array.from(lintHistogram.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)
    .map(([finding, count]) => ({ finding, count }));

  const runOutcome = {};
  for (const item of runs) {
    const key = `${item.event || "unknown"}:${item.outcome || "unknown"}`;
    runOutcome[key] = (runOutcome[key] || 0) + 1;
  }

  const funnel = {
    total_generations: gen.length,
    accepted_first_pass: gen.filter((g) => g.validation === "accepted" && g.strategy === "model").length,
    repaired_success: gen.filter((g) => g.validation === "repaired").length,
    fallback: gen.filter((g) => g.validation === "fallback").length,
    error: gen.filter((g) => g.validation === "error").length,
  };

  const withEx = gen.filter((g) => Number(g.exemplars_used || 0) > 0);
  const withoutEx = gen.filter((g) => Number(g.exemplars_used || 0) === 0);
  const impact = (rows) => {
    const total = rows.length;
    const success = rows.filter((r) => r.validation === "accepted" || r.validation === "repaired").length;
    const fallback = rows.filter((r) => r.validation === "fallback").length;
    return {
      total,
      success_rate: total ? Number(((success / total) * 100).toFixed(2)) : 0,
      fallback_rate: total ? Number(((fallback / total) * 100).toFixed(2)) : 0,
    };
  };

  const recent = gen.slice(-50);
  const baseline = gen.slice(Math.max(0, gen.length - 250), Math.max(0, gen.length - 50));
  const rate = (rows, key) => {
    if (!rows.length) return 0;
    return rows.filter((r) => r.validation === key).length / rows.length;
  };
  const recentFallbackRate = rate(recent, "fallback");
  const baselineFallbackRate = rate(baseline, "fallback");
  const driftAlerts = [];
  if (recent.length >= 20 && baseline.length >= 20 && recentFallbackRate > baselineFallbackRate * 1.4 + 0.05) {
    driftAlerts.push(
      `Fallback rate spike: recent ${(recentFallbackRate * 100).toFixed(1)}% vs baseline ${(baselineFallbackRate * 100).toFixed(1)}%`
    );
  }

  const intentClusters = Object.entries(intentStats)
    .map(([intent, stats]) => ({
      intent,
      total: stats.total,
      success_rate: stats.total ? Number(((stats.success / stats.total) * 100).toFixed(2)) : 0,
      fallback_rate: stats.total ? Number(((stats.fallback / stats.total) * 100).toFixed(2)) : 0,
    }))
    .sort((a, b) => b.total - a.total);

  const exemplarLibrary = gen
    .filter((g) => g.validation === "accepted" || g.validation === "repaired")
    .filter((g) => typeof g.program === "string" && g.program.trim())
    .slice(-120)
    .reverse()
    .map((g, idx) => ({
      id: `${Date.parse(g.ts || "") || 0}-${idx}`,
      ts: g.ts,
      prompt: String(g.prompt || "").slice(0, 220),
      family: String(g.family || detectPromptFamily(g.prompt || "")),
      strategy: g.strategy || "unknown",
      validation: g.validation || "unknown",
      lint_initial_count: Number(g.lint_initial_count || 0),
      lint_final_count: Number(g.lint_final_count || 0),
      program: g.program,
      exemplar_sources: Array.isArray(g.exemplar_sources) ? g.exemplar_sources : [],
    }));

  json(res, 200, {
    totals: {
      generations: gen.length,
      runs: runs.length,
      replays: replays.length,
      pinned_exemplars: pinned.length,
    },
    by_validation: byValidation,
    by_strategy: byStrategy,
    run_outcome: runOutcome,
    recent_generations: recentGenerations,
    top_lint_findings: topLintFindings,
    validator_heatmap: Array.from(statementHeatmap.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([statement, count]) => ({ statement, count })),
    repair_funnel: funnel,
    scorecards: {
      days_7: scoreWindow(7),
      days_30: scoreWindow(30),
    },
    intent_clusters: intentClusters,
    exemplar_impact: {
      with_exemplars: impact(withEx),
      without_exemplars: impact(withoutEx),
    },
    drift: {
      alerts: driftAlerts,
      recent_fallback_rate: Number((recentFallbackRate * 100).toFixed(2)),
      baseline_fallback_rate: Number((baselineFallbackRate * 100).toFixed(2)),
    },
    exemplar_library: exemplarLibrary,
    replay_stats: {
      total: replays.length,
      ok: replays.filter((r) => r.outcome === "ok").length,
      errors: replays.filter((r) => r.outcome === "error").length,
    },
  });
}

async function serveStatic(urlPath, res) {
  const cleanPath = urlPath === "/" ? "/index.html" : urlPath;
  const fullPath = path.join(ROOT, cleanPath);

  if (!fullPath.startsWith(ROOT)) {
    res.writeHead(403, securityHeaders("text/plain; charset=utf-8"));
    res.end("Forbidden");
    return;
  }

  try {
    const data = await fs.readFile(fullPath);
    const ext = path.extname(fullPath);
    const contentType = CONTENT_TYPES[ext] || "application/octet-stream";
    res.writeHead(200, securityHeaders(contentType));
    res.end(data);
  } catch {
    res.writeHead(404, securityHeaders("text/plain; charset=utf-8"));
    res.end("Not found");
  }
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (req.method === "POST" && url.pathname.startsWith("/api/")) {
    if (!isAuthorized(req)) {
      json(res, 401, { error: `Unauthorized. Provide ${API_KEY_HEADER} header.` });
      return;
    }
    if (isRateLimited(req)) {
      json(res, 429, { error: "Rate limit exceeded. Try again shortly." });
      return;
    }
  }

  if (req.method === "POST" && url.pathname === "/api/generate") {
    await handleGenerate(req, res);
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/prg") {
    await handlePrgUpload(req, res);
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/run-event") {
    await handleRunEvent(req, res);
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/exemplars/pinned") {
    await handleGetPinnedExemplars(req, res);
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/exemplars/pinned") {
    await handleSetPinnedExemplar(req, res);
    return;
  }

  if (req.method === "POST" && url.pathname === "/api/replay") {
    await handleReplay(req, res);
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/metrics") {
    await handleMetrics(req, res);
    return;
  }

  if (req.method === "GET" && url.pathname === "/api/metrics/detail") {
    await handleMetricsDetail(req, res);
    return;
  }

  if (req.method === "GET") {
    await serveStatic(url.pathname, res);
    return;
  }

  res.writeHead(405, securityHeaders("text/plain; charset=utf-8"));
  res.end("Method not allowed");
});

server.listen(PORT, HOST, () => {
  console.log(`C64 vibe server listening on http://${HOST}:${PORT}`);
});
