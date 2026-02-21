const LOAD_ADDRESS = 0x0801;

export const BASIC_V2_TOKENS = [
  ["PRINT#", 0x98],
  ["INPUT#", 0x84],
  ["GOSUB", 0x8d],
  ["RETURN", 0x8e],
  ["RESTORE", 0x8c],
  ["VERIFY", 0x95],
  ["PRINT", 0x99],
  ["GOTO", 0x89],
  ["CLOSE", 0xa0],
  ["RIGHT$", 0xc9],
  ["LEFT$", 0xc8],
  ["MID$", 0xca],
  ["STR$", 0xc4],
  ["CHR$", 0xc7],
  ["INPUT", 0x85],
  ["NEXT", 0x82],
  ["DATA", 0x83],
  ["READ", 0x87],
  ["LIST", 0x9b],
  ["CONT", 0x9a],
  ["WAIT", 0x92],
  ["LOAD", 0x93],
  ["SAVE", 0x94],
  ["POKE", 0x97],
  ["STOP", 0x90],
  ["OPEN", 0x9f],
  ["THEN", 0xa7],
  ["STEP", 0xa9],
  ["SGN", 0xb4],
  ["INT", 0xb5],
  ["ABS", 0xb6],
  ["USR", 0xb7],
  ["FRE", 0xb8],
  ["POS", 0xb9],
  ["SQR", 0xba],
  ["RND", 0xbb],
  ["LOG", 0xbc],
  ["EXP", 0xbd],
  ["COS", 0xbe],
  ["SIN", 0xbf],
  ["TAN", 0xc0],
  ["ATN", 0xc1],
  ["PEEK", 0xc2],
  ["LEN", 0xc3],
  ["VAL", 0xc5],
  ["ASC", 0xc6],
  ["CLR", 0x9c],
  ["CMD", 0x9d],
  ["SYS", 0x9e],
  ["GET", 0xa1],
  ["NEW", 0xa2],
  ["TAB(", 0xa3],
  ["SPC(", 0xa6],
  ["FOR", 0x81],
  ["DIM", 0x86],
  ["LET", 0x88],
  ["RUN", 0x8a],
  ["IF", 0x8b],
  ["REM", 0x8f],
  ["END", 0x80],
  ["ON", 0x91],
  ["DEF", 0x96],
  ["TO", 0xa4],
  ["FN", 0xa5],
  ["NOT", 0xa8],
  ["AND", 0xaf],
  ["OR", 0xb0],
  ["GO", 0xcb],
  ["+", 0xaa],
  ["-", 0xab],
  ["*", 0xac],
  ["/", 0xad],
  ["^", 0xae],
  [">", 0xb1],
  ["=", 0xb2],
  ["<", 0xb3],
  ["?", 0x99],
];

const TOKENS = [...BASIC_V2_TOKENS].sort((a, b) => b[0].length - a[0].length);

const ASCII_TO_PETSCII = new Map(
  Array.from({ length: 128 }, (_, i) => [String.fromCharCode(i), i])
);

function toPetsciiByte(ch) {
  if (ASCII_TO_PETSCII.has(ch)) return ASCII_TO_PETSCII.get(ch);
  return 32;
}

function encodeLineBody(body) {
  const out = [];
  const upper = body.toUpperCase();
  let i = 0;
  let inQuote = false;
  let remMode = false;

  while (i < body.length) {
    const raw = body[i];

    if (raw === '"') {
      inQuote = !inQuote;
      out.push(toPetsciiByte(raw));
      i += 1;
      continue;
    }

    if (!inQuote && !remMode) {
      let matched = false;
      for (const [word, token] of TOKENS) {
        const tokenText = word.toUpperCase();
        if (!upper.startsWith(tokenText, i)) continue;

        const first = tokenText[0];
        const last = tokenText[tokenText.length - 1];
        const needsLeftBoundary = /[A-Z]/.test(first);
        const needsRightBoundary = /[A-Z$]/.test(last);

        const prev = i === 0 ? " " : upper[i - 1];
        const next = i + tokenText.length >= upper.length ? " " : upper[i + tokenText.length];
        const boundaryPrev = !/[A-Z0-9]/.test(prev);
        const boundaryNext = !/[A-Z0-9]/.test(next);

        if ((needsLeftBoundary && !boundaryPrev) || (needsRightBoundary && !boundaryNext)) {
          continue;
        }

        out.push(token);
        i += tokenText.length;
        matched = true;
        if (tokenText === "REM") remMode = true;
        break;
      }
      if (matched) continue;
    }

    out.push(toPetsciiByte(raw));
    i += 1;
  }

  out.push(0x00);
  return out;
}

export function buildPrgBinary(source) {
  const rawLines = source
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const m = line.match(/^(\d+)\s+(.*)$/);
      if (!m) return null;
      return { lineNumber: Number(m[1]), body: m[2] };
    })
    .filter(Boolean)
    .sort((a, b) => a.lineNumber - b.lineNumber);

  if (!rawLines.length) {
    throw new Error("No numbered BASIC lines to export.");
  }

  const bytes = [];
  bytes.push(LOAD_ADDRESS & 0xff, (LOAD_ADDRESS >> 8) & 0xff);

  let cursor = LOAD_ADDRESS;
  for (const line of rawLines) {
    const bodyBytes = encodeLineBody(line.body);
    const lineLen = 2 + 2 + bodyBytes.length;
    const nextLineAddress = cursor + lineLen;

    bytes.push(nextLineAddress & 0xff, (nextLineAddress >> 8) & 0xff);
    bytes.push(line.lineNumber & 0xff, (line.lineNumber >> 8) & 0xff);
    bytes.push(...bodyBytes);

    cursor = nextLineAddress;
  }

  bytes.push(0x00, 0x00);
  return new Uint8Array(bytes);
}
