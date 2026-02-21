import assert from "node:assert/strict";
import { BASIC_V2_TOKENS, buildPrgBinary } from "./prg.js";
import { applyOperations } from "./llm-client.js";

function runTests() {
  const source = `10 PRINT "HI"\n20 END`;
  const bytes = buildPrgBinary(source);

  assert.equal(bytes[0], 0x01, "PRG must start with low-byte load address");
  assert.equal(bytes[1], 0x08, "PRG must start with high-byte load address");
  assert.equal(bytes[bytes.length - 1], 0x00, "PRG should end with null terminator");
  assert.equal(bytes[bytes.length - 2], 0x00, "PRG should end with null terminator");

  const clearSource = `10 PRINT CHR$(147)\n20 END`;
  const clearBytes = buildPrgBinary(clearSource);
  const hasChrCallToken = clearBytes.some(
    (value, i) => value === 0xc7 && clearBytes[i + 1] === 0x28
  );
  assert.equal(
    hasChrCallToken,
    true,
    "CHR$ call should be tokenized as C7 28 to avoid BAD SUBSCRIPT at runtime"
  );

  const tokenByteSet = new Set(BASIC_V2_TOKENS.map(([, value]) => value));
  for (let b = 0x80; b <= 0xcb; b += 1) {
    assert.equal(tokenByteSet.has(b), true, `missing BASIC token byte 0x${b.toString(16)}`);
  }

  const tabSpc = buildPrgBinary(`10 PRINT TAB(10):PRINT SPC(2):?"OK"\n20 END`);
  assert.equal(tabSpc.includes(0xa3), true, "TAB( should be tokenized");
  assert.equal(tabSpc.includes(0xa6), true, "SPC( should be tokenized");
  assert.equal(tabSpc.includes(0x99), true, "PRINT/? should be tokenized");

  const replaced = applyOperations("10 PRINT \"A\"", [
    { op: "replace_file", path: "program.bas", content: "10 PRINT \"B\"\n20 END" },
  ]);
  assert.equal(replaced.includes('"B"'), true, "replace_file operation should update source");

  const ranged = applyOperations("10 A\n20 B\n30 C", [
    { op: "replace_line_range", path: "program.bas", startLine: 2, endLine: 2, content: "20 Z" },
  ]);
  assert.equal(ranged, "10 A\n20 Z\n30 C", "replace_line_range should patch target lines");

  console.log("PRG and patch tests passed.");
}

runTests();
