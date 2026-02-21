import assert from "node:assert/strict";
import { executeBasic, generateProgramFromPrompt } from "./basic-engine.js";

function runTests() {
  const hello = executeBasic(`10 PRINT CHR$(147)\n20 PRINT "HI"\n30 END`);
  assert.equal(hello.screen[0].trim(), "HI", "should print text on first screen row");

  const poke = executeBasic(`10 POKE 53280,2\n20 POKE 53281,6\n30 END`);
  assert.equal(poke.border, 2, "border color should be set from POKE 53280");
  assert.equal(poke.background, 6, "background color should be set from POKE 53281");

  const loop = executeBasic(`10 LET A=1\n20 PRINT A\n30 A=A+1\n40 IF A<4 THEN 20\n50 END`);
  assert.equal(loop.logs.some((l) => l.includes("program halted")), true, "finite loop should halt");

  const infinite = executeBasic(`10 PRINT "X"\n20 GOTO 10`);
  assert.equal(
    infinite.logs.some((l) => l.includes("too many steps")),
    true,
    "infinite loop should stop on guardrail"
  );

  const prompt = generateProgramFromPrompt("make border color demo");
  assert.equal(prompt.code.includes("POKE 53280"), true, "color prompt should generate poke program");

  const ball = generateProgramFromPrompt("create a bouncing ball demo");
  assert.equal(ball.code.includes("POKE 1024+40*Y+20"), true, "ball prompt should use screen-memory motion");

  const converter = executeBasic(`10 INPUT F
20 IF F=-999 THEN PRINT "GOODBYE!": END
30 C=(F-32)*5/9
40 PRINT F;" F=";C;" C"
50 END`, {
    inputProvider: () => "212",
  });
  assert.equal(
    converter.screen.some((line) => line.includes("212 F=100")),
    true,
    "converter should compute and print result"
  );

  const queue = ["1", "10", "-999"];
 const kmMi = executeBasic(`10 PRINT "KM TO MILES / MILES TO KM"
20 PRINT "1=KM->MI, 2=MI->KM, -999=EXIT"
30 INPUT C
40 IF C=-999 THEN END
50 IF C=1 THEN GOTO 80
60 IF C=2 THEN GOTO 110
70 PRINT "INVALID OPTION": GOTO 20
80 PRINT "ENTER KILOMETERS"
90 INPUT V
100 IF V=-999 THEN END
105 R=V*0.62137: PRINT V;" KM = ";R;" MI": GOTO 20
110 PRINT "ENTER MILES"
120 INPUT V
130 IF V=-999 THEN END
140 R=V*1.60934: PRINT V;" MI = ";R;" KM"
150 GOTO 20`, {
    inputProvider: () => queue.shift() ?? "-999",
  });
  assert.equal(
    kmMi.screen.some((line) => line.includes("10 KM = 6.213")),
    true,
    "two-way converter should run with queued INPUT values and print converted result"
  );

  console.log("All tests passed.");
}

runTests();
