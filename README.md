# C64 Vibe Coder MVP

A Bolt-style AI coding interface for Commodore 64 development.

## What this iteration includes

- Split-pane interface:
  - Left: chat + code editor + patch/apply controls
  - Right: runtime with simulated C64 monitor and embedded `jsc64` emulator mode
- Structured AI patch flow via `/api/generate`.
- Server-side generate -> lint -> repair pipeline for C64 BASIC V2 robustness.
- Local learning loop via `runtime/runs.jsonl`:
  - logs generation outcomes (`accepted`/`repaired`/`fallback`)
  - logs run events from UI
  - reuses successful generations as ranked exemplars for future prompts
  - exposes aggregate metrics at `/api/metrics`
- Patch operations supported:
  - `replace_file`
  - `replace_line_range`
  - `append_lines`
- Tokenized binary `.prg` export with full C64 BASIC V2 token byte coverage (`0x80`-`0xCB`) and operators/functions.
- Local persistence using `localStorage`.
- Embedded open-source emulator vendor: `vendor/jsc64` (GPLv2 upstream project).

## Run locally

```bash
cd /Users/seanmee/AIProjects/llm_engineering/output/c64-vibe-mvp
export OPENAI_API_KEY="your_key_here"   # optional but recommended
export OPENAI_MODEL="gpt-4.1-mini"      # optional
npm start
```

Open: <http://localhost:8787>

If `OPENAI_API_KEY` is missing or API fails, the app automatically falls back to local template generation.
If model output is syntactically suspect, server performs an automatic repair pass before fallback.

## Test

```bash
cd /Users/seanmee/AIProjects/llm_engineering/output/c64-vibe-mvp
npm test
```

## Real emulator mode

1. Click `Use Real Emulator`.
2. Leave `Emulator URL` blank to use embedded `jsc64`, or set an external emulator URL.
3. Click `Save URL`.

When embedded `jsc64` is active, clicking `Run` uploads current PRG to `/api/prg` and loads it into emulator automatically.

## Notes

- Backend endpoint is implemented in `server.mjs`.
- Frontend structured patch client lives in `llm-client.js`.
- PRG tokenizer/export logic lives in `prg.js`.
