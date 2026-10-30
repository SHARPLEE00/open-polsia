// Load test — the real control plane (HTTP API + worker + cron + Postgres store) driven by
// parallel simulated agents. The LLM is replaced by SimRuntime (scripted tool calls with think
// time) and the Docker sandbox by an in-memory one, so this measures the SCHEDULER, not the model.
//
// Run:  pnpm loadtest                         (60s, default mix)
//       LOADTEST_SECONDS=120 LOADTEST_CHAT=12 LOADTEST_POLLERS=30 pnpm loadtest
//       LOADTEST_OUT=results.json pnpm loadtest
//
// WARNING: wipes the "demo" company's data (same as the dashboard's "new company" button).
import { writeFileSync } from "node:fs";
import { monitorEventLoopDelay, performance } from "node:perf_hooks";
import { PgStore } from "../src/core/pg-store.ts";
import { db } from "../src/core/db.ts";
import { resetCompany } from "../src/core/queries.ts";
import { registerSandboxProvider } from "../src/sandbox/factory.ts";
import { startDashboard } from "../src/services/dashboard.ts";
import type { Store } from "../src/core/store.ts";
import type { Sandbox } from "../src/sandbox/types.ts";
import type { AgentEvent } from "../src/core/types.ts";
import type { AgentRuntime, RunOpts } from "../src/runtime/types.ts";
import type { ToolDef } from "../src/tools/registry.ts";

const env = (k: string, d: number) => Number(process.env[k] ?? d);
const SECONDS = env("LOADTEST_SECONDS", 60);
const CHAT_AGENTS = env("LOADTEST_CHAT", 8); // concurrent chat/orchestrator sessions
const POLLERS = env("LOADTEST_POLLERS", 20); // dashboard tabs / observers polling /api/state
const BEACON_RPS = env("LOADTEST_BEACON_RPS", 50); // analytics hits from deployed apps
const CRON_EVERY_MS = env("LOADTEST_CRON_MS", 5000); // CEO daily-cycle trigger (compressed time)
const THINK_MS = env("LOADTEST_THINK_MS", 40); // median simulated LLM step time
const CHAT_PAUSE_MS = env("LOADTEST_CHAT_PAUSE_MS", 1000); // founder idle time between chat turns
const SEED = env("LOADTEST_SEED", 42);
const COMPANY = "demo";

// ── deterministic RNG so baseline and optimized runs see the same agent behaviour ─────────
let rngState = SEED >>> 0;
function rand(): number {
  rngState = (rngState + 0x6d2b79f5) >>> 0;
  let t = rngState;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
/** Log-normal-ish think time with a fat tail (occasional slow model turn). */
const think = () => sleep(THINK_MS * Math.exp((rand() - 0.5) * 1.2) * (rand() < 0.03 ? 6 : 1));

// ── latency recording ─────────────────────────────────────────────────────────────────────
const samples = new Map<string, number[]>();
const errors = new Map<string, number>();
let recording = false;
function record(name: string, ms: number) {
  if (!recording) return;
  let a = samples.get(name);
  if (!a) samples.set(name, (a = []));
  a.push(ms);
}
function recordError(name: string) {
  if (recording) errors.set(name, (errors.get(name) ?? 0) + 1);
}
function pct(sorted: number[], p: number) {
  if (!sorted.length) return 0;
  return sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)];
}
function summarize(a: number[]) {
  const s = [...a].sort((x, y) => x - y);
  return { n: s.length, p50: pct(s, 50), p95: pct(s, 95), p99: pct(s, 99), max: s[s.length - 1] ?? 0 };
}

// ── instrumented store: times every scheduler state-I/O call the agents/worker make ──────────
function instrument(store: Store): Store {
  return new Proxy(store, {
    get(target, prop, recv) {
      const v = Reflect.get(target, prop, recv);
      if (typeof v !== "function") return v;
      return async (...args: unknown[]) => {
        const t0 = performance.now();
        try {
          return await v.apply(target, args);
        } finally {
          record(`store.${String(prop)}`, performance.now() - t0);
        }
      };
    },
  });
}

// ── in-memory sandbox (docker exec replaced by a short fixed delay) ─────────────────────────
class SimSandbox implements Sandbox {
  private files = 0;
  async exec() {
    await sleep(5 + rand() * 10);
    this.files++;
    return { exitCode: 0, stdout: "ok", stderr: "" };
  }
  workdir() { return "/work"; }
  async seed() { this.files++; }
  async isEmpty() { return this.files === 0; }
  async previewUrl() { return null; }
  id() { return null; }
  async release() {}
  async dispose() {}
}

// ── SimRuntime: plays each agent's tool sequence, through the same log→execute→log path as
//    tools/registry.ts toPiTool, so the task-event write load matches a real run ─────────────
class SimRuntime implements AgentRuntime {
  async *run(opts: RunOpts): AsyncIterable<AgentEvent> {
    const byName = new Map(opts.tools.map((t) => [t.name, t] as const));
    const has = (n: string) => byName.has(n);
    const queue: AgentEvent[] = [];
    const call = async (name: string, args: Record<string, unknown>, simulateOnly = false) => {
      await think();
      opts.toolCtx.log("tool_call", { name, args });
      const tool = byName.get(name) as ToolDef | undefined;
      const result = simulateOnly || !tool
        ? { ok: true, summary: `simulated ${name}`, data: { body: "x".repeat(2000) } }
        : await tool.execute(args as never, opts.toolCtx);
      opts.toolCtx.log("tool_result", { name, result });
      return result;
    };
    queue.push({ type: "text", text: "Thinking… " });
    yield* drain(queue);

    if (has("create_task_proposal")) {
      // CEO/Planner
      await call("read_state", {});
      const n = 1 + Math.floor(rand() * 3);
      for (let i = 0; i < n; i++) {
        await call("create_task_proposal", {
          title: `Proposal ${i} ${Date.now()}`,
          description: "Ship the next step toward the goal.",
          tag: rand() < 0.6 ? "engineering" : "research",
          priority: Math.floor(rand() * 4),
        });
      }
      await call("write_report", { day_summary: "Today was a planning day. Tomorrow: ship." });
    } else if (has("bash")) {
      // Engineering
      await call("ls", { path: "." });
      for (let i = 0; i < 3; i++) await call("write_file", { path: `views/p${i}.ejs`, content: "<h1>hi</h1>".repeat(50) });
      await call("bash", { command: "npm test" });
      await finish(this.outcome(), call);
    } else if (has("save_report")) {
      // Research
      await call("web_search", { query: "market" }, true);
      await call("web_search", { query: "competitors" }, true);
      await call("save_report", { name: "Findings", content: "# Findings\n" + "lorem ".repeat(300) });
      await finish(this.outcome(), call);
    } else if (has("create_task")) {
      // Chat orchestrator
      await call("get_context", {});
      const list = await call("get_tasks", {});
      const some = /^(\w{8}) /m.exec(list.summary)?.[1];
      if (some) await call("get_task_details", { task_id: some });
      const r = rand();
      if (r < 0.25) {
        await call("create_task", {
          title: `Chat task ${Date.now()}`,
          description: "Build what the founder asked for, and why.",
          tag: rand() < 0.6 ? "engineering" : "research",
          priority: Math.floor(rand() * 4),
        });
      } else if (r < 0.4) {
        const sug = await call("get_tasks", { status: "suggested" });
        const id = /^(\w{8}) /m.exec(sug.summary)?.[1];
        if (id) await call("approve_task", { task_id: id });
      } else if (some) {
        await call("get_task_execution_logs", { task_id: some });
      }
    }
    queue.push({ type: "text", text: "Done." }, { type: "final", text: "" });
    yield* drain(queue);

    function* drain(q: AgentEvent[]) {
      while (q.length) yield q.shift()!;
    }
  }

  // ~93% complete · 3% fail · 3% checkpoint (resume) · 1% end silently (status guard path)
  private outcome(): "complete" | "fail" | "resume" | "silent" {
    const r = rand();
    return r < 0.93 ? "complete" : r < 0.96 ? "fail" : r < 0.99 ? "resume" : "silent";
  }
}
async function finish(
  o: "complete" | "fail" | "resume" | "silent",
  call: (n: string, a: Record<string, unknown>) => Promise<unknown>,
) {
  if (o === "complete") await call("complete_task", { summary: "done", artifacts: [{ type: "files", ref: "views/" }] });
  else if (o === "fail") await call("fail_task", { reason: "simulated failure" });
  else if (o === "resume") await call("resume_task", { note: "continue from views/" });
}

// ── HTTP clients ──────────────────────────────────────────────────────────────────────────
async function timed(name: string, url: string, init?: RequestInit): Promise<Response | null> {
  const t0 = performance.now();
  try {
    const res = await fetch(url, init);
    await res.arrayBuffer();
    record(name, performance.now() - t0);
    if (!res.ok) recordError(name);
    return res;
  } catch {
    recordError(name);
    return null;
  }
}

async function chatClient(base: string, until: number) {
  while (Date.now() < until) {
    const t0 = performance.now();
    try {
      const res = await fetch(`${base}/api/chat`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: "Please queue the next most important thing." }),
      });
      const reader = res.body!.getReader();
      let first = true;
      for (;;) {
        const { done } = await reader.read();
        if (first) { record("POST /api/chat (first event)", performance.now() - t0); first = false; }
        if (done) break;
      }
    } catch {
      recordError("POST /api/chat (first event)");
    }
    await sleep(CHAT_PAUSE_MS * (0.5 + rand()));
  }
}

async function poller(base: string, until: number) {
  await sleep(rand() * 1500);
  while (Date.now() < until) {
    const res = await timed("GET /api/state", `${base}/api/state`);
    if (res && rand() < 0.1) {
      // Founder actions from the dashboard: approve / reject a suggestion, open a report.
      const s = (await (await fetch(`${base}/api/state`)).json()) as { tasks: { id: string; status: string }[]; reports: { id: number }[] };
      const sug = s.tasks.find((t) => t.status === "suggested");
      if (sug) await timed(rand() < 0.8 ? "POST /api/approve" : "POST /api/reject", `${base}/api/${rand() < 0.8 ? "approve" : "reject"}/${sug.id}`, { method: "POST" });
      if (s.reports[0]) await timed("GET /api/report", `${base}/api/report?id=${s.reports[0].id}`);
      await timed("GET /api/messages", `${base}/api/messages`);
    }
    await sleep(1500);
  }
}

async function beacon(base: string, until: number) {
  const gap = 1000 / Math.max(1, BEACON_RPS);
  const inflight: Promise<unknown>[] = [];
  while (Date.now() < until) {
    inflight.push(timed("GET /api/beacon/pixel", `${base}/api/beacon/pixel?s=brewbox&v=v${Math.floor(rand() * 500)}&p=/`));
    await sleep(gap);
  }
  await Promise.all(inflight);
}

async function cron(base: string, until: number) {
  await sleep(1000);
  while (Date.now() < until) {
    await timed("POST /api/cron", `${base}/api/cron`, { method: "POST" });
    await sleep(CRON_EVERY_MS);
  }
}

// ── main ──────────────────────────────────────────────────────────────────────────────────
async function main() {
  process.env.SANDBOX_PROVIDER = "sim";
  registerSandboxProvider("sim", async () => new SimSandbox());

  const store = instrument(new PgStore());
  const app = await startDashboard({ store, runtime: new SimRuntime(), port: 0 });
  await resetCompany(COMPANY);
  await db()`DELETE FROM visits WHERE company_id = ${COMPANY}`;
  await db()`UPDATE companies SET onboarded_at = now(), profile = '{"goal":"ship"}'::jsonb WHERE id = ${COMPANY}`;
  const base = `http://localhost:${app.port}`;

  // Seed a realistic backlog + history so queries aren't running on empty tables.
  for (let i = 0; i < 40; i++) {
    await store.create({ companyId: COMPANY, title: `seed ${i}`, description: "seed", tag: i % 2 ? "research" : "engineering", status: "suggested", priority: (i % 4) as 0 | 1 | 2 | 3 });
  }

  const loop = monitorEventLoopDelay({ resolution: 10 });
  loop.enable();
  const startedAt = new Date();
  recording = true;
  const until = Date.now() + SECONDS * 1000;
  console.log(`load: ${SECONDS}s · ${CHAT_AGENTS} chat agents · ${POLLERS} pollers · ${BEACON_RPS} beacon rps · cron every ${CRON_EVERY_MS}ms`);
  await Promise.all([
    ...Array.from({ length: CHAT_AGENTS }, () => chatClient(base, until)),
    ...Array.from({ length: POLLERS }, () => poller(base, until)),
    beacon(base, until),
    cron(base, until),
  ]);
  recording = false;
  loop.disable();
  // Let the in-flight execution settle so outcomes are counted fairly.
  await sleep(3000);
  await app.stop();

  // Outcomes for tasks that STARTED during the run.
  const rows = await db()<{ status: string; n: number }[]>`
    SELECT status, count(*)::int n FROM tasks
    WHERE company_id = ${COMPANY} AND started_at >= ${startedAt} GROUP BY status`;
  const by = Object.fromEntries(rows.map((r) => [r.status, r.n]));
  const completed = by.completed ?? 0;
  const failed = by.failed ?? 0;
  const stuck = by.needs_continuation ?? 0;
  const terminal = completed + failed + stuck;
  // Dispatch latency: a task entering `todo` (chat create_task) → the worker starting it.
  const [disp] = await db()<{ n: number; p50: number; p95: number }[]>`
    SELECT count(*)::int n,
      coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY ms), 0)::float p50,
      coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms), 0)::float p95
    FROM (
      SELECT extract(epoch FROM (s.ts - q.ts)) * 1000 AS ms
      FROM task_events q
      JOIN tasks t ON t.id = q.task_id
      JOIN LATERAL (
        SELECT ts FROM task_events
        WHERE task_id = q.task_id AND id > q.id AND type = 'status_change' AND payload->>'to' = 'in_progress'
        ORDER BY id LIMIT 1) s ON true
      WHERE t.company_id = ${COMPANY} AND q.ts >= ${startedAt}
        AND q.type = 'status_change' AND q.payload->>'to' = 'todo'
    ) x`;
  const [{ queued }] = await db()<{ queued: number }[]>`
    SELECT count(*)::int queued FROM tasks WHERE company_id = ${COMPANY} AND status IN ('todo','needs_continuation')`;

  const routes = Object.fromEntries([...samples].map(([k, v]) => [k, { ...summarize(v), errors: errors.get(k) ?? 0 }]));
  const api = [...samples].filter(([k]) => !k.startsWith("store.")).flatMap(([, v]) => v);
  const storeOps = [...samples].filter(([k]) => k.startsWith("store.")).flatMap(([, v]) => v);
  const result = {
    config: { SECONDS, CHAT_AGENTS, CHAT_PAUSE_MS, POLLERS, BEACON_RPS, CRON_EVERY_MS, THINK_MS, SEED },
    apiOverall: summarize(api),
    storeOverall: summarize(storeOps),
    routes,
    eventLoopDelayMs: { p50: loop.percentile(50) / 1e6, p99: loop.percentile(99) / 1e6, max: loop.max / 1e6 },
    dispatchLatencyMs: disp,
    backlogAtEnd: queued,
    tasks: { started: terminal + (by.in_progress ?? 0), completed, failed, needsContinuation: stuck, inProgress: by.in_progress ?? 0, successRate: terminal ? completed / terminal : 0 },
  };

  const f = (n: number) => n.toFixed(1).padStart(8);
  console.log(`\n${"route".padEnd(34)}${"n".padStart(7)}${"p50".padStart(8)}${"p95".padStart(8)}${"p99".padStart(8)}${"max".padStart(8)}  err`);
  for (const [k, v] of Object.entries(routes).sort()) console.log(`${k.padEnd(34)}${String(v.n).padStart(7)}${f(v.p50)}${f(v.p95)}${f(v.p99)}${f(v.max)}  ${v.errors}`);
  const o = result.apiOverall;
  console.log(`\nAPI overall      n=${o.n}  p50=${o.p50.toFixed(1)}ms  p95=${o.p95.toFixed(1)}ms  p99=${o.p99.toFixed(1)}ms  max=${o.max.toFixed(1)}ms`);
  const so = result.storeOverall;
  console.log(`state I/O        n=${so.n}  p50=${so.p50.toFixed(1)}ms  p95=${so.p95.toFixed(1)}ms  p99=${so.p99.toFixed(1)}ms`);
  console.log(`event loop delay p99=${result.eventLoopDelayMs.p99.toFixed(1)}ms  max=${result.eventLoopDelayMs.max.toFixed(1)}ms`);
  console.log(`dispatch (todo→running) n=${disp.n} p50=${disp.p50.toFixed(0)}ms p95=${disp.p95.toFixed(0)}ms · still queued at end: ${queued}`);
  console.log(`tasks            ${JSON.stringify(result.tasks)}`);
  if (process.env.LOADTEST_OUT) writeFileSync(process.env.LOADTEST_OUT, JSON.stringify(result, null, 2));
  await db().end();
  process.exit(0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
