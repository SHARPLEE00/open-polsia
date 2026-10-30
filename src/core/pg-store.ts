// PgStore — Postgres-backed Store. The Phase 1 persistence (replaces InMemoryStore).
import { randomUUID } from "node:crypto";
import { db } from "./db.ts";
import type { CreateTaskInput, Store } from "./store.ts";
import type { Task, TaskEvent, TaskResult, TaskStatus } from "./types.ts";

type TaskRow = {
  id: string;
  company_id: string;
  title: string;
  description: string;
  tag: string;
  priority: number;
  status: string;
  result: TaskResult | null;
  resume_note: string | null;
  created_at: Date;
  started_at: Date | null;
  ended_at: Date | null;
};

async function hydrate(row: TaskRow, withEvents: boolean): Promise<Task> {
  const sql = db();
  const events: TaskEvent[] = withEvents
    ? (await sql<{ ts: Date; type: string; actor: string; payload: unknown }[]>`
        SELECT ts, type, actor, payload FROM task_events WHERE task_id = ${row.id} ORDER BY id`).map((e) => ({
        ts: e.ts.getTime(),
        type: e.type as TaskEvent["type"],
        actor: e.actor,
        payload: e.payload,
      }))
    : [];
  return {
    id: row.id,
    companyId: row.company_id,
    title: row.title,
    description: row.description,
    tag: row.tag as Task["tag"],
    priority: row.priority as Task["priority"],
    status: row.status as TaskStatus,
    result: row.result ?? undefined,
    resumeNote: row.resume_note ?? undefined,
    events,
    createdAt: row.created_at.getTime(),
    startedAt: row.started_at?.getTime(),
    endedAt: row.ended_at?.getTime(),
  };
}

type EventRow = { task_id: string; type: string; actor: string; payload: string };

/**
 * Coalesces task_events inserts into one multi-row INSERT per event-loop turn. Agents log two
 * events per tool call (fire-and-forget), so under parallel load one-INSERT-per-event saturates
 * the pool and queues the scheduler's own reads behind log writes. Order is preserved (single
 * queue, one flush in flight) and every append resolves only once its row is durable.
 */
class EventWriter {
  private pending: { row: EventRow; done: (err?: unknown) => void }[] = [];
  private flushing = false;

  append(row: EventRow): Promise<void> {
    return new Promise((resolve, reject) => {
      this.pending.push({ row, done: (err) => (err ? reject(err) : resolve()) });
      if (!this.flushing) setImmediate(() => void this.flush());
    });
  }

  private async flush(): Promise<void> {
    if (this.flushing) return;
    this.flushing = true;
    try {
      while (this.pending.length) {
        const batch = this.pending.splice(0, 500);
        try {
          await insertEvents(batch.map((b) => b.row));
          for (const b of batch) b.done();
        } catch {
          // One bad row (e.g. FK on a deleted task) must not drop its neighbours' logs:
          // fall back to row-by-row so each caller gets its own result.
          for (const b of batch) {
            try {
              await insertEvents([b.row]);
              b.done();
            } catch (err) {
              b.done(err);
            }
          }
        }
      }
    } finally {
      this.flushing = false;
    }
  }
}

async function insertEvents(rows: EventRow[]): Promise<void> {
  const sql = db();
  await sql`
    INSERT INTO task_events (task_id, type, actor, payload)
    SELECT * FROM unnest(
      ${sql.array(rows.map((r) => r.task_id))}::text[],
      ${sql.array(rows.map((r) => r.type))}::text[],
      ${sql.array(rows.map((r) => r.actor))}::text[],
      ${sql.array(rows.map((r) => r.payload))}::jsonb[])`;
}

const events = new EventWriter();

export class PgStore implements Store {
  async create(input: CreateTaskInput): Promise<Task> {
    const sql = db();
    const id = randomUUID().slice(0, 8);
    const status = input.status ?? "todo";
    const [row] = await sql<TaskRow[]>`
      INSERT INTO tasks (id, company_id, title, description, tag, priority, status)
      VALUES (${id}, ${input.companyId}, ${input.title}, ${input.description}, ${input.tag}, ${input.priority ?? 1}, ${status})
      RETURNING *`;
    await this.event(id, "system", "status_change", { to: status });
    return hydrate(row, true);
  }

  async get(id: string, opts?: { events?: boolean }): Promise<Task | undefined> {
    const [row] = await db()<TaskRow[]>`SELECT * FROM tasks WHERE id = ${id}`;
    return row ? hydrate(row, opts?.events ?? true) : undefined;
  }

  async nextTodo(companyId: string): Promise<Task | undefined> {
    const [row] = await db()<TaskRow[]>`
      SELECT * FROM tasks WHERE company_id = ${companyId} AND status = 'todo'
      ORDER BY created_at LIMIT 1`;
    return row ? hydrate(row, false) : undefined;
  }

  async setStatus(id: string, status: TaskStatus, result?: TaskResult): Promise<void> {
    const sql = db();
    const started = status === "in_progress" ? sql`now()` : sql`started_at`;
    const ended = ["completed", "failed", "needs_continuation"].includes(status) ? sql`now()` : sql`ended_at`;
    await sql`
      UPDATE tasks SET status = ${status},
        result = ${result ? sql.json(result as unknown as Parameters<typeof sql.json>[0]) : sql`result`},
        started_at = ${started}, ended_at = ${ended}
      WHERE id = ${id}`;
    await this.event(id, "system", "status_change", { to: status });
  }

  async setPriority(id: string, priority: number): Promise<void> {
    await db()`UPDATE tasks SET priority = ${priority} WHERE id = ${id}`;
  }

  async checkpoint(id: string, note: string): Promise<void> {
    await db()`UPDATE tasks SET status='needs_continuation', resume_note=${note}, ended_at=now() WHERE id = ${id}`;
    await this.event(id, "system", "status_change", { to: "needs_continuation", resumeNote: note });
  }

  async event(taskId: string, actor: string, type: TaskEvent["type"], payload: unknown): Promise<void> {
    await events.append({ task_id: taskId, type, actor, payload: JSON.stringify(payload ?? {}) });
  }

  async list(companyId: string, status?: TaskStatus): Promise<Task[]> {
    const sql = db();
    const rows = status
      ? await sql<TaskRow[]>`SELECT * FROM tasks WHERE company_id = ${companyId} AND status = ${status} ORDER BY created_at DESC`
      : await sql<TaskRow[]>`SELECT * FROM tasks WHERE company_id = ${companyId} ORDER BY created_at DESC`;
    return Promise.all(rows.map((r) => hydrate(r, false)));
  }
}
