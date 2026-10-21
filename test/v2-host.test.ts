import { afterEach, describe, expect, test } from "bun:test"
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "fs"
import { join } from "path"

const FIXTURE = join(import.meta.dir, "fixtures", "v2-host.ts")
const homes: string[] = []

afterEach(() => {
  for (const dir of homes.splice(0)) rmSync(dir, { recursive: true, force: true })
})

function home(): string {
  mkdirSync("/tmp/opencode", { recursive: true })
  const dir = mkdtempSync("/tmp/opencode/v2-host-test-")
  homes.push(dir)
  mkdirSync(join(dir, "proj"))
  return dir
}

async function host(
  dir: string,
  input: { job?: Record<string, unknown>; calls?: [string, Record<string, unknown>][] } = {}
): Promise<string[]> {
  const workdir = join(dir, "proj")
  const proc = Bun.spawn(["bun", FIXTURE, JSON.stringify({ workdir, calls: [], ...input })], {
    cwd: workdir,
    env: { ...process.env, HOME: dir },
    stdout: "pipe",
    stderr: "pipe",
  })
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ])
  if (code !== 0) throw new Error(`fixture exited ${code}: ${stderr}`)
  return JSON.parse(stdout.trim().split("\n").at(-1)!)
}

const JOB = {
  slug: "cycle",
  name: "cycle",
  schedule: "0 * * * *",
  run: { prompt: "Run the cycle." },
  createdAt: "2026-10-21T00:00:00.000Z",
}

describe("OpenCode 2 tools refuse prompt words read as flags", () => {
  for (const word of ["-c", "--", "--continue", "-5"]) {
    test(`schedule_job, update_job and run_job refuse ${word}`, async () => {
      const dir = home()
      const scopeRoot = join(dir, "proj")
      const prompt = `use ${word} now`
      const [scheduled, updated, ran] = await host(dir, {
        job: { ...JOB, workdir: scopeRoot },
        calls: [
          ["schedule_job", { name: "new", schedule: "0 * * * *", prompt, scopeRoot }],
          ["update_job", { name: "cycle", prompt, scopeRoot }],
          ["run_job", { name: "cycle", prompt, scopeRoot }],
        ],
      })
      for (const output of [scheduled, updated, ran]) {
        expect(output).toContain(`prompt word "${word}" as a flag`)
      }
      expect(scheduled).toContain("Invalid run spec")
      expect(updated).toContain("Invalid run spec")
      expect(ran).toContain("Invalid run override")
    }, 20_000)
  }
})

describe("plugin start on OpenCode 2", () => {
  test("rewrites invocations stored in the old -- form", async () => {
    const dir = home()
    const jobsDir = join(dir, ".config/opencode/scheduler/scopes/legacy-scope/jobs")
    mkdirSync(jobsDir, { recursive: true })
    const old = join(jobsDir, "old.json")
    const refused = join(jobsDir, "refused.json")
    const handEdited = join(jobsDir, "hand-edited.json")
    const oldArgs = ["run", "--session", "ses_x", "--", "Run", "the", "cycle."]
    writeFileSync(
      old,
      JSON.stringify({ ...JOB, slug: "old", run: { session: "ses_x", prompt: "Run the cycle." }, invocation: { command: "/opt/opencode", args: oldArgs } })
    )
    writeFileSync(
      refused,
      JSON.stringify({ ...JOB, slug: "refused", run: { prompt: "use -c now" }, invocation: { command: "opencode", args: ["run", "--", "use", "-c", "now"] } })
    )
    const handEditedText = JSON.stringify({
      ...JOB,
      slug: "hand-edited",
      invocation: { command: "opencode", args: ["run", "--", "Something", "else."] },
    })
    writeFileSync(handEdited, handEditedText)

    await host(dir)

    expect(JSON.parse(readFileSync(old, "utf-8")).invocation).toEqual({
      command: "/opt/opencode",
      args: ["run", "--session", "ses_x", "Run", "the", "cycle."],
    })
    expect(JSON.parse(readFileSync(refused, "utf-8")).invocation.args).toEqual(["run", "--", "use", "-c", "now"])
    expect(readFileSync(handEdited, "utf-8")).toBe(handEditedText)

    const migrated = readFileSync(old, "utf-8")
    await host(dir)
    expect(readFileSync(old, "utf-8")).toBe(migrated)
  }, 20_000)
})
