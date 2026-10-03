import { afterEach, describe, expect, test } from "bun:test"
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, unlinkSync, writeFileSync } from "fs"
import { join } from "path"
import { SUPERVISOR_SCRIPT } from "../src/supervisor"

const SCOPE = "test-scope"
const SLUG = "job"

interface Sandbox {
  home: string
  jobPath: string
  lockPath: string
  runsPath: string
  logPath: string
  script: string
}

const sandboxes: string[] = []

afterEach(() => {
  for (const dir of sandboxes.splice(0)) rmSync(dir, { recursive: true, force: true })
})

function sandbox(job: Record<string, unknown>): Sandbox {
  const home = mkdtempSync("/tmp/opencode/supervisor-test-")
  sandboxes.push(home)
  const scopeDir = join(home, ".config/opencode/scheduler/scopes", SCOPE)
  mkdirSync(join(scopeDir, "jobs"), { recursive: true })
  const script = join(home, "supervisor.pl")
  writeFileSync(script, SUPERVISOR_SCRIPT)
  const jobPath = join(scopeDir, "jobs", `${SLUG}.json`)
  writeFileSync(jobPath, JSON.stringify({ scopeId: SCOPE, slug: SLUG, name: "Job", schedule: "* * * * *", workdir: home, ...job }))
  return {
    home,
    jobPath,
    lockPath: join(scopeDir, "locks", `${SLUG}.json`),
    runsPath: join(scopeDir, "runs", `${SLUG}.jsonl`),
    logPath: join(home, ".config/opencode/logs/scheduler", SCOPE, `${SLUG}.log`),
    script,
  }
}

function shellJob(command: string, extra: Record<string, unknown> = {}): Record<string, unknown> {
  return { invocation: { command: "/bin/sh", args: ["-c", command] }, ...extra }
}

function start(box: Sandbox) {
  return Bun.spawn(["perl", box.script, box.jobPath], { env: { ...process.env, HOME: box.home }, stdout: "ignore", stderr: "pipe" })
}

async function waitFor<T>(read: () => T | undefined, timeoutMs = 5_000): Promise<T> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const value = read()
    if (value !== undefined) return value
    await Bun.sleep(25)
  }
  throw new Error("timed out waiting")
}

function readJson(path: string): Record<string, any> | undefined {
  try {
    return JSON.parse(readFileSync(path, "utf-8"))
  } catch {
    return undefined
  }
}

async function childPid(box: Sandbox): Promise<number> {
  return waitFor(() => readJson(box.lockPath)?.childPid as number | undefined)
}

function alive(pid: number): boolean {
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

describe("supervisor.pl", () => {
  test("records a successful run", async () => {
    const box = sandbox(shellJob("exit 0", { lastRunError: "old error" }))
    expect(await start(box).exited).toBe(0)

    const job = readJson(box.jobPath)!
    expect(job.lastRunStatus).toBe("success")
    expect(job.lastRunExitCode).toBe(0)
    expect(job.lastRunError).toBeUndefined()
    expect(existsSync(box.lockPath)).toBe(false)
    expect(readFileSync(box.runsPath, "utf-8").trim().split("\n")).toHaveLength(1)
  })

  test("does not recreate a job deleted during the run", async () => {
    const box = sandbox(shellJob("sleep 1"))
    const proc = start(box)
    await childPid(box)
    unlinkSync(box.jobPath)
    await proc.exited

    expect(existsSync(box.jobPath)).toBe(false)
    expect(existsSync(box.runsPath)).toBe(false)
    expect(existsSync(box.lockPath)).toBe(false)
    expect(readFileSync(box.logPath, "utf-8")).toContain("job deleted; result not recorded")
  })

  test("keeps edits made to the job during the run", async () => {
    const box = sandbox(shellJob("sleep 1"))
    const proc = start(box)
    await childPid(box)
    const edited = { ...readJson(box.jobPath)!, schedule: "0 9 * * *", name: "Edited" }
    writeFileSync(box.jobPath, JSON.stringify(edited))
    await proc.exited

    const job = readJson(box.jobPath)!
    expect(job.schedule).toBe("0 9 * * *")
    expect(job.name).toBe("Edited")
    expect(job.lastRunStatus).toBe("success")
  })

  test("SIGTERM stops the run and records it", async () => {
    const box = sandbox(shellJob("sleep 30"))
    const proc = start(box)
    const child = await childPid(box)
    const t0 = Date.now()
    proc.kill("SIGTERM")
    await proc.exited

    expect(Date.now() - t0).toBeLessThan(6_000)
    expect(alive(child)).toBe(false)
    const job = readJson(box.jobPath)!
    expect(job.lastRunStatus).toBe("failed")
    expect(job.lastRunError).toBe("stopped")
    expect(job.lastRunExitCode).toBe(143)
    expect(existsSync(box.lockPath)).toBe(false)
  })

  // systemctl stop signals every process of the service at once.
  test("SIGTERM to the supervisor and the run together", async () => {
    const box = sandbox(shellJob("sleep 30"))
    const proc = start(box)
    const child = await childPid(box)
    const t0 = Date.now()
    process.kill(-child, "SIGTERM")
    proc.kill("SIGTERM")
    await proc.exited

    expect(Date.now() - t0).toBeLessThan(2_000)
    const job = readJson(box.jobPath)!
    expect(job.lastRunError).toBe("stopped")
    expect(job.lastRunExitCode).toBe(143)
  })

  test("timeout stops the run and records it", async () => {
    const box = sandbox(shellJob("sleep 30", { timeoutSeconds: 1 }))
    const proc = start(box)
    const child = await childPid(box)
    const t0 = Date.now()
    await proc.exited

    expect(Date.now() - t0).toBeLessThan(6_000)
    expect(alive(child)).toBe(false)
    const job = readJson(box.jobPath)!
    expect(job.lastRunStatus).toBe("failed")
    expect(job.lastRunError).toBe("timeout")
    expect(job.lastRunExitCode).toBe(124)
  }, 10_000)
})
