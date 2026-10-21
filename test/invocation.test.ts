import { afterEach, describe, expect, test } from "bun:test"
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "fs"
import { join } from "path"
import {
  buildOpencodeV2Args,
  legacyOpencodeV2Args,
  migrateV2Invocations,
  promptFlagWord,
  validateV2Prompt,
  type JobRunSpec,
} from "../src/invocation"

describe("buildOpencodeV2Args", () => {
  test("appends the prompt words without a -- separator", () => {
    expect(buildOpencodeV2Args({ session: "X", prompt: "Run the cycle." })).toEqual([
      "run",
      "--session",
      "X",
      "Run",
      "the",
      "cycle.",
    ])
  })

  test("sends the prompt as one line", () => {
    expect(buildOpencodeV2Args({ prompt: "  first line\n\nsecond\t line " })).toEqual([
      "run",
      "first",
      "line",
      "second",
      "line",
    ])
  })

  test("refuses a prompt word that would be read as a flag", () => {
    expect(() => buildOpencodeV2Args({ prompt: "use -c now" })).toThrow('"-c"')
  })
})

describe("prompt flag words", () => {
  for (const word of ["-c", "--", "--continue", "-5", "-x"]) {
    test(`refuses ${word}`, () => {
      expect(promptFlagWord(`run ${word} now`)).toBe(word)
      expect(() => validateV2Prompt(`run ${word} now`)).toThrow(`"${word}"`)
    })
  }

  for (const prompt of ["4H cycle - health", "send an e-mail", "Run the cycle.", "", undefined]) {
    test(`accepts ${JSON.stringify(prompt)}`, () => {
      expect(promptFlagWord(prompt)).toBeUndefined()
      expect(() => validateV2Prompt(prompt)).not.toThrow()
    })
  }
})

const sandboxes: string[] = []

afterEach(() => {
  for (const dir of sandboxes.splice(0)) rmSync(dir, { recursive: true, force: true })
})

function scopes(): string {
  mkdirSync("/tmp/opencode", { recursive: true })
  const dir = mkdtempSync("/tmp/opencode/invocation-test-")
  sandboxes.push(dir)
  return dir
}

function writeJob(scopesDir: string, scope: string, slug: string, job: Record<string, unknown>): string {
  const jobsDir = join(scopesDir, scope, "jobs")
  mkdirSync(jobsDir, { recursive: true })
  const path = join(jobsDir, `${slug}.json`)
  writeFileSync(path, JSON.stringify({ slug, name: slug, schedule: "0 * * * *", ...job }, null, 2))
  return path
}

function readJob(path: string): Record<string, any> {
  return JSON.parse(readFileSync(path, "utf-8"))
}

const runOf = (raw: unknown) => (raw as { run: JobRunSpec }).run

function migrate(scopesDir: string) {
  const warnings: string[] = []
  const result = migrateV2Invocations(scopesDir, runOf, (message) => warnings.push(message))
  return { ...result, warnings }
}

describe("migrateV2Invocations", () => {
  const run: JobRunSpec = { session: "ses_trading4h", prompt: "Run the auto-trade skill's 4H cycle." }

  test("rewrites the old -- form and keeps the rest of the job", () => {
    const dir = scopes()
    const path = writeJob(dir, "a", "trading-4h", {
      run,
      lastRunStatus: "success",
      extra: { kept: true },
      invocation: { command: "/usr/bin/opencode", args: legacyOpencodeV2Args(run) },
    })

    const { migrated, warnings } = migrate(dir)

    expect(migrated).toEqual([path])
    expect(warnings).toEqual([])
    const job = readJob(path)
    expect(job.invocation).toEqual({ command: "/usr/bin/opencode", args: buildOpencodeV2Args(run) })
    expect(job.invocation.args).not.toContain("--")
    expect(job.lastRunStatus).toBe("success")
    expect(job.extra).toEqual({ kept: true })
    expect(readdirSync(join(dir, "a", "jobs"))).toEqual(["trading-4h.json"])
  })

  test("leaves other invocations alone", () => {
    const dir = scopes()
    const v1 = writeJob(dir, "a", "v1", {
      run,
      workdir: "/w",
      invocation: { command: "opencode", args: ["run", "--dir", "/w", "--session", "ses_trading4h", "--", "Run", "it."] },
    })
    const edited = writeJob(dir, "b", "edited", {
      run,
      invocation: { command: "opencode", args: [...legacyOpencodeV2Args(run), "extra"] },
    })
    const current = writeJob(dir, "b", "current", {
      run,
      invocation: { command: "opencode", args: buildOpencodeV2Args(run) },
    })
    const none = writeJob(dir, "b", "none", { run })
    const before = [v1, edited, current, none].map((path) => readFileSync(path, "utf-8"))

    expect(migrate(dir)).toEqual({ migrated: [], refused: [], warnings: [] })
    expect([v1, edited, current, none].map((path) => readFileSync(path, "utf-8"))).toEqual(before)
  })

  test("a second pass changes nothing", () => {
    const dir = scopes()
    const path = writeJob(dir, "a", "job", {
      run,
      invocation: { command: "opencode", args: legacyOpencodeV2Args(run) },
    })

    expect(migrate(dir).migrated).toEqual([path])
    const once = readFileSync(path, "utf-8")
    expect(migrate(dir)).toEqual({ migrated: [], refused: [], warnings: [] })
    expect(readFileSync(path, "utf-8")).toBe(once)
  })

  test("a refused prompt keeps its old invocation and warns once", () => {
    const dir = scopes()
    const refusedRun: JobRunSpec = { prompt: "use -c now" }
    const legacy = legacyOpencodeV2Args(refusedRun)
    const path = writeJob(dir, "a", "flaggy", {
      run: refusedRun,
      invocation: { command: "opencode", args: legacy },
    })

    const { migrated, refused, warnings } = migrate(dir)

    expect(migrated).toEqual([])
    expect(refused).toEqual([path])
    expect(warnings).toHaveLength(1)
    expect(warnings[0]).toContain("a/flaggy.json")
    expect(warnings[0]).toContain('"-c"')
    expect(readJob(path).invocation.args).toEqual(legacy)
  })

  test("skips unreadable job files and a missing scopes dir", () => {
    const dir = scopes()
    mkdirSync(join(dir, "a", "jobs"), { recursive: true })
    writeFileSync(join(dir, "a", "jobs", "broken.json"), "{")

    expect(migrate(dir)).toEqual({ migrated: [], refused: [], warnings: [] })
    expect(migrate(join(dir, "missing"))).toEqual({ migrated: [], refused: [], warnings: [] })
  })
})
