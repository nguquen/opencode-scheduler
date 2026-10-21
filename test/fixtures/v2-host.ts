// Loads the plugin the way OpenCode 2 does (setup() with a stub context), seeds
// the project's scope with an optional job, runs the given tool calls and
// prints their outputs as JSON. Run with HOME pointing at a throwaway dir:
// the plugin resolves its storage from the home directory at import time.
import { readdirSync, writeFileSync } from "fs"
import { join } from "path"
import plugin from "../../src/index"

const { workdir, job, calls } = JSON.parse(process.argv[2]) as {
  workdir: string
  job?: Record<string, unknown>
  calls: [string, Record<string, unknown>][]
}

const tools: Record<string, { execute: (args: unknown) => Promise<{ content: string }> }> = {}
const ctx = {
  location: { directory: workdir, project: { directory: workdir } },
  rpc: { register: async () => ({ events: { emit: async () => {} }, dispose: async () => {} }) },
  tool: { transform: async (edit: (editor: unknown) => void) => edit({ add: (def: any) => (tools[def.name] = def) }) },
}
const dispose = await plugin.setup(ctx as never)

if (job) {
  const scopes = join(process.env.HOME!, ".config/opencode/scheduler/scopes")
  const scope = readdirSync(scopes).find((name) => name.startsWith("proj-"))!
  writeFileSync(join(scopes, scope, "jobs", `${job.slug}.json`), JSON.stringify(job))
}

const outputs: string[] = []
for (const [name, args] of calls) outputs.push((await tools[name].execute(args)).content)
await dispose()
console.log(JSON.stringify(outputs))
