// Shared between the server plugin (src/index.ts) and the OpenCode 2 TUI
// plugin (src/tui.tsx). Keep this file free of runtime imports: the TUI loads
// it straight from the package, and the server bundle must not pull in
// @opencode/plugin on OpenCode 1 hosts.
import type { Rpc } from "@opencode/plugin"

export type JobState = "running" | "success" | "failed" | "stale" | "never"

export interface JobSummary {
  slug: string
  name: string
  schedule: string
  scheduleText: string
  state: JobState
  nextRunAt?: string
  lastRunAt?: string
  lastRunSource?: "manual" | "scheduled"
  lastRunExitCode?: number
  lastRunError?: string
}

export interface JobListOutput {
  scopeIds: string[]
  jobs: JobSummary[]
}

const emptyObject = { type: "object", properties: {}, additionalProperties: false } as const

export const SchedulerRpc = {
  id: "opencode-scheduler",
  methods: {
    list: {
      input: emptyObject,
      output: {
        type: "object",
        properties: {
          scopeIds: { type: "array", items: { type: "string" } },
          jobs: {
            type: "array",
            items: {
              type: "object",
              properties: {
                slug: { type: "string" },
                name: { type: "string" },
                schedule: { type: "string" },
                scheduleText: { type: "string" },
                state: { type: "string", enum: ["running", "success", "failed", "stale", "never"] },
                nextRunAt: { type: "string" },
                lastRunAt: { type: "string" },
                lastRunSource: { type: "string", enum: ["manual", "scheduled"] },
                lastRunExitCode: { type: "number" },
                lastRunError: { type: "string" },
              },
              required: ["slug", "name", "schedule", "scheduleText", "state"],
            },
          },
        },
        required: ["scopeIds", "jobs"],
      },
    },
  },
  events: {
    changed: {
      schema: {
        type: "object",
        properties: { scopeId: { type: "string" } },
        required: ["scopeId"],
        additionalProperties: false,
      },
    },
  },
} as const satisfies Rpc.PortableDefinition
