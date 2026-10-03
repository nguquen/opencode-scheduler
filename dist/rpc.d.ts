export type JobState = "running" | "success" | "failed" | "stale" | "never";
export interface JobSummary {
    slug: string;
    name: string;
    schedule: string;
    scheduleText: string;
    state: JobState;
    nextRunAt?: string;
    lastRunAt?: string;
    lastRunSource?: "manual" | "scheduled";
    lastRunExitCode?: number;
    lastRunError?: string;
}
export interface JobListOutput {
    scopeIds: string[];
    jobs: JobSummary[];
}
export declare const SchedulerRpc: {
    readonly id: "opencode-scheduler";
    readonly methods: {
        readonly list: {
            readonly input: {
                readonly type: "object";
                readonly properties: {};
                readonly additionalProperties: false;
            };
            readonly output: {
                readonly type: "object";
                readonly properties: {
                    readonly scopeIds: {
                        readonly type: "array";
                        readonly items: {
                            readonly type: "string";
                        };
                    };
                    readonly jobs: {
                        readonly type: "array";
                        readonly items: {
                            readonly type: "object";
                            readonly properties: {
                                readonly slug: {
                                    readonly type: "string";
                                };
                                readonly name: {
                                    readonly type: "string";
                                };
                                readonly schedule: {
                                    readonly type: "string";
                                };
                                readonly scheduleText: {
                                    readonly type: "string";
                                };
                                readonly state: {
                                    readonly type: "string";
                                    readonly enum: readonly ["running", "success", "failed", "stale", "never"];
                                };
                                readonly nextRunAt: {
                                    readonly type: "string";
                                };
                                readonly lastRunAt: {
                                    readonly type: "string";
                                };
                                readonly lastRunSource: {
                                    readonly type: "string";
                                    readonly enum: readonly ["manual", "scheduled"];
                                };
                                readonly lastRunExitCode: {
                                    readonly type: "number";
                                };
                                readonly lastRunError: {
                                    readonly type: "string";
                                };
                            };
                            readonly required: readonly ["slug", "name", "schedule", "scheduleText", "state"];
                        };
                    };
                };
                readonly required: readonly ["scopeIds", "jobs"];
            };
        };
    };
    readonly events: {
        readonly changed: {
            readonly schema: {
                readonly type: "object";
                readonly properties: {
                    readonly scopeId: {
                        readonly type: "string";
                    };
                };
                readonly required: readonly ["scopeId"];
                readonly additionalProperties: false;
            };
        };
    };
};
