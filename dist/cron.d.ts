export declare function splitCronExpression(cron: string): [string, string, string, string, string];
export declare function uniqueSorted(values: number[]): number[];
export declare function parseCronField(field: string, min: number, max: number, label: string, allowSundaySeven?: boolean): number[] | null;
export declare function parseCronNumber(value: string, min: number, max: number, label: string, allowSundaySeven: boolean): number;
export declare function validateCronExpression(cron: string): void;
export declare function nextCronRun(cron: string, from?: Date): Date | undefined;
