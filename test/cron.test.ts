import { describe, expect, test } from "bun:test"
import { nextCronRun } from "../src/cron"

const at = (y: number, mo: number, d: number, h = 0, mi = 0, s = 0) => new Date(y, mo - 1, d, h, mi, s)

describe("nextCronRun", () => {
  test("is strictly after `from`, even on an exact match", () => {
    expect(nextCronRun("0 9 * * *", at(2026, 10, 3, 9, 0))).toEqual(at(2026, 10, 4, 9, 0))
    expect(nextCronRun("* * * * *", at(2026, 10, 3, 9, 0, 30))).toEqual(at(2026, 10, 3, 9, 1))
  })

  test("daily time later today and tomorrow", () => {
    expect(nextCronRun("30 14 * * *", at(2026, 10, 3, 9, 15))).toEqual(at(2026, 10, 3, 14, 30))
    expect(nextCronRun("30 14 * * *", at(2026, 10, 3, 15, 0))).toEqual(at(2026, 10, 4, 14, 30))
  })

  test("minute and hour steps", () => {
    expect(nextCronRun("*/15 * * * *", at(2026, 10, 3, 9, 7))).toEqual(at(2026, 10, 3, 9, 15))
    expect(nextCronRun("*/15 * * * *", at(2026, 10, 3, 9, 50))).toEqual(at(2026, 10, 3, 10, 0))
    expect(nextCronRun("0 */6 * * *", at(2026, 10, 3, 13, 0))).toEqual(at(2026, 10, 3, 18, 0))
  })

  test("comma lists", () => {
    expect(nextCronRun("0 9,17 * * *", at(2026, 10, 3, 10, 0))).toEqual(at(2026, 10, 3, 17, 0))
  })

  test("day of week, with 7 meaning Sunday", () => {
    // 2026-10-03 is a Saturday.
    expect(nextCronRun("0 9 * * 1", at(2026, 10, 3, 12, 0))).toEqual(at(2026, 10, 5, 9, 0))
    expect(nextCronRun("0 9 * * 7", at(2026, 10, 3, 12, 0))).toEqual(at(2026, 10, 4, 9, 0))
  })

  test("month rollover and year rollover", () => {
    expect(nextCronRun("0 0 1 * *", at(2026, 10, 3))).toEqual(at(2026, 11, 1))
    expect(nextCronRun("0 0 1 1 *", at(2026, 10, 3))).toEqual(at(2027, 1, 1))
  })

  test("skips months without the day", () => {
    expect(nextCronRun("0 0 31 * *", at(2026, 4, 1))).toEqual(at(2026, 5, 31))
    expect(nextCronRun("0 0 29 2 *", at(2026, 3, 1))).toEqual(at(2028, 2, 29))
  })

  test("restricted day-of-month and day-of-week match either", () => {
    // The 15th, or any Monday — whichever comes first.
    expect(nextCronRun("0 9 15 * 1", at(2026, 10, 3, 12, 0))).toEqual(at(2026, 10, 5, 9, 0))
    expect(nextCronRun("0 9 15 * 1", at(2026, 10, 13, 12, 0))).toEqual(at(2026, 10, 15, 9, 0))
  })

  test("a starred day field defers to the other one", () => {
    expect(nextCronRun("0 9 */1 * 1", at(2026, 10, 3, 12, 0))).toEqual(at(2026, 10, 5, 9, 0))
  })

  test("undefined for dates that never occur", () => {
    expect(nextCronRun("0 0 31 2 *", at(2026, 1, 1))).toBeUndefined()
  })

  test("rejects invalid expressions", () => {
    expect(() => nextCronRun("0 9 * *", at(2026, 1, 1))).toThrow()
    expect(() => nextCronRun("0 25 * * *", at(2026, 1, 1))).toThrow()
  })
})
