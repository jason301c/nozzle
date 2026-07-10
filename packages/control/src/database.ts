export type ControlBindingValue = ArrayBuffer | boolean | null | number | string | Uint8Array

export interface ControlRunResult {
  readonly meta: Readonly<Record<string, unknown>>
  readonly success: boolean
}

export interface ControlStatement {
  bind(...values: readonly ControlBindingValue[]): ControlStatement
  first<T = Record<string, unknown>>(): Promise<T | null>
  run(): Promise<ControlRunResult>
}

export interface ControlDatabase {
  prepare(sql: string): ControlStatement
}
