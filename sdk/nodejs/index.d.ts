// Atelier SDK v2 type definitions — single-layer AtelierDomain.
// 15 capabilities: lifecycle / handlers / send / request / wait / receipts / events /
// storage / queries / supervisor / subscribe / notify / C4 objects / logger / ports.

export type Json = Record<string, any>;

export interface MessageReceipt {
  message_id: string;
  target: string;
  status: "sent" | "received" | "started" | "completed" | "execution_failed" | "target_invalid" | "contract_invalid";
  data?: Json;
  seq?: number;
  [key: string]: any;
}

export interface Message {
  message_id: string;
  target: string;
  message_type: string;
  source_domain: string;
  params: Json;
  data: Json;
}

export type Handler = (msg: Message) => unknown | Promise<unknown>;

export interface DomainOptions {
  maxPending?: number;
  resultTtlMs?: number;
}

export declare class DomainRequestError extends Error {
  status: string;
  data: Json | null;
  message_id: string | null;
}

export declare class DomainExecutionFailed extends Error {
  payload: Json;
}

export declare class FileNamespace {
  root: string;
  prefix: string;
  matches(path: string): boolean;
  normalize(path: string): string;
  resolve(path: string, forWrite: boolean): string;
  objectId(path: string): string | null;
  objectIdForPath(physical: string): string | null;
}

export interface Logger {
  debug(message: string, fields?: Json): void;
  info(message: string, fields?: Json): void;
  warn(message: string, fields?: Json): void;
  error(message: string, fields?: Json): void;
}

export declare class AtelierDomain {
  constructor(domain: string, options?: DomainOptions);
  domain: string;

  // 2. handlers
  handler(name: string, fn: Handler, description?: string): this;

  // 3/4/5. messages
  /** 字符串目标返回单个 id，数组目标返回有序 id 列表。 */
  send(target: string, messageType: string, params?: Json, data?: Json, options?: { timeout?: number }): Promise<string>;
  send(targets: string[], messageType: string, params?: Json, data?: Json, options?: { timeout?: number }): Promise<string[]>;
  request(target: string, messageType: string, params?: Json, data?: Json, options?: { timeout?: number }): Promise<MessageReceipt>;
  waitResult(messageId: string, options?: { timeout?: number }): Promise<MessageReceipt>;
  waitResults(messageIds: string[], options?: { timeout?: number }): Promise<MessageReceipt[]>;
  forgetResult(messageId: string): boolean;

  // 6. receipts
  reportReceipt(messageId: string, target: string, status: string, data?: Json): void;

  // 7. events
  /** 通用事件登记；`onLog` / `onNotify` / `onObservation` / `onTelemetry` 都是它的特化。 */
  on(kind: "log" | "notify" | "observation" | "telemetry" | "receipt", fn: (payload: Json) => void): this;
  onReceipt(fn: (frame: MessageReceipt) => void): this;
  onLog(fn: (payload: Json) => void): this;
  onNotify(fn: (payload: Json) => void): this;
  onObservation(fn: (payload: Json) => void): this;
  onTelemetry(fn: (payload: Json) => void): this;

  // 8. storage
  storageInsert(recordId: string, category: string, data: Json, upsert?: boolean): Promise<Json>;
  storageUpdate(recordId: string, data: Json, patch?: boolean): Promise<Json>;
  storageDelete(recordId?: string, category?: string): Promise<Json>;
  storageQuery(options?: { domain?: string; record_id?: string; category?: string; filter?: Json; limit?: number }): Promise<Json>;

  // 9. queries
  queryLogs(params?: Json): Promise<Json>;
  queryHandlers(params?: Json): Promise<Json>;
  queryRuntime(params?: Json): Promise<Json>;
  queryObserver(params?: Json): Promise<Json>;
  queryProgress(): Promise<Json>;

  // 10. supervisor
  supervisorStart(domain: string, spec?: Json): Promise<Json>;
  supervisorStop(domain: string): Promise<Json>;
  supervisorRestart(domain: string): Promise<Json>;
  supervisorUpdate(domain: string, spec?: Json, probeHandler?: string): Promise<Json>;

  // 11. subscribe
  subscribeLogs(): Promise<Json>;
  unsubscribeLogs(): Promise<Json>;
  subscribeTelemetry(): Promise<Json>;
  unsubscribeTelemetry(): Promise<Json>;
  subscribeNotify(): Promise<Json>;
  unsubscribeNotify(): Promise<Json>;
  subscribeObserver(): Promise<Json>;
  unsubscribeObserver(): Promise<Json>;

  // 12. notify
  notify(level: string, message: string, title?: string, details?: Json): Promise<Json>;

  // 13. C4 objects
  enableFileInterception(root?: string): FileNamespace;
  putObject(virtualPath: string): Promise<Json>;
  getObject(objectId: string): Promise<Json>;
  deleteObject(objectId: string): Promise<Json>;
  fileObjectId(virtualPath: string): string | null;
  refreshFileIndex(): Promise<Json>;

  // 14/15. logger and ports
  readonly logger: Logger;
  readonly bindings: Record<string, { host: string; port: number }>;

  // 1. lifecycle
  runAsync(options?: { onStart?: () => unknown | Promise<unknown>; onClose?: () => unknown | Promise<unknown> }): Promise<void>;
  run(options?: { onStart?: () => unknown | Promise<unknown>; onClose?: () => unknown | Promise<unknown> }): Promise<void>;
  shutdown(): void;
}
