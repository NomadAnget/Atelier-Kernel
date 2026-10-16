export interface DeliveryMessage<TData = Record<string, any>, TParams = Record<string, any>> {
  message_id: string;
  source_domain: string;
  target: string;
  targets: string[];
  message_type: string;
  params: TParams;
  data: TData;
}

export interface StorageQueryOptions {
  domain?: string;
  recordId?: string;
  record_id?: string;
  category?: string;
  filter?: Record<string, any>;
  filter_map?: Record<string, any>;
  limit?: number;
}

export interface StorageDeleteOptions {
  recordId?: string;
  record_id?: string;
  category?: string;
}

export interface AtelierDomainOptions {
  domain?: string;
  stageDelayMs?: number;
}

export interface LogOptions {
  layer?: string;
  component?: string;
  phase?: string;
  consoleOnly?: boolean;
  console_only?: boolean;
}

export interface ProgressOptions {
  key?: string | null;
  frac?: number | null;
  waiting?: boolean;
  done?: boolean;
  layer?: string;
  component?: string;
}

export class DomainLogger {
  defaultLayer: string;
  defaultComponent: string;
  constructor(domain: AtelierDomain, defaultLayer?: string, defaultComponent?: string);
  debug(msg: any, opts?: LogOptions): void;
  info(msg: any, opts?: LogOptions): void;
  warn(msg: any, opts?: LogOptions): void;
  warning(msg: any, opts?: LogOptions): void;
  error(msg: any, opts?: LogOptions): void;
  critical(msg, opts?: LogOptions): void;
  phase(level: string, phaseName: string, msg: any, opts?: LogOptions): void;
  progress(msg: any, options?: ProgressOptions): void;
}

export class DomainExecutionFailed extends Error {
  readonly payload: Record<string, any>;
  constructor(payload: Record<string, any>);
}

export class AtelierDomain {
  readonly logger: DomainLogger;
  constructor(options?: AtelierDomainOptions);
  handler<TData = Record<string, any>, TParams = Record<string, any>>(
    name: string,
    fn: (msg: DeliveryMessage<TData, TParams>, domain: AtelierDomain) => any | Promise<any>
  ): this;
  onReceipt(fn: (receiptFrame: Record<string, any>) => void): this;
  send(
    targets: string[],
    messageType: string,
    params?: Record<string, any>,
    data?: Record<string, any>,
    seq?: number | null
  ): number;
  storageInsert(recordId: string, category: string, data: Record<string, any>, upsert?: boolean): number;
  storageUpdate(recordId: string, data: Record<string, any>, patch?: boolean): number;
  storageDelete(options?: StorageDeleteOptions): number;
  storageDelete(recordId?: string, category?: string): number;
  storageQuery(options?: StorageQueryOptions): number;
  storageQuery(
    domain?: string,
    recordId?: string,
    category?: string,
    filter?: Record<string, any>,
    limit?: number
  ): number;
  reportReceipt(
    messageId: string,
    target: string,
    status: "received" | "started" | "completed" | "execution_failed",
    data?: Record<string, any> | null
  ): void;
  run(): void;
}
