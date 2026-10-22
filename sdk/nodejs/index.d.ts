export interface DeliveryMessage<TData = Record<string, any>, TParams = Record<string, any>> {
  logical_id: string;
  signal?: AbortSignal;
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
  maxPending?: number;
  maxOutbox?: number;
  maxFrameBytes?: number;
  shutdownGrace?: number;
}

export interface DomainContext {
  logical_id?:string;
  message_id?:string; source_domain?:string; target?:string; message_type?:string;
}
export interface RequestOptions { timeout?:number; signal?:AbortSignal; }
export interface TargetResult {
  message_id:string;
  target:string|null;status:string;receipt:Record<string,any>|null;
  error:{status:string;data:any;message_id:string|null}|null;
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
  state?: "running" | "completed" | "failed" | "cancelled" | null;
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
  critical(msg: any, opts?: LogOptions): void;
  phase(level: string, phaseName: string, msg: any, opts?: LogOptions): void;
  progress(msg: any, options?: ProgressOptions): void;
  queryLogs(options?: QueryLogsOptions, requestOptions?: RequestOptions): Promise<QueryLogsResponse>;
  queryLogsAsync(options?: QueryLogsOptions, requestOptions?: RequestOptions): Promise<QueryLogsResponse>;
  queryProgress(requestOptions?: RequestOptions): Promise<QueryProgressResponse>;
  queryProgressAsync(requestOptions?: RequestOptions): Promise<QueryProgressResponse>;
}

export class DomainExecutionFailed extends Error {
  readonly payload: Record<string, any>;
  constructor(payload: Record<string, any>);
}

export interface QueryHandlersOptions {
  domain?: string;
  target?: string;
  handler?: string;
  includeInactive?: boolean;
  include_inactive?: boolean;
}

export interface HandlerDescriptor {
  target: string;
  domain: string;
  handler: string;
  description: string;
  status: string;
}

export interface QueryHandlersResponse {
  count: number;
  routes: string[];
  handlers: HandlerDescriptor[];
  description?: string | null;
}

export interface QueryLogsOptions {
  domain?: string;
  domainId?: string;
  domain_id?: string;
  source_domain?: string;
  level?: string;
  layer?: string;
  component?: string;
  phase?: string;
  sinceTs?: number;
  since_ts?: number;
  limit?: number;
  beforeId?: number;
  before_id?: number;
}

export interface QueryLogsResponse {
  logs: Array<Record<string, any>>;
  /** Number of logs in this page, not the database total. */
  total: number;
  has_more: boolean;
  next_before_id: number | null;
}

export interface QueryProgressResponse {
  progress: Array<Record<string, any>>;
  count: number;
}

export class DomainTelemetry {
  constructor(domain: AtelierDomain);
  emit(metricName: string, value: any, tags?: Record<string, any>): number;
  emitAsync(metricName: string, value: any, tags?: Record<string, any>, options?: RequestOptions): Promise<Record<string, any>>;
}

export class DomainStorageClient {
  constructor(domain: AtelierDomain);
  insert(recordId: string, category: string, data: any, upsert?: boolean): Promise<string>;
  update(recordId: string, data: any, patch?: boolean): Promise<string>;
  delete(options?: StorageDeleteOptions): Promise<string>;
  delete(recordId?: string, category?: string): Promise<string>;
  query(options?: StorageQueryOptions): Promise<string>;
  query(domain?: string, recordId?: string, category?: string, filter?: Record<string, any>, limit?: number): Promise<string>;
}

export class DomainRegistryClient {
  constructor(domain: AtelierDomain);
  queryHandlers(options?: QueryHandlersOptions, requestOptions?: RequestOptions): Promise<QueryHandlersResponse>;
  getDescription(target: string, requestOptions?: RequestOptions): Promise<string | null>;
}

export interface QueryRuntimeOptions {
  domain?: string;
  domainId?: string;
  domain_id?: string;
}

export interface DomainAssembly {
  domain_id: string;
  status: string;
  instance_id: number;
  routes: string[];
  handlers: Record<string, string>;
  process?: Record<string, any>;
}

export class DomainRuntimeClient {
  constructor(domain: AtelierDomain);
  query(options?: QueryRuntimeOptions | string, requestOptions?: RequestOptions): Promise<Record<string, any>>;
  listDomains(requestOptions?: RequestOptions): Promise<DomainAssembly[]>;
  getDomain(domainId: string, requestOptions?: RequestOptions): Promise<DomainAssembly | null>;
}

export interface QueryObserverOptions {
  limit?: number;
  messageId?: string;
  message_id?: string;
  target?: string;
  sourceDomain?: string;
  source_domain?: string;
}

export class DomainObserverClient {
  constructor(domain: AtelierDomain);
  query(options?: QueryObserverOptions, requestOptions?: RequestOptions): Promise<Array<Record<string, any>>>;
  subscribe(requestOptions?: RequestOptions): Promise<Record<string, any>>;
  unsubscribe(requestOptions?: RequestOptions): Promise<Record<string, any>>;
}

export interface SupervisorUpdateOptions {
  domain: string;
  spec?: Record<string, any>;
  probeHandler?: string;
  probe_handler?: string;
  timeoutMs?: number;
  timeout_ms?: number;
}

export class DomainSupervisorClient {
  constructor(domain: AtelierDomain);
  start(domain: string, spec?: Record<string, any>, options?: RequestOptions): Promise<Record<string, any>>;
  stop(domain: string, options?: RequestOptions): Promise<Record<string, any>>;
  restart(domain: string, options?: RequestOptions): Promise<Record<string, any>>;
  update(domain: string, spec?: Record<string, any>, options?: Partial<SupervisorUpdateOptions> & RequestOptions): Promise<Record<string, any>>;
}

export class AtelierDomain {
  readonly context:DomainContext;
  readonly logger: DomainLogger;
  readonly telemetry: DomainTelemetry;
  readonly storage: DomainStorageClient;
  readonly registry: DomainRegistryClient;
  readonly runtime: DomainRuntimeClient;
  readonly observer: DomainObserverClient;
  readonly supervisor: DomainSupervisorClient;
  shutdown(options?:{grace?:number}):Promise<void>;
  requestMany(messageIds:string[],options?:RequestOptions):Promise<TargetResult[]>;
  waitResult(messageId:string,options?:RequestOptions):Promise<Record<string,any>>;
  waitResults(messageIds:string[],options?:RequestOptions):Promise<TargetResult[]>;
  forgetResult(messageId:string):boolean;
  queryHandlersAsync(options?:QueryHandlersOptions,requestOptions?:RequestOptions):Promise<QueryHandlersResponse>;
  getHandlerDescriptionAsync(target:string,requestOptions?:RequestOptions):Promise<string|null>;
  queryLogsAsync(options?:QueryLogsOptions,requestOptions?:RequestOptions):Promise<QueryLogsResponse>;
  queryProgressAsync(requestOptions?:RequestOptions):Promise<QueryProgressResponse>;
  queryRuntimeAsync(options?: QueryRuntimeOptions, requestOptions?: RequestOptions): Promise<Record<string, any>>;
  queryObserverAsync(options?: QueryObserverOptions, requestOptions?: RequestOptions): Promise<Record<string, any>>;
  subscribeObserverAsync(requestOptions?: RequestOptions): Promise<Record<string, any>>;
  unsubscribeObserverAsync(requestOptions?: RequestOptions): Promise<Record<string, any>>;
  supervisorStartAsync(options: { domain: string; spec?: Record<string, any> }, requestOptions?: RequestOptions): Promise<Record<string, any>>;
  supervisorStopAsync(options: { domain: string }, requestOptions?: RequestOptions): Promise<Record<string, any>>;
  supervisorRestartAsync(options: { domain: string }, requestOptions?: RequestOptions): Promise<Record<string, any>>;
  supervisorUpdateAsync(options: SupervisorUpdateOptions, requestOptions?: RequestOptions): Promise<Record<string, any>>;
  subscribeLogsAsync(options?: { level?: string; sourceDomain?: string; source_domain?: string }, requestOptions?: RequestOptions): Promise<Record<string, any>>;
  unsubscribeLogsAsync(requestOptions?: RequestOptions): Promise<Record<string, any>>;
  subscribeTelemetryAsync(requestOptions?: RequestOptions): Promise<Record<string, any>>;
  unsubscribeTelemetryAsync(requestOptions?: RequestOptions): Promise<Record<string, any>>;
  subscribeNotifyAsync(requestOptions?: RequestOptions): Promise<Record<string, any>>;
  unsubscribeNotifyAsync(requestOptions?: RequestOptions): Promise<Record<string, any>>;
  storageInsertAsync(recordId:string,category:string,data:Record<string,any>,upsert?:boolean,options?:RequestOptions):Promise<any>;
  storageUpdateAsync(recordId:string,data:Record<string,any>,patch?:boolean,options?:RequestOptions):Promise<any>;
  storageDeleteAsync(options?:StorageDeleteOptions,requestOptions?:RequestOptions):Promise<any>;
  storageQueryAsync(options?:StorageQueryOptions,requestOptions?:RequestOptions):Promise<any>;
  runAsync(options?: { onStart?: () => any | Promise<any>; onClose?: () => any | Promise<any> }): Promise<void>;
  close(): void;
  request(target: string, messageType: string, params?: Record<string, any>, data?: Record<string, any>, options?: { timeout?: number; signal?: AbortSignal }): Promise<Record<string, any>>;
  constructor(options?: AtelierDomainOptions);
  handler<TData = Record<string, any>, TParams = Record<string, any>>(
    name: string,
    fn: (msg: DeliveryMessage<TData, TParams>, domain: AtelierDomain) => any | Promise<any>
  ): this;
  on<TData = Record<string, any>, TParams = Record<string, any>>(
    name: string,
    fn: (msg: DeliveryMessage<TData, TParams>, domain: AtelierDomain) => any | Promise<any>
  ): this;
  onReceipt(fn: (receiptFrame: Record<string, any>) => void): this;
  onLog(callback: (record: Record<string, any>) => any | Promise<any>, options?: { level?: string; sourceDomain?: string; source_domain?: string }): this;
  onTelemetry(callback: (metric: Record<string, any>) => any | Promise<any>): this;
  onNotify(callback: (alert: Record<string, any>) => any | Promise<any>): this;
  onObservation(callback: (event: Record<string, any>) => any | Promise<any>): this;
  notify(level: string, message: string, title?: string, details?: Record<string, any>): number;
  notifyAsync(level: string, message: string, title?: string, details?: Record<string, any>, options?: RequestOptions): Promise<Record<string, any>>;
  discoverHandlers(options?: QueryHandlersOptions): Promise<QueryHandlersResponse>;
  queryHandlers(options?:QueryHandlersOptions):Promise<string>;
  queryHandlers(domain?:string,target?:string,handler?:string):Promise<string>;
  queryLogs(options?:QueryLogsOptions):Promise<string>;
  queryProgress():Promise<string>;
  send(target:string,messageType:string,params?:Record<string,any>,data?:any,options?:RequestOptions):Promise<string>;
  send(targets:string[],messageType:string,params?:Record<string,any>,data?:any,options?:RequestOptions):Promise<string[]>;
  storageInsert(recordId:string,category:string,data:any,upsert?:boolean):Promise<string>;
  storageUpdate(recordId:string,data:any,patch?:boolean):Promise<string>;
  storageDelete(options?:StorageDeleteOptions):Promise<string>;
  storageDelete(recordId?:string,category?:string):Promise<string>;
  storageQuery(options?:StorageQueryOptions):Promise<string>;
  storageQuery(domain?:string,recordId?:string,category?:string,filter?:Record<string,any>,limit?:number):Promise<string>;
  reportReceipt(
    messageId: string,
    target: string,
    status: "received" | "started" | "completed" | "execution_failed",
    data?: Record<string, any> | null
  ): void;
  run(): void;
}

export class DomainRequestError extends Error {
  readonly status: string;
  readonly data: any;
  readonly message_id: string | null;
  constructor(status: string, data?: any, messageId?: string | null);
}

export const KERNEL_REGISTRY_TARGET: "kernel.registry";
export const KERNEL_STORAGE_TARGET: "kernel.storage";
export const KERNEL_LOGGER_TARGET: "kernel.logger";
export const KERNEL_TELEMETRY_TARGET: "kernel.telemetry";
export const KERNEL_NOTIFY_TARGET: "kernel.notify";
export const KERNEL_RUNTIME_TARGET: "kernel.runtime";
export const KERNEL_OBSERVER_TARGET: "kernel.observer";
export const KERNEL_SUPERVISOR_TARGET: "kernel.supervisor";

export const MSG_REGISTER_HANDLERS: "RegisterHandlers";
export const MSG_UNREGISTER_HANDLERS: "UnregisterHandlers";
export const MSG_QUERY_HANDLERS: "QueryHandlers";
export const MSG_QUERY_LOGS: "QueryLogs";
export const MSG_QUERY_PROGRESS: "QueryProgress";
export const MSG_SUBSCRIBE_LOGS: "SubscribeLogs";
export const MSG_UNSUBSCRIBE_LOGS: "UnsubscribeLogs";
export const MSG_SUBSCRIBE_TELEMETRY: "SubscribeTelemetry";
export const MSG_UNSUBSCRIBE_TELEMETRY: "UnsubscribeTelemetry";
export const MSG_SUBSCRIBE_NOTIFY: "SubscribeNotify";
export const MSG_UNSUBSCRIBE_NOTIFY: "UnsubscribeNotify";
export const MSG_QUERY_RUNTIME: "QueryRuntime";
export const MSG_QUERY_DOMAIN_ASSEMBLY: "QueryDomainAssembly";
export const MSG_QUERY_OBSERVER: "QueryObserver";
export const MSG_SUBSCRIBE_OBSERVER: "SubscribeObserver";
export const MSG_UNSUBSCRIBE_OBSERVER: "UnsubscribeObserver";
export const MSG_START_DOMAIN: "StartDomain";
export const MSG_STOP_DOMAIN: "StopDomain";
export const MSG_RESTART_DOMAIN: "RestartDomain";
export const MSG_UPDATE_DOMAIN: "UpdateDomain";
export const MSG_SYSTEM_LOG_EVENT: "LogEvent";
export const MSG_SYSTEM_TELEMETRY_EVENT: "TelemetryEvent";
export const MSG_SYSTEM_NOTIFY_EVENT: "NotifyEvent";
export const MSG_SYSTEM_OBSERVER_EVENT: "ObservationEvent";
export const MSG_STORAGE_INSERT: "StorageInsert";
export const MSG_STORAGE_QUERY: "StorageQuery";
export const MSG_STORAGE_UPDATE: "StorageUpdate";
export const MSG_STORAGE_DELETE: "StorageDelete";
export const MSG_DELIVERY_DURABLE: "Durable";
export const MSG_DELIVERY_VOLATILE: "Volatile";
export const MSG_DELIVERY_EPHEMERAL: "Ephemeral";
