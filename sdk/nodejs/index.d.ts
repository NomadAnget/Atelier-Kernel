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
  category?: string;
  filter?: Record<string, any>;
  limit?: number;
}

export interface StorageDeleteOptions {
  recordId?: string;
  category?: string;
}

export class DomainExecutionFailed extends Error {
  readonly payload: Record<string, any>;
  constructor(payload: Record<string, any>);
}

export class AtelierDomain {
  constructor();
  handler<TData = Record<string, any>, TParams = Record<string, any>>(
    name: string,
    fn: (msg: DeliveryMessage<TData, TParams>) => any | Promise<any>
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
  storageQuery(options?: StorageQueryOptions): number;
  reportReceipt(
    messageId: string,
    target: string,
    status: "received" | "started" | "completed" | "execution_failed",
    data?: Record<string, any> | null
  ): void;
  run(): void;
}
