import { EventEmitter } from '../event-emitter.js';
import { BasicExpression, OrderBy } from '../query/ir.js';
import { IndexInterface } from '../indexes/base-index.js';
import { ChangeMessage, Subscription, SubscriptionEvents, SubscriptionStatus, SubscriptionUnsubscribedEvent } from '../types.js';
import { CollectionImpl } from './index.js';
type RequestSnapshotOptions = {
    where?: BasicExpression<boolean>;
    optimizedOnly?: boolean;
    trackLoadSubsetPromise?: boolean;
    /** Optional orderBy to pass to loadSubset for backend optimization */
    orderBy?: OrderBy;
    /** Optional limit to pass to loadSubset for backend optimization */
    limit?: number;
};
type RequestLimitedSnapshotOptions = {
    orderBy: OrderBy;
    limit: number;
    /** All column values for cursor (first value used for local index, all values for sync layer) */
    minValues?: Array<unknown>;
    /** Row offset for offset-based pagination (passed to sync layer) */
    offset?: number;
};
type CollectionSubscriptionOptions = {
    includeInitialState?: boolean;
    /** Pre-compiled expression for filtering changes */
    whereExpression?: BasicExpression<boolean>;
    /** Callback to call when the subscription is unsubscribed */
    onUnsubscribe?: (event: SubscriptionUnsubscribedEvent) => void;
};
export declare class CollectionSubscription extends EventEmitter<SubscriptionEvents> implements Subscription {
    private collection;
    private callback;
    private options;
    private loadedInitialState;
    private snapshotSent;
    /**
     * Track all loadSubset calls made by this subscription so we can unload them on cleanup.
     * We store the exact LoadSubsetOptions we passed to loadSubset to ensure symmetric unload.
     */
    private loadedSubsets;
    private sentKeys;
    private limitedSnapshotRowCount;
    private lastSentKey;
    private filteredCallback;
    private orderByIndex;
    private _status;
    private pendingLoadSubsetPromises;
    private truncateCleanup;
    get status(): SubscriptionStatus;
    constructor(collection: CollectionImpl<any, any, any, any, any>, callback: (changes: Array<ChangeMessage<any, any>>) => void, options: CollectionSubscriptionOptions);
    /**
     * Handle collection truncate event by resetting state and re-requesting subsets.
     * This is called when the sync layer receives a must-refetch and clears all data.
     *
     * IMPORTANT: We intentionally do NOT clear sentKeys here. The truncate event is emitted
     * BEFORE delete events are sent to subscribers. If we cleared sentKeys, the delete events
     * would be filtered out by filterAndFlipChanges (which skips deletes for keys not in sentKeys).
     * By keeping sentKeys intact, delete events pass through, and when new data arrives,
     * inserts will still be emitted correctly (the type is already 'insert' so no conversion needed).
     */
    private handleTruncate;
    setOrderByIndex(index: IndexInterface<any>): void;
    /**
     * Set subscription status and emit events if changed
     */
    private setStatus;
    /**
     * Track a loadSubset promise and manage loading status
     */
    private trackLoadSubsetPromise;
    hasLoadedInitialState(): boolean;
    hasSentAtLeastOneSnapshot(): boolean;
    emitEvents(changes: Array<ChangeMessage<any, any>>): void;
    /**
     * Sends the snapshot to the callback.
     * Returns a boolean indicating if it succeeded.
     * It can only fail if there is no index to fulfill the request
     * and the optimizedOnly option is set to true,
     * or, the entire state was already loaded.
     */
    requestSnapshot(opts?: RequestSnapshotOptions): boolean;
    /**
     * Sends a snapshot that fulfills the `where` clause and all rows are bigger or equal to the cursor.
     * Requires a range index to be set with `setOrderByIndex` prior to calling this method.
     * It uses that range index to load the items in the order of the index.
     *
     * For multi-column orderBy:
     * - Uses first value from `minValues` for LOCAL index operations (wide bounds, ensures no missed rows)
     * - Uses all `minValues` to build a precise composite cursor for SYNC layer loadSubset
     *
     * Note 1: it may load more rows than the provided LIMIT because it loads all values equal to the first cursor value + limit values greater.
     *         This is needed to ensure that it does not accidentally skip duplicate values when the limit falls in the middle of some duplicated values.
     * Note 2: it does not send keys that have already been sent before.
     */
    requestLimitedSnapshot({ orderBy, limit, minValues, offset, }: RequestLimitedSnapshotOptions): void;
    /**
     * Filters and flips changes for keys that have not been sent yet.
     * Deletes are filtered out for keys that have not been sent yet.
     * Updates are flipped into inserts for keys that have not been sent yet.
     */
    private filterAndFlipChanges;
    private trackSentKeys;
    unsubscribe(): void;
}
export {};
