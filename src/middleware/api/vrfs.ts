import { URL } from "node:url";

/* ============================================================
   Types
   ============================================================ */

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];

export interface JsonObject {
    [key: string]: JsonValue | undefined;
}

export type HttpMethod = "GET" | "POST";

export type RequestStatus =
    | "success"
    | "http_error"
    | "timeout"
    | "network_error"
    | "rate_limited"
    | "aborted"
    | "response_too_large";

export type ServiceHealth = "ok" | "degraded" | "unknown";
export type OwnershipValue = boolean | "unknown";

export interface VRFSUser {
    uid: number;
    username: string;
    nickname?: string;
    name?: string;
}

export interface VRFSProfile {
    uid?: number;
    username?: string;
    profileCountry?: string;
    followersCount?: number;
    followingCount?: number;
    userTag?: string;
    tiktokName?: string;
    youtubeName?: string;
    twitchName?: string;
    instagramName?: string;
    requestResult?: string;
    [key: string]: unknown;
}

export interface VRFSOutfit {
    id?: number | string;
    name?: string;
    createdAt?: string;
    updatedAt?: string;
    slots?: Record<string, string | null>;
    [key: string]: unknown;
}

export interface VRFSItem {
    id?: number | string;
    sku?: string;
    sku_base?: string;
    name?: string;
    title?: string;
    section?: string;
    category?: string;
    category_name?: string;
    thumb?: string;
    image?: string;
    image_url?: string;
    thumbnail?: string;
    thumbnail_url?: string;
    texture_url?: string;
    thumbnail_url_ktx?: string;
    isFree?: boolean | number | string;
    price?: string | number;
    coins?: number | string;
    coins_price?: number | string;
    credits?: number | string;
    item_id?: number | string;
    itemId?: number | string;
    [key: string]: unknown;
}

export interface VRFSMarketplaceAuthor {
    uid?: number | string;
    username?: string;
    nickname?: string;
    name?: string;
    avatar_url?: string;
    badge_id?: number | string;
    highlight_color?: string;
    [key: string]: unknown;
}

export interface VRFSMarketplaceItem {
    id: number | string;
    sku?: string;
    sku_base?: string;
    title?: string;
    name?: string;
    color?: unknown;
    meta?: unknown;
    category_id?: number | string;
    sort_order?: number | string;
    is_active?: boolean | number | string;
    is_visible?: boolean | number | string;
    owners_count?: number | string;
    gifts_left?: number | string;
    coins_price?: number | string;
    owner_uid?: number | string;
    creator_uid?: number | string;
    creator?: string;
    texture_id?: number | string;
    texture_url?: string;
    thumbnail_url?: string;
    thumbnail_url_ktx?: string;
    subtitle?: string;
    highlight_type?: string;
    created_at?: string;
    updated_at?: string;
    activated_at?: string;
    available_for_send?: number | string | boolean;
    badges?: unknown[];
    author?: VRFSMarketplaceAuthor;
    [key: string]: unknown;
}

export interface SebyOwnershipResponse {
    ok: boolean;
    health: ServiceHealth;
    results: Record<string, boolean>;
    cached: number;
    retry_after?: number;
    [key: string]: unknown;
}

export interface SebyLookupResponse {
    ok: boolean;
    health: ServiceHealth;
    results: Record<string, unknown>;
    [key: string]: unknown;
}

export interface OwnershipCheckOptions {
    batchSize?: number;
    minBatchSize?: number;
    maxBatchSize?: number;
    delayMs?: number;
    maxMissingRetries?: number;
    signal?: AbortSignal;
    forceRefresh?: boolean;
    onProgress?: (progress: OwnershipProgress) => void;
}

export interface OwnershipProgress {
    processed: number;
    total: number;
    confirmed: number;
    unknown: number;
    owned: number;
    notOwned: number;
    batchNumber: number;
    batchSize: number;
    health: ServiceHealth;
    cached: number;
}

export interface OwnershipCheckResult {
    uid: number;
    results: Record<string, OwnershipValue>;
    unknown: string[];
    owned: string[];
    notOwned: string[];
    requested: number;
    confirmed: number;
    cached: number;
    apiRequests: number;
    retries: number;
    batches: number;
    durationMs: number;
    health: ServiceHealth;
    partial: boolean;
}

export interface CatalogStats {
    items: number;
    sections: number;
    free: number;
    premium: number;
    loadedAt: number;
    ageMs: number;
}

export interface MarketplaceStats {
    items: number;
    active: number;
    inactive: number;
    creators: number;
    owners: number;
    loadedAt: number;
    ageMs: number;
}

export interface VRFSServiceHealth {
    service: "vrfs" | "seby";
    ok: boolean;
    health: ServiceHealth;
    latencyMs: number;
    error?: string;
}

export interface VRFSApiHealth {
    ok: boolean;
    services: VRFSServiceHealth[];
    latencyMs: number;
}

export interface VRFSRequestOptions {
    method?: HttpMethod;
    headers?: Record<string, string>;
    body?: unknown;
    timeoutMs?: number;
    maxResponseBytes?: number;
    maxRedirects?: number;
    signal?: AbortSignal;
    retries?: number;
}

export interface VRFSClientOptions {
    baseUrl?: string;
    apiKey?: string;
    requestTimeoutMs?: number;
    maxResponseBytes?: number;
    maxRedirects?: number;
    retries?: number;
    userAgent?: string;

    catalogTtlMs?: number;
    marketplaceTtlMs?: number;
    playerTtlMs?: number;
    ownershipTtlMs?: number;

    ownershipBatchSize?: number;
    ownershipMinBatchSize?: number;
    ownershipMaxBatchSize?: number;
    ownershipRetryCount?: number;
    ownershipDelayMs?: number;
}

export interface VRFSLogger {
    debug?: (message: string, meta?: Record<string, unknown>) => void;
    info?: (message: string, meta?: Record<string, unknown>) => void;
    warn?: (message: string, meta?: Record<string, unknown>) => void;
    error?: (message: string, meta?: Record<string, unknown>) => void;
}

/* ============================================================
   Errors
   ============================================================ */

export class VRFSApiError extends Error {
    public readonly code: string;
    public readonly status: number;
    public readonly retryable: boolean;
    public readonly retryAfterMs?: number;
    public readonly endpoint?: string;
    public readonly details?: unknown;
    public readonly attempts: number;

    public constructor(
        message: string,
        options: {
            code: string;
            status?: number;
            retryable?: boolean;
            retryAfterMs?: number;
            endpoint?: string;
            details?: unknown;
            attempts?: number;
        },
    ) {
        super(message);
        this.name = "VRFSApiError";

        this.code = options.code;
        this.status = options.status ?? 0;
        this.retryable = options.retryable ?? false;
        this.retryAfterMs = options.retryAfterMs;
        this.endpoint = options.endpoint;
        this.details = options.details;
        this.attempts = options.attempts ?? 1;

        Object.setPrototypeOf(this, new.target.prototype);
    }
}

export class VRFSTimeoutError extends VRFSApiError {
    public constructor(
        message: string,
        endpoint?: string,
        attempts = 1,
    ) {
        super(message, {
            code: "TIMEOUT",
            retryable: true,
            endpoint,
            attempts,
        });

        this.name = "VRFSTimeoutError";
    }
}

export class VRFSRateLimitError extends VRFSApiError {
    public constructor(
        message: string,
        retryAfterMs: number,
        endpoint?: string,
        attempts = 1,
    ) {
        super(message, {
            code: "RATE_LIMITED",
            status: 429,
            retryable: true,
            retryAfterMs,
            endpoint,
            attempts,
        });

        this.name = "VRFSRateLimitError";
    }
}

export class VRFSInvalidResponseError extends VRFSApiError {
    public constructor(
        message: string,
        endpoint?: string,
        details?: unknown,
        attempts = 1,
    ) {
        super(message, {
            code: "INVALID_RESPONSE",
            retryable: false,
            endpoint,
            details,
            attempts,
        });

        this.name = "VRFSInvalidResponseError";
    }
}

export class VRFSResponseTooLargeError extends VRFSApiError {
    public constructor(
        message: string,
        endpoint?: string,
    ) {
        super(message, {
            code: "RESPONSE_TOO_LARGE",
            retryable: false,
            endpoint,
        });

        this.name = "VRFSResponseTooLargeError";
    }
}

/* ============================================================
   Constants
   ============================================================ */

const DEFAULTS = Object.freeze({
    vrfsBaseUrl: "https://api.vrfs.gg",
    sebyBaseUrl: "https://vrfs.sebyplay.xyz/lockerchecker",

    timeoutMs: 15_000,
    maxResponseBytes: 8 * 1024 * 1024,
    maxRedirects: 3,

    retries: 2,
    retryDelayMs: 750,

    userAgent: "UNTITLED-VRFS/2.0",

    catalogTtlMs: 30 * 60 * 1000,
    marketplaceTtlMs: 5 * 60 * 1000,
    playerTtlMs: 5 * 60 * 1000,

    playerCacheSize: 500,
    profileCacheSize: 250,
    outfitCacheSize: 250,

    maxOwnershipSkus: 5_000,
    ownershipBatchSize: 250,
    ownershipMinBatchSize: 5,
    ownershipMaxBatchSize: 500,
    ownershipRetryCount: 2,
    ownershipDelayMs: 150,

    vrfsConcurrency: 6,
    sebyConcurrency: 4,
});

const VRFS_ENDPOINTS = Object.freeze({
    username: "/webhooks/v1/get_username.php",
    profile: "/webhooks/v1/get_user_profile.php",
    outfits: "/webhooks/v1/player/outfit/list.php",
    marketplace: "/webhooks/v1/market/get_all.php",
});

const SEBY_ENDPOINTS = Object.freeze({
    lookup: "/api.php?action=lookup",
    check: "/api.php?action=check",
    items: "/data/items.json",
});

const RETRYABLE_STATUSES = new Set([
    408,
    425,
    429,
    500,
    502,
    503,
    504,
    520,
    521,
    522,
    523,
    524,
]);

/* ============================================================
   Small helpers
   ============================================================ */

const sleep = (
    ms: number,
    signal?: AbortSignal,
): Promise<void> =>
    new Promise((resolve, reject) => {
        if (signal?.aborted) {
            reject(
                new VRFSApiError("Request aborted.", {
                    code: "ABORTED",
                }),
            );
            return;
        }

        if (ms <= 0) {
            resolve();
            return;
        }

        const timer = setTimeout(() => {
            signal?.removeEventListener("abort", onAbort);
            resolve();
        }, ms);

        const onAbort = () => {
            clearTimeout(timer);
            signal?.removeEventListener("abort", onAbort);

            reject(
                new VRFSApiError("Request aborted.", {
                    code: "ABORTED",
                }),
            );
        };

        signal?.addEventListener("abort", onAbort, { once: true });
    });

const clamp = (
    value: number,
    min: number,
    max: number,
): number =>
    Math.max(min, Math.min(max, value));

const isRecord = (
    value: unknown,
): value is Record<string, unknown> =>
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value);

const asString = (value: unknown): string =>
    value === undefined || value === null
        ? ""
        : String(value);

const asPositiveInt = (value: unknown): number | null => {
    const number = Number(value);

    return Number.isInteger(number) && number > 0
        ? number
        : null;
};

const asBoolean = (value: unknown): boolean | null => {
    if (
        value === true ||
        value === 1 ||
        value === "1" ||
        value === "true"
    ) {
        return true;
    }

    if (
        value === false ||
        value === 0 ||
        value === "0" ||
        value === "false"
    ) {
        return false;
    }

    return null;
};

const uniqueStrings = (
    values: readonly unknown[],
    maxLength = 255,
): string[] => {
    const seen = new Set<string>();
    const result: string[] = [];

    for (const value of values) {
        const normalized = String(value ?? "").trim();

        if (
            !normalized ||
            normalized.length > maxLength ||
            seen.has(normalized)
        ) {
            continue;
        }

        seen.add(normalized);
        result.push(normalized);
    }

    return result;
};

const chunk = <T>(
    values: T[],
    size: number,
): T[][] => {
    const result: T[][] = [];

    for (let i = 0; i < values.length; i += size) {
        result.push(values.slice(i, i + size));
    }

    return result;
};

const normalizeBaseUrl = (value: string): string => {
    const url = new URL(value);

    if (
        url.protocol !== "https:" &&
        url.protocol !== "http:"
    ) {
        throw new Error(`Unsupported protocol: ${url.protocol}`);
    }

    return url.toString().replace(/\/+$/, "");
};

const safeUrl = (value: string): string => {
    try {
        const url = new URL(value);

        for (const key of [
            "key",
            "api_key",
            "token",
            "access_token",
            "authorization",
            "password",
            "secret",
        ]) {
            url.searchParams.delete(key);
        }

        return url.toString();
    } catch {
        return "[invalid-url]";
    }
};

const parseRetryAfter = (
    headers: Headers,
): number | undefined => {
    const value = headers.get("retry-after");

    if (!value) {
        return undefined;
    }

    const seconds = Number(value);

    if (Number.isFinite(seconds) && seconds >= 0) {
        return Math.ceil(seconds * 1000);
    }

    const timestamp = Date.parse(value);

    if (Number.isNaN(timestamp)) {
        return undefined;
    }

    return Math.max(0, timestamp - Date.now());
};

const parseJson = <T>(
    text: string,
): T | null => {
    if (!text.trim()) {
        return null;
    }

    try {
        return JSON.parse(text) as T;
    } catch {
        return null;
    }
};

const errorMessage = (
    data: unknown,
    fallback: string,
): string => {
    if (isRecord(data)) {
        for (const key of [
            "error",
            "message",
            "detail",
            "reason",
            "description",
        ]) {
            const value = data[key];

            if (
                typeof value === "string" &&
                value.trim()
            ) {
                return value.trim();
            }
        }
    }

    if (
        typeof data === "string" &&
        data.trim()
    ) {
        return data.trim();
    }

    return fallback;
};

/* ============================================================
   TTL Cache
   ============================================================ */

export class TTLCache<T> {
    private readonly map = new Map<
        string,
        {
            value: T;
            expiresAt: number;
        }
    >();

    public constructor(
        private readonly ttlMs: number,
        private readonly maxEntries = 500,
    ) {}

    public get(key: string): T | undefined {
        const entry = this.map.get(key);

        if (!entry) {
            return undefined;
        }

        if (entry.expiresAt <= Date.now()) {
            this.map.delete(key);
            return undefined;
        }

        this.map.delete(key);
        this.map.set(key, entry);

        return entry.value;
    }

    public set(
        key: string,
        value: T,
        ttlMs = this.ttlMs,
    ): void {
        this.map.delete(key);

        this.map.set(key, {
            value,
            expiresAt: Date.now() + Math.max(1, ttlMs),
        });

        while (this.map.size > this.maxEntries) {
            const oldest = this.map.keys().next().value;

            if (oldest === undefined) {
                break;
            }

            this.map.delete(oldest);
        }
    }

    public delete(key: string): void {
        this.map.delete(key);
    }

    public clear(): void {
        this.map.clear();
    }

    public get size(): number {
        this.cleanup();
        return this.map.size;
    }

    public cleanup(): void {
        const now = Date.now();

        for (const [key, entry] of this.map) {
            if (entry.expiresAt <= now) {
                this.map.delete(key);
            }
        }
    }
}

/* ============================================================
   Single-flight
   ============================================================ */

export class SingleFlight {
    private readonly inflight = new Map<
        string,
        Promise<unknown>
    >();

    public run<T>(
        key: string,
        task: () => Promise<T>,
    ): Promise<T> {
        const existing = this.inflight.get(key);

        if (existing) {
            return existing as Promise<T>;
        }

        const promise = task().finally(() => {
            this.inflight.delete(key);
        });

        this.inflight.set(key, promise);

        return promise;
    }

    public clear(): void {
        this.inflight.clear();
    }

    public get size(): number {
        return this.inflight.size;
    }
}

/* ============================================================
   Concurrency limiter
   ============================================================ */

class Semaphore {
    private active = 0;

    private readonly queue: Array<{
        resolve: () => void;
        reject: (error: Error) => void;
        signal?: AbortSignal;
        aborted: boolean;
    }> = [];

    public constructor(
        private readonly max: number,
    ) {}

    public async acquire(
        signal?: AbortSignal,
    ): Promise<() => void> {
        if (signal?.aborted) {
            throw new VRFSApiError(
                "Request aborted.",
                {
                    code: "ABORTED",
                },
            );
        }

        if (this.active < this.max) {
            this.active++;

            return () => this.release();
        }

        let entry!: {
            resolve: () => void;
            reject: (error: Error) => void;
            signal?: AbortSignal;
            aborted: boolean;
        };

        await new Promise<void>((resolve, reject) => {
            entry = {
                signal,
                aborted: false,

                resolve: () => {
                    signal?.removeEventListener(
                        "abort",
                        onAbort,
                    );

                    resolve();
                },

                reject: (error) => {
                    signal?.removeEventListener(
                        "abort",
                        onAbort,
                    );

                    reject(error);
                },
            };

            const onAbort = () => {
                if (entry.aborted) {
                    return;
                }

                entry.aborted = true;

                const index = this.queue.indexOf(entry);

                if (index !== -1) {
                    this.queue.splice(index, 1);
                }

                entry.reject(
                    new VRFSApiError(
                        "Request aborted.",
                        {
                            code: "ABORTED",
                        },
                    ),
                );
            };

            signal?.addEventListener(
                "abort",
                onAbort,
                { once: true },
            );

            this.queue.push(entry);
        });

        this.active++;

        return () => this.release();
    }

    private release(): void {
        this.active = Math.max(
            0,
            this.active - 1,
        );

        while (this.queue.length > 0) {
            const next = this.queue.shift();

            if (!next || next.aborted) {
                continue;
            }

            next.resolve();
            break;
        }
    }

    public get activeCount(): number {
        return this.active;
    }

    public get queuedCount(): number {
        return this.queue.length;
    }
}

/* ============================================================
   HTTP client
   ============================================================ */

interface HttpResult {
    status: number;
    headers: Headers;
    text: string;
    url: string;
}

async function fetchWithLimits(
    url: string,
    options: {
        method: HttpMethod;
        headers: Record<string, string>;
        body?: string;
        timeoutMs: number;
        maxResponseBytes: number;
        maxRedirects: number;
        signal?: AbortSignal;
    },
): Promise<HttpResult> {
    let currentUrl = url;

    for (
        let redirect = 0;
        redirect <= options.maxRedirects;
        redirect++
    ) {
        if (options.signal?.aborted) {
            throw new VRFSApiError(
                "Request aborted.",
                {
                    code: "ABORTED",
                    endpoint: safeUrl(currentUrl),
                },
            );
        }

        const controller = new AbortController();

        const onAbort = () => {
            controller.abort();
        };

        options.signal?.addEventListener(
            "abort",
            onAbort,
            { once: true },
        );

        const timeout = setTimeout(() => {
            controller.abort();
        }, options.timeoutMs);

        try {
            const response = await fetch(
                currentUrl,
                {
                    method: options.method,
                    headers: options.headers,
                    body: options.body,
                    redirect: "manual",
                    signal: controller.signal,
                },
            );

            clearTimeout(timeout);
            options.signal?.removeEventListener(
                "abort",
                onAbort,
            );

            if (
                [301, 302, 303, 307, 308].includes(
                    response.status,
                )
            ) {
                const location =
                    response.headers.get("location");

                if (!location) {
                    throw new VRFSApiError(
                        "Redirect did not contain a location.",
                        {
                            code: "INVALID_REDIRECT",
                            status: response.status,
                            endpoint: safeUrl(currentUrl),
                        },
                    );
                }

                currentUrl = new URL(
                    location,
                    currentUrl,
                ).toString();

                continue;
            }

            if (!response.body) {
                return {
                    status: response.status,
                    headers: response.headers,
                    text: "",
                    url: currentUrl,
                };
            }

            const reader = response.body.getReader();

            const chunks: Uint8Array[] = [];
            let total = 0;

            try {
                while (true) {
                    const { done, value } =
                        await reader.read();

                    if (done) {
                        break;
                    }

                    if (!value) {
                        continue;
                    }

                    total += value.byteLength;

                    if (
                        total >
                        options.maxResponseBytes
                    ) {
                        await reader.cancel();

                        throw new VRFSResponseTooLargeError(
                            `Response exceeded ${options.maxResponseBytes} bytes.`,
                            safeUrl(currentUrl),
                        );
                    }

                    chunks.push(value);
                }
            } finally {
                reader.releaseLock();
            }

            const buffer = Buffer.concat(
                chunks.map((chunk) =>
                    Buffer.from(chunk),
                ),
            );

            return {
                status: response.status,
                headers: response.headers,
                text: buffer.toString("utf8"),
                url: currentUrl,
            };
        } catch (error) {
            clearTimeout(timeout);

            options.signal?.removeEventListener(
                "abort",
                onAbort,
            );

            if (
                error instanceof VRFSApiError
            ) {
                throw error;
            }

            if (
                options.signal?.aborted
            ) {
                throw new VRFSApiError(
                    "Request aborted.",
                    {
                        code: "ABORTED",
                        endpoint: safeUrl(currentUrl),
                    },
                );
            }

            if (
                error instanceof DOMException &&
                error.name === "AbortError"
            ) {
                throw new VRFSTimeoutError(
                    `Request timed out after ${options.timeoutMs}ms.`,
                    safeUrl(currentUrl),
                );
            }

            throw new VRFSApiError(
                error instanceof Error
                    ? error.message
                    : "Network request failed.",
                {
                    code: "NETWORK_ERROR",
                    retryable: true,
                    endpoint: safeUrl(currentUrl),
                    details: error,
                },
            );
        }
    }

    throw new VRFSApiError(
        "Too many redirects.",
        {
            code: "TOO_MANY_REDIRECTS",
            endpoint: safeUrl(currentUrl),
        },
    );
}

/* ============================================================
   Base API client
   ============================================================ */

class BaseClient {
    protected readonly baseUrl: string;
    protected readonly timeoutMs: number;
    protected readonly maxResponseBytes: number;
    protected readonly maxRedirects: number;
    protected readonly retries: number;
    protected readonly userAgent: string;
    protected readonly logger: VRFSLogger;
    protected readonly semaphore: Semaphore;

    protected rateLimitedUntil = 0;

    public constructor(
        options: VRFSClientOptions,
        baseUrl: string,
        concurrency: number,
    ) {
        this.baseUrl = normalizeBaseUrl(baseUrl);

        this.timeoutMs = Math.max(
            1_000,
            options.requestTimeoutMs ??
                DEFAULTS.timeoutMs,
        );

        this.maxResponseBytes = Math.max(
            1_024,
            options.maxResponseBytes ??
                DEFAULTS.maxResponseBytes,
        );

        this.maxRedirects = clamp(
            Math.floor(
                options.maxRedirects ??
                    DEFAULTS.maxRedirects,
            ),
            0,
            5,
        );

        this.retries = clamp(
            Math.floor(
                options.retries ??
                    DEFAULTS.retries,
            ),
            0,
            3,
        );

        this.userAgent =
            options.userAgent ??
            DEFAULTS.userAgent;

        this.logger =
            options.logger ??
            console;

        this.semaphore =
            new Semaphore(concurrency);
    }

    protected buildUrl(
        pathname: string,
        params: Record<string, unknown> = {},
    ): string {
        const url = new URL(
            pathname,
            `${this.baseUrl}/`,
        );

        for (const [key, value] of Object.entries(
            params,
        )) {
            if (
                value !== undefined &&
                value !== null
            ) {
                url.searchParams.set(
                    key,
                    String(value),
                );
            }
        }

        return url.toString();
    }

    protected async request<T>(
        url: string,
        options: VRFSRequestOptions = {},
    ): Promise<T> {
        const method =
            options.method ?? "GET";

        const retries = clamp(
            Math.floor(
                options.retries ??
                    this.retries,
            ),
            0,
            3,
        );

        let lastError: unknown;

        for (
            let attempt = 1;
            attempt <= retries + 1;
            attempt++
        ) {
            try {
                while (
                    Date.now() <
                    this.rateLimitedUntil
                ) {
                    await sleep(
                        Math.min(
                            this.rateLimitedUntil -
                                Date.now(),
                            1_000,
                        ),
                        options.signal,
                    );
                }

                const release =
                    await this.semaphore.acquire(
                        options.signal,
                    );

                try {
                    let body: string | undefined;

                    if (
                        options.body !== undefined
                    ) {
                        body =
                            typeof options.body ===
                            "string"
                                ? options.body
                                : JSON.stringify(
                                      options.body,
                                  );
                    }

                    const headers: Record<
                        string,
                        string
                    > = {
                        Accept:
                            "application/json, text/plain, */*",
                        "User-Agent":
                            this.userAgent,
                        ...options.headers,
                    };

                    if (body !== undefined) {
                        headers["Content-Type"] ??=
                            "application/json";
                    }

                    const response =
                        await fetchWithLimits(
                            url,
                            {
                                method,
                                headers,
                                body,
                                timeoutMs:
                                    options.timeoutMs ??
                                    this.timeoutMs,
                                maxResponseBytes:
                                    options.maxResponseBytes ??
                                    this.maxResponseBytes,
                                maxRedirects:
                                    options.maxRedirects ??
                                    this.maxRedirects,
                                signal:
                                    options.signal,
                            },
                        );

                    if (
                        response.status === 429
                    ) {
                        const retryAfter =
                            parseRetryAfter(
                                response.headers,
                            ) ?? 1_000;

                        this.rateLimitedUntil =
                            Math.max(
                                this.rateLimitedUntil,
                                Date.now() +
                                    retryAfter,
                            );

                        throw new VRFSRateLimitError(
                            "Upstream rate limited the request.",
                            retryAfter,
                            safeUrl(response.url),
                            attempt,
                        );
                    }

                    const parsed =
                        parseJson<T>(
                            response.text,
                        );

                    if (
                        response.status < 200 ||
                        response.status >= 300
                    ) {
                        const retryable =
                            RETRYABLE_STATUSES.has(
                                response.status,
                            );

                        throw new VRFSApiError(
                            errorMessage(
                                parsed,
                                `Upstream returned HTTP ${response.status}.`,
                            ),
                            {
                                code: `HTTP_${response.status}`,
                                status:
                                    response.status,
                                retryable,
                                endpoint:
                                    safeUrl(
                                        response.url,
                                    ),
                                details:
                                    parsed ??
                                    response.text.slice(
                                        0,
                                        2_000,
                                    ),
                                attempts: attempt,
                            },
                        );
                    }

                    if (
                        parsed !== null
                    ) {
                        return parsed;
                    }

                    if (
                        response.text.trim()
                    ) {
                        return response.text as T;
                    }

                    return null as T;
                } finally {
                    release();
                }
            } catch (error) {
                lastError = error;

                if (
                    error instanceof
                        VRFSApiError &&
                    error.code === "ABORTED"
                ) {
                    throw error;
                }

                const retryable =
                    error instanceof
                    VRFSApiError
                        ? error.retryable
                        : true;

                if (
                    !retryable ||
                    attempt > retries
                ) {
                    break;
                }

                const retryAfter =
                    error instanceof
                    VRFSApiError
                        ? error.retryAfterMs
                        : undefined;

                const delay = clamp(
                    retryAfter ??
                        DEFAULTS.retryDelayMs *
                            Math.pow(
                                2,
                                attempt - 1,
                            ),
                    250,
                    5_000,
                );

                this.logger.warn?.(
                    "[VRFS] Retrying request.",
                    {
                        endpoint:
                            safeUrl(url),
                        attempt,
                        maxAttempts:
                            retries + 1,
                        delayMs: delay,
                        error:
                            error instanceof
                            Error
                                ? error.message
                                : String(error),
                    },
                );

                await sleep(
                    delay,
                    options.signal,
                );
            }
        }

        if (
            lastError instanceof
            VRFSApiError
        ) {
            throw lastError;
        }

        throw new VRFSApiError(
            "Request failed.",
            {
                code: "NETWORK_ERROR",
                retryable: false,
                endpoint: safeUrl(url),
                details: lastError,
                attempts:
                    retries + 1,
            },
        );
    }
}

/* ============================================================
   VRFS Client
   ============================================================ */

export class VRFSClient extends BaseClient {
    private readonly apiKey?: string;

    private readonly players: TTLCache<VRFSUser>;
    private readonly profiles: TTLCache<VRFSProfile>;
    private readonly outfits: TTLCache<VRFSOutfit[]>;
    private readonly catalog: TTLCache<VRFSItem[]>;
    private readonly marketplace: TTLCache<
        VRFSMarketplaceItem[]
    >;

    private readonly flights =
        new SingleFlight();

    public constructor(
        options: VRFSClientOptions = {},
    ) {
        super(
            options,
            options.baseUrl ??
                DEFAULTS.vrfsBaseUrl,
            DEFAULTS.vrfsConcurrency,
        );

        this.apiKey =
            options.apiKey ??
            process.env.VRFS_API_KEY;

        this.players = new TTLCache(
            options.playerTtlMs ??
                DEFAULTS.playerTtlMs,
            DEFAULTS.playerCacheSize,
        );

        this.profiles = new TTLCache(
            options.playerTtlMs ??
                DEFAULTS.playerTtlMs,
            DEFAULTS.profileCacheSize,
        );

        this.outfits = new TTLCache(
            options.playerTtlMs ??
                DEFAULTS.playerTtlMs,
            DEFAULTS.outfitCacheSize,
        );

        this.catalog = new TTLCache(
            options.catalogTtlMs ??
                DEFAULTS.catalogTtlMs,
            2,
        );

        this.marketplace = new TTLCache(
            options.marketplaceTtlMs ??
                DEFAULTS.marketplaceTtlMs,
            2,
        );
    }

    public get configuration() {
        return {
            baseUrl: this.baseUrl,
            hasApiKey: Boolean(
                this.apiKey,
            ),
            timeoutMs: this.timeoutMs,
            retries: this.retries,
            maxResponseBytes:
                this.maxResponseBytes,
            maxConcurrentRequests:
                DEFAULTS.vrfsConcurrency,
        };
    }

    private requireApiKey(): string {
        if (!this.apiKey) {
            throw new VRFSApiError(
                "VRFS_API_KEY is not configured.",
                {
                    code: "MISSING_API_KEY",
                },
            );
        }

        return this.apiKey;
    }

    private requireUid(
        uid: number,
    ): number {
        if (
            !Number.isInteger(uid) ||
            uid <= 0
        ) {
            throw new VRFSApiError(
                "UID must be a positive integer.",
                {
                    code: "INVALID_UID",
                },
            );
        }

        return uid;
    }

    public async getUsername(
        uid: number,
        signal?: AbortSignal,
        forceRefresh = false,
    ): Promise<VRFSUser> {
        const validUid =
            this.requireUid(uid);

        const key = `user:${validUid}`;

        if (!forceRefresh) {
            const cached =
                this.players.get(key);

            if (cached) {
                return cached;
            }
        }

        return this.flights.run(
            `username:${validUid}`,
            async () => {
                const data =
                    await this.request<unknown>(
                        this.buildUrl(
                            VRFS_ENDPOINTS.username,
                            {
                                uid: validUid,
                            },
                        ),
                        { signal },
                    );

                const user =
                    normalizeUser(
                        data,
                        validUid,
                    );

                this.players.set(
                    key,
                    user,
                );

                return user;
            },
        );
    }

    public async getProfile(
        uid: number,
        signal?: AbortSignal,
        forceRefresh = false,
    ): Promise<VRFSProfile> {
        const validUid =
            this.requireUid(uid);

        const key =
            `profile:${validUid}`;

        if (!forceRefresh) {
            const cached =
                this.profiles.get(key);

            if (cached) {
                return cached;
            }
        }

        const apiKey =
            this.requireApiKey();

        return this.flights.run(
            `profile:${validUid}`,
            async () => {
                const data =
                    await this.request<unknown>(
                        this.buildUrl(
                            VRFS_ENDPOINTS.profile,
                            {
                                uid: validUid,
                                key: apiKey,
                            },
                        ),
                        { signal },
                    );

                const profile =
                    normalizeProfile(
                        data,
                        validUid,
                    );

                this.profiles.set(
                    key,
                    profile,
                );

                return profile;
            },
        );
    }

    public async getOutfits(
        uid: number,
        signal?: AbortSignal,
        forceRefresh = false,
    ): Promise<VRFSOutfit[]> {
        const validUid =
            this.requireUid(uid);

        const key =
            `outfits:${validUid}`;

        if (!forceRefresh) {
            const cached =
                this.outfits.get(key);

            if (cached) {
                return cached;
            }
        }

        const apiKey =
            this.requireApiKey();

        return this.flights.run(
            `outfits:${validUid}`,
            async () => {
                const data =
                    await this.request<unknown>(
                        this.buildUrl(
                            VRFS_ENDPOINTS.outfits,
                            {
                                uid: validUid,
                                key: apiKey,
                            },
                        ),
                        { signal },
                    );

                const outfits =
                    normalizeOutfits(data);

                this.outfits.set(
                    key,
                    outfits,
                );

                return outfits;
            },
        );
    }

    public async getPlayer(
        uid: number,
        signal?: AbortSignal,
        forceRefresh = false,
    ): Promise<
        VRFSUser & {
            profile?: VRFSProfile;
            outfits?: VRFSOutfit[];
        }
    > {
        const validUid =
            this.requireUid(uid);

        const [
            username,
            profile,
            outfits,
        ] = await Promise.allSettled([
            this.getUsername(
                validUid,
                signal,
                forceRefresh,
            ),
            this.getProfile(
                validUid,
                signal,
                forceRefresh,
            ),
            this.getOutfits(
                validUid,
                signal,
                forceRefresh,
            ),
        ]);

        if (
            username.status ===
            "rejected"
        ) {
            throw username.reason;
        }

        return {
            ...username.value,

            ...(profile.status ===
            "fulfilled"
                ? {
                      profile:
                          profile.value,
                  }
                : {}),

            ...(outfits.status ===
            "fulfilled"
                ? {
                      outfits:
                          outfits.value,
                  }
                : {}),
        };
    }

    public async getMarketplace(
        signal?: AbortSignal,
        forceRefresh = false,
    ): Promise<
        VRFSMarketplaceItem[]
    > {
        if (!forceRefresh) {
            const cached =
                this.marketplace.get(
                    "all",
                );

            if (cached) {
                return cached;
            }
        }

        const apiKey =
            this.requireApiKey();

        return this.flights.run(
            "marketplace",
            async () => {
                const data =
                    await this.request<unknown>(
                        this.buildUrl(
                            VRFS_ENDPOINTS.marketplace,
                            {
                                key: apiKey,
                            },
                        ),
                        { signal },
                    );

                const items =
                    normalizeMarketplace(
                        data,
                    );

                this.marketplace.set(
                    "all",
                    items,
                );

                return items;
            },
        );
    }

    public async searchMarketplace(
        query: string,
        limit = 100,
        signal?: AbortSignal,
    ): Promise<
        VRFSMarketplaceItem[]
    > {
        const normalized =
            normalizeSearch(query);

        if (!normalized) {
            return [];
        }

        const items =
            await this.getMarketplace(
                signal,
            );

        return items
            .map((item) => ({
                item,
                score:
                    marketplaceScore(
                        item,
                        normalized,
                    ),
            }))
            .filter(
                (entry) =>
                    entry.score > 0,
            )
            .sort(
                (a, b) =>
                    b.score - a.score,
            )
            .slice(
                0,
                Math.max(1, limit),
            )
            .map(
                (entry) => entry.item,
            );
    }

    public async getMarketplaceItem(
        query: string | number,
        signal?: AbortSignal,
    ): Promise<
        VRFSMarketplaceItem | null
    > {
        const normalized =
            normalizeSearch(query);

        if (!normalized) {
            return null;
        }

        const items =
            await this.getMarketplace(
                signal,
            );

        const direct = items.find(
            (item) =>
                String(item.id)
                    .toLowerCase() ===
                    normalized ||
                String(item.sku ?? "")
                    .toLowerCase() ===
                    normalized,
        );

        if (direct) {
            return direct;
        }

        const results =
            await this.searchMarketplace(
                normalized,
                10,
                signal,
            );

        return results.length === 1
            ? results[0]
            : null;
    }

    public async getCatalog(
        signal?: AbortSignal,
        forceRefresh = false,
    ): Promise<VRFSItem[]> {
        if (!forceRefresh) {
            const cached =
                this.catalog.get("all");

            if (cached) {
                return cached;
            }
        }

        return this.flights.run(
            "catalog",
            async () => {
                const url = new URL(
                    SEBY_ENDPOINTS.items,
                    `${DEFAULTS.sebyBaseUrl}/`,
                ).toString();

                const data =
                    await this.request<unknown>(
                        url,
                        { signal },
                    );

                const items =
                    normalizeItems(data);

                this.catalog.set(
                    "all",
                    items,
                );

                return items;
            },
        );
    }

    public async searchCatalog(
        query: string,
        limit = 100,
        signal?: AbortSignal,
    ): Promise<VRFSItem[]> {
        const normalized =
            normalizeSearch(query);

        if (!normalized) {
            return [];
        }

        const items =
            await this.getCatalog(
                signal,
            );

        return items
            .map((item) => ({
                item,
                score:
                    catalogScore(
                        item,
                        normalized,
                    ),
            }))
            .filter(
                (entry) =>
                    entry.score > 0,
            )
            .sort(
                (a, b) =>
                    b.score - a.score,
            )
            .slice(
                0,
                Math.max(1, limit),
            )
            .map(
                (entry) => entry.item,
            );
    }

    public async getCatalogItem(
        query: string | number,
        signal?: AbortSignal,
    ): Promise<VRFSItem | null> {
        const normalized =
            normalizeSearch(query);

        if (!normalized) {
            return null;
        }

        const items =
            await this.getCatalog(
                signal,
            );

        const direct = items.find(
            (item) =>
                String(
                    item.id ??
                        item.item_id ??
                        item.itemId ??
                        "",
                ).toLowerCase() ===
                    normalized ||
                getSku(item).toLowerCase() ===
                    normalized,
        );

        if (direct) {
            return direct;
        }

        const exactName = items.find(
            (item) =>
                getItemName(item)
                    .toLowerCase() ===
                normalized,
        );

        if (exactName) {
            return exactName;
        }

        const results =
            await this.searchCatalog(
                normalized,
                10,
                signal,
            );

        return results.length === 1
            ? results[0]
            : null;
    }

    public getCatalogStats(
        items: VRFSItem[] =
            this.catalog.get("all") ??
            [],
    ): CatalogStats {
        const sections =
            new Set(
                items
                    .map(getSection)
                    .filter(Boolean),
            );

        const free =
            items.filter(
                isItemFree,
            ).length;

        return {
            items: items.length,
            sections: sections.size,
            free,
            premium: Math.max(
                0,
                items.length - free,
            ),
            loadedAt: Date.now(),
            ageMs: 0,
        };
    }

    public getMarketplaceStats(
        items: VRFSMarketplaceItem[] =
            this.marketplace.get(
                "all",
            ) ?? [],
    ): MarketplaceStats {
        const active =
            items.filter(
                getMarketplaceActive,
            ).length;

        const creators =
            new Set(
                items
                    .map(
                        getMarketplaceCreatorUid,
                    )
                    .filter(Boolean),
            ).size;

        const owners =
            items.reduce(
                (sum, item) =>
                    sum +
                    getMarketplaceOwners(
                        item,
                    ),
                0,
            );

        return {
            items: items.length,
            active,
            inactive:
                items.length - active,
            creators,
            owners,
            loadedAt: Date.now(),
            ageMs: 0,
        };
    }

    public clearCaches(): void {
        this.catalog.clear();
        this.marketplace.clear();
        this.players.clear();
        this.profiles.clear();
        this.outfits.clear();
        this.flights.clear();

        this.rateLimitedUntil = 0;
    }

    public clearPlayerCache(
        uid?: number,
    ): void {
        if (uid === undefined) {
            this.players.clear();
            this.profiles.clear();
            this.outfits.clear();
            return;
        }

        const validUid =
            this.requireUid(uid);

        this.players.delete(
            `user:${validUid}`,
        );

        this.profiles.delete(
            `profile:${validUid}`,
        );

        this.outfits.delete(
            `outfits:${validUid}`,
        );
    }

    public getCacheStats(): Record<
        string,
        number
    > {
        return {
            players:
                this.players.size,
            profiles:
                this.profiles.size,
            outfits:
                this.outfits.size,
            catalog:
                this.catalog.size,
            marketplace:
                this.marketplace.size,
            singleFlight:
                this.flights.size,
        };
    }

    public async health(
        uid = 1,
        signal?: AbortSignal,
    ): Promise<VRFSServiceHealth> {
        const started =
            Date.now();

        try {
            await this.getUsername(
                this.requireUid(uid),
                signal,
                true,
            );

            return {
                service: "vrfs",
                ok: true,
                health: "ok",
                latencyMs:
                    Date.now() - started,
            };
        } catch (error) {
            return {
                service: "vrfs",
                ok: false,
                health: "unknown",
                latencyMs:
                    Date.now() - started,
                error:
                    error instanceof Error
                        ? error.message
                        : String(error),
            };
        }
    }
}

/* ============================================================
   Seby Client
   ============================================================ */

export class SebyLockerClient extends BaseClient {
    private readonly flights =
        new SingleFlight();

    private readonly batchSize: number;
    private readonly minBatchSize: number;
    private readonly maxBatchSize: number;
    private readonly retryCount: number;
    private readonly delayMs: number;

    public constructor(
        options: VRFSClientOptions = {},
    ) {
        super(
            options,
            options.baseUrl ??
                DEFAULTS.sebyBaseUrl,
            DEFAULTS.sebyConcurrency,
        );

        this.batchSize = clamp(
            Math.floor(
                options.ownershipBatchSize ??
                    DEFAULTS.ownershipBatchSize,
            ),
            1,
            500,
        );

        this.minBatchSize = clamp(
            Math.floor(
                options.ownershipMinBatchSize ??
                    DEFAULTS.ownershipMinBatchSize,
            ),
            1,
            this.batchSize,
        );

        this.maxBatchSize = clamp(
            Math.floor(
                options.ownershipMaxBatchSize ??
                    DEFAULTS.ownershipMaxBatchSize,
            ),
            this.minBatchSize,
            500,
        );

        this.retryCount = clamp(
            Math.floor(
                options.ownershipRetryCount ??
                    DEFAULTS.ownershipRetryCount,
            ),
            0,
            3,
        );

        this.delayMs = Math.max(
            0,
            Math.floor(
                options.ownershipDelayMs ??
                    DEFAULTS.ownershipDelayMs,
            ),
        );
    }

    private buildSebyUrl(
        pathname: string,
    ): string {
        return new URL(
            pathname,
            `${this.baseUrl}/`,
        ).toString();
    }

    private async post<T>(
        pathname: string,
        body: unknown,
        signal?: AbortSignal,
    ): Promise<T> {
        return this.request<T>(
            this.buildSebyUrl(pathname),
            {
                method: "POST",
                body,
                signal,
            },
        );
    }

    public async getItems(
        signal?: AbortSignal,
    ): Promise<VRFSItem[]> {
        return this.flights.run(
            "seby:items",
            async () => {
                const data =
                    await this.request<unknown>(
                        this.buildSebyUrl(
                            SEBY_ENDPOINTS.items,
                        ),
                        { signal },
                    );

                return normalizeItems(
                    data,
                );
            },
        );
    }

    public async lookup(
        uid: number,
        skus: string[],
        signal?: AbortSignal,
    ): Promise<SebyLookupResponse> {
        const validUid =
            requireUid(uid);

        const normalized =
            uniqueStrings(skus);

        if (!normalized.length) {
            throw new VRFSApiError(
                "At least one SKU is required.",
                {
                    code: "EMPTY_SKU_LIST",
                },
            );
        }

        const response =
            await this.post<unknown>(
                SEBY_ENDPOINTS.lookup,
                {
                    uid: String(validUid),
                    skus: normalized,
                },
                signal,
            );

        return normalizeSebyLookupResponse(
            response,
        );
    }

    public async checkOwnershipRequest(
        uid: number,
        skus: string[],
        signal?: AbortSignal,
    ): Promise<SebyOwnershipResponse> {
        const validUid =
            requireUid(uid);

        const normalized =
            uniqueStrings(skus);

        if (!normalized.length) {
            throw new VRFSApiError(
                "At least one SKU is required.",
                {
                    code: "EMPTY_SKU_LIST",
                },
            );
        }

        const response =
            await this.post<unknown>(
                SEBY_ENDPOINTS.check,
                {
                    uid: String(validUid),
                    skus: normalized,
                },
                signal,
            );

        return normalizeSebyResponse(
            response,
        );
    }

    public async checkOwnership(
        uid: number,
        skus: string[],
        options: OwnershipCheckOptions = {},
    ): Promise<OwnershipCheckResult> {
        const validUid =
            requireUid(uid);

        const normalized =
            uniqueStrings(skus);

        if (
            normalized.length >
            DEFAULTS.maxOwnershipSkus
        ) {
            throw new VRFSApiError(
                `Too many SKUs. Maximum is ${DEFAULTS.maxOwnershipSkus}.`,
                {
                    code: "TOO_MANY_SKUS",
                },
            );
        }

        const started =
            Date.now();

        const results: Record<
            string,
            OwnershipValue
        > = {};

        if (!normalized.length) {
            return {
                uid: validUid,
                results,
                unknown: [],
                owned: [],
                notOwned: [],
                requested: 0,
                confirmed: 0,
                cached: 0,
                apiRequests: 0,
                retries: 0,
                batches: 0,
                durationMs: 0,
                health: "ok",
                partial: false,
            };
        }

        let batchSize = clamp(
            Math.floor(
                options.batchSize ??
                    this.batchSize,
            ),
            Math.max(
                1,
                Math.floor(
                    options.minBatchSize ??
                        this.minBatchSize,
                ),
            ),
            Math.min(
                500,
                Math.floor(
                    options.maxBatchSize ??
                        this.maxBatchSize,
                ),
            ),
        );

        const minBatchSize =
            Math.max(
                1,
                Math.floor(
                    options.minBatchSize ??
                        this.minBatchSize,
                ),
            );

        const delayMs =
            Math.max(
                0,
                Math.floor(
                    options.delayMs ??
                        this.delayMs,
                ),
            );

        const missingRetries =
            clamp(
                Math.floor(
                    options.maxMissingRetries ??
                        this.retryCount,
                ),
                0,
                3,
            );

        let remaining =
            normalized.slice();

        let apiRequests = 0;
        let retries = 0;
        let batches = 0;
        let cached = 0;
        let batchNumber = 0;

        let health: ServiceHealth =
            "ok";

        /*
         * Important:
         * This loop has a hard upper bound through the
         * shrinking remaining array. It can never retry
         * forever on 429s.
         */
        let consecutiveRateLimits = 0;

        while (remaining.length) {
            if (
                options.signal?.aborted
            ) {
                throw new VRFSApiError(
                    "Ownership request aborted.",
                    {
                        code: "ABORTED",
                    },
                );
            }

            const batch =
                remaining.slice(
                    0,
                    batchSize,
                );

            batchNumber++;

            try {
                const response =
                    await this.checkOwnershipRequest(
                        validUid,
                        batch,
                        options.signal,
                    );

                apiRequests++;
                batches++;
                consecutiveRateLimits = 0;

                cached += Number.isFinite(
                    response.cached,
                )
                    ? response.cached
                    : 0;

                if (
                    response.health ===
                    "degraded"
                ) {
                    health = "degraded";
                }

                for (const sku of batch) {
                    const value =
                        response.results[
                            sku
                        ];

                    if (
                        typeof value ===
                        "boolean"
                    ) {
                        results[sku] =
                            value;
                    }
                }

                remaining =
                    remaining.slice(
                        batch.length,
                    );

                let missing =
                    batch.filter(
                        (sku) =>
                            results[sku] ===
                            undefined,
                    );

                let retryBatchSize =
                    Math.max(
                        minBatchSize,
                        Math.ceil(
                            batchSize / 2,
                        ),
                    );

                for (
                    let attempt = 1;
                    attempt <=
                        missingRetries &&
                    missing.length;
                    attempt++
                ) {
                    retries++;

                    if (delayMs > 0) {
                        await sleep(
                            Math.min(
                                5_000,
                                delayMs *
                                    (attempt + 1),
                            ),
                            options.signal,
                        );
                    }

                    const unresolved: string[] =
                        [];

                    for (const retryBatch of chunk(
                        missing,
                        retryBatchSize,
                    )) {
                        try {
                            const retryResponse =
                                await this.checkOwnershipRequest(
                                    validUid,
                                    retryBatch,
                                    options.signal,
                                );

                            apiRequests++;
                            batches++;

                            cached +=
                                Number.isFinite(
                                    retryResponse.cached,
                                )
                                    ? retryResponse.cached
                                    : 0;

                            if (
                                retryResponse.health ===
                                "degraded"
                            ) {
                                health =
                                    "degraded";
                            }

                            for (const sku of retryBatch) {
                                const value =
                                    retryResponse
                                        .results[
                                        sku
                                    ];

                                if (
                                    typeof value ===
                                    "boolean"
                                ) {
                                    results[sku] =
                                        value;
                                }
                            }

                            unresolved.push(
                                ...retryBatch.filter(
                                    (sku) =>
                                        results[
                                            sku
                                        ] ===
                                        undefined,
                                ),
                            );
                        } catch (error) {
                            if (
                                error instanceof
                                    VRFSRateLimitError ||
                                (error instanceof
                                    VRFSApiError &&
                                    error.status ===
                                        429)
                            ) {
                                health =
                                    "degraded";

                                await sleep(
                                    Math.min(
                                        5_000,
                                        error instanceof
                                            VRFSApiError
                                            ? error.retryAfterMs ??
                                              1_000
                                            : 1_000,
                                    ),
                                    options.signal,
                                );

                                unresolved.push(
                                    ...retryBatch,
                                );

                                continue;
                            }

                            unresolved.push(
                                ...retryBatch,
                            );
                        }
                    }

                    missing =
                        uniqueStrings(
                            unresolved,
                        );

                    retryBatchSize =
                        Math.max(
                            minBatchSize,
                            Math.floor(
                                retryBatchSize /
                                    2,
                            ),
                        );
                }

                for (const sku of missing) {
                    if (
                        results[sku] ===
                        undefined
                    ) {
                        results[sku] =
                            "unknown";
                    }
                }

                options.onProgress?.({
                    processed:
                        normalized.length -
                        remaining.length,
                    total:
                        normalized.length,
                    confirmed:
                        Object.values(
                            results,
                        ).filter(
                            (value) =>
                                typeof value ===
                                "boolean",
                        ).length,
                    unknown:
                        Object.values(
                            results,
                        ).filter(
                            (value) =>
                                value ===
                                "unknown",
                        ).length,
                    owned:
                        Object.values(
                            results,
                        ).filter(
                            (value) =>
                                value === true,
                        ).length,
                    notOwned:
                        Object.values(
                            results,
                        ).filter(
                            (value) =>
                                value === false,
                        ).length,
                    batchNumber,
                    batchSize,
                    health,
                    cached,
                });

                if (
                    remaining.length &&
                    delayMs > 0
                ) {
                    await sleep(
                        Math.min(
                            5_000,
                            delayMs,
                        ),
                        options.signal,
                    );
                }
            } catch (error) {
                if (
                    options.signal?.aborted
                ) {
                    throw error;
                }

                if (
                    error instanceof
                        VRFSApiError &&
                    [400, 413, 414].includes(
                        error.status,
                    ) &&
                    batchSize >
                        minBatchSize
                ) {
                    batchSize =
                        Math.max(
                            minBatchSize,
                            Math.floor(
                                batchSize / 2,
                            ),
                        );

                    this.logger.warn?.(
                        "[SEBY] Reducing ownership batch size.",
                        {
                            previousSize:
                                batch.length,
                            newSize:
                                batchSize,
                            status:
                                error.status,
                        },
                    );

                    continue;
                }

                if (
                    error instanceof
                        VRFSRateLimitError ||
                    (error instanceof
                        VRFSApiError &&
                        error.status ===
                            429)
                ) {
                    consecutiveRateLimits++;

                    health =
                        "degraded";

                    /*
                     * HARD STOP.
                     * Never sit in an infinite 429 loop.
                     */
                    if (
                        consecutiveRateLimits >=
                        3
                    ) {
                        for (const sku of batch) {
                            if (
                                results[sku] ===
                                undefined
                            ) {
                                results[sku] =
                                    "unknown";
                            }
                        }

                        remaining =
                            remaining.slice(
                                batch.length,
                            );

                        consecutiveRateLimits = 0;

                        continue;
                    }

                    await sleep(
                        Math.min(
                            5_000,
                            error instanceof
                                VRFSApiError
                                ? error.retryAfterMs ??
                                  1_000
                                : 1_000,
                        ),
                        options.signal,
                    );

                    continue;
                }

                health =
                    "degraded";

                for (const sku of batch) {
                    if (
                        results[sku] ===
                        undefined
                    ) {
                        results[sku] =
                            "unknown";
                    }
                }

                remaining =
                    remaining.slice(
                        batch.length,
                    );
            }
        }

        for (const sku of normalized) {
            if (
                results[sku] ===
                undefined
            ) {
                results[sku] =
                    "unknown";
            }
        }

        const owned =
            normalized.filter(
                (sku) =>
                    results[sku] === true,
            );

        const notOwned =
            normalized.filter(
                (sku) =>
                    results[sku] === false,
            );

        const unknown =
            normalized.filter(
                (sku) =>
                    results[sku] ===
                    "unknown",
            );

        return {
            uid: validUid,
            results,
            unknown,
            owned,
            notOwned,
            requested:
                normalized.length,
            confirmed:
                owned.length +
                notOwned.length,
            cached,
            apiRequests,
            retries,
            batches,
            durationMs:
                Date.now() - started,
            health,
            partial:
                unknown.length > 0,
        };
    }

    public async checkOwnershipBatched(
        uid: number,
        skus: string[],
        options: OwnershipCheckOptions = {},
    ): Promise<OwnershipCheckResult> {
        return this.checkOwnership(
            uid,
            skus,
            options,
        );
    }

    public async health(
        signal?: AbortSignal,
    ): Promise<VRFSServiceHealth> {
        const started =
            Date.now();

        try {
            await this.getItems(
                signal,
            );

            return {
                service: "seby",
                ok: true,
                health: "ok",
                latencyMs:
                    Date.now() - started,
            };
        } catch (error) {
            return {
                service: "seby",
                ok: false,
                health: "unknown",
                latencyMs:
                    Date.now() - started,
                error:
                    error instanceof Error
                        ? error.message
                        : String(error),
            };
        }
    }

    public clearFlights(): void {
        this.flights.clear();
    }

    public getStatus(): Record<
        string,
        unknown
    > {
        return {
            baseUrl: this.baseUrl,
            rateLimitedForMs:
                Math.max(
                    0,
                    this.rateLimitedUntil -
                        Date.now(),
                ),
            inflightRequests:
                this.flights.size,
            activeRequests:
                this.semaphore
                    .activeCount,
            queuedRequests:
                this.semaphore
                    .queuedCount,
        };
    }
}

/* ============================================================
   Normalizers
   ============================================================ */

function unwrapData(
    input: unknown,
): unknown {
    if (!isRecord(input)) {
        return input;
    }

    if (
        "data" in input &&
        input.data !== undefined
    ) {
        if (
            isRecord(input.data) &&
            "data" in input.data &&
            input.data.data !== undefined
        ) {
            return input.data.data;
        }

        return input.data;
    }

    return input;
}

function normalizeUser(
    input: unknown,
    fallbackUid: number,
): VRFSUser {
    if (typeof input === "string") {
        const username =
            input.trim();

        if (!username) {
            throw new VRFSInvalidResponseError(
                "Username endpoint returned an empty username.",
            );
        }

        return {
            uid: fallbackUid,
            username,
        };
    }

    const root =
        unwrapData(input);

    if (!isRecord(root)) {
        throw new VRFSInvalidResponseError(
            "Username response was invalid.",
            undefined,
            root,
        );
    }

    const uid =
        asPositiveInt(
            root.uid ??
                root.userId ??
                root.id,
        ) ?? fallbackUid;

    const username =
        asString(
            root.username ??
                root.nickname ??
                root.name,
        ).trim();

    if (!username) {
        throw new VRFSInvalidResponseError(
            "Username response did not contain a username.",
            undefined,
            root,
        );
    }

    return {
        uid,
        username,
        nickname:
            typeof root.nickname ===
            "string"
                ? root.nickname
                : undefined,
        name:
            typeof root.name ===
            "string"
                ? root.name
                : undefined,
    };
}

function normalizeProfile(
    input: unknown,
    fallbackUid: number,
): VRFSProfile {
    const root =
        unwrapData(input);

    if (!isRecord(root)) {
        throw new VRFSInvalidResponseError(
            "Profile response was invalid.",
            undefined,
            root,
        );
    }

    const profile: VRFSProfile = {
        uid:
            asPositiveInt(
                root.uid ??
                    root.userId ??
                    root.id,
            ) ?? fallbackUid,

        username:
            typeof root.username ===
            "string"
                ? root.username
                : undefined,
    };

    if (
        typeof root.requestResult ===
        "string"
    ) {
        profile.requestResult =
            root.requestResult;
    }

    for (const key of [
        "followersCount",
        "followingCount",
    ]) {
        if (root[key] !== undefined) {
            const value = Number(
                root[key],
            );

            if (Number.isFinite(value)) {
                profile[key] =
                    value;
            }
        }
    }

    for (const key of [
        "profileCountry",
        "userTag",
        "tiktokName",
        "youtubeName",
        "twitchName",
        "instagramName",
    ]) {
        if (
            typeof root[key] ===
            "string"
        ) {
            profile[key] =
                root[key];
        }
    }

    return profile;
}

function normalizeOutfits(
    input: unknown,
): VRFSOutfit[] {
    const root =
        unwrapData(input);

    const array = Array.isArray(root)
        ? root
        : isRecord(root) &&
            Array.isArray(
                root.outfits,
            )
          ? root.outfits
          : [];

    return array
        .filter(isRecord)
        .map((item) => {
            const slots =
                isRecord(item.slots)
                    ? Object.fromEntries(
                          Object.entries(
                              item.slots,
                          ).map(
                              ([
                                  key,
                                  value,
                              ]) => [
                                  key,
                                  value ===
                                  null
                                      ? null
                                      : String(
                                            value,
                                        ),
                              ],
                          ),
                      )
                    : undefined;

            return {
                id:
                    typeof item.id ===
                        "number" ||
                    typeof item.id ===
                        "string"
                        ? item.id
                        : undefined,

                name:
                    typeof item.name ===
                    "string"
                        ? item.name
                        : undefined,

                createdAt:
                    typeof item.createdAt ===
                    "string"
                        ? item.createdAt
                        : undefined,

                updatedAt:
                    typeof item.updatedAt ===
                    "string"
                        ? item.updatedAt
                        : undefined,

                slots,
            };
        });
}

function normalizeItems(
    input: unknown,
): VRFSItem[] {
    const root =
        unwrapData(input);

    let array: unknown[];

    if (Array.isArray(root)) {
        array = root;
    } else if (
        isRecord(root) &&
        Array.isArray(root.items)
    ) {
        array = root.items;
    } else {
        throw new VRFSInvalidResponseError(
            "Catalog response did not contain an item array.",
            undefined,
            input,
        );
    }

    return array
        .filter(isRecord)
        .map((item) => ({
            ...item,

            id:
                item.id as
                    | string
                    | number
                    | undefined,

            item_id:
                item.item_id as
                    | string
                    | number
                    | undefined,

            itemId:
                item.itemId as
                    | string
                    | number
                    | undefined,

            sku:
                typeof item.sku ===
                "string"
                    ? item.sku
                    : undefined,

            sku_base:
                typeof item.sku_base ===
                "string"
                    ? item.sku_base
                    : undefined,

            name:
                typeof item.name ===
                "string"
                    ? item.name
                    : undefined,

            title:
                typeof item.title ===
                "string"
                    ? item.title
                    : undefined,

            section:
                typeof item.section ===
                "string"
                    ? item.section
                    : undefined,

            category:
                typeof item.category ===
                "string"
                    ? item.category
                    : undefined,

            category_name:
                typeof item.category_name ===
                "string"
                    ? item.category_name
                    : undefined,
        }))
        .filter(
            (item) =>
                getSku(item).length > 0,
        );
}

function normalizeMarketplace(
    input: unknown,
): VRFSMarketplaceItem[] {
    const root =
        unwrapData(input);

    let array: unknown[];

    if (Array.isArray(root)) {
        array = root;
    } else if (
        isRecord(root) &&
        Array.isArray(root.items)
    ) {
        array = root.items;
    } else {
        throw new VRFSInvalidResponseError(
            "Marketplace response did not contain an item array.",
            undefined,
            input,
        );
    }

    return array
        .filter(isRecord)
        .filter(
            (item) =>
                item.id !==
                    undefined &&
                item.id !== null,
        )
        .map((item) => ({
            ...item,

            id: item.id as
                | string
                | number,

            title:
                typeof item.title ===
                "string"
                    ? item.title
                    : undefined,

            name:
                typeof item.name ===
                "string"
                    ? item.name
                    : undefined,

            sku:
                typeof item.sku ===
                "string"
                    ? item.sku
                    : undefined,

            sku_base:
                typeof item.sku_base ===
                "string"
                    ? item.sku_base
                    : undefined,

            author:
                isRecord(item.author)
                    ? {
                          ...item.author,
                          uid:
                              item.author
                                  .uid as
                                  | string
                                  | number
                                  | undefined,
                      }
                    : undefined,
        }));
}

function normalizeSebyResponse(
    input: unknown,
): SebyOwnershipResponse {
    if (!isRecord(input)) {
        throw new VRFSInvalidResponseError(
            "Ownership response was invalid.",
            undefined,
            input,
        );
    }

    const ok =
        asBoolean(input.ok);

    if (ok === null) {
        throw new VRFSInvalidResponseError(
            "Ownership response did not contain a valid status.",
            undefined,
            input,
        );
    }

    const healthValue =
        typeof input.health ===
        "string"
            ? input.health.toLowerCase()
            : "unknown";

    const health: ServiceHealth =
        healthValue === "ok"
            ? "ok"
            : healthValue ===
                "degraded"
              ? "degraded"
              : "unknown";

    const results: Record<
        string,
        boolean
    > = {};

    if (
        isRecord(input.results)
    ) {
        for (const [
            sku,
            value,
        ] of Object.entries(
            input.results,
        )) {
            const normalized =
                asBoolean(value);

            if (
                normalized !== null
            ) {
                results[sku] =
                    normalized;
            }
        }
    }

    const cached = Number(
        input.cached,
    );

    const retryAfter = Number(
        input.retry_after,
    );

    return {
        ...input,
        ok,
        health,
        results,
        cached:
            Number.isFinite(cached) &&
            cached >= 0
                ? cached
                : 0,
        ...(Number.isFinite(
            retryAfter,
        ) &&
        retryAfter >= 0
            ? {
                  retry_after:
                      retryAfter,
              }
            : {}),
    };
}

function normalizeSebyLookupResponse(
    input: unknown,
): SebyLookupResponse {
    if (!isRecord(input)) {
        throw new VRFSInvalidResponseError(
            "Lookup response was invalid.",
            undefined,
            input,
        );
    }

    const ok =
        asBoolean(input.ok);

    if (ok === null) {
        throw new VRFSInvalidResponseError(
            "Lookup response did not contain a valid status.",
            undefined,
            input,
        );
    }

    const healthValue =
        typeof input.health ===
        "string"
            ? input.health.toLowerCase()
            : "unknown";

    const health: ServiceHealth =
        healthValue === "ok"
            ? "ok"
            : healthValue ===
                "degraded"
              ? "degraded"
              : "unknown";

    return {
        ...input,
        ok,
        health,
        results: isRecord(
            input.results,
        )
            ? input.results
            : {},
    };
}

/* ============================================================
   Search / item helpers
   ============================================================ */

function normalizeSearch(
    value: string | number,
): string {
    return String(value ?? "")
        .trim()
        .toLowerCase();
}

export function getSku(
    item:
        | VRFSItem
        | VRFSMarketplaceItem
        | Record<string, unknown>,
): string {
    const value =
        item.sku ??
        item.sku_base;

    return typeof value ===
        "string"
        ? value.trim()
        : "";
}

export function getItemName(
    item:
        | VRFSItem
        | VRFSMarketplaceItem
        | Record<string, unknown>,
): string {
    const value =
        item.name ??
        item.title;

    if (
        typeof value ===
            "string" &&
        value.trim()
    ) {
        return value.trim();
    }

    const sku =
        getSku(item);

    if (!sku) {
        return "Unknown Item";
    }

    const parts =
        sku.split("*");

    const raw =
        parts.length >= 4
            ? parts
                  .slice(3)
                  .join("*")
            : sku;

    return raw
        .replace(
            /([a-z])([A-Z])/g,
            "$1 $2",
        )
        .replace(
            /[-_]+/g,
            " ",
        )
        .replace(
            /\b\w/g,
            (char) =>
                char.toUpperCase(),
        )
        .trim() || "Unknown Item";
}

export function getSection(
    item: VRFSItem,
): string {
    return String(
        item.section ??
            item.category ??
            item.category_name ??
            "Other",
    ).trim() || "Other";
}

export function isItemFree(
    item: VRFSItem,
): boolean {
    return (
        item.isFree === true ||
        item.isFree === 1 ||
        item.isFree === "1" ||
        (typeof item.price ===
            "string" &&
            item.price
                .trim()
                .toLowerCase() ===
                "free")
    );
}

export function getItemCredits(
    item: VRFSItem,
): number | null {
    for (const value of [
        item.credits,
        item.coins,
        item.coins_price,
    ]) {
        if (
            value === undefined ||
            value === null
        ) {
            continue;
        }

        const number =
            Number(value);

        if (
            Number.isFinite(number)
        ) {
            return number;
        }
    }

    if (
        typeof item.price ===
        "string"
    ) {
        const match =
            item.price.match(
                /[\d,]+(?:\.\d+)?/,
            );

        if (match) {
            const number =
                Number(
                    match[0].replace(
                        /,/g,
                        "",
                    ),
                );

            if (
                Number.isFinite(number)
            ) {
                return number;
            }
        }
    }

    return null;
}

export function getMarketplaceActive(
    item: VRFSMarketplaceItem,
): boolean {
    return (
        item.is_active === true ||
        item.is_active === 1 ||
        item.is_active === "1"
    );
}

export function getMarketplaceOwners(
    item: VRFSMarketplaceItem,
): number {
    const number = Number(
        item.owners_count ?? 0,
    );

    return Number.isFinite(
        number,
    ) && number >= 0
        ? number
        : 0;
}

export function getMarketplaceCreator(
    item: VRFSMarketplaceItem,
): string {
    return (
        String(
            item.author?.nickname ??
                item.author?.username ??
                item.author?.name ??
                item.creator ??
                "Unknown",
        ).trim() ||
        "Unknown"
    );
}

export function getMarketplaceCreatorUid(
    item: VRFSMarketplaceItem,
): string {
    return String(
        item.author?.uid ??
            item.owner_uid ??
            item.creator_uid ??
            "",
    ).trim();
}

function marketplaceScore(
    item: VRFSMarketplaceItem,
    query: string,
): number {
    const title =
        String(
            item.title ??
                item.name ??
                "",
        ).toLowerCase();

    const sku =
        String(
            item.sku ?? "",
        ).toLowerCase();

    const base =
        String(
            item.sku_base ?? "",
        ).toLowerCase();

    const creator =
        getMarketplaceCreator(
            item,
        ).toLowerCase();

    const id =
        String(item.id)
            .toLowerCase();

    if (id === query) {
        return 1000;
    }

    if (
        title === query ||
        sku === query ||
        base === query
    ) {
        return 950;
    }

    if (title.startsWith(query)) {
        return 800;
    }

    if (
        sku.startsWith(query) ||
        base.startsWith(query)
    ) {
        return 700;
    }

    if (title.includes(query)) {
        return 600;
    }

    if (
        sku.includes(query) ||
        base.includes(query)
    ) {
        return 500;
    }

    if (
        creator.includes(query)
    ) {
        return 300;
    }

    return 0;
}

function catalogScore(
    item: VRFSItem,
    query: string,
): number {
    const name =
        getItemName(
            item,
        ).toLowerCase();

    const sku =
        getSku(item)
            .toLowerCase();

    const section =
        getSection(
            item,
        ).toLowerCase();

    const id =
        String(
            item.id ??
                item.item_id ??
                item.itemId ??
                "",
        ).toLowerCase();

    if (
        id === query ||
        sku === query
    ) {
        return 1000;
    }

    if (name === query) {
        return 950;
    }

    if (
        name.startsWith(query)
    ) {
        return 800;
    }

    if (
        sku.startsWith(query)
    ) {
        return 700;
    }

    if (
        name.includes(query)
    ) {
        return 600;
    }

    if (
        sku.includes(query)
    ) {
        return 500;
    }

    if (
        section.includes(query)
    ) {
        return 300;
    }

    return 0;
}

/* ============================================================
   Singleton clients / public API
   ============================================================ */

export const vrfs =
    new VRFSClient();

export const seby =
    new SebyLockerClient();

export async function getUsername(
    uid: number,
    signal?: AbortSignal,
    forceRefresh = false,
): Promise<VRFSUser> {
    return vrfs.getUsername(
        uid,
        signal,
        forceRefresh,
    );
}

export async function getProfile(
    uid: number,
    signal?: AbortSignal,
    forceRefresh = false,
): Promise<VRFSProfile> {
    return vrfs.getProfile(
        uid,
        signal,
        forceRefresh,
    );
}

export async function getOutfits(
    uid: number,
    signal?: AbortSignal,
    forceRefresh = false,
): Promise<VRFSOutfit[]> {
    return vrfs.getOutfits(
        uid,
        signal,
        forceRefresh,
    );
}

export async function getPlayer(
    uid: number,
    signal?: AbortSignal,
    forceRefresh = false,
): Promise<
    VRFSUser & {
        profile?: VRFSProfile;
        outfits?: VRFSOutfit[];
    }
> {
    return vrfs.getPlayer(
        uid,
        signal,
        forceRefresh,
    );
}

export async function getMarketplace(
    signal?: AbortSignal,
    forceRefresh = false,
): Promise<
    VRFSMarketplaceItem[]
> {
    return vrfs.getMarketplace(
        signal,
        forceRefresh,
    );
}

export async function getMarketplaceItem(
    query: string | number,
    signal?: AbortSignal,
): Promise<
    VRFSMarketplaceItem | null
> {
    return vrfs.getMarketplaceItem(
        query,
        signal,
    );
}

export async function searchMarketplace(
    query: string,
    limit = 100,
    signal?: AbortSignal,
): Promise<
    VRFSMarketplaceItem[]
> {
    return vrfs.searchMarketplace(
        query,
        limit,
        signal,
    );
}

export async function getCatalog(
    signal?: AbortSignal,
    forceRefresh = false,
): Promise<VRFSItem[]> {
    return vrfs.getCatalog(
        signal,
        forceRefresh,
    );
}

export async function getCatalogItem(
    query: string | number,
    signal?: AbortSignal,
): Promise<VRFSItem | null> {
    return vrfs.getCatalogItem(
        query,
        signal,
    );
}

export async function searchCatalog(
    query: string,
    limit = 100,
    signal?: AbortSignal,
): Promise<VRFSItem[]> {
    return vrfs.searchCatalog(
        query,
        limit,
        signal,
    );
}

export async function checkOwnership(
    uid: number,
    skus: string[],
    options: OwnershipCheckOptions = {},
): Promise<OwnershipCheckResult> {
    return seby.checkOwnership(
        uid,
        skus,
        options,
    );
}

export async function getSebyItems(
    signal?: AbortSignal,
): Promise<VRFSItem[]> {
    return seby.getItems(
        signal,
    );
}

export async function sebyLookup(
    uid: number,
    skus: string[],
    signal?: AbortSignal,
): Promise<SebyLookupResponse> {
    return seby.lookup(
        uid,
        skus,
        signal,
    );
}

export async function sebyCheckOwnershipRequest(
    uid: number,
    skus: string[],
    signal?: AbortSignal,
): Promise<SebyOwnershipResponse> {
    return seby.checkOwnershipRequest(
        uid,
        skus,
        signal,
    );
}

export async function health(
    uid = 1,
    signal?: AbortSignal,
): Promise<VRFSApiHealth> {
    const started =
        Date.now();

    const [
        vrfsHealth,
        sebyHealth,
    ] = await Promise.all([
        vrfs.health(
            uid,
            signal,
        ),
        seby.health(
            signal,
        ),
    ]);

    return {
        ok:
            vrfsHealth.ok &&
            sebyHealth.ok,

        services: [
            vrfsHealth,
            sebyHealth,
        ],

        latencyMs:
            Date.now() - started,
    };
}

export function clearCaches(): void {
    vrfs.clearCaches();
    seby.clearFlights();
}

export function getStatus(): Record<
    string,
    unknown
> {
    return {
        vrfs: {
            configuration:
                vrfs.configuration,
            caches:
                vrfs.getCacheStats(),
        },

        seby:
            seby.getStatus(),
    };
}

export function getSkuForItem(
    item:
        | VRFSItem
        | VRFSMarketplaceItem
        | Record<string, unknown>,
): string {
    return getSku(item);
}

export function getNameForItem(
    item:
        | VRFSItem
        | VRFSMarketplaceItem
        | Record<string, unknown>,
): string {
    return getItemName(item);
}

export function getSectionForItem(
    item: VRFSItem,
): string {
    return getSection(item);
}

export function isFreeItem(
    item: VRFSItem,
): boolean {
    return isItemFree(item);
}

export function getCreditsForItem(
    item: VRFSItem,
): number | null {
    return getItemCredits(item);
}

export function isMarketplaceItemActive(
    item: VRFSMarketplaceItem,
): boolean {
    return getMarketplaceActive(
        item,
    );
}

export function getMarketplaceOwnerCount(
    item: VRFSMarketplaceItem,
): number {
    return getMarketplaceOwners(
        item,
    );
}

export function getMarketplaceCreatorName(
    item: VRFSMarketplaceItem,
): string {
    return getMarketplaceCreator(
        item,
    );
}

export function getMarketplaceCreatorId(
    item: VRFSMarketplaceItem,
): string {
    return getMarketplaceCreatorUid(
        item,
    );
}

export default {
    VRFSClient,
    SebyLockerClient,

    VRFSApiError,
    VRFSTimeoutError,
    VRFSRateLimitError,
    VRFSInvalidResponseError,
    VRFSResponseTooLargeError,

    TTLCache,
    SingleFlight,

    vrfs,
    seby,

    getUsername,
    getProfile,
    getOutfits,
    getPlayer,

    getMarketplace,
    getMarketplaceItem,
    searchMarketplace,

    getCatalog,
    getCatalogItem,
    searchCatalog,

    checkOwnership,
    getSebyItems,
    sebyLookup,
    sebyCheckOwnershipRequest,

    health,
    clearCaches,
    getStatus,

    getSku,
    getItemName,
    getSection,
    isItemFree,
    getItemCredits,

    getMarketplaceActive,
    getMarketplaceOwners,
    getMarketplaceCreator,
    getMarketplaceCreatorUid,

    getSkuForItem,
    getNameForItem,
    getSectionForItem,
    isFreeItem,
    getCreditsForItem,
    isMarketplaceItemActive,
    getMarketplaceOwnerCount,
    getMarketplaceCreatorName,
    getMarketplaceCreatorId,
};
