import https from "node:https";
import http from "node:http";
import { URL } from "node:url";
export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };
export type HttpMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
export type RequestStatus = "success" | "http_error" | "timeout" | "network_error" | "invalid_json" | "invalid_response" | "rate_limited" | "aborted";
export type ServiceHealth = "ok" | "degraded" | "unknown";
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
    userTag?: string;
    tiktokName?: string;
    youtubeName?: string;
    twitchName?: string;
    instagramName?: string;
    [key: string]: unknown;
}
export interface VRFSOutfit {
    id?: number | string;
    name?: string;
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
    isFree?: boolean | number | string;
    price?: string | number;
    coins?: number | string;
    coins_price?: number | string;
    [key: string]: unknown;
}
export interface VRFSMarketplaceItem {
    id: number | string;
    title?: string;
    sku?: string;
    sku_base?: string;
    category_id?: number | string;
    is_active?: boolean | number | string;
    coins_price?: number | string;
    owners_count?: number | string;
    gifts_left?: number | string;
    author?: {
        uid?: number | string;
        username?: string;
        nickname?: string;
        [key: string]: unknown;
    };
    thumbnail_url?: string;
    texture_url?: string;
    thumbnail?: string;
    image_url?: string;
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
    results?: Record<string, unknown>;
    [key: string]: unknown;
}
export interface VRFSRequestOptions {
    method?: HttpMethod;
    headers?: Record<string, string>;
    body?: JsonValue;
    timeoutMs?: number;
    maxResponseBytes?: number;
    signal?: AbortSignal;
    retries?: number;
    retryBaseDelayMs?: number;
    retryMaxDelayMs?: number;
}
export interface VRFSRequestResult<T> {
    status: RequestStatus;
    httpStatus: number;
    data: T | null;
    rawText: string;
    headers: Record<string, string | string[] | undefined>;
    attempts: number;
    durationMs: number;
    retryAfterMs?: number;
}
export interface VRFSClientOptions {
    baseUrl?: string;
    apiKey?: string;
    requestTimeoutMs?: number;
    maxResponseBytes?: number;
    retries?: number;
    retryBaseDelayMs?: number;
    retryMaxDelayMs?: number;
    userAgent?: string;
    logger?: VRFSLogger;
}
export interface VRFSLogger {
    debug?: (message: string, meta?: Record<string, unknown>) => void;
    info?: (message: string, meta?: Record<string, unknown>) => void;
    warn?: (message: string, meta?: Record<string, unknown>) => void;
    error?: (message: string, meta?: Record<string, unknown>) => void;
}
export class VRFSApiError extends Error {
    public readonly code: string;
    public readonly status: number;
    public readonly retryable: boolean;
    public readonly retryAfterMs?: number;
    public readonly endpoint?: string;
    public readonly details?: unknown;
    public readonly attempts: number;
    public constructor(message: string, options: { code: string; status?: number; retryable?: boolean; retryAfterMs?: number; endpoint?: string; details?: unknown; attempts?: number }) {
        super(message);
        this.name = "VRFSApiError";
        this.code = options.code;
        this.status = options.status ?? 0;
        this.retryable = options.retryable ?? false;
        this.retryAfterMs = options.retryAfterMs;
        this.endpoint = options.endpoint;
        this.details = options.details;
        this.attempts = options.attempts ?? 1;
    }
}
export class VRFSTimeoutError extends VRFSApiError {
    public constructor(message: string, endpoint?: string, attempts = 1) {
        super(message, { code: "TIMEOUT", retryable: true, endpoint, attempts });
        this.name = "VRFSTimeoutError";
    }
}
export class VRFSRateLimitError extends VRFSApiError {
    public constructor(message: string, retryAfterMs: number, endpoint?: string, attempts = 1) {
        super(message, { code: "RATE_LIMITED", status: 429, retryable: true, retryAfterMs, endpoint, attempts });
        this.name = "VRFSRateLimitError";
    }
}
export class VRFSInvalidResponseError extends VRFSApiError {
    public constructor(message: string, endpoint?: string, details?: unknown, attempts = 1) {
        super(message, { code: "INVALID_RESPONSE", retryable: false, endpoint, details, attempts });
        this.name = "VRFSInvalidResponseError";
    }
}
const DEFAULTS = Object.freeze({
    baseUrl: "https://api.vrfs.gg",
    requestTimeoutMs: 15_000,
    maxResponseBytes: 8 * 1024 * 1024,
    retries: 3,
    retryBaseDelayMs: 500,
    retryMaxDelayMs: 8_000,
    userAgent: "UNTITLED-VRFS-Middleware/1.0"
});
const ENDPOINTS = Object.freeze({
    webhooks: "/webhooks/v1",
    username: "/webhooks/v1/get_username.php",
    profile: "/webhooks/v1/get_user_profile.php",
    outfits: "/webhooks/v1/player/outfit/list.php",
    marketplace: "/webhooks/v1/market/get_all.php"
});
const SEBY_DEFAULT_BASE = "https://vrfs.sebyplay.xyz/lockerchecker";
const SEBY_ENDPOINTS = Object.freeze({
    lookup: "/api.php?action=lookup",
    check: "/api.php?action=check",
    items: "/data/items.json"
});
const RETRYABLE_HTTP_STATUSES = new Set([408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524]);
const NON_RETRYABLE_HTTP_STATUSES = new Set([400, 401, 403, 404, 405, 409, 410, 422]);
const SENSITIVE_HEADERS = new Set(["authorization", "x-api-key", "api-key", "cookie", "set-cookie"]);
const sleep = (ms: number, signal?: AbortSignal): Promise<void> => new Promise((resolve, reject) => {
    if (signal?.aborted) {
        reject(new VRFSApiError("Request aborted.", { code: "ABORTED", retryable: false }));
        return;
    }
    const timer = setTimeout(resolve, ms);
    const abort = (): void => {
        clearTimeout(timer);
        reject(new VRFSApiError("Request aborted.", { code: "ABORTED", retryable: false }));
    };
    signal?.addEventListener("abort", abort, { once: true });
});
const clamp = (value: number, min: number, max: number): number => Math.max(min, Math.min(max, value));
const randomJitter = (amount: number): number => Math.floor(Math.random() * amount);
const isObject = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const isRecord = (value: unknown): value is Record<string, unknown> => isObject(value);
const toStringSafe = (value: unknown): string => value === undefined || value === null ? "" : String(value);
const toPositiveInteger = (value: unknown): number | null => {
    const number = Number(value);
    return Number.isInteger(number) && number > 0 ? number : null;
};
const toBoolean = (value: unknown): boolean | null => {
    if (value === true || value === 1 || value === "1" || value === "true") return true;
    if (value === false || value === 0 || value === "0" || value === "false") return false;
    return null;
};
const normalizeBaseUrl = (value: string): string => {
    const url = new URL(value);
    if (url.protocol !== "https:" && url.protocol !== "http:") throw new Error(`Unsupported URL protocol: ${url.protocol}`);
    return url.toString().replace(/\/+$/, "");
};
const parseRetryAfter = (headers: Record<string, string | string[] | undefined>): number | undefined => {
    const raw = headers["retry-after"];
    const value = Array.isArray(raw) ? raw[0] : raw;
    if (!value) return undefined;
    const seconds = Number(value);
    if (Number.isFinite(seconds) && seconds >= 0) return Math.ceil(seconds * 1000);
    const timestamp = Date.parse(value);
    if (!Number.isNaN(timestamp)) return Math.max(0, timestamp - Date.now());
    return undefined;
};
const redactHeaders = (headers: Record<string, string | string[] | undefined>): Record<string, string | string[] | undefined> => {
    const output: Record<string, string | string[] | undefined> = {};
    for (const [key, value] of Object.entries(headers)) output[key] = SENSITIVE_HEADERS.has(key.toLowerCase()) ? "[REDACTED]" : value;
    return output;
};
const parseJsonSafely = <T>(text: string): T | null => {
    if (!text.trim()) return null;
    try {
        return JSON.parse(text) as T;
    } catch {
        return null;
    }
};
const extractErrorMessage = (data: unknown, fallback: string): string => {
    if (isRecord(data)) {
        for (const key of ["error", "message", "detail", "reason"]) {
            const value = data[key];
            if (typeof value === "string" && value.trim()) return value.trim();
        }
    }
    return fallback;
};
const sanitizePath = (url: string): string => {
    try {
        const parsed = new URL(url);
        for (const key of ["key", "api_key", "token", "authorization"]) parsed.searchParams.delete(key);
        return `${parsed.pathname}${parsed.search}`;
    } catch {
        return "[invalid-url]";
    }
};
const getRetryDelay = (attempt: number, retryAfterMs: number | undefined, base: number, max: number): number => {
    if (retryAfterMs !== undefined) return clamp(retryAfterMs, 250, max);
    const exponential = Math.min(max, base * Math.pow(2, Math.max(0, attempt - 1)));
    return clamp(exponential + randomJitter(Math.max(1, Math.floor(exponential * 0.25))), 250, max);
};
const httpRequest = (url: string, options: { method: HttpMethod; headers: Record<string, string>; body?: string; timeoutMs: number; maxResponseBytes: number; signal?: AbortSignal }): Promise<{ status: number; headers: Record<string, string | string[] | undefined>; text: string }> => new Promise((resolve, reject) => {
    let parsed: URL;
    try {
        parsed = new URL(url);
    } catch {
        reject(new VRFSApiError(`Invalid URL: ${url}`, { code: "INVALID_URL", retryable: false }));
        return;
    }
    const transport = parsed.protocol === "https:" ? https : http;
    let settled = false;
    let bytes = 0;
    const chunks: Buffer[] = [];
    const finishReject = (error: Error): void => {
        if (settled) return;
        settled = true;
        reject(error);
    };
    const finishResolve = (value: { status: number; headers: Record<string, string | string[] | undefined>; text: string }): void => {
        if (settled) return;
        settled = true;
        resolve(value);
    };
    const request = transport.request(parsed, {
        method: options.method,
        headers: options.headers,
        timeout: options.timeoutMs
    }, response => {
        response.on("data", chunk => {
            const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
            bytes += buffer.length;
            if (bytes > options.maxResponseBytes) {
                response.destroy();
                finishReject(new VRFSApiError(`Response exceeded the ${options.maxResponseBytes} byte limit.`, { code: "RESPONSE_TOO_LARGE", retryable: false }));
                return;
            }
            chunks.push(buffer);
        });
        response.on("end", () => {
            finishResolve({
                status: Number(response.statusCode ?? 0),
                headers: response.headers,
                text: Buffer.concat(chunks).toString("utf8")
            });
        });
        response.on("error", finishReject);
    });
    const timeout = setTimeout(() => {
        request.destroy();
        finishReject(new VRFSTimeoutError(`Request timed out after ${options.timeoutMs}ms.`));
    }, options.timeoutMs + 250);
    request.once("close", () => clearTimeout(timeout));
    request.on("timeout", () => {
        request.destroy();
        finishReject(new VRFSTimeoutError(`Request timed out after ${options.timeoutMs}ms.`));
    });
    request.on("error", finishReject);
    const abort = (): void => {
        request.destroy();
        finishReject(new VRFSApiError("Request aborted.", { code: "ABORTED", retryable: false }));
    };
    if (options.signal?.aborted) {
        abort();
        return;
    }
    options.signal?.addEventListener("abort", abort, { once: true });
    if (options.body !== undefined) request.write(options.body);
    request.end();
});
export class VRFSClient {
    private readonly baseUrl: string;
    private readonly apiKey?: string;
    private readonly requestTimeoutMs: number;
    private readonly maxResponseBytes: number;
    private readonly retries: number;
    private readonly retryBaseDelayMs: number;
    private readonly retryMaxDelayMs: number;
    private readonly userAgent: string;
    private readonly logger: VRFSLogger;
    private rateLimitedUntil = 0;
    public constructor(options: VRFSClientOptions = {}) {
        this.baseUrl = normalizeBaseUrl(options.baseUrl ?? DEFAULTS.baseUrl);
        this.apiKey = options.apiKey ?? process.env.VRFS_API_KEY;
        this.requestTimeoutMs = options.requestTimeoutMs ?? DEFAULTS.requestTimeoutMs;
        this.maxResponseBytes = options.maxResponseBytes ?? DEFAULTS.maxResponseBytes;
        this.retries = clamp(options.retries ?? DEFAULTS.retries, 0, 10);
        this.retryBaseDelayMs = Math.max(50, options.retryBaseDelayMs ?? DEFAULTS.retryBaseDelayMs);
        this.retryMaxDelayMs = Math.max(this.retryBaseDelayMs, options.retryMaxDelayMs ?? DEFAULTS.retryMaxDelayMs);
        this.userAgent = options.userAgent ?? DEFAULTS.userAgent;
        this.logger = options.logger ?? console;
    }
    public get configuration(): Readonly<{ baseUrl: string; hasApiKey: boolean; requestTimeoutMs: number; maxResponseBytes: number; retries: number }> {
        return {
            baseUrl: this.baseUrl,
            hasApiKey: Boolean(this.apiKey),
            requestTimeoutMs: this.requestTimeoutMs,
            maxResponseBytes: this.maxResponseBytes,
            retries: this.retries
        };
    }
    private buildUrl(pathname: string, params: Record<string, unknown> = {}): string {
        const url = new URL(pathname, `${this.baseUrl}/`);
        for (const [key, value] of Object.entries(params)) {
            if (value !== undefined && value !== null) url.searchParams.set(key, String(value));
        }
        return url.toString();
    }
    private async waitForLocalRateLimit(signal?: AbortSignal): Promise<void> {
        while (Date.now() < this.rateLimitedUntil) await sleep(Math.min(this.rateLimitedUntil - Date.now(), 1_000), signal);
    }
    private async execute<T>(url: string, options: VRFSRequestOptions = {}): Promise<VRFSRequestResult<T>> {
        const method = options.method ?? "GET";
        const timeoutMs = options.timeoutMs ?? this.requestTimeoutMs;
        const maxResponseBytes = options.maxResponseBytes ?? this.maxResponseBytes;
        const retries = clamp(options.retries ?? this.retries, 0, 10);
        const started = Date.now();
        let lastError: VRFSApiError | null = null;
        for (let attempt = 1; attempt <= retries + 1; attempt++) {
            await this.waitForLocalRateLimit(options.signal);
            const headers: Record<string, string> = {
                Accept: "application/json, text/plain, */*",
                "User-Agent": this.userAgent,
                ...options.headers
            };
            let body: string | undefined;
            if (options.body !== undefined) {
                body = JSON.stringify(options.body);
                headers["Content-Type"] ??= "application/json";
                headers["Content-Length"] = String(Buffer.byteLength(body));
            }
            try {
                const response = await httpRequest(url, {
                    method,
                    headers,
                    body,
                    timeoutMs,
                    maxResponseBytes,
                    signal: options.signal
                });
                const retryAfterMs = parseRetryAfter(response.headers);
                const parsed = parseJsonSafely<T>(response.text);
                if (response.status === 429) {
                    this.rateLimitedUntil = Math.max(this.rateLimitedUntil, Date.now() + (retryAfterMs ?? 1_000));
                    lastError = new VRFSRateLimitError(`VRFS rate limit reached.`, retryAfterMs ?? 1_000, sanitizePath(url), attempt);
                    if (attempt <= retries) {
                        await sleep(getRetryDelay(attempt, retryAfterMs, this.retryBaseDelayMs, this.retryMaxDelayMs), options.signal);
                        continue;
                    }
                    return {
                        status: "rate_limited",
                        httpStatus: response.status,
                        data: null,
                        rawText: response.text,
                        headers: redactHeaders(response.headers),
                        attempts: attempt,
                        durationMs: Date.now() - started,
                        retryAfterMs
                    };
                }
                if (response.status < 200 || response.status >= 300) {
                    const retryable = RETRYABLE_HTTP_STATUSES.has(response.status) && !NON_RETRYABLE_HTTP_STATUSES.has(response.status);
                    lastError = new VRFSApiError(extractErrorMessage(parsed, `VRFS returned HTTP ${response.status}.`), {
                        code: `HTTP_${response.status}`,
                        status: response.status,
                        retryable,
                        retryAfterMs,
                        endpoint: sanitizePath(url),
                        details: parsed ?? response.text.slice(0, 1_000),
                        attempts: attempt
                    });
                    if (retryable && attempt <= retries) {
                        await sleep(getRetryDelay(attempt, retryAfterMs, this.retryBaseDelayMs, this.retryMaxDelayMs), options.signal);
                        continue;
                    }
                    return {
                        status: "http_error",
                        httpStatus: response.status,
                        data: parsed,
                        rawText: response.text,
                        headers: redactHeaders(response.headers),
                        attempts: attempt,
                        durationMs: Date.now() - started,
                        retryAfterMs
                    };
                }
                if (response.text.trim() && parsed === null) {
                    lastError = new VRFSApiError("VRFS returned a successful response containing invalid JSON.", {
                        code: "INVALID_JSON",
                        status: response.status,
                        retryable: false,
                        endpoint: sanitizePath(url),
                        details: response.text.slice(0, 1_000),
                        attempts: attempt
                    });
                    return {
                        status: "invalid_json",
                        httpStatus: response.status,
                        data: null,
                        rawText: response.text,
                        headers: redactHeaders(response.headers),
                        attempts: attempt,
                        durationMs: Date.now() - started
                    };
                }
                return {
                    status: "success",
                    httpStatus: response.status,
                    data: parsed,
                    rawText: response.text,
                    headers: redactHeaders(response.headers),
                    attempts: attempt,
                    durationMs: Date.now() - started
                };
            } catch (error) {
                if (error instanceof VRFSApiError) lastError = error;
                else lastError = new VRFSApiError(error instanceof Error ? error.message : "Unknown network error.", {
                    code: "NETWORK_ERROR",
                    retryable: true,
                    endpoint: sanitizePath(url),
                    attempts: attempt
                });
                if (!lastError.retryable || attempt > retries) break;
                const delay = getRetryDelay(attempt, lastError.retryAfterMs, this.retryBaseDelayMs, this.retryMaxDelayMs);
                this.logger.warn?.("[VRFS] Retrying request.", {
                    endpoint: sanitizePath(url),
                    method,
                    attempt,
                    delayMs: delay,
                    error: lastError.message
                });
                await sleep(delay, options.signal);
            }
        }
        const error = lastError ?? new VRFSApiError("VRFS request failed.", { code: "REQUEST_FAILED", retryable: false });
        if (error.code === "TIMEOUT") {
            return {
                status: "timeout",
                httpStatus: 0,
                data: null,
                rawText: "",
                headers: {},
                attempts: error.attempts,
                durationMs: Date.now() - started
            };
        }
        if (error.code === "ABORTED") {
            return {
                status: "aborted",
                httpStatus: 0,
                data: null,
                rawText: "",
                headers: {},
                attempts: error.attempts,
                durationMs: Date.now() - started
            };
        }
        return {
            status: "network_error",
            httpStatus: error.status,
            data: null,
            rawText: "",
            headers: {},
            attempts: error.attempts,
            durationMs: Date.now() - started
        };
    }
    private async requestJson<T>(pathname: string, params: Record<string, unknown> = {}, options: VRFSRequestOptions = {}): Promise<T> {
        const url = this.buildUrl(pathname, params);
        const result = await this.execute<T>(url, options);
        if (result.status !== "success") {
            throw new VRFSApiError(`VRFS request failed: ${result.status}.`, {
                code: result.status.toUpperCase(),
                status: result.httpStatus,
                retryable: false,
                endpoint: sanitizePath(url),
                details: result.data ?? result.rawText,
                attempts: result.attempts,
                retryAfterMs: result.retryAfterMs
            });
        }
        return result.data as T;
    }
    public async getUsername(uid: number, signal?: AbortSignal): Promise<VRFSUser> {
        const validUid = toPositiveInteger(uid);
        if (!validUid) throw new VRFSApiError("UID must be a positive integer.", { code: "INVALID_UID" });
        const data = await this.requestJson<unknown>(ENDPOINTS.username, { uid: validUid }, { signal });
        return normalizeUser(data, validUid);
    }
    public async getProfile(uid: number, signal?: AbortSignal): Promise<VRFSProfile> {
        const validUid = toPositiveInteger(uid);
        if (!validUid) throw new VRFSApiError("UID must be a positive integer.", { code: "INVALID_UID" });
        const data = await this.requestJson<unknown>(ENDPOINTS.profile, { uid: validUid, ...(this.apiKey ? { key: this.apiKey } : {}) }, { signal });
        return normalizeProfile(data, validUid);
    }
    public async getOutfits(uid: number, signal?: AbortSignal): Promise<VRFSOutfit[]> {
        const validUid = toPositiveInteger(uid);
        if (!validUid) throw new VRFSApiError("UID must be a positive integer.", { code: "INVALID_UID" });
        const data = await this.requestJson<unknown>(ENDPOINTS.outfits, { uid: validUid, ...(this.apiKey ? { key: this.apiKey } : {}) }, { signal });
        return normalizeOutfits(data);
    }
    public async getMarketplace(signal?: AbortSignal): Promise<VRFSMarketplaceItem[]> {
        const data = await this.requestJson<unknown>(ENDPOINTS.marketplace, { ...(this.apiKey ? { key: this.apiKey } : {}) }, { signal });
        return normalizeMarketplace(data);
    }
    public async health(signal?: AbortSignal): Promise<{ ok: boolean; latencyMs: number }> {
        const started = Date.now();
        try {
            await this.getUsername(1, signal);
            return { ok: true, latencyMs: Date.now() - started };
        } catch {
            return { ok: false, latencyMs: Date.now() - started };
        }
    }
}
export class SebyLockerClient {
    private readonly baseUrl: string;
    private readonly requestTimeoutMs: number;
    private readonly maxResponseBytes: number;
    private readonly retries: number;
    private readonly retryBaseDelayMs: number;
    private readonly retryMaxDelayMs: number;
    private readonly userAgent: string;
    private readonly logger: VRFSLogger;
    private localRateLimitUntil = 0;
    private ownershipQueue: Promise<void> = Promise.resolve();
    public constructor(options: { baseUrl?: string; requestTimeoutMs?: number; maxResponseBytes?: number; retries?: number; retryBaseDelayMs?: number; retryMaxDelayMs?: number; userAgent?: string; logger?: VRFSLogger } = {}) {
        this.baseUrl = normalizeBaseUrl(options.baseUrl ?? SEBY_DEFAULT_BASE);
        this.requestTimeoutMs = options.requestTimeoutMs ?? DEFAULTS.requestTimeoutMs;
        this.maxResponseBytes = options.maxResponseBytes ?? DEFAULTS.maxResponseBytes;
        this.retries = clamp(options.retries ?? DEFAULTS.retries, 0, 10);
        this.retryBaseDelayMs = Math.max(50, options.retryBaseDelayMs ?? DEFAULTS.retryBaseDelayMs);
        this.retryMaxDelayMs = Math.max(this.retryBaseDelayMs, options.retryMaxDelayMs ?? DEFAULTS.retryMaxDelayMs);
        this.userAgent = options.userAgent ?? DEFAULTS.userAgent;
        this.logger = options.logger ?? console;
    }
    private buildUrl(pathname: string): string {
        return new URL(pathname, `${this.baseUrl}/`).toString();
    }
    private async rawRequest<T>(url: string, options: VRFSRequestOptions = {}): Promise<VRFSRequestResult<T>> {
        const method = options.method ?? "GET";
        const timeoutMs = options.timeoutMs ?? this.requestTimeoutMs;
        const maxResponseBytes = options.maxResponseBytes ?? this.maxResponseBytes;
        const retries = clamp(options.retries ?? this.retries, 0, 10);
        const started = Date.now();
        for (let attempt = 1; attempt <= retries + 1; attempt++) {
            while (Date.now() < this.localRateLimitUntil) await sleep(Math.min(this.localRateLimitUntil - Date.now(), 1_000), options.signal);
            const headers: Record<string, string> = {
                Accept: "application/json, text/plain, */*",
                "User-Agent": this.userAgent,
                ...options.headers
            };
            let body: string | undefined;
            if (options.body !== undefined) {
                body = JSON.stringify(options.body);
                headers["Content-Type"] ??= "application/json";
                headers["Content-Length"] = String(Buffer.byteLength(body));
            }
            try {
                const response = await httpRequest(url, { method, headers, body, timeoutMs, maxResponseBytes, signal: options.signal });
                const parsed = parseJsonSafely<T>(response.text);
                const retryAfterMs = parseRetryAfter(response.headers);
                if (response.status === 429) {
                    this.localRateLimitUntil = Math.max(this.localRateLimitUntil, Date.now() + (retryAfterMs ?? 1_000));
                    if (attempt <= retries) {
                        await sleep(getRetryDelay(attempt, retryAfterMs, this.retryBaseDelayMs, this.retryMaxDelayMs), options.signal);
                        continue;
                    }
                    return { status: "rate_limited", httpStatus: 429, data: parsed, rawText: response.text, headers: redactHeaders(response.headers), attempts: attempt, durationMs: Date.now() - started, retryAfterMs };
                }
                if (response.status < 200 || response.status >= 300) {
                    const retryable = RETRYABLE_HTTP_STATUSES.has(response.status);
                    if (retryable && attempt <= retries) {
                        await sleep(getRetryDelay(attempt, retryAfterMs, this.retryBaseDelayMs, this.retryMaxDelayMs), options.signal);
                        continue;
                    }
                    return { status: "http_error", httpStatus: response.status, data: parsed, rawText: response.text, headers: redactHeaders(response.headers), attempts: attempt, durationMs: Date.now() - started, retryAfterMs };
                }
                if (response.text.trim() && parsed === null) return { status: "invalid_json", httpStatus: response.status, data: null, rawText: response.text, headers: redactHeaders(response.headers), attempts: attempt, durationMs: Date.now() - started };
                return { status: "success", httpStatus: response.status, data: parsed, rawText: response.text, headers: redactHeaders(response.headers), attempts: attempt, durationMs: Date.now() - started };
            } catch (error) {
                const retryable = error instanceof VRFSApiError ? error.retryable : true;
                if (!retryable || attempt > retries) {
                    return {
                        status: error instanceof VRFSTimeoutError ? "timeout" : error instanceof VRFSApiError && error.code === "ABORTED" ? "aborted" : "network_error",
                        httpStatus: error instanceof VRFSApiError ? error.status : 0,
                        data: null,
                        rawText: "",
                        headers: {},
                        attempts: attempt,
                        durationMs: Date.now() - started
                    };
                }
                const delay = getRetryDelay(attempt, error instanceof VRFSApiError ? error.retryAfterMs : undefined, this.retryBaseDelayMs, this.retryMaxDelayMs);
                this.logger.warn?.("[SEBY] Retrying request.", { endpoint: sanitizePath(url), attempt, delayMs: delay });
                await sleep(delay, options.signal);
            }
        }
        return { status: "network_error", httpStatus: 0, data: null, rawText: "", headers: {}, attempts: retries + 1, durationMs: Date.now() - started };
    }
    private enqueueOwnership<T>(task: () => Promise<T>): Promise<T> {
        const run = this.ownershipQueue.then(task, task);
        this.ownershipQueue = run.then(() => undefined, () => undefined);
        return run;
    }
    public async getItems(signal?: AbortSignal): Promise<VRFSItem[]> {
        const result = await this.rawRequest<unknown>(this.buildUrl(SEBY_ENDPOINTS.items), { signal });
        if (result.status !== "success") throw this.resultError("Unable to retrieve Seby item catalog.", result);
        return normalizeItems(result.data);
    }
    public async lookup(uid: number, skus: string[], signal?: AbortSignal): Promise<SebyLookupResponse> {
        const validUid = toPositiveInteger(uid);
        if (!validUid) throw new VRFSApiError("UID must be a positive integer.", { code: "INVALID_UID" });
        const normalizedSkus = normalizeSkus(skus);
        if (!normalizedSkus.length) throw new VRFSApiError("At least one SKU is required.", { code: "EMPTY_SKU_LIST" });
        return this.enqueueOwnership(async () => {
            const result = await this.rawRequest<unknown>(this.buildUrl(SEBY_ENDPOINTS.lookup), {
                method: "POST",
                body: { uid: String(validUid), skus: normalizedSkus },
                signal
            });
            if (result.status !== "success") throw this.resultError("Seby lookup request failed.", result);
            return normalizeSebyResponse(result.data);
        });
    }
    public async checkOwnership(uid: number, skus: string[], signal?: AbortSignal): Promise<SebyOwnershipResponse> {
        const validUid = toPositiveInteger(uid);
        if (!validUid) throw new VRFSApiError("UID must be a positive integer.", { code: "INVALID_UID" });
        const normalizedSkus = normalizeSkus(skus);
        if (!normalizedSkus.length) throw new VRFSApiError("At least one SKU is required.", { code: "EMPTY_SKU_LIST" });
        return this.enqueueOwnership(async () => {
            const result = await this.rawRequest<unknown>(this.buildUrl(SEBY_ENDPOINTS.check), {
                method: "POST",
                body: { uid: String(validUid), skus: normalizedSkus },
                signal
            });
            if (result.status !== "success") throw this.resultError("Seby ownership request failed.", result);
            const response = normalizeSebyResponse(result.data);
            if (!response.ok) throw new VRFSInvalidResponseError("Seby returned an unsuccessful response.");
            return response;
        });
    }
    public async checkOwnershipBatched(uid: number, skus: string[], options: { batchSize?: number; minBatchSize?: number; maxBatchSize?: number; delayMs?: number; signal?: AbortSignal; onProgress?: (progress: { processed: number; total: number; batchSize: number; health: ServiceHealth }) => void } = {}): Promise<{ results: Record<string, boolean | "unknown">; health: ServiceHealth; batches: number; cached: number }> {
        const normalizedSkus = normalizeSkus(skus);
        const batchMin = Math.max(1, options.minBatchSize ?? 5);
        let batchSize = clamp(options.batchSize ?? 250, batchMin, Math.max(batchMin, options.maxBatchSize ?? 500));
        const delayMs = Math.max(0, options.delayMs ?? 150);
        const results: Record<string, boolean | "unknown"> = {};
        let index = 0;
        let batches = 0;
        let cached = 0;
        let health: ServiceHealth = "ok";
        while (index < normalizedSkus.length) {
            const batch = normalizedSkus.slice(index, index + batchSize);
            try {
                const response = await this.checkOwnership(uid, batch, options.signal);
                batches++;
                if (response.health === "degraded") health = "degraded";
                for (const sku of batch) {
                    const value = response.results[sku];
                    if (typeof value === "boolean") results[sku] = value;
                    else results[sku] = "unknown";
                }
                cached += Number.isFinite(response.cached) ? response.cached : 0;
                index += batch.length;
                options.onProgress?.({ processed: index, total: normalizedSkus.length, batchSize, health });
                if (index < normalizedSkus.length && delayMs > 0) await sleep(delayMs, options.signal);
            } catch (error) {
                if (error instanceof VRFSApiError && [400, 413, 414].includes(error.status) && batchSize > batchMin) {
                    batchSize = Math.max(batchMin, Math.floor(batchSize / 2));
                    this.logger.warn?.("[SEBY] Reducing ownership batch size.", { previousBatchSize: batch.length, nextBatchSize: batchSize });
                    continue;
                }
                for (const sku of batch) results[sku] = "unknown";
                health = "degraded";
                index += batch.length;
                batches++;
                options.onProgress?.({ processed: index, total: normalizedSkus.length, batchSize, health });
            }
        }
        return { results, health, batches, cached };
    }
    public async health(signal?: AbortSignal): Promise<{ ok: boolean; health: ServiceHealth; latencyMs: number }> {
        const started = Date.now();
        try {
            const items = await this.getItems(signal);
            return { ok: Array.isArray(items), health: "ok", latencyMs: Date.now() - started };
        } catch {
            return { ok: false, health: "unknown", latencyMs: Date.now() - started };
        }
    }
    private resultError(message: string, result: VRFSRequestResult<unknown>): VRFSApiError {
        return new VRFSApiError(message, {
            code: result.status.toUpperCase(),
            status: result.httpStatus,
            retryable: result.status === "rate_limited" || result.status === "timeout" || result.status === "network_error",
            retryAfterMs: result.retryAfterMs,
            attempts: result.attempts,
            details: result.data ?? result.rawText
        });
    }
}
export function normalizeUser(input: unknown, fallbackUid?: number): VRFSUser {
    const root = unwrapData(input);
    if (typeof root === "string") return { uid: fallbackUid ?? 0, username: root.trim() || `Player ${fallbackUid ?? 0}` };
    if (!isRecord(root)) throw new VRFSInvalidResponseError("VRFS username response was not an object or string.");
    const uid = toPositiveInteger(root.uid ?? root.userId ?? root.id) ?? fallbackUid ?? 0;
    const username = toStringSafe(root.username ?? root.nickname ?? root.name ?? root.data && isRecord(root.data) ? root.data.username : undefined).trim();
    if (!username) throw new VRFSInvalidResponseError("VRFS username response did not contain a username.");
    return { uid, username, nickname: typeof root.nickname === "string" ? root.nickname : undefined, name: typeof root.name === "string" ? root.name : undefined };
}
export function normalizeProfile(input: unknown, fallbackUid?: number): VRFSProfile {
    const root = unwrapData(input);
    if (!isRecord(root)) throw new VRFSInvalidResponseError("VRFS profile response was not an object.");
    const profile: VRFSProfile = { ...root };
    profile.uid = toPositiveInteger(root.uid ?? root.userId ?? root.id) ?? fallbackUid;
    if (typeof root.profileCountry === "string") profile.profileCountry = root.profileCountry;
    if (root.followersCount !== undefined) profile.followersCount = Number(root.followersCount) || 0;
    for (const key of ["userTag", "tiktokName", "youtubeName", "twitchName", "instagramName"]) {
        if (typeof root[key] === "string") profile[key] = root[key];
    }
    return profile;
}
export function normalizeOutfits(input: unknown): VRFSOutfit[] {
    const root = unwrapData(input);
    const array = Array.isArray(root) ? root : isRecord(root) && Array.isArray(root.outfits) ? root.outfits : [];
    return array.filter(isRecord).map(item => ({
        ...item,
        id: typeof item.id === "number" || typeof item.id === "string" ? item.id : undefined,
        name: typeof item.name === "string" ? item.name : undefined,
        slots: isRecord(item.slots) ? Object.fromEntries(Object.entries(item.slots).map(([key, value]) => [key, value === null ? null : String(value)])) : undefined
    }));
}
export function normalizeMarketplace(input: unknown): VRFSMarketplaceItem[] {
    const root = unwrapData(input);
    const array = Array.isArray(root) ? root : isRecord(root) && Array.isArray(root.items) ? root.items : [];
    return array.filter(isRecord).filter(item => item.id !== undefined && item.id !== null).map(item => ({
        ...item,
        id: item.id as string | number,
        title: typeof item.title === "string" ? item.title : undefined,
        sku: typeof item.sku === "string" ? item.sku : typeof item.sku_base === "string" ? item.sku_base : undefined,
        category_id: item.category_id as string | number | undefined,
        is_active: item.is_active as boolean | number | string | undefined,
        coins_price: item.coins_price as number | string | undefined,
        owners_count: item.owners_count as number | string | undefined,
        gifts_left: item.gifts_left as number | string | undefined,
        author: isRecord(item.author) ? {
            ...item.author,
            uid: item.author.uid as string | number | undefined,
            username: typeof item.author.username === "string" ? item.author.username : undefined,
            nickname: typeof item.author.nickname === "string" ? item.author.nickname : undefined
        } : undefined
    }));
}
export function normalizeItems(input: unknown): VRFSItem[] {
    const root = unwrapData(input);
    const array = Array.isArray(root) ? root : isRecord(root) && Array.isArray(root.items) ? root.items : [];
    return array.filter(isRecord).map(item => ({
        ...item,
        id: item.id as string | number | undefined,
        sku: typeof item.sku === "string" ? item.sku : undefined,
        sku_base: typeof item.sku_base === "string" ? item.sku_base : undefined,
        name: typeof item.name === "string" ? item.name : undefined,
        title: typeof item.title === "string" ? item.title : undefined,
        section: typeof item.section === "string" ? item.section : undefined,
        category: typeof item.category === "string" ? item.category : undefined,
        category_name: typeof item.category_name === "string" ? item.category_name : undefined,
        thumb: typeof item.thumb === "string" ? item.thumb : undefined,
        isFree: item.isFree as boolean | number | string | undefined,
        price: item.price as string | number | undefined,
        coins: item.coins as number | string | undefined
    })).filter(item => Boolean(getSku(item)));
}
export function normalizeSebyResponse(input: unknown): SebyOwnershipResponse {
    if (!isRecord(input)) throw new VRFSInvalidResponseError("Seby returned a non-object response.");
    const ok = toBoolean(input.ok);
    if (ok === null) throw new VRFSInvalidResponseError("Seby response is missing a valid 'ok' field.", undefined, input);
    const healthValue = typeof input.health === "string" ? input.health.toLowerCase() : "unknown";
    const health: ServiceHealth = healthValue === "ok" ? "ok" : healthValue === "degraded" ? "degraded" : "unknown";
    const results: Record<string, boolean> = {};
    if (isRecord(input.results)) {
        for (const [sku, value] of Object.entries(input.results)) {
            const normalized = toBoolean(value);
            if (normalized !== null) results[String(sku)] = normalized;
        }
    }
    const cached = Number(input.cached);
    return {
        ok,
        health,
        results,
        cached: Number.isFinite(cached) && cached >= 0 ? cached : 0,
        ...(Number.isFinite(Number(input.retry_after)) ? { retry_after: Number(input.retry_after) } : {}),
        ...input
    };
}
export function normalizeSkus(input: readonly unknown[]): string[] {
    const output: string[] = [];
    const seen = new Set<string>();
    for (const value of input) {
        const sku = String(value ?? "").trim();
        if (!sku || sku.length > 255 || seen.has(sku)) continue;
        seen.add(sku);
        output.push(sku);
    }
    return output;
}
export function getSku(item: VRFSItem | VRFSMarketplaceItem | Record<string, unknown>): string {
    const value = item.sku ?? item.sku_base;
    return typeof value === "string" ? value.trim() : "";
}
export function getItemName(item: VRFSItem | VRFSMarketplaceItem | Record<string, unknown>): string {
    const value = item.name ?? item.title;
    if (typeof value === "string" && value.trim()) return value.trim();
    const sku = getSku(item);
    return sku || "Unknown Item";
}
export function isItemFree(item: VRFSItem): boolean {
    if (item.isFree === true || item.isFree === 1 || item.isFree === "1") return true;
    if (typeof item.price === "string" && item.price.trim().toLowerCase() === "free") return true;
    return false;
}
export function getMarketplaceActive(item: VRFSMarketplaceItem): boolean {
    return item.is_active === true || item.is_active === 1 || item.is_active === "1";
}
export function getMarketplaceOwners(item: VRFSMarketplaceItem): number {
    const value = Number(item.owners_count ?? 0);
    return Number.isFinite(value) && value >= 0 ? value : 0;
}
export function unwrapData(input: unknown): unknown {
    if (!isRecord(input)) return input;
    if (input.data !== undefined) {
        if (isRecord(input.data) && input.data.data !== undefined) return input.data.data;
        return input.data;
    }
    return input;
}
export const vrfs = new VRFSClient();
export const seby = new SebyLockerClient();
export default {
    VRFSClient,
    SebyLockerClient,
    VRFSApiError,
    VRFSTimeoutError,
    VRFSRateLimitError,
    VRFSInvalidResponseError,
    normalizeUser,
    normalizeProfile,
    normalizeOutfits,
    normalizeMarketplace,
    normalizeItems,
    normalizeSebyResponse,
    normalizeSkus,
    getSku,
    getItemName,
    isItemFree,
    getMarketplaceActive,
    getMarketplaceOwners,
    vrfs,
    seby
};
