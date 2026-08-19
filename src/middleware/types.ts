export type JsonPrimitive=string|number|boolean|null;
export type JsonValue=JsonPrimitive|JsonObject|JsonValue[];
export interface JsonObject{[key:string]:JsonValue|undefined;}
export type HttpMethod="GET"|"POST"|"PUT"|"PATCH"|"DELETE";
export type RequestStatus="success"|"http_error"|"timeout"|"network_error"|"invalid_json"|"invalid_response"|"rate_limited"|"aborted"|"response_too_large";
export type ServiceHealth="ok"|"degraded"|"unknown";
export type OwnershipValue=true|false|"unknown";
export type OwnershipResults=Record<string,OwnershipValue>;
export interface VRFSUsernameResponse{ok?:boolean;uid?:number|string;username?:string;nickname?:string;name?:string;avatar?:string;data?:{username?:string;nickname?:string;name?:string};[key:string]:unknown;}
export interface VRFSProfile{uid?:number|string;username?:string;profileCountry?:string;followersCount?:number|string;followingCount?:number|string;userTag?:string;tiktokName?:string;youtubeName?:string;twitchName?:string;instagramName?:string;requestResult?:string;[key:string]:unknown;}
export interface VRFSOutfit{id?:number|string;name?:string;createdAt?:string;updatedAt?:string;slots?:Record<string,string|null|undefined>;[key:string]:unknown;}
export interface VRFSOutfitsResponse{ok?:boolean;status?:string;data?:VRFSOutfit[];[key:string]:unknown;}
export interface VRFSItem{section?:string;category?:string;category_name?:string;name?:string;title?:string;sku?:string;sku_base?:string;thumb?:string;image?:string;image_url?:string;thumbnail?:string;thumbnail_url?:string;texture_url?:string;thumbnail_url_ktx?:string;isFree?:boolean|number|string;price?:string|number;coins?:number|string;coins_price?:number|string;credits?:number|string;id?:number|string;item_id?:number|string;itemId?:number|string;[key:string]:unknown;}
export interface VRFSMarketplaceAuthor{uid?:number|string;username?:string;nickname?:string;name?:string;avatar_url?:string;badge_id?:number|string;highlight_color?:string;[key:string]:unknown;}
export interface VRFSMarketplaceItem{id:number|string;title?:string;name?:string;sku?:string;sku_base?:string;category_id?:number|string;category?:string;is_active?:boolean|number|string;is_visible?:boolean|number|string;sort_order?:number|string;owners_count?:number|string;gifts_left?:number|string;coins_price?:number|string;owner_uid?:number|string;creator_uid?:number|string;creator?:string;texture_id?:number|string;texture_url?:string;thumbnail_url?:string;thumbnail_url_ktx?:string;subtitle?:string;color?:unknown;meta?:unknown;highlight_type?:string;created_at?:string;updated_at?:string;activated_at?:string;available_for_send?:number|string|boolean;badges?:unknown[];author?:VRFSMarketplaceAuthor;[key:string]:unknown;}
export interface VRFSMarketplaceResponse{ok?:boolean;data?:VRFSMarketplaceItem[]|{items?:VRFSMarketplaceItem[];[key:string]:unknown};[key:string]:unknown;}
export interface SebyOwnershipResponse{ok?:boolean;health?:string;results?:Record<string,boolean>;cached?:number|string;retry_after?:number|string;error?:string;message?:string;[key:string]:unknown;}
export interface SebyLookupResponse{ok?:boolean;health?:string;results?:Record<string,unknown>;cached?:number|string;retry_after?:number|string;error?:string;message?:string;[key:string]:unknown;}
export interface OwnershipCheckOptions{batchSize?:number;minBatchSize?:number;maxBatchSize?:number;delayMs?:number;timeoutMs?:number;maxMissingRetries?:number;forceRefresh?:boolean;signal?:AbortSignal;onProgress?:(progress:OwnershipProgress)=>void;}
export interface OwnershipProgress{processed:number;total:number;confirmed:number;unknown:number;owned:number;notOwned:number;batchNumber:number;batchSize:number;health:ServiceHealth;cached:number;}
export interface OwnershipCheckResult{uid:number|string;results:OwnershipResults;unknown:string[];owned:string[];notOwned:string[];requested:number;confirmed:number;cached:number;apiRequests:number;retries:number;batches:number;durationMs:number;health:ServiceHealth;partial:boolean;failed:number;}
export interface CatalogStats{items:number;sections:number;free:number;premium:number;loadedAt:number;ageMs:number;}
export interface MarketplaceStats{items:number;active:number;inactive:number;creators:number;owners:number;loadedAt:number;ageMs:number;}
export interface VRFSServiceHealth{service:"vrfs"|"seby";ok:boolean;health:ServiceHealth;latencyMs:number;error?:string;}
export interface VRFSApiHealth{ok:boolean;services:VRFSServiceHealth[];latencyMs:number;}
export interface CacheEntry<T>{value:T;createdAt:number;expiresAt:number;}
export interface CacheOptions{ttlMs:number;maxEntries?:number;}
export interface RequestOptions{method?:HttpMethod;headers?:Record<string,string>;body?:unknown;timeoutMs?:number;retries?:number;retryDelayMs?:number;signal?:AbortSignal;}
export interface HttpResponse<T=unknown>{status:number;headers:Record<string,string|string[]|undefined>;data:T|null;text:string;url:string;}
export interface VRFSRequestResult<T=unknown>{status:RequestStatus;httpStatus:number;data:T|null;rawText:string;headers:Record<string,string|string[]|undefined>;attempts:number;durationMs:number;retryAfterMs?:number;url:string;}
export interface VRFSLogger{debug?:(message:string,meta?:Record<string,unknown>)=>void;info?:(message:string,meta?:Record<string,unknown>)=>void;warn?:(message:string,meta?:Record<string,unknown>)=>void;error?:(message:string,meta?:Record<string,unknown>)=>void;}
export interface VRFSClientOptions{baseUrl?:string;apiKey?:string;requestTimeoutMs?:number;maxResponseBytes?:number;maxRedirects?:number;retries?:number;retryBaseDelayMs?:number;retryMaxDelayMs?:number;userAgent?:string;logger?:VRFSLogger;catalogTtlMs?:number;marketplaceTtlMs?:number;playerTtlMs?:number;ownershipTtlMs?:number;ownershipBatchSize?:number;ownershipMinBatchSize?:number;ownershipMaxBatchSize?:number;ownershipRetryCount?:number;ownershipDelayMs?:number;}
export class VRFSApiError extends Error{readonly code:string;readonly status:number;readonly url:string;readonly body:unknown;readonly retryable:boolean;readonly retryAfterMs?:number;readonly endpoint?:string;readonly details?:unknown;readonly attempts:number;constructor(message:string,status:number,url:string,body:unknown=null,retryable=false,options?:{retryAfterMs?:number;endpoint?:string;details?:unknown;attempts?:number}){super(message);this.name="VRFSApiError";this.code=`HTTP_${status}`;this.status=status;this.url=url;this.body=body;this.retryable=retryable;this.retryAfterMs=options?.retryAfterMs;this.endpoint=options?.endpoint;this.details=options?.details;this.attempts=options?.attempts??1;Object.setPrototypeOf(this,new.target.prototype);}}
export class VRFSTimeoutError extends VRFSApiError{constructor(message:string,url="",attempts=1){super(message,0,url,null,true,{endpoint:url,attempts});this.name="VRFSTimeoutError";Object.setPrototypeOf(this,new.target.prototype);}}
export class VRFSRateLimitError extends VRFSApiError{constructor(message:string,retryAfterMs:number,url="",attempts=1){super(message,429,url,null,true,{retryAfterMs,endpoint:url,attempts});this.name="VRFSRateLimitError";Object.setPrototypeOf(this,new.target.prototype);}}
export class VRFSInvalidResponseError extends VRFSApiError{constructor(message:string,url="",details?:unknown,attempts=1){super(message,200,url,details,false,{endpoint:url,details,attempts});this.name="VRFSInvalidResponseError";Object.setPrototypeOf(this,new.target.prototype);}}
export class VRFSValidationError extends Error{readonly code="VALIDATION_ERROR";constructor(message:string){super(message);this.name="VRFSValidationError";Object.setPrototypeOf(this,new.target.prototype);}}
export class VRFSResponseTooLargeError extends VRFSApiError{constructor(message:string,url=""){super(message,0,url,null,false,{endpoint:url});this.name="VRFSResponseTooLargeError";this.code="RESPONSE_TOO_LARGE";Object.setPrototypeOf(this,new.target.prototype);}}
export interface VRFSHealthOptions{uid?:number;signal?:AbortSignal;}
export interface MarketplaceSearchOptions{limit?:number;signal?:AbortSignal;forceRefresh?:boolean;}
export interface CatalogSearchOptions{limit?:number;signal?:AbortSignal;forceRefresh?:boolean;}
export interface HTTPRetryPolicy{retries:number;baseDelayMs:number;maxDelayMs:number;}
export interface HTTPClientOptions{timeoutMs:number;maxResponseBytes:number;maxRedirects:number;retryPolicy:HTTPRetryPolicy;userAgent:string;}
export interface APIRequestContext{service:"vrfs"|"seby";endpoint:string;method:HttpMethod;attempt:number;maxAttempts:number;durationMs:number;}
