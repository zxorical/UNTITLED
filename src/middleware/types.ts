export type JsonPrimitive=string|number|boolean|null;
export type JsonValue=JsonPrimitive|JsonObject|JsonValue[];
export interface JsonObject{[key:string]:JsonValue|undefined;}
export interface VRFSUsernameResponse{ok?:boolean;uid?:number|string;username?:string;nickname?:string;name?:string;avatar?:string;data?:{username?:string;nickname?:string;name?:string};}
export interface VRFSProfile{uid?:number|string;profileCountry?:string;followersCount?:number|string;followingCount?:number|string;userTag?:string;tiktokName?:string;youtubeName?:string;twitchName?:string;instagramName?:string;[key:string]:unknown;}
export interface VRFSOutfit{ id?:number|string; name?:string; createdAt?:string; updatedAt?:string; slots?:Record<string,string|null|undefined>; [key:string]:unknown;}
export interface VRFSOutfitsResponse{ok?:boolean;data?:VRFSOutfit[];[key:string]:unknown;}
export interface VRFSItem{section?:string;category?:string;category_name?:string;name?:string;title?:string;sku?:string;sku_base?:string;thumb?:string;image?:string;image_url?:string;thumbnail?:string;thumbnail_url?:string;texture_url?:string;isFree?:boolean|number|string;price?:string|number;coins?:number|string;coins_price?:number|string;credits?:number|string;id?:number|string;item_id?:number|string;itemId?:number|string;[key:string]:unknown;}
export interface VRFSMarketplaceAuthor{uid?:number|string;username?:string;nickname?:string;name?:string;[key:string]:unknown;}
export interface VRFSMarketplaceItem{ id?:number|string;title?:string;name?:string;sku?:string;sku_base?:string;category_id?:number|string;category?:string;is_active?:boolean|number|string;owners_count?:number|string;gifts_left?:number|string;coins_price?:number|string;author?:VRFSMarketplaceAuthor;owner_uid?:number|string;creator_uid?:number|string;creator?:string;thumbnail_url?:string;texture_url?:string;thumbnail?:string;image_url?:string;[key:string]:unknown;}
export interface VRFSMarketplaceResponse{ok?:boolean;data?:VRFSMarketplaceItem[]|{items?:VRFSMarketplaceItem[];[key:string]:unknown};[key:string]:unknown;}
export interface SebyOwnershipResponse{ok?:boolean;health?:string;results?:Record<string,boolean>;cached?:number|string;retry_after?:number|string;error?:string;message?:string;[key:string]:unknown;}
export type OwnershipValue=true|false|"unknown";
export type OwnershipResults=Record<string,OwnershipValue>;
export interface OwnershipCheckOptions{batchSize?:number;minBatchSize?:number;maxBatchSize?:number;delayMs?:number;timeoutMs?:number;signal?:AbortSignal;}
export interface OwnershipCheckResult{uid:number|string;results:OwnershipResults;requested:number;confirmed:number;owned:number;notOwned:number;unknown:number;cached:number;failed:number;}
export interface CacheEntry<T>{value:T;createdAt:number;expiresAt:number;}
export interface CacheOptions{ttlMs:number;maxEntries?:number;}
export interface RequestOptions{method?:"GET"|"POST"|"PUT"|"PATCH"|"DELETE";headers?:Record<string,string>;body?:unknown;timeoutMs?:number;retries?:number;retryDelayMs?:number;signal?:AbortSignal;}
export interface HttpResponse<T=unknown>{status:number;headers:Record<string,string|string[]|undefined>;data:T|null;text:string;url:string;}
export class VRFSApiError extends Error{readonly status:number;readonly url:string;readonly body:unknown;readonly retryable:boolean;constructor(message:string,status:number,url:string,body:unknown=null,retryable=false){super(message);this.name="VRFSApiError";this.status=status;this.url=url;this.body=body;this.retryable=retryable;}}
export class VRFSValidationError extends Error{constructor(message:string){super(message);this.name="VRFSValidationError";}}
