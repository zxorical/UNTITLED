import https from "node:https";
import http from "node:http";
import {URL} from "node:url";
export type JsonPrimitive=string|number|boolean|null;
export type JsonValue=JsonPrimitive|JsonObject|JsonValue[];
export interface JsonObject{[key:string]:JsonValue|undefined;}
export type HttpMethod="GET"|"POST"|"PUT"|"PATCH"|"DELETE";
export type RequestStatus="success"|"http_error"|"timeout"|"network_error"|"invalid_json"|"invalid_response"|"rate_limited"|"aborted"|"response_too_large";
export type ServiceHealth="ok"|"degraded"|"unknown";
export type OwnershipValue=boolean|"unknown";
export interface VRFSUser{uid:number;username:string;nickname?:string;name?:string;}
export interface VRFSProfile{uid?:number;username?:string;profileCountry?:string;followersCount?:number;followingCount?:number;userTag?:string;tiktokName?:string;youtubeName?:string;twitchName?:string;instagramName?:string;requestResult?:string;[key:string]:unknown;}
export interface VRFSOutfit{id?:number|string;name?:string;createdAt?:string;updatedAt?:string;slots?:Record<string,string|null>;[key:string]:unknown;}
export interface VRFSItem{id?:number|string;sku?:string;sku_base?:string;name?:string;title?:string;section?:string;category?:string;category_name?:string;thumb?:string;image?:string;image_url?:string;thumbnail?:string;thumbnail_url?:string;texture_url?:string;thumbnail_url_ktx?:string;isFree?:boolean|number|string;price?:string|number;coins?:number|string;coins_price?:number|string;credits?:number|string;item_id?:number|string;itemId?:number|string;[key:string]:unknown;}
export interface VRFSMarketplaceAuthor{uid?:number|string;username?:string;nickname?:string;name?:string;avatar_url?:string;badge_id?:number|string;highlight_color?:string;[key:string]:unknown;}
export interface VRFSMarketplaceItem{id:number|string;sku?:string;sku_base?:string;title?:string;name?:string;color?:unknown;meta?:unknown;category_id?:number|string;sort_order?:number|string;is_active?:boolean|number|string;is_visible?:boolean|number|string;owners_count?:number|string;gifts_left?:number|string;coins_price?:number|string;owner_uid?:number|string;creator_uid?:number|string;creator?:string;texture_id?:number|string;texture_url?:string;thumbnail_url?:string;thumbnail_url_ktx?:string;subtitle?:string;highlight_type?:string;created_at?:string;updated_at?:string;activated_at?:string;available_for_send?:number|string|boolean;badges?:unknown[];author?:VRFSMarketplaceAuthor;[key:string]:unknown;}
export interface VRFSMarketplaceResponse{data?:VRFSMarketplaceItem[]|{items?:VRFSMarketplaceItem[];[key:string]:unknown};[key:string]:unknown;}
export interface SebyOwnershipResponse{ok:boolean;health:ServiceHealth;results:Record<string,boolean>;cached:number;retry_after?:number;[key:string]:unknown;}
export interface SebyLookupResponse{ok:boolean;health:ServiceHealth;results:Record<string,unknown>;[key:string]:unknown;}
export interface VRFSRequestOptions{method?:HttpMethod;headers?:Record<string,string>;body?:unknown;timeoutMs?:number;maxResponseBytes?:number;maxRedirects?:number;signal?:AbortSignal;retries?:number;retryBaseDelayMs?:number;retryMaxDelayMs?:number;}
export interface VRFSRequestResult<T>{status:RequestStatus;httpStatus:number;data:T|null;rawText:string;headers:Record<string,string|string[]|undefined>;attempts:number;durationMs:number;retryAfterMs?:number;url:string;}
export interface VRFSClientOptions{baseUrl?:string;apiKey?:string;requestTimeoutMs?:number;maxResponseBytes?:number;maxRedirects?:number;retries?:number;retryBaseDelayMs?:number;retryMaxDelayMs?:number;userAgent?:string;logger?:VRFSLogger;catalogTtlMs?:number;marketplaceTtlMs?:number;playerTtlMs?:number;ownershipTtlMs?:number;ownershipBatchSize?:number;ownershipMinBatchSize?:number;ownershipMaxBatchSize?:number;ownershipRetryCount?:number;ownershipDelayMs?:number;}
export interface VRFSLogger{debug?:(message:string,meta?:Record<string,unknown>)=>void;info?:(message:string,meta?:Record<string,unknown>)=>void;warn?:(message:string,meta?:Record<string,unknown>)=>void;error?:(message:string,meta?:Record<string,unknown>)=>void;}
export interface OwnershipCheckOptions{batchSize?:number;minBatchSize?:number;maxBatchSize?:number;delayMs?:number;maxMissingRetries?:number;signal?:AbortSignal;forceRefresh?:boolean;onProgress?:(progress:OwnershipProgress)=>void;}
export interface OwnershipProgress{processed:number;total:number;confirmed:number;unknown:number;owned:number;notOwned:number;batchNumber:number;batchSize:number;health:ServiceHealth;cached:number;}
export interface OwnershipCheckResult{uid:number;results:Record<string,OwnershipValue>;unknown:string[];owned:string[];notOwned:string[];requested:number;confirmed:number;cached:number;apiRequests:number;retries:number;batches:number;durationMs:number;health:ServiceHealth;partial:boolean;}
export interface CatalogStats{items:number;sections:number;free:number;premium:number;loadedAt:number;ageMs:number;}
export interface MarketplaceStats{items:number;active:number;inactive:number;creators:number;owners:number;loadedAt:number;ageMs:number;}
export interface VRFSServiceHealth{service:"vrfs"|"seby";ok:boolean;health:ServiceHealth;latencyMs:number;error?:string;}
export interface VRFSApiHealth{ok:boolean;services:VRFSServiceHealth[];latencyMs:number;}
export class VRFSApiError extends Error{
public readonly code:string;
public readonly status:number;
public readonly retryable:boolean;
public readonly retryAfterMs?:number;
public readonly endpoint?:string;
public readonly details?:unknown;
public readonly attempts:number;
public constructor(message:string,options:{code:string;status?:number;retryable?:boolean;retryAfterMs?:number;endpoint?:string;details?:unknown;attempts?:number}){
super(message);
this.name="VRFSApiError";
this.code=options.code;
this.status=options.status??0;
this.retryable=options.retryable??false;
this.retryAfterMs=options.retryAfterMs;
this.endpoint=options.endpoint;
this.details=options.details;
this.attempts=options.attempts??1;
Object.setPrototypeOf(this,new.target.prototype);
}
}
export class VRFSTimeoutError extends VRFSApiError{
public constructor(message:string,endpoint?:string,attempts=1){
super(message,{code:"TIMEOUT",retryable:true,endpoint,attempts});
this.name="VRFSTimeoutError";
}
}
export class VRFSRateLimitError extends VRFSApiError{
public constructor(message:string,retryAfterMs:number,endpoint?:string,attempts=1){
super(message,{code:"RATE_LIMITED",status:429,retryable:true,retryAfterMs,endpoint,attempts});
this.name="VRFSRateLimitError";
}
}
export class VRFSInvalidResponseError extends VRFSApiError{
public constructor(message:string,endpoint?:string,details?:unknown,attempts=1){
super(message,{code:"INVALID_RESPONSE",retryable:false,endpoint,details,attempts});
this.name="VRFSInvalidResponseError";
}
}
export class VRFSResponseTooLargeError extends VRFSApiError{
public constructor(message:string,endpoint?:string){
super(message,{code:"RESPONSE_TOO_LARGE",retryable:false,endpoint});
this.name="VRFSResponseTooLargeError";
}
}
const DEFAULTS=Object.freeze({
vrfsBaseUrl:"https://api.vrfs.gg",
sebyBaseUrl:"https://vrfs.sebyplay.xyz/lockerchecker",
requestTimeoutMs:15_000,
maxResponseBytes:16*1024*1024,
maxRedirects:5,
retries:3,
retryBaseDelayMs:500,
retryMaxDelayMs:8_000,
userAgent:"UNTITLED-VRFS-Middleware/1.0",
catalogTtlMs:30*60*1000,
marketplaceTtlMs:5*60*1000,
playerTtlMs:5*60*1000,
ownershipTtlMs:30*60*1000,
ownershipBatchSize:250,
ownershipMinBatchSize:5,
ownershipMaxBatchSize:500,
ownershipRetryCount:2,
ownershipDelayMs:150,
maxSkuLength:255
});
const VRFS_ENDPOINTS=Object.freeze({
username:"/webhooks/v1/get_username.php",
profile:"/webhooks/v1/get_user_profile.php",
outfits:"/webhooks/v1/player/outfit/list.php",
marketplace:"/webhooks/v1/market/get_all.php"
});
const SEBY_ENDPOINTS=Object.freeze({
lookup:"/api.php?action=lookup",
check:"/api.php?action=check",
items:"/data/items.json"
});
const RETRYABLE_HTTP_STATUSES=new Set([408,425,429,500,502,503,504,520,521,522,523,524]);
const NON_RETRYABLE_HTTP_STATUSES=new Set([400,401,403,404,405,409,410,411,413,414,415,422]);
const SENSITIVE_QUERY_KEYS=new Set(["key","api_key","token","access_token","authorization","password","secret"]);
const sleep=(ms:number,signal?:AbortSignal):Promise<void>=>new Promise((resolve,reject)=>{
if(ms<=0){resolve();return;}
if(signal?.aborted){reject(new VRFSApiError("Request aborted.",{code:"ABORTED"}));return;}
const onAbort=()=>{clearTimeout(timer);signal?.removeEventListener("abort",onAbort);reject(new VRFSApiError("Request aborted.",{code:"ABORTED"}));};
const timer=setTimeout(()=>{signal?.removeEventListener("abort",onAbort);resolve();},ms);
signal?.addEventListener("abort",onAbort,{once:true});
});
const clamp=(value:number,min:number,max:number):number=>Math.max(min,Math.min(max,value));
const jitter=(amount:number):number=>amount<=0?0:Math.floor(Math.random()*(amount+1));
const isRecord=(value:unknown):value is Record<string,unknown>=>typeof value==="object"&&value!==null&&!Array.isArray(value);
const asString=(value:unknown):string=>value===undefined||value===null?"":String(value);
const asPositiveInt=(value:unknown):number|null=>{const n=Number(value);return Number.isInteger(n)&&n>0?n:null;};
const asBoolean=(value:unknown):boolean|null=>{if(value===true||value===1||value==="1"||value==="true")return true;if(value===false||value===0||value==="0"||value==="false")return false;return null;};
const uniqueStrings=(values:readonly unknown[],maxLength=DEFAULTS.maxSkuLength):string[]=>{const seen=new Set<string>();const output:string[]=[];for(const value of values){const normalized=String(value??"").trim();if(!normalized||normalized.length>maxLength||seen.has(normalized))continue;seen.add(normalized);output.push(normalized);}return output;};
const chunk=<T>(values:T[],size:number):T[][]=>{const output:T[][]=[];for(let i=0;i<values.length;i+=size)output.push(values.slice(i,i+size));return output;};
const normalizeBaseUrl=(value:string):string=>{const url=new URL(value);if(url.protocol!=="https:"&&url.protocol!=="http:")throw new Error(`Unsupported protocol: ${url.protocol}`);return url.toString().replace(/\/+$/,"");};
const sanitizeEndpoint=(value:string):string=>{try{const url=new URL(value);for(const key of SENSITIVE_QUERY_KEYS)url.searchParams.delete(key);return `${url.origin}${url.pathname}${url.search}`;}catch{return "[invalid-url]";}};
const normalizeHeaders=(headers:Record<string,string|string[]|undefined>):Record<string,string|string[]|undefined>=>{const output:Record<string,string|string[]|undefined>={};for(const[key,value]of Object.entries(headers))output[key.toLowerCase()]=value;return output;};
const redactHeaders=(headers:Record<string,string|string[]|undefined>):Record<string,string|string[]|undefined>=>{const output:Record<string,string|string[]|undefined>={};for(const[key,value]of Object.entries(headers)){const normalized=key.toLowerCase();output[key]=["authorization","proxy-authorization","cookie","set-cookie","x-api-key","api-key"].includes(normalized)?"[REDACTED]":value;}return output;};
const parseRetryAfter=(headers:Record<string,string|string[]|undefined>):number|undefined=>{const raw=headers["retry-after"];const value=Array.isArray(raw)?raw[0]:raw;if(!value)return undefined;const seconds=Number(value);if(Number.isFinite(seconds)&&seconds>=0)return Math.ceil(seconds*1000);const timestamp=Date.parse(value);return Number.isNaN(timestamp)?undefined:Math.max(0,timestamp-Date.now());};
const parseJson=<T>(text:string):T|null=>{if(!text.trim())return null;try{return JSON.parse(text) as T;}catch{return null;}};
const errorMessage=(data:unknown,fallback:string):string=>{if(isRecord(data)){for(const key of ["error","message","detail","reason","description"]){const value=data[key];if(typeof value==="string"&&value.trim())return value.trim();}}if(typeof data==="string"&&data.trim())return data.trim();return fallback;};
const retryDelay=(attempt:number,retryAfterMs:number|undefined,base:number,max:number):number=>{if(retryAfterMs!==undefined)return clamp(retryAfterMs,250,max);const exponential=Math.min(max,base*Math.pow(2,Math.max(0,attempt-1)));return clamp(exponential+jitter(Math.floor(exponential*0.25)),250,max);};
const httpRequest=(url:string,options:{method:HttpMethod;headers:Record<string,string>;body?:string;timeoutMs:number;maxResponseBytes:number;maxRedirects:number;signal?:AbortSignal;redirectDepth?:number;}):Promise<{status:number;headers:Record<string,string|string[]|undefined>;text:string;url:string}>=>
new Promise((resolve,reject)=>{
let parsed:URL;
try{parsed=new URL(url);}catch{reject(new VRFSApiError(`Invalid URL: ${url}`,{code:"INVALID_URL"}));return;}
const depth=options.redirectDepth??0;
if(depth>options.maxRedirects){reject(new VRFSApiError("Too many redirects.",{code:"TOO_MANY_REDIRECTS",endpoint:sanitizeEndpoint(url)}));return;}
const transport=parsed.protocol==="https:"?https:http;
let settled=false;
let bytes=0;
const chunks:Buffer[]=[];
let timeout:NodeJS.Timeout|undefined;
let abortHandler:(()=>void)|undefined;
const cleanup=()=>{if(timeout)clearTimeout(timeout);if(options.signal&&abortHandler)options.signal.removeEventListener("abort",abortHandler);};
const rejectOnce=(error:Error)=>{if(settled)return;settled=true;cleanup();reject(error);};
const resolveOnce=(value:{status:number;headers:Record<string,string|string[]|undefined>;text:string;url:string})=>{if(settled)return;settled=true;cleanup();resolve(value);};
const request=transport.request(parsed,{method:options.method,headers:options.headers,timeout:options.timeoutMs},response=>{
const responseHeaders=normalizeHeaders(response.headers as Record<string,string|string[]|undefined>);
const status=Number(response.statusCode??0);
if([301,302,303,307,308].includes(status)&&response.headers.location){
const location=response.headers.location;
const nextLocation=Array.isArray(location)?location[0]:location;
response.resume();
if(!nextLocation){rejectOnce(new VRFSApiError("Redirect response did not include a location.",{code:"INVALID_REDIRECT",status,endpoint:sanitizeEndpoint(url)}));return;}
let nextUrl:string;
try{nextUrl=new URL(nextLocation,url).toString();}catch{rejectOnce(new VRFSApiError("Redirect location was invalid.",{code:"INVALID_REDIRECT",status,endpoint:sanitizeEndpoint(url)}));return;}
httpRequest(nextUrl,{...options,redirectDepth:depth+1}).then(resolveOnce).catch(rejectOnce);
return;
}
response.on("data",(chunk:Buffer|string)=>{
if(settled)return;
const buffer=Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk);
bytes+=buffer.length;
if(bytes>options.maxResponseBytes){response.destroy();rejectOnce(new VRFSResponseTooLargeError(`Response exceeded ${options.maxResponseBytes} bytes.`,sanitizeEndpoint(url)));return;}
chunks.push(buffer);
});
response.once("end",()=>{if(settled)return;resolveOnce({status,headers:responseHeaders,text:Buffer.concat(chunks).toString("utf8"),url});});
response.once("error",error=>rejectOnce(error));
});
timeout=setTimeout(()=>{request.destroy();rejectOnce(new VRFSTimeoutError(`Request timed out after ${options.timeoutMs}ms.`,sanitizeEndpoint(url)));},options.timeoutMs+250);
request.once("error",error=>rejectOnce(error));
abortHandler=()=>{request.destroy();rejectOnce(new VRFSApiError("Request aborted.",{code:"ABORTED",endpoint:sanitizeEndpoint(url)}));};
if(options.signal?.aborted){abortHandler();return;}
options.signal?.addEventListener("abort",abortHandler,{once:true});
if(options.body!==undefined)request.write(options.body);
request.end();
});
export class TTLCache<T>{
private readonly map=new Map<string,{value:T;expiresAt:number}>();
private readonly ttlMs:number;
private readonly maxEntries:number;
public constructor(ttlMs:number,maxEntries=10_000){this.ttlMs=Math.max(1,ttlMs);this.maxEntries=Math.max(1,Math.floor(maxEntries));}
private purgeExpired():void{const now=Date.now();for(const[key,entry]of this.map){if(entry.expiresAt<=now)this.map.delete(key);}}
public get(key:string):T|undefined{const entry=this.map.get(key);if(!entry)return undefined;if(entry.expiresAt<=Date.now()){this.map.delete(key);return undefined;}this.map.delete(key);this.map.set(key,entry);return entry.value;}
public set(key:string,value:T,ttlMs=this.ttlMs):void{this.map.delete(key);this.map.set(key,{value,expiresAt:Date.now()+Math.max(1,ttlMs)});this.purgeExpired();while(this.map.size>this.maxEntries){const oldest=this.map.keys().next().value;if(oldest===undefined)break;this.map.delete(oldest);}}
public delete(key:string):void{this.map.delete(key);}
public clear():void{this.map.clear();}
public get size():number{this.purgeExpired();return this.map.size;}
public has(key:string):boolean{return this.get(key)!==undefined;}
public cleanup():void{this.purgeExpired();}
}
export class SingleFlight{
private readonly inflight=new Map<string,Promise<unknown>>();
public run<T>(key:string,task:()=>Promise<T>):Promise<T>{const existing=this.inflight.get(key);if(existing)return existing as Promise<T>;const promise=task().finally(()=>this.inflight.delete(key));this.inflight.set(key,promise);return promise;}
public clear():void{this.inflight.clear();}
public get size():number{return this.inflight.size;}
}
export class VRFSClient{
private readonly baseUrl:string;
private readonly apiKey?:string;
private readonly requestTimeoutMs:number;
private readonly maxResponseBytes:number;
private readonly maxRedirects:number;
private readonly retries:number;
private readonly retryBaseDelayMs:number;
private readonly retryMaxDelayMs:number;
private readonly userAgent:string;
private readonly logger:VRFSLogger;
private readonly catalogCache:TTLCache<VRFSItem[]>;
private readonly marketplaceCache:TTLCache<VRFSMarketplaceItem[]>;
private readonly playerCache:TTLCache<VRFSUser>;
private readonly profileCache:TTLCache<VRFSProfile>;
private readonly outfitsCache:TTLCache<VRFSOutfit[]>;
private readonly requestFlights=new SingleFlight();
private localRateLimitedUntil=0;
public constructor(options:VRFSClientOptions={}){this.baseUrl=normalizeBaseUrl(options.baseUrl??DEFAULTS.vrfsBaseUrl);this.apiKey=options.apiKey??process.env.VRFS_API_KEY;this.requestTimeoutMs=Math.max(100,options.requestTimeoutMs??DEFAULTS.requestTimeoutMs);this.maxResponseBytes=Math.max(1024,options.maxResponseBytes??DEFAULTS.maxResponseBytes);this.maxRedirects=Math.max(0,Math.floor(options.maxRedirects??DEFAULTS.maxRedirects));this.retries=clamp(Math.floor(options.retries??DEFAULTS.retries),0,10);this.retryBaseDelayMs=Math.max(50,options.retryBaseDelayMs??DEFAULTS.retryBaseDelayMs);this.retryMaxDelayMs=Math.max(this.retryBaseDelayMs,options.retryMaxDelayMs??DEFAULTS.retryMaxDelayMs);this.userAgent=options.userAgent??DEFAULTS.userAgent;this.logger=options.logger??console;this.catalogCache=new TTLCache(options.catalogTtlMs??DEFAULTS.catalogTtlMs,10);this.marketplaceCache=new TTLCache(options.marketplaceTtlMs??DEFAULTS.marketplaceTtlMs,10);this.playerCache=new TTLCache(options.playerTtlMs??DEFAULTS.playerTtlMs,5_000);this.profileCache=new TTLCache(options.playerTtlMs??DEFAULTS.playerTtlMs,5_000);this.outfitsCache=new TTLCache(options.playerTtlMs??DEFAULTS.playerTtlMs,5_000);}
public get configuration():Readonly<Record<string,unknown>>{return{baseUrl:this.baseUrl,hasApiKey:Boolean(this.apiKey),requestTimeoutMs:this.requestTimeoutMs,maxResponseBytes:this.maxResponseBytes,maxRedirects:this.maxRedirects,retries:this.retries,userAgent:this.userAgent};}
private buildUrl(pathname:string,params:Record<string,unknown>={}):string{const url=new URL(pathname,`${this.baseUrl}/`);for(const[key,value]of Object.entries(params)){if(value!==undefined&&value!==null)url.searchParams.set(key,String(value));}return url.toString();}
private async waitRateLimit(signal?:AbortSignal):Promise<void>{while(Date.now()<this.localRateLimitedUntil)await sleep(Math.min(this.localRateLimitedUntil-Date.now(),1_000),signal);}
private shouldRetryStatus(status:number):boolean{return RETRYABLE_HTTP_STATUSES.has(status)&&!NON_RETRYABLE_HTTP_STATUSES.has(status);}
private async execute<T>(url:string,options:VRFSRequestOptions={}):Promise<VRFSRequestResult<T>>{
const method=options.method??"GET";
const retries=clamp(Math.floor(options.retries??this.retries),0,10);
const timeoutMs=options.timeoutMs??this.requestTimeoutMs;
const maxResponseBytes=options.maxResponseBytes??this.maxResponseBytes;
const maxRedirects=options.maxRedirects??this.maxRedirects;
const base=options.retryBaseDelayMs??this.retryBaseDelayMs;
const maxDelay=options.retryMaxDelayMs??this.retryMaxDelayMs;
const started=Date.now();
let lastError:VRFSApiError|undefined;
for(let attempt=1;attempt<=retries+1;attempt++){
await this.waitRateLimit(options.signal);
let body:string|undefined;
const headers:Record<string,string>={Accept:"application/json, text/plain, */*","User-Agent":this.userAgent,...options.headers};
if(options.body!==undefined){try{body=typeof options.body==="string"?options.body:JSON.stringify(options.body);}catch(error){throw new VRFSApiError("Failed to serialize request body.",{code:"REQUEST_SERIALIZATION",endpoint:sanitizeEndpoint(url),details:error});}headers["Content-Type"]??="application/json";headers["Content-Length"]=String(Buffer.byteLength(body));}
try{
const response=await httpRequest(url,{method,headers,body,timeoutMs,maxResponseBytes,maxRedirects,signal:options.signal});
const retryAfterMs=parseRetryAfter(response.headers);
const parsed=parseJson<T>(response.text);
if(response.status===429){this.localRateLimitedUntil=Math.max(this.localRateLimitedUntil,Date.now()+(retryAfterMs??1_000));lastError=new VRFSRateLimitError("Rate limit received from upstream.",retryAfterMs??1_000,sanitizeEndpoint(url),attempt);if(attempt<=retries){await sleep(retryDelay(attempt,retryAfterMs,base,maxDelay),options.signal);continue;}return{status:"rate_limited",httpStatus:429,data:parsed,rawText:response.text,headers:redactHeaders(response.headers),attempts:attempt,durationMs:Date.now()-started,retryAfterMs,url:sanitizeEndpoint(url)};}
if(response.status<200||response.status>=300){const retryable=this.shouldRetryStatus(response.status);lastError=new VRFSApiError(errorMessage(parsed,`Upstream returned HTTP ${response.status}.`),{code:`HTTP_${response.status}`,status:response.status,retryable,retryAfterMs,endpoint:sanitizeEndpoint(url),details:parsed??response.text.slice(0,2_000),attempts:attempt});if(retryable&&attempt<=retries){await sleep(retryDelay(attempt,retryAfterMs,base,maxDelay),options.signal);continue;}return{status:"http_error",httpStatus:response.status,data:parsed,rawText:response.text,headers:redactHeaders(response.headers),attempts:attempt,durationMs:Date.now()-started,retryAfterMs,url:sanitizeEndpoint(url)};}
if(response.text.trim()&&parsed===null)return{status:"invalid_json",httpStatus:response.status,data:null,rawText:response.text,headers:redactHeaders(response.headers),attempts:attempt,durationMs:Date.now()-started,url:sanitizeEndpoint(url)};
return{status:"success",httpStatus:response.status,data:parsed,rawText:response.text,headers:redactHeaders(response.headers),attempts:attempt,durationMs:Date.now()-started,url:sanitizeEndpoint(url)};
}catch(error){
if(error instanceof VRFSApiError)lastError=error;
else lastError=new VRFSApiError(error instanceof Error?error.message:"Unknown network error.",{code:"NETWORK_ERROR",retryable:true,endpoint:sanitizeEndpoint(url),attempts:attempt,details:error});
if(!lastError.retryable||attempt>retries)break;
const wait=retryDelay(attempt,lastError.retryAfterMs,base,maxDelay);
this.logger.warn?.("[VRFS] Retrying request.",{endpoint:sanitizeEndpoint(url),method,attempt,maxAttempts:retries+1,waitMs:wait,error:lastError.message});
await sleep(wait,options.signal);
}
}
if(lastError instanceof VRFSTimeoutError)return{status:"timeout",httpStatus:0,data:null,rawText:"",headers:{},attempts:lastError.attempts,durationMs:Date.now()-started,url:sanitizeEndpoint(url)};
if(lastError?.code==="ABORTED")return{status:"aborted",httpStatus:0,data:null,rawText:"",headers:{},attempts:lastError.attempts,durationMs:Date.now()-started,url:sanitizeEndpoint(url)};
return{status:"network_error",httpStatus:lastError?.status??0,data:null,rawText:"",headers:{},attempts:lastError?.attempts??retries+1,durationMs:Date.now()-started,url:sanitizeEndpoint(url)};
}
private async request<T>(pathname:string,params:Record<string,unknown>={},options:VRFSRequestOptions={}):Promise<T>{
const url=this.buildUrl(pathname,params);
const result=await this.execute<T>(url,options);
if(result.status!=="success")throw new VRFSApiError(`VRFS request failed: ${result.status}.`,{code:result.status.toUpperCase(),status:result.httpStatus,retryable:result.status==="rate_limited"||result.status==="timeout"||result.status==="network_error",retryAfterMs:result.retryAfterMs,endpoint:result.url,details:result.data??result.rawText.slice(0,2_000),attempts:result.attempts});
if(result.data===null&&result.rawText.trim())throw new VRFSInvalidResponseError("VRFS returned an empty parsed response.",result.url,null,result.attempts);
return result.data as T;
}
private requireUid(uid:number):number{if(!Number.isInteger(uid)||uid<=0)throw new VRFSApiError("UID must be a positive integer.",{code:"INVALID_UID"});return uid;}
private requireApiKey():string{if(!this.apiKey)throw new VRFSApiError("VRFS_API_KEY is not configured.",{code:"MISSING_API_KEY"});return this.apiKey;}
public async getUsername(uid:number,signal?:AbortSignal,forceRefresh=false):Promise<VRFSUser>{const validUid=this.requireUid(uid);const key=`user:${validUid}`;if(!forceRefresh){const cached=this.playerCache.get(key);if(cached)return cached;}return this.requestFlights.run(`username:${validUid}`,async()=>{const data=await this.request<unknown>(VRFS_ENDPOINTS.username,{uid:validUid},{signal});const user=normalizeUser(data,validUid);this.playerCache.set(key,user);return user;});}
public async getProfile(uid:number,signal?:AbortSignal,forceRefresh=false):Promise<VRFSProfile>{const validUid=this.requireUid(uid);const key=`profile:${validUid}`;if(!forceRefresh){const cached=this.profileCache.get(key);if(cached)return cached;}const apiKey=this.requireApiKey();return this.requestFlights.run(`profile:${validUid}`,async()=>{const data=await this.request<unknown>(VRFS_ENDPOINTS.profile,{uid:validUid,key:apiKey},{signal});const profile=normalizeProfile(data,validUid);this.profileCache.set(key,profile);return profile;});}
public async getOutfits(uid:number,signal?:AbortSignal,forceRefresh=false):Promise<VRFSOutfit[]>{const validUid=this.requireUid(uid);const key=`outfits:${validUid}`;if(!forceRefresh){const cached=this.outfitsCache.get(key);if(cached)return cached;}const apiKey=this.requireApiKey();return this.requestFlights.run(`outfits:${validUid}`,async()=>{const data=await this.request<unknown>(VRFS_ENDPOINTS.outfits,{uid:validUid,key:apiKey},{signal});const outfits=normalizeOutfits(data);this.outfitsCache.set(key,outfits);return outfits;});}
public async getPlayer(uid:number,signal?:AbortSignal,forceRefresh=false):Promise<VRFSUser&{profile?:VRFSProfile;outfits?:VRFSOutfit[]}>{const validUid=this.requireUid(uid);const[username,profile,outfits]=await Promise.allSettled([this.getUsername(validUid,signal,forceRefresh),this.getProfile(validUid,signal,forceRefresh),this.getOutfits(validUid,signal,forceRefresh)]);if(username.status==="rejected")throw username.reason;return{...username.value,...(profile.status==="fulfilled"?{profile:profile.value}:{}),...(outfits.status==="fulfilled"?{outfits:outfits.value}:{})};}
public async getMarketplace(signal?:AbortSignal,forceRefresh=false):Promise<VRFSMarketplaceItem[]>{if(!forceRefresh){const cached=this.marketplaceCache.get("all");if(cached)return cached;}const apiKey=this.requireApiKey();return this.requestFlights.run("marketplace:all",async()=>{const data=await this.request<unknown>(VRFS_ENDPOINTS.marketplace,{key:apiKey},{signal});const items=normalizeMarketplace(data);if(!items.length&&!looksLikeEmptyMarketplaceResponse(data))throw new VRFSInvalidResponseError("Marketplace response contained no valid items.",this.buildUrl(VRFS_ENDPOINTS.marketplace),data);this.marketplaceCache.set("all",items);return items;});}
public async searchMarketplace(query:string,limit=100,signal?:AbortSignal):Promise<VRFSMarketplaceItem[]>{const normalized=normalizeSearch(query);if(!normalized)return[];const marketplace=await this.getMarketplace(signal);return marketplace.map(item=>({item,score:marketplaceScore(item,normalized)})).filter(entry=>entry.score>0).sort((a,b)=>b.score-a.score||String(a.item.title??"").localeCompare(String(b.item.title??""))).slice(0,Math.max(1,limit)).map(entry=>entry.item);}
public async getMarketplaceItem(query:string|number,signal?:AbortSignal):Promise<VRFSMarketplaceItem|null>{const normalized=normalizeSearch(query);if(!normalized)return null;const marketplace=await this.getMarketplace(signal);const direct=marketplace.find(item=>String(item.id).toLowerCase()===normalized||String(item.sku??"").toLowerCase()===normalized);if(direct)return direct;const results=await this.searchMarketplace(normalized,10,signal);return results.length===1?results[0]:null;}
public async getCatalog(signal?:AbortSignal,forceRefresh=false):Promise<VRFSItem[]>{if(!forceRefresh){const cached=this.catalogCache.get("all");if(cached)return cached;}return this.requestFlights.run("catalog:all",async()=>{const data=await this.request<unknown>(SEBY_ENDPOINTS.items,{},{signal});const items=normalizeItems(data);if(!items.length&&!looksLikeEmptyCatalogResponse(data))throw new VRFSInvalidResponseError("Catalog response contained no valid items.",this.sebyCatalogEndpoint(),data);this.catalogCache.set("all",items);return items;});}
private sebyCatalogEndpoint():string{return `${normalizeBaseUrl(DEFAULTS.sebyBaseUrl)}${SEBY_ENDPOINTS.items}`;}
public async searchCatalog(query:string,limit=100,signal?:AbortSignal):Promise<VRFSItem[]>{const normalized=normalizeSearch(query);if(!normalized)return[];const catalog=await this.getCatalog(signal);return catalog.map(item=>({item,score:catalogScore(item,normalized)})).filter(entry=>entry.score>0).sort((a,b)=>b.score-a.score||getItemName(a.item).localeCompare(getItemName(b.item))).slice(0,Math.max(1,limit)).map(entry=>entry.item);}
public async getCatalogItem(query:string|number,signal?:AbortSignal):Promise<VRFSItem|null>{const normalized=normalizeSearch(query);if(!normalized)return null;const catalog=await this.getCatalog(signal);const direct=catalog.find(item=>String(item.id??item.item_id??item.itemId??"").toLowerCase()===normalized||getSku(item).toLowerCase()===normalized);if(direct)return direct;const exactName=catalog.find(item=>getItemName(item).toLowerCase()===normalized);if(exactName)return exactName;const results=await this.searchCatalog(normalized,10,signal);return results.length===1?results[0]:null;}
public async checkOwnership(uid:number,skus:string[],options:OwnershipCheckOptions={}):Promise<OwnershipCheckResult>{return seby.checkOwnership(uid,skus,options);}
public getCatalogStats(catalog:VRFSItem[]=this.catalogCache.get("all")??[]):CatalogStats{const sections=new Set(catalog.map(item=>getSection(item)).filter(Boolean));const free=catalog.filter(isItemFree).length;return{items:catalog.length,sections:sections.size,free,premium:Math.max(0,catalog.length-free),loadedAt:Date.now(),ageMs:0};}
public getMarketplaceStats(marketplace:VRFSMarketplaceItem[]=this.marketplaceCache.get("all")??[]):MarketplaceStats{const active=marketplace.filter(getMarketplaceActive).length;const creators=new Set(marketplace.map(getMarketplaceCreatorUid).filter(Boolean)).size;const owners=marketplace.reduce((sum,item)=>sum+getMarketplaceOwners(item),0);return{items:marketplace.length,active,inactive:Math.max(0,marketplace.length-active),creators,owners,loadedAt:Date.now(),ageMs:0};}
public clearCaches():void{this.catalogCache.clear();this.marketplaceCache.clear();this.playerCache.clear();this.profileCache.clear();this.outfitsCache.clear();}
public clearPlayerCache(uid?:number):void{if(uid===undefined){this.playerCache.clear();this.profileCache.clear();this.outfitsCache.clear();return;}const validUid=this.requireUid(uid);this.playerCache.delete(`user:${validUid}`);this.profileCache.delete(`profile:${validUid}`);this.outfitsCache.delete(`outfits:${validUid}`);}
public getCacheStats():Record<string,number>{return{catalog:this.catalogCache.size,marketplace:this.marketplaceCache.size,players:this.playerCache.size,profiles:this.profileCache.size,outfits:this.outfitsCache.size,singleFlight:this.requestFlights.size};}
public async health(uid=1,signal?:AbortSignal):Promise<VRFSServiceHealth>{const started=Date.now();try{await this.getUsername(this.requireUid(uid),signal,true);return{service:"vrfs",ok:true,health:"ok",latencyMs:Date.now()-started};}catch(error){return{service:"vrfs",ok:false,health:"unknown",latencyMs:Date.now()-started,error:error instanceof Error?error.message:String(error)};}}
}
export class SebyLockerClient{
private readonly baseUrl:string;
private readonly requestTimeoutMs:number;
private readonly maxResponseBytes:number;
private readonly maxRedirects:number;
private readonly retries:number;
private readonly retryBaseDelayMs:number;
private readonly retryMaxDelayMs:number;
private readonly userAgent:string;
private readonly logger:VRFSLogger;
private readonly requestFlights=new SingleFlight();
private localRateLimitedUntil=0;
private readonly defaultBatchSize:number;
private readonly defaultMinBatchSize:number;
private readonly defaultMaxBatchSize:number;
private readonly defaultRetryCount:number;
private readonly defaultDelayMs:number;
public constructor(options:VRFSClientOptions={}){this.baseUrl=normalizeBaseUrl(options.baseUrl??DEFAULTS.sebyBaseUrl);this.requestTimeoutMs=Math.max(100,options.requestTimeoutMs??DEFAULTS.requestTimeoutMs);this.maxResponseBytes=Math.max(1024,options.maxResponseBytes??DEFAULTS.maxResponseBytes);this.maxRedirects=Math.max(0,Math.floor(options.maxRedirects??DEFAULTS.maxRedirects));this.retries=clamp(Math.floor(options.retries??DEFAULTS.retries),0,10);this.retryBaseDelayMs=Math.max(50,options.retryBaseDelayMs??DEFAULTS.retryBaseDelayMs);this.retryMaxDelayMs=Math.max(this.retryBaseDelayMs,options.retryMaxDelayMs??DEFAULTS.retryMaxDelayMs);this.userAgent=options.userAgent??DEFAULTS.userAgent;this.logger=options.logger??console;this.defaultBatchSize=clamp(Math.floor(options.ownershipBatchSize??DEFAULTS.ownershipBatchSize),1,500);this.defaultMinBatchSize=clamp(Math.floor(options.ownershipMinBatchSize??DEFAULTS.ownershipMinBatchSize),1,this.defaultBatchSize);this.defaultMaxBatchSize=clamp(Math.floor(options.ownershipMaxBatchSize??DEFAULTS.ownershipMaxBatchSize),this.defaultMinBatchSize,500);this.defaultRetryCount=clamp(Math.floor(options.ownershipRetryCount??DEFAULTS.ownershipRetryCount),0,5);this.defaultDelayMs=Math.max(0,Math.floor(options.ownershipDelayMs??DEFAULTS.ownershipDelayMs));}
private buildUrl(pathname:string):string{return new URL(pathname,`${this.baseUrl}/`).toString();}
private async waitRateLimit(signal?:AbortSignal):Promise<void>{while(Date.now()<this.localRateLimitedUntil)await sleep(Math.min(this.localRateLimitedUntil-Date.now(),1_000),signal);}
private shouldRetryStatus(status:number):boolean{return RETRYABLE_HTTP_STATUSES.has(status)&&!NON_RETRYABLE_HTTP_STATUSES.has(status);}
private async execute<T>(url:string,options:VRFSRequestOptions={}):Promise<VRFSRequestResult<T>>{
const method=options.method??"GET";
const retries=clamp(Math.floor(options.retries??this.retries),0,10);
const timeoutMs=options.timeoutMs??this.requestTimeoutMs;
const maxResponseBytes=options.maxResponseBytes??this.maxResponseBytes;
const maxRedirects=options.maxRedirects??this.maxRedirects;
const base=options.retryBaseDelayMs??this.retryBaseDelayMs;
const maxDelay=options.retryMaxDelayMs??this.retryMaxDelayMs;
const started=Date.now();
let lastError:VRFSApiError|undefined;
for(let attempt=1;attempt<=retries+1;attempt++){
await this.waitRateLimit(options.signal);
let body:string|undefined;
const headers:Record<string,string>={Accept:"application/json, text/plain, */*","User-Agent":this.userAgent,...options.headers};
if(options.body!==undefined){body=typeof options.body==="string"?options.body:JSON.stringify(options.body);headers["Content-Type"]??="application/json";headers["Content-Length"]=String(Buffer.byteLength(body));}
try{
const response=await httpRequest(url,{method,headers,body,timeoutMs,maxResponseBytes,maxRedirects,signal:options.signal});
const retryAfterMs=parseRetryAfter(response.headers);
const parsed=parseJson<T>(response.text);
if(response.status===429){this.localRateLimitedUntil=Math.max(this.localRateLimitedUntil,Date.now()+(retryAfterMs??1_000));lastError=new VRFSRateLimitError("Seby rate limit received.",retryAfterMs??1_000,sanitizeEndpoint(url),attempt);if(attempt<=retries){await sleep(retryDelay(attempt,retryAfterMs,base,maxDelay),options.signal);continue;}return{status:"rate_limited",httpStatus:429,data:parsed,rawText:response.text,headers:redactHeaders(response.headers),attempts:attempt,durationMs:Date.now()-started,retryAfterMs,url:sanitizeEndpoint(url)};}
if(response.status<200||response.status>=300){const retryable=this.shouldRetryStatus(response.status);lastError=new VRFSApiError(errorMessage(parsed,`Seby returned HTTP ${response.status}.`),{code:`HTTP_${response.status}`,status:response.status,retryable,retryAfterMs,endpoint:sanitizeEndpoint(url),details:parsed??response.text.slice(0,2_000),attempts:attempt});if(retryable&&attempt<=retries){await sleep(retryDelay(attempt,retryAfterMs,base,maxDelay),options.signal);continue;}return{status:"http_error",httpStatus:response.status,data:parsed,rawText:response.text,headers:redactHeaders(response.headers),attempts:attempt,durationMs:Date.now()-started,retryAfterMs,url:sanitizeEndpoint(url)};}
if(response.text.trim()&&parsed===null)return{status:"invalid_json",httpStatus:response.status,data:null,rawText:response.text,headers:redactHeaders(response.headers),attempts:attempt,durationMs:Date.now()-started,url:sanitizeEndpoint(url)};
return{status:"success",httpStatus:response.status,data:parsed,rawText:response.text,headers:redactHeaders(response.headers),attempts:attempt,durationMs:Date.now()-started,url:sanitizeEndpoint(url)};
}catch(error){
if(error instanceof VRFSApiError)lastError=error;
else lastError=new VRFSApiError(error instanceof Error?error.message:"Unknown network error.",{code:"NETWORK_ERROR",retryable:true,endpoint:sanitizeEndpoint(url),attempts:attempt,details:error});
if(!lastError.retryable||attempt>retries)break;
const wait=retryDelay(attempt,lastError.retryAfterMs,base,maxDelay);
this.logger.warn?.("[SEBY] Retrying request.",{endpoint:sanitizeEndpoint(url),attempt,maxAttempts:retries+1,waitMs:wait,error:lastError.message});
await sleep(wait,options.signal);
}
}
if(lastError?.code==="TIMEOUT")return{status:"timeout",httpStatus:0,data:null,rawText:"",headers:{},attempts:lastError.attempts,durationMs:Date.now()-started,url:sanitizeEndpoint(url)};
if(lastError?.code==="ABORTED")return{status:"aborted",httpStatus:0,data:null,rawText:"",headers:{},attempts:lastError.attempts,durationMs:Date.now()-started,url:sanitizeEndpoint(url)};
return{status:"network_error",httpStatus:lastError?.status??0,data:null,rawText:"",headers:{},attempts:lastError?.attempts??retries+1,durationMs:Date.now()-started,url:sanitizeEndpoint(url)};
}
private enqueue<T>(key:string,task:()=>Promise<T>):Promise<T>{return this.requestFlights.run(key,task);}
private resultError(message:string,result:VRFSRequestResult<unknown>):VRFSApiError{return new VRFSApiError(message,{code:result.status.toUpperCase(),status:result.httpStatus,retryable:result.status==="rate_limited"||result.status==="timeout"||result.status==="network_error",retryAfterMs:result.retryAfterMs,endpoint:result.url,details:result.data??result.rawText.slice(0,2_000),attempts:result.attempts});}
private async post<T>(pathname:string,body:unknown,signal?:AbortSignal):Promise<T>{const url=this.buildUrl(pathname);const result=await this.execute<T>(url,{method:"POST",body,signal});if(result.status!=="success")throw this.resultError(`Seby request failed: ${result.status}.`,result);if(result.data===null)throw new VRFSInvalidResponseError("Seby returned an empty response.",result.url);return result.data as T;}
public async getItems(signal?:AbortSignal):Promise<VRFSItem[]>{return this.enqueue("seby:items",async()=>{const result=await this.execute<unknown>(this.buildUrl(SEBY_ENDPOINTS.items),{signal});if(result.status!=="success")throw this.resultError("Unable to retrieve catalog.",result);const items=normalizeItems(result.data);if(!items.length&&!looksLikeEmptyCatalogResponse(result.data))throw new VRFSInvalidResponseError("Catalog contained no valid entries.",result.url,result.data,result.attempts);return items;});}
public async lookup(uid:number,skus:string[],signal?:AbortSignal):Promise<SebyLookupResponse>{const validUid=requireUid(uid);const normalized=uniqueStrings(skus);if(!normalized.length)throw new VRFSApiError("At least one SKU is required.",{code:"EMPTY_SKU_LIST"});const response=await this.post<unknown>(SEBY_ENDPOINTS.lookup,{uid:String(validUid),skus:normalized},signal);return normalizeSebyLookupResponse(response);}
public async checkOwnershipRequest(uid:number,skus:string[],signal?:AbortSignal):Promise<SebyOwnershipResponse>{const validUid=requireUid(uid);const normalized=uniqueStrings(skus);if(!normalized.length)throw new VRFSApiError("At least one SKU is required.",{code:"EMPTY_SKU_LIST"});const response=await this.post<unknown>(SEBY_ENDPOINTS.check,{uid:String(validUid),skus:normalized},signal);return normalizeSebyResponse(response);}
public async checkOwnership(uid:number,skus:string[],options:OwnershipCheckOptions={}):Promise<OwnershipCheckResult>{
const validUid=requireUid(uid);
const normalized=uniqueStrings(skus);
const started=Date.now();
const results:Record<string,OwnershipValue>={};
if(!normalized.length)return{uid:validUid,results,unknown:[],owned:[],notOwned:[],requested:0,confirmed:0,cached:0,apiRequests:0,retries:0,batches:0,durationMs:0,health:"ok",partial:false};
const minBatchSize=Math.max(1,Math.floor(options.minBatchSize??this.defaultMinBatchSize));
const maxBatchSize=Math.min(500,Math.max(minBatchSize,Math.floor(options.maxBatchSize??this.defaultMaxBatchSize)));
let batchSize=clamp(Math.floor(options.batchSize??this.defaultBatchSize),minBatchSize,maxBatchSize);
const delayMs=Math.max(0,Math.floor(options.delayMs??this.defaultDelayMs));
const maxMissingRetries=clamp(Math.floor(options.maxMissingRetries??this.defaultRetryCount),0,5);
let apiRequests=0;
let retries=0;
let batches=0;
let cached=0;
let health:ServiceHealth="ok";
let remaining=normalized.slice();
let batchNumber=0;
while(remaining.length){
if(options.signal?.aborted)throw new VRFSApiError("Ownership request aborted.",{code:"ABORTED"});
const batch=remaining.slice(0,batchSize);
batchNumber++;
try{
const response=await this.checkOwnershipRequest(validUid,batch,options.signal);
apiRequests++;
batches++;
cached+=Number.isFinite(response.cached)?response.cached:0;
if(response.health==="degraded")health="degraded";
for(const sku of batch){const value=response.results[sku];if(typeof value==="boolean")results[sku]=value;}
remaining=remaining.slice(batch.length);
let missing=batch.filter(sku=>results[sku]===undefined);
let retryBatchSize=Math.max(minBatchSize,Math.min(batchSize,Math.ceil(batchSize/2)));
for(let attempt=1;attempt<=maxMissingRetries&&missing.length;attempt++){
retries++;
if(delayMs>0)await sleep(delayMs*(attempt+1),options.signal);
const unresolved:string[]=[];
for(const retryBatch of chunk(missing,retryBatchSize)){
try{
const retryResponse=await this.checkOwnershipRequest(validUid,retryBatch,options.signal);
apiRequests++;
batches++;
cached+=Number.isFinite(retryResponse.cached)?retryResponse.cached:0;
if(retryResponse.health==="degraded")health="degraded";
for(const sku of retryBatch){const value=retryResponse.results[sku];if(typeof value==="boolean")results[sku]=value;}
unresolved.push(...retryBatch.filter(sku=>results[sku]===undefined));
}catch(error){
if(error instanceof VRFSRateLimitError||error instanceof VRFSApiError&&error.status===429){await sleep(error instanceof VRFSRateLimitError?error.retryAfterMs??1_000:1_000,options.signal);continue;}
unresolved.push(...retryBatch);
}
}
missing=uniqueStrings(unresolved);
retryBatchSize=Math.max(minBatchSize,Math.floor(retryBatchSize/2));
}
for(const sku of missing)if(results[sku]===undefined)results[sku]="unknown";
options.onProgress?.({processed:normalized.length-remaining.length,total:normalized.length,confirmed:Object.values(results).filter(value=>typeof value==="boolean").length,unknown:Object.values(results).filter(value=>value==="unknown").length,owned:Object.values(results).filter(value=>value===true).length,notOwned:Object.values(results).filter(value=>value===false).length,batchNumber,batchSize,health,cached});
if(remaining.length&&delayMs>0)await sleep(delayMs,options.signal);
}catch(error){
if(error instanceof VRFSApiError&&[400,413,414].includes(error.status)&&batchSize>minBatchSize){batchSize=Math.max(minBatchSize,Math.floor(batchSize/2));this.logger.warn?.("[SEBY] Reducing ownership batch size.",{previousSize:batch.length,newSize:batchSize,status:error.status});continue;}
if(error instanceof VRFSRateLimitError||error instanceof VRFSApiError&&error.status===429){retries++;await sleep(error instanceof VRFSRateLimitError?error.retryAfterMs??1_000:1_000,options.signal);continue;}
for(const sku of batch)if(results[sku]===undefined)results[sku]="unknown";
health="degraded";
remaining=remaining.slice(batch.length);
}
}
for(const sku of normalized)if(results[sku]===undefined)results[sku]="unknown";
const owned=normalized.filter(sku=>results[sku]===true);
const notOwned=normalized.filter(sku=>results[sku]===false);
const unknown=normalized.filter(sku=>results[sku]==="unknown");
return{uid:validUid,results,unknown,owned,notOwned,requested:normalized.length,confirmed:owned.length+notOwned.length,cached,apiRequests,retries,batches,durationMs:Date.now()-started,health,partial:unknown.length>0};
}
public async checkOwnershipBatched(uid:number,skus:string[],options:OwnershipCheckOptions={}):Promise<OwnershipCheckResult>{return this.checkOwnership(uid,skus,options);}
public async health(signal?:AbortSignal):Promise<VRFSServiceHealth>{const started=Date.now();try{const result=await this.getItems(signal);return{service:"seby",ok:Array.isArray(result),health:"ok",latencyMs:Date.now()-started};}catch(error){return{service:"seby",ok:false,health:"unknown",latencyMs:Date.now()-started,error:error instanceof Error?error.message:String(error)};}}
public clearFlights():void{this.requestFlights.clear();}
public getStatus():Record<string,unknown>{return{baseUrl:this.baseUrl,rateLimitedForMs:Math.max(0,this.localRateLimitedUntil-Date.now()),inflightRequests:this.requestFlights.size};}
}
function requireUid(uid:number):number{if(!Number.isInteger(uid)||uid<=0)throw new VRFSApiError("UID must be a positive integer.",{code:"INVALID_UID"});return uid;}
function unwrapData(input:unknown):unknown{if(!isRecord(input))return input;if("data" in input&&input.data!==undefined){if(isRecord(input.data)&&"data" in input.data&&input.data.data!==undefined)return input.data.data;return input.data;}return input;}
function normalizeUser(input:unknown,fallbackUid:number):VRFSUser{
if(typeof input==="string"){const username=input.trim();if(!username)throw new VRFSInvalidResponseError("Username endpoint returned an empty username.");return{uid:fallbackUid,username};}
const root=unwrapData(input);
if(!isRecord(root))throw new VRFSInvalidResponseError("Username response was not valid.",undefined,root);
const uid=asPositiveInt(root.uid??root.userId??root.id)??fallbackUid;
const username=asString(root.username??root.nickname??root.name).trim();
if(!username)throw new VRFSInvalidResponseError("Username response did not contain a username.",undefined,root);
return{uid,username,nickname:typeof root.nickname==="string"?root.nickname:undefined,name:typeof root.name==="string"?root.name:undefined};
}
function normalizeProfile(input:unknown,fallbackUid:number):VRFSProfile{
const root=unwrapData(input);
if(!isRecord(root))throw new VRFSInvalidResponseError("Profile response was not valid.",undefined,root);
const profile:VRFSProfile={...root};
profile.uid=asPositiveInt(root.uid??root.userId??root.id)??fallbackUid;
if(typeof root.requestResult==="string")profile.requestResult=root.requestResult;
if(root.followersCount!==undefined){const followers=Number(root.followersCount);if(Number.isFinite(followers))profile.followersCount=followers;}
if(root.followingCount!==undefined){const following=Number(root.followingCount);if(Number.isFinite(following))profile.followingCount=following;}
for(const key of ["profileCountry","userTag","tiktokName","youtubeName","twitchName","instagramName"])if(typeof root[key]==="string")profile[key]=root[key];
return profile;
}
function normalizeOutfits(input:unknown):VRFSOutfit[]{
const root=unwrapData(input);
if(isRecord(input)&&typeof input.status==="string"&&input.status.toLowerCase()==="success"&&Array.isArray(input.data)&&input.data.length===0)return[];
const array=Array.isArray(root)?root:isRecord(root)&&Array.isArray(root.outfits)?root.outfits:[];
return array.filter(isRecord).map(item=>{const slots=isRecord(item.slots)?Object.fromEntries(Object.entries(item.slots).map(([key,value])=>[key,value===null?null:String(value)])):undefined;return{...item,id:typeof item.id==="number"||typeof item.id==="string"?item.id:undefined,name:typeof item.name==="string"?item.name:undefined,slots};});
}
function normalizeItems(input:unknown):VRFSItem[]{
const root=unwrapData(input);
const array=Array.isArray(root)?root:isRecord(root)&&Array.isArray(root.items)?root.items:[];
return array.filter(isRecord).map(item=>({...item,id:item.id as string|number|undefined,item_id:item.item_id as string|number|undefined,itemId:item.itemId as string|number|undefined,sku:typeof item.sku==="string"?item.sku:undefined,sku_base:typeof item.sku_base==="string"?item.sku_base:undefined,name:typeof item.name==="string"?item.name:undefined,title:typeof item.title==="string"?item.title:undefined,section:typeof item.section==="string"?item.section:undefined,category:typeof item.category==="string"?item.category:undefined,category_name:typeof item.category_name==="string"?item.category_name:undefined,thumb:typeof item.thumb==="string"?item.thumb:undefined,image:typeof item.image==="string"?item.image:undefined,image_url:typeof item.image_url==="string"?item.image_url:undefined,thumbnail:typeof item.thumbnail==="string"?item.thumbnail:undefined,thumbnail_url:typeof item.thumbnail_url==="string"?item.thumbnail_url:undefined,texture_url:typeof item.texture_url==="string"?item.texture_url:undefined,isFree:item.isFree as boolean|number|string|undefined,price:item.price as string|number|undefined,coins:item.coins as number|string|undefined,coins_price:item.coins_price as number|string|undefined,credits:item.credits as number|string|undefined})).filter(item=>getSku(item).length>0);
}
function normalizeMarketplace(input:unknown):VRFSMarketplaceItem[]{
const root=unwrapData(input);
const array=Array.isArray(root)?root:isRecord(root)&&Array.isArray(root.items)?root.items:[];
return array.filter(isRecord).filter(item=>item.id!==undefined&&item.id!==null).map(item=>{
const author=isRecord(item.author)?{...item.author,uid:item.author.uid as string|number|undefined,username:typeof item.author.username==="string"?item.author.username:undefined,nickname:typeof item.author.nickname==="string"?item.author.nickname:undefined,name:typeof item.author.name==="string"?item.author.name:undefined,avatar_url:typeof item.author.avatar_url==="string"?item.author.avatar_url:undefined,badge_id:item.author.badge_id as number|string|undefined,highlight_color:typeof item.author.highlight_color==="string"?item.author.highlight_color:undefined}:undefined;
return{...item,id:item.id as string|number,title:typeof item.title==="string"?item.title:undefined,name:typeof item.name==="string"?item.name:undefined,sku:typeof item.sku==="string"?item.sku:undefined,sku_base:typeof item.sku_base==="string"?item.sku_base:undefined,category_id:item.category_id as string|number|undefined,is_active:item.is_active as boolean|number|string|undefined,is_visible:item.is_visible as boolean|number|string|undefined,owners_count:item.owners_count as number|string|undefined,gifts_left:item.gifts_left as number|string|undefined,coins_price:item.coins_price as number|string|undefined,owner_uid:item.owner_uid as string|number|undefined,creator_uid:item.creator_uid as string|number|undefined,creator:typeof item.creator==="string"?item.creator:undefined,texture_id:item.texture_id as string|number|undefined,texture_url:typeof item.texture_url==="string"?item.texture_url:undefined,thumbnail_url:typeof item.thumbnail_url==="string"?item.thumbnail_url:undefined,thumbnail_url_ktx:typeof item.thumbnail_url_ktx==="string"?item.thumbnail_url_ktx:undefined,subtitle:typeof item.subtitle==="string"?item.subtitle:undefined,highlight_type:typeof item.highlight_type==="string"?item.highlight_type:undefined,created_at:typeof item.created_at==="string"?item.created_at:undefined,updated_at:typeof item.updated_at==="string"?item.updated_at:undefined,activated_at:typeof item.activated_at==="string"?item.activated_at:undefined,available_for_send:item.available_for_send as number|string|boolean|undefined,badges:Array.isArray(item.badges)?item.badges:undefined,author};
});
}
function normalizeSebyResponse(input:unknown):SebyOwnershipResponse{
if(!isRecord(input))throw new VRFSInvalidResponseError("Ownership response was not valid.",undefined,input);
const ok=asBoolean(input.ok);
if(ok===null)throw new VRFSInvalidResponseError("Ownership response did not contain a valid status.",undefined,input);
const healthValue=typeof input.health==="string"?input.health.toLowerCase():"unknown";
const health:ServiceHealth=healthValue==="ok"?"ok":healthValue==="degraded"?"degraded":"unknown";
const results:Record<string,boolean>={};
if(isRecord(input.results))for(const[sku,value]of Object.entries(input.results)){const normalized=asBoolean(value);if(normalized!==null)results[sku]=normalized;}
const cachedRaw=Number(input.cached);
const retryRaw=Number(input.retry_after);
return{...input,ok,health,results,cached:Number.isFinite(cachedRaw)&&cachedRaw>=0?cachedRaw:0,...(Number.isFinite(retryRaw)&&retryRaw>=0?{retry_after:retryRaw}:{})};
}
function normalizeSebyLookupResponse(input:unknown):SebyLookupResponse{
if(!isRecord(input))throw new VRFSInvalidResponseError("Lookup response was not valid.",undefined,input);
const ok=asBoolean(input.ok);
if(ok===null)throw new VRFSInvalidResponseError("Lookup response did not contain a valid status.",undefined,input);
const healthValue=typeof input.health==="string"?input.health.toLowerCase():"unknown";
const health:ServiceHealth=healthValue==="ok"?"ok":healthValue==="degraded"?"degraded":"unknown";
const results=isRecord(input.results)?{...input.results}:{} as Record<string,unknown>;
return{...input,ok,health,results};
}
function normalizeSearch(value:string|number):string{return String(value??"").trim().toLowerCase();}
export function getSku(item:VRFSItem|VRFSMarketplaceItem|Record<string,unknown>):string{const value=item.sku??item.sku_base;return typeof value==="string"?value.trim():"";}
export function getItemName(item:VRFSItem|VRFSMarketplaceItem|Record<string,unknown>):string{const value=item.name??item.title;if(typeof value==="string"&&value.trim())return value.trim();const sku=getSku(item);if(!sku)return"Unknown Item";const parts=sku.split("*");const raw=parts.length>=4?parts.slice(3).join("*"):sku;return raw.replace(/([a-z])([A-Z])/g,"$1 $2").replace(/[-_]+/g," ").replace(/\b\w/g,char=>char.toUpperCase()).trim()||"Unknown Item";}
export function getSection(item:VRFSItem):string{return String(item.section??item.category??item.category_name??"Other").trim()||"Other";}
export function isItemFree(item:VRFSItem):boolean{if(item.isFree===true||item.isFree===1||item.isFree==="1")return true;if(typeof item.price==="string"&&item.price.trim().toLowerCase()==="free")return true;return false;}
export function getItemCredits(item:VRFSItem):number|null{for(const value of[item.credits,item.coins,item.coins_price]){if(value===undefined||value===null)continue;const number=Number(value);if(Number.isFinite(number))return number;}if(typeof item.price==="string"){const match=item.price.match(/[\d,]+(?:\.\d+)?/);if(match){const number=Number(match[0].replace(/,/g,""));if(Number.isFinite(number))return number;}}return null;}
export function getMarketplaceActive(item:VRFSMarketplaceItem):boolean{return item.is_active===true||item.is_active===1||item.is_active==="1";}
export function getMarketplaceOwners(item:VRFSMarketplaceItem):number{const number=Number(item.owners_count??0);return Number.isFinite(number)&&number>=0?number:0;}
export function getMarketplaceCreator(item:VRFSMarketplaceItem):string{return String(item.author?.nickname??item.author?.username??item.author?.name??item.creator??"Unknown").trim()||"Unknown";}
export function getMarketplaceCreatorUid(item:VRFSMarketplaceItem):string{return String(item.author?.uid??item.owner_uid??item.creator_uid??"").trim();}
function marketplaceScore(item:VRFSMarketplaceItem,query:string):number{
const title=String(item.title??item.name??"").toLowerCase();
const sku=String(item.sku??"").toLowerCase();
const base=String(item.sku_base??"").toLowerCase();
const creator=getMarketplaceCreator(item).toLowerCase();
const id=String(item.id).toLowerCase();
if(id===query)return 1000;
if(title===query||sku===query||base===query)return 950;
if(title.startsWith(query))return 800;
if(sku.startsWith(query)||base.startsWith(query))return 700;
if(title.includes(query))return 600;
if(sku.includes(query)||base.includes(query))return 500;
if(creator.includes(query))return 300;
return 0;
}
function catalogScore(item:VRFSItem,query:string):number{
const name=getItemName(item).toLowerCase();
const sku=getSku(item).toLowerCase();
const section=getSection(item).toLowerCase();
const id=String(item.id??item.item_id??item.itemId??"").toLowerCase();
if(id===query||sku===query)return 1000;
if(name===query)return 950;
if(name.startsWith(query))return 800;
if(sku.startsWith(query))return 700;
if(name.includes(query))return 600;
if(sku.includes(query))return 500;
if(section.includes(query))return 300;
return 0;
}
function looksLikeEmptyCatalogResponse(input:unknown):boolean{if(Array.isArray(input))return input.length===0;if(isRecord(input)){if(Array.isArray(input.data))return input.data.length===0;if(Array.isArray(input.items))return input.items.length===0;}return false;}
function looksLikeEmptyMarketplaceResponse(input:unknown):boolean{if(Array.isArray(input))return input.length===0;if(isRecord(input)){if(Array.isArray(input.data))return input.data.length===0;if(isRecord(input.data)&&Array.isArray(input.data.items))return input.data.items.length===0;if(Array.isArray(input.items))return input.items.length===0;}return false;}
export const vrfs=new VRFSClient();
export const seby=new SebyLockerClient();
export async function getUsername(uid:number,signal?:AbortSignal,forceRefresh=false):Promise<VRFSUser>{return vrfs.getUsername(uid,signal,forceRefresh);}
export async function getProfile(uid:number,signal?:AbortSignal,forceRefresh=false):Promise<VRFSProfile>{return vrfs.getProfile(uid,signal,forceRefresh);}
export async function getOutfits(uid:number,signal?:AbortSignal,forceRefresh=false):Promise<VRFSOutfit[]>{return vrfs.getOutfits(uid,signal,forceRefresh);}
export async function getPlayer(uid:number,signal?:AbortSignal,forceRefresh=false):Promise<VRFSUser&{profile?:VRFSProfile;outfits?:VRFSOutfit[]}>{return vrfs.getPlayer(uid,signal,forceRefresh);}
export async function getMarketplace(signal?:AbortSignal,forceRefresh=false):Promise<VRFSMarketplaceItem[]>{return vrfs.getMarketplace(signal,forceRefresh);}
export async function getMarketplaceItem(query:string|number,signal?:AbortSignal):Promise<VRFSMarketplaceItem|null>{return vrfs.getMarketplaceItem(query,signal);}
export async function searchMarketplace(query:string,limit=100,signal?:AbortSignal):Promise<VRFSMarketplaceItem[]>{return vrfs.searchMarketplace(query,limit,signal);}
export async function getCatalog(signal?:AbortSignal,forceRefresh=false):Promise<VRFSItem[]>{return vrfs.getCatalog(signal,forceRefresh);}
export async function getCatalogItem(query:string|number,signal?:AbortSignal):Promise<VRFSItem|null>{return vrfs.getCatalogItem(query,signal);}
export async function searchCatalog(query:string,limit=100,signal?:AbortSignal):Promise<VRFSItem[]>{return vrfs.searchCatalog(query,limit,signal);}
export async function checkOwnership(uid:number,skus:string[],options:OwnershipCheckOptions={}):Promise<OwnershipCheckResult>{return seby.checkOwnership(uid,skus,options);}
export async function getSebyItems(signal?:AbortSignal):Promise<VRFSItem[]>{return seby.getItems(signal);}
export async function sebyLookup(uid:number,skus:string[],signal?:AbortSignal):Promise<SebyLookupResponse>{return seby.lookup(uid,skus,signal);}
export async function sebyCheckOwnershipRequest(uid:number,skus:string[],signal?:AbortSignal):Promise<SebyOwnershipResponse>{return seby.checkOwnershipRequest(uid,skus,signal);}
export async function health(uid=1,signal?:AbortSignal):Promise<VRFSApiHealth>{
const started=Date.now();
const[vrfsHealth,sebyHealth]=await Promise.allSettled([vrfs.health(uid,signal),seby.health(signal)]);
const services:VRFSServiceHealth[]=[
vrfsHealth.status==="fulfilled"?vrfsHealth.value:{service:"vrfs",ok:false,health:"unknown",latencyMs:Date.now()-started,error:vrfsHealth.reason instanceof Error?vrfsHealth.reason.message:String(vrfsHealth.reason)},
sebyHealth.status==="fulfilled"?sebyHealth.value:{service:"seby",ok:false,health:"unknown",latencyMs:Date.now()-started,error:sebyHealth.reason instanceof Error?sebyHealth.reason.message:String(sebyHealth.reason)}
];
return{ok:services.every(service=>service.ok),services,latencyMs:Date.now()-started};
}
export function clearCaches():void{vrfs.clearCaches();seby.clearFlights();}
export function getStatus():Record<string,unknown>{return{vrfs:{configuration:vrfs.configuration,caches:vrfs.getCacheStats()},seby:seby.getStatus()};}
export function getSkuForItem(item:VRFSItem|VRFSMarketplaceItem|Record<string,unknown>):string{return getSku(item);}
export function getNameForItem(item:VRFSItem|VRFSMarketplaceItem|Record<string,unknown>):string{return getItemName(item);}
export function getSectionForItem(item:VRFSItem):string{return getSection(item);}
export function isFreeItem(item:VRFSItem):boolean{return isItemFree(item);}
export function getCreditsForItem(item:VRFSItem):number|null{return getItemCredits(item);}
export function isMarketplaceItemActive(item:VRFSMarketplaceItem):boolean{return getMarketplaceActive(item);}
export function getMarketplaceOwnerCount(item:VRFSMarketplaceItem):number{return getMarketplaceOwners(item);}
export function getMarketplaceCreatorName(item:VRFSMarketplaceItem):string{return getMarketplaceCreator(item);}
export function getMarketplaceCreatorId(item:VRFSMarketplaceItem):string{return getMarketplaceCreatorUid(item);}
export default{VRFSClient,SebyLockerClient,VRFSApiError,VRFSTimeoutError,VRFSRateLimitError,VRFSInvalidResponseError,VRFSResponseTooLargeError,TTLCache,SingleFlight,vrfs,seby,getUsername,getProfile,getOutfits,getPlayer,getMarketplace,getMarketplaceItem,searchMarketplace,getCatalog,getCatalogItem,searchCatalog,checkOwnership,getSebyItems,sebyLookup,sebyCheckOwnershipRequest,health,clearCaches,getStatus,getSku,getItemName,getSection,isItemFree,getItemCredits,getMarketplaceActive,getMarketplaceOwners,getMarketplaceCreator,getMarketplaceCreatorUid,getSkuForItem,getNameForItem,getSectionForItem,isFreeItem,getCreditsForItem,isMarketplaceItemActive,getMarketplaceOwnerCount,getMarketplaceCreatorName,getMarketplaceCreatorId};
