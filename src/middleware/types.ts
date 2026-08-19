export type JsonPrimitive=string|number|boolean|null;
export type JsonValue=JsonPrimitive|JsonObject|JsonValue[];
export interface JsonObject{[key:string]:JsonValue|undefined;}
export interface CacheEntry<T>{value:T;createdAt:number;expiresAt:number;}
export interface CacheOptions{ttlMs:number;maxEntries?:number;}
export class VRFSApiError extends Error{
public readonly code:string;
public readonly status:number;
public readonly url:string;
public readonly body:unknown;
public readonly retryable:boolean;
public constructor(message:string,status:number,url:string,body:unknown=null,retryable=false,code?:string){
super(message);
this.name="VRFSApiError";
this.code=code??`HTTP_${status}`;
this.status=status;
this.url=url;
this.body=body;
this.retryable=retryable;
Object.setPrototypeOf(this,new.target.prototype);
}
}
export class VRFSValidationError extends Error{
public constructor(message:string){
super(message);
this.name="VRFSValidationError";
Object.setPrototypeOf(this,new.target.prototype);
}
}
export interface RequestOptions{method?:"GET"|"POST"|"PUT"|"PATCH"|"DELETE";headers?:Record<string,string>;body?:unknown;timeoutMs?:number;retries?:number;retryDelayMs?:number;signal?:AbortSignal;}
export interface HttpResponse<T=unknown>{status:number;headers:Record<string,string|string[]|undefined>;data:T|null;text:string;url:string;}
