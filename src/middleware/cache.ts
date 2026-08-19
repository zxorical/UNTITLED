import {CacheEntry,CacheOptions} from "./api/types";
export class MemoryCache<T=unknown>{private readonly entries=new Map<string,CacheEntry<T>>();private readonly ttlMs:number;private readonly maxEntries:number;constructor(options:CacheOptions){if(!Number.isFinite(options.ttlMs)||options.ttlMs<=0)throw new Error("Cache TTL must be greater than zero.");this.ttlMs=options.ttlMs;this.maxEntries=Math.max(1,Math.floor(options.maxEntries??1000));}
private isExpired(entry:CacheEntry<T>):boolean{return Date.now()>=entry.expiresAt;}
private enforceLimit():void{while(this.entries.size>this.maxEntries){const oldest=this.entries.keys().next().value;if(oldest===undefined)break;this.entries.delete(oldest);}}
get(key:string):T|undefined{const entry=this.entries.get(key);if(!entry)return undefined;if(this.isExpired(entry)){this.entries.delete(key);return undefined;}this.entries.delete(key);this.entries.set(key,entry);return entry.value;}
has(key:string):boolean{return this.get(key)!==undefined;}
set(key:string,value:T,ttlMs=this.ttlMs):void{const now=Date.now();this.entries.delete(key);this.entries.set(key,{value,createdAt:now,expiresAt:now+ttlMs});this.enforceLimit();}
delete(key:string):boolean{return this.entries.delete(key);}
clear():void{this.entries.clear();}
size():number{return this.entries.size;}
keys():string[]{this.cleanupExpired();return [...this.entries.keys()];}
entriesArray():Array<[string,T]>{this.cleanupExpired();return [...this.entries.entries()].map(([key,entry])=>[key,entry.value]);}
cleanupExpired():number{let removed=0;for(const[key,entry]of this.entries){if(this.isExpired(entry)){this.entries.delete(key);removed++;}}return removed;}
getOrSet(key:string,factory:()=>Promise<T>|T,ttlMs=this.ttlMs):Promise<T>{const cached=this.get(key);if(cached!==undefined)return Promise.resolve(cached);return Promise.resolve(factory()).then(value=>{this.set(key,value,ttlMs);return value;});}}
export class AsyncCache<T=unknown>extends MemoryCache<T>{private readonly pending=new Map<string,Promise<T>>();async getOrSetAsync(key:string,factory:()=>Promise<T>,ttlMs=this["ttlMs"]):Promise<T>{const cached=this.get(key);if(cached!==undefined)return cached;const existing=this.pending.get(key);if(existing)return existing;const promise=factory().then(value=>{this.set(key,value,ttlMs);this.pending.delete(key);return value;}).catch(error=>{this.pending.delete(key);throw error;});this.pending.set(key,promise);return promise;}}
export function createCache<T=unknown>(ttlMs:number,maxEntries=1000):MemoryCache<T>{return new MemoryCache<T>({ttlMs,maxEntries});}
