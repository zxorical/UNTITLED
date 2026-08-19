import type {CacheEntry,CacheOptions} from "./types.js";
export class MiddlewareCache<T>{
private readonly entries=new Map<string,CacheEntry<T>>();
private readonly ttlMs:number;
private readonly maxEntries:number;
public constructor(options:CacheOptions){
this.ttlMs=Math.max(1,Math.floor(options.ttlMs));
this.maxEntries=Math.max(1,Math.floor(options.maxEntries??10_000));
}
private purgeExpired():void{
const now=Date.now();
for(const[key,entry]of this.entries)if(entry.expiresAt<=now)this.entries.delete(key);
}
public get(key:string):T|undefined{
const entry=this.entries.get(key);
if(!entry)return undefined;
if(entry.expiresAt<=Date.now()){this.entries.delete(key);return undefined;}
this.entries.delete(key);
this.entries.set(key,entry);
return entry.value;
}
public set(key:string,value:T,ttlMs=this.ttlMs):void{
this.entries.delete(key);
const now=Date.now();
this.entries.set(key,{value,createdAt:now,expiresAt:now+Math.max(1,Math.floor(ttlMs))});
this.purgeExpired();
while(this.entries.size>this.maxEntries){
const first=this.entries.keys().next().value;
if(first===undefined)break;
this.entries.delete(first);
}
}
public has(key:string):boolean{return this.get(key)!==undefined;}
public delete(key:string):boolean{return this.entries.delete(key);}
public clear():void{this.entries.clear();}
public cleanup():void{this.purgeExpired();}
public get size():number{this.purgeExpired();return this.entries.size;}
public keys():string[]{this.purgeExpired();return [...this.entries.keys()];}
}
export const createCache=<T>(options:CacheOptions):MiddlewareCache<T>=>new MiddlewareCache<T>(options);
export default MiddlewareCache;
