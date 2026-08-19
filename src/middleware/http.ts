import https from "node:https";
import http from "node:http";
import {URL} from "node:url";
import type {HttpMethod,RequestOptions,HttpResponse} from "./types.js";
export class MiddlewareHttpError extends Error{
public readonly status:number;
public readonly url:string;
public readonly body:string;
public readonly retryable:boolean;
public constructor(message:string,status:number,url:string,body="",retryable=false){
super(message);
this.name="MiddlewareHttpError";
this.status=status;
this.url=url;
this.body=body;
this.retryable=retryable;
Object.setPrototypeOf(this,new.target.prototype);
}
}
export interface HttpClientOptions{
timeoutMs?:number;
maxResponseBytes?:number;
userAgent?:string;
maxRedirects?:number;
}
const DEFAULT_TIMEOUT=15_000;
const DEFAULT_MAX_RESPONSE=16*1024*1024;
const DEFAULT_USER_AGENT="UNTITLED-Middleware/1.0";
const sleep=(ms:number):Promise<void>=>new Promise(resolve=>setTimeout(resolve,ms));
const isRetryableStatus=(status:number):boolean=>[408,425,429,500,502,503,504,520,521,522,523,524].includes(status);
export class HttpClient{
private readonly timeoutMs:number;
private readonly maxResponseBytes:number;
private readonly userAgent:string;
private readonly maxRedirects:number;
public constructor(options:HttpClientOptions={}){
this.timeoutMs=Math.max(100,options.timeoutMs??DEFAULT_TIMEOUT);
this.maxResponseBytes=Math.max(1024,options.maxResponseBytes??DEFAULT_MAX_RESPONSE);
this.userAgent=options.userAgent??DEFAULT_USER_AGENT;
this.maxRedirects=Math.max(0,Math.floor(options.maxRedirects??5));
}
public async request<T=unknown>(url:string,options:RequestOptions={}):Promise<HttpResponse<T>>{
const retries=Math.max(0,Math.min(10,Math.floor(options.retries??3)));
const baseDelay=Math.max(50,Math.floor(options.retryDelayMs??500));
let lastError:unknown;
for(let attempt=0;attempt<=retries;attempt++){
try{
return await this.perform<T>(url,options);
}catch(error){
lastError=error;
const retryable=error instanceof MiddlewareHttpError?error.retryable:true;
if(!retryable||attempt>=retries)throw error;
await sleep(Math.min(baseDelay*Math.pow(2,attempt),8_000));
}
}
throw lastError instanceof Error?lastError:new Error("HTTP request failed.");
}
private async perform<T>(url:string,options:RequestOptions,redirectDepth=0):Promise<HttpResponse<T>>{
const parsed=new URL(url);
if(redirectDepth>this.maxRedirects)throw new MiddlewareHttpError("Too many redirects.",0,url,"",false);
const method:HttpMethod=options.method??"GET";
const headers:Record<string,string>={"User-Agent":this.userAgent,"Accept":"application/json, text/plain, */*",...options.headers};
let body:string|undefined;
if(options.body!==undefined){
body=typeof options.body==="string"?options.body:JSON.stringify(options.body);
headers["Content-Type"]??="application/json";
headers["Content-Length"]=String(Buffer.byteLength(body));
}
const transport=parsed.protocol==="https:"?https:http;
return new Promise<HttpResponse<T>>((resolve,reject)=>{
let settled=false;
let bytes=0;
const chunks:Buffer[]=[];
let timer:NodeJS.Timeout|undefined;
const cleanup=()=>{if(timer)clearTimeout(timer);if(options.signal)options.signal.removeEventListener("abort",abort);};
const fail=(error:Error)=>{if(settled)return;settled=true;cleanup();reject(error);};
const succeed=(response:HttpResponse<T>)=>{if(settled)return;settled=true;cleanup();resolve(response);};
const request=transport.request(parsed,{method,headers,timeout:this.timeoutMs},response=>{
const status=Number(response.statusCode??0);
const location=response.headers.location;
if([301,302,303,307,308].includes(status)&&location){
response.resume();
const next=Array.isArray(location)?location[0]:location;
if(!next){fail(new MiddlewareHttpError("Invalid redirect.",status,url,"",false));return;}
const redirectUrl=new URL(next,url).toString();
this.perform<T>(redirectUrl,options,redirectDepth+1).then(succeed).catch(fail);
return;
}
response.on("data",(chunk:Buffer|string)=>{
const buffer=Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk);
bytes+=buffer.length;
if(bytes>this.maxResponseBytes){response.destroy();fail(new MiddlewareHttpError("Response exceeded size limit.",status,url,"",false));return;}
chunks.push(buffer);
});
response.once("end",()=>{
const text=Buffer.concat(chunks).toString("utf8");
let data:T|null=null;
if(text.trim()){
try{data=JSON.parse(text) as T;}catch{data=null;}
}
if(status<200||status>=300){
fail(new MiddlewareHttpError(`HTTP ${status}`,status,url,text.slice(0,2_000),isRetryableStatus(status)));
return;
}
if(text.trim()&&data===null){
succeed({status,headers:response.headers as Record<string,string|string[]|undefined>,data:null,text,url});
return;
}
succeed({status,headers:response.headers as Record<string,string|string[]|undefined>,data,text,url});
});
response.once("error",error=>fail(error));
});
timer=setTimeout(()=>{request.destroy();fail(new MiddlewareHttpError("Request timed out.",0,url,"",true));},this.timeoutMs+250);
request.once("error",error=>fail(error));
const abort=()=>{request.destroy();fail(new MiddlewareHttpError("Request aborted.",0,url,"",false));};
if(options.signal?.aborted){abort();return;}
options.signal?.addEventListener("abort",abort,{once:true});
if(body!==undefined)request.write(body);
request.end();
});
}
}
export const httpClient=new HttpClient();
export default httpClient;
