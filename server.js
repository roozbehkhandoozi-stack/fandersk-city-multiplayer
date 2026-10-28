'use strict';
const http=require('http'), fs=require('fs'), path=require('path'), crypto=require('crypto');
const {WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000, ROOT=path.join(__dirname,'public'), DATA=path.join(__dirname,'data','db.json');
let db={}; try{db=JSON.parse(fs.readFileSync(DATA,'utf8'))}catch(e){}
let writeTimer=null; function persist(){clearTimeout(writeTimer);writeTimer=setTimeout(()=>{fs.mkdirSync(path.dirname(DATA),{recursive:true});fs.writeFileSync(DATA,JSON.stringify(db,null,2))},100)}
const locks=new Map();
function json(res,code,obj){res.writeHead(code,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});res.end(JSON.stringify(obj))}
function body(req){return new Promise((resolve,reject)=>{let s='';req.on('data',c=>{s+=c;if(s.length>1e6)req.destroy()});req.on('end',()=>{try{resolve(s?JSON.parse(s):{})}catch(e){reject(e)}});req.on('error',reject)})}
function safeDoc(p){return typeof p==='string'&&/^accounts\/n_[0-9a-f]+$/.test(p)}
const server=http.createServer(async(req,res)=>{
  try{
    const u=new URL(req.url,'http://x');
    if(u.pathname==='/api/doc'&&req.method==='GET'){
      const p=u.searchParams.get('path');if(!safeDoc(p))return json(res,400,{error:'bad path'});return json(res,200,{exists:Object.prototype.hasOwnProperty.call(db,p),data:db[p]||null});
    }
    if(u.pathname==='/api/doc'&&(req.method==='POST'||req.method==='PATCH')){
      const b=await body(req);if(!safeDoc(b.path)||!b.data||typeof b.data!=='object')return json(res,400,{error:'bad request'});
      if(req.method==='POST')db[b.path]=b.data;else db[b.path]=Object.assign({},db[b.path]||{},b.data);persist();return json(res,200,{ok:true});
    }
    if(u.pathname==='/api/lock'&&req.method==='POST'){
      const b=await body(req);if(!safeDoc(b.path))return json(res,400,{error:'bad path'});let now=Date.now(),cur=locks.get(b.path),ttl=Math.max(1000,Math.min(15000,+b.ttlMs||8000));
      if(cur&&cur.until>now&&cur.holder!==b.holder)return json(res,200,{acquired:false});locks.set(b.path,{holder:String(b.holder||''),until:now+ttl});return json(res,200,{acquired:true});
    }
    let f=u.pathname==='/'?'/index.html':decodeURIComponent(u.pathname);f=path.normalize(f).replace(/^(\.\.[/\\])+/, '');let fp=path.join(ROOT,f);
    if(!fp.startsWith(ROOT))return json(res,403,{error:'forbidden'});fs.readFile(fp,(e,d)=>{if(e){res.writeHead(404);return res.end('Not found')}let ext=path.extname(fp);let ct=ext==='.html'?'text/html; charset=utf-8':ext==='.js'?'text/javascript; charset=utf-8':ext==='.css'?'text/css; charset=utf-8':'application/octet-stream';res.writeHead(200,{'Content-Type':ct});res.end(d)});
  }catch(e){json(res,500,{error:'server error'})}
});
const wss=new WebSocketServer({server,path:'/ws'}), clients=new Map();
function peers(){return [...clients].map(([ws,c])=>({peer:c.id,isMe:false,kind:'viewer',presence:c.presence||{}}))}
function broadcastPeers(){for(const [ws,c] of clients){if(ws.readyState!==1)continue;let ps=[...clients].map(([w,x])=>({peer:x.id,isMe:w===ws,kind:'viewer',presence:x.presence||{}}));ws.send(JSON.stringify({type:'peers',peers:ps}))}}
function broadcast(o){let s=JSON.stringify(o);for(const ws of clients.keys())if(ws.readyState===1)ws.send(s)}
wss.on('connection',ws=>{let c={id:crypto.randomBytes(8).toString('hex'),presence:{}};clients.set(ws,c);ws.send(JSON.stringify({type:'hello',id:c.id}));broadcastPeers();
 ws.on('message',buf=>{let m;try{m=JSON.parse(String(buf))}catch(e){return}if(m.type==='presence'&&m.data&&typeof m.data==='object'){c.presence=m.data;broadcastPeers()}else if(m.type==='event'&&['chat','arrest'].includes(m.event)){broadcast({type:'event',event:m.event,data:m.data||{}})}});
 ws.on('close',()=>{clients.delete(ws);broadcastPeers()});
});
server.listen(PORT,()=>console.log('Fandersk City multiplayer: http://localhost:'+PORT));
