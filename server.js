'use strict';
/* MrBoroo — Servidor del Modo SALA (sin dependencias)
   - Sirve los archivos estáticos del juego.
   - WebSocket en /ws para tiempo real.
   - Respaldo HTTP (long-polling) en /api para redes que bloquean WebSocket.
*/
const http=require('http');
const crypto=require('crypto');
const fs=require('fs');
const path=require('path');

const PORT=parseInt(process.env.PORT||'8080',10);
const GUID='258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MIME={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8',
  '.json':'application/json; charset=utf-8','.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg',
  '.svg':'image/svg+xml','.ico':'image/x-icon','.txt':'text/plain; charset=utf-8','.webp':'image/webp','.mp3':'audio/mpeg'};

const TARGET=600, BASE_SPEED=10, MAX_SPEED=19, RAMP=0.011, TICK=66, GRACE=1.5;
const COLS=['#7c5cff','#ff5a8a','#22d3ee','#ffb020','#7ef29d','#a78bfa','#ff7a2f','#3ddc97'];

const rooms=new Map();
const clients=new Map();
let seq=0;
function clamp(v,a,b){return Math.max(a,Math.min(b,v));}
function rnd6(){let c='';for(let i=0;i<6;i++)c+=Math.floor(Math.random()*10);return c;}
function nid(){return 'c'+(++seq);}

/* ---------- transporte ---------- */
function encodeBuffer(buf,op){op=op||0x1;
  let h;
  if(buf.length<126){h=Buffer.from([0x80|op,buf.length]);}
  else if(buf.length<65536){h=Buffer.alloc(4);h[0]=0x80|op;h[1]=126;h.writeUInt16BE(buf.length,2);}
  else{h=Buffer.alloc(10);h[0]=0x80|op;h[1]=127;h.writeBigUInt64BE(BigInt(buf.length),2);}
  return Buffer.concat([h,buf]);
}
function drain(c){const q=c.queue;c.queue=[];return q;}
function push(c,obj){
  if(!c)return;
  if(c.ws&&c.ws.writable){try{c.ws.write(encodeBuffer(Buffer.from(JSON.stringify(obj),'utf8'),1));}catch(e){}}
  else{c.queue.push(obj);if(c.waiter){const w=c.waiter;c.waiter=null;w();}}
}
function broadcast(r,obj){for(const p of r.players.values())push(p.client,obj);}

/* ---------- salas ---------- */
function pPub(p){return {id:p.id,name:p.name,color:p.color,lane:p.lane,jump:p.jump,alive:p.alive,shield:p.shield,kills:p.kills,order:p.order};}
function roster(r){const a=[];for(const p of r.players.values())a.push(pPub(p));a.sort((x,y)=>x.order-y.order);return a;}
function aliveCount(r){let n=0;for(const p of r.players.values())if(p.alive)n++;return n;}

function start(r){
  r.seed=Math.floor(Math.random()*1e9);
  r.distance=0;r.speed=BASE_SPEED;r.elapsed=0;r.started=true;r.over=false;
  let order=0;
  for(const p of r.players.values()){p.alive=true;p.shield=1;p.kills=0;p.lane=1;p.jump=false;p.order=order++;}
  broadcast(r,{type:'start',seed:r.seed,target:TARGET,speed:r.speed,players:roster(r)});
}
function endRound(r){
  if(r.over)return;r.over=true;r.started=false;
  let winner=null,last=null,lastK=-1;
  for(const p of r.players.values()){if(p.alive)winner=p;if(p.kills>lastK){lastK=p.kills;last=p;}}
  if(!winner)winner=last||null;
  broadcast(r,{type:'end',winner:winner?winner.id:null,players:roster(r)});
}
function leave(r,p){
  r.players.delete(p.id);
  if(p.client){p.client.room=null;p.client.playerId=null;}
  if(r.players.size===0){rooms.delete(r.code);return;}
  if(r.host===p.id){const first=r.players.values().next().value;r.host=first.id;}
  broadcast(r,{type:'leave',id:p.id,newHost:r.host});
  broadcast(r,{type:'roster',host:r.host,players:roster(r)});
}

function handle(c,raw){
  let m;try{m=JSON.parse(raw);}catch(e){return;}
  const a=m.action;
  if(a==='create'){
    const name=String(m.name||'MrBoroo').slice(0,16);
    const p={id:nid(),name:name,color:COLS[0],lane:1,jump:false,alive:true,shield:1,kills:0,order:0,client:c};
    const r={code:rnd6(),host:p.id,players:new Map([[p.id,p]]),started:false,over:false,seed:0,distance:0,speed:BASE_SPEED,elapsed:0};
    rooms.set(r.code,r);
    c.room=r.code;c.playerId=p.id;
    push(c,{type:'joined',room:r.code,you:p.id,host:r.host,players:roster(r)});
    return;
  }
  if(a==='join'){
    const name=String(m.name||'MrBoroo').slice(0,16);
    const code=String(m.code||'').trim();
    const r=rooms.get(code);
    if(!r){push(c,{type:'error',msg:'Sala no encontrada'});return;}
    if(r.started){push(c,{type:'error',msg:'La partida ya está en curso'});return;}
    if(r.players.size>=8){push(c,{type:'error',msg:'La sala está llena (máx. 8)'});return;}
    const p={id:nid(),name:name,color:COLS[r.players.size%COLS.length],lane:1,jump:false,alive:true,shield:1,kills:0,order:r.players.size,client:c};
    r.players.set(p.id,p);
    c.room=r.code;c.playerId=p.id;
    push(c,{type:'joined',room:r.code,you:p.id,host:r.host,players:roster(r)});
    broadcast(r,{type:'roster',host:r.host,players:roster(r)});
    return;
  }
  if(a==='start'){
    const r=rooms.get(c.room||'');if(!r)return;
    if(r.host!==c.playerId){push(c,{type:'error',msg:'Solo el anfitrión puede iniciar'});return;}
    if(r.players.size<2){push(c,{type:'error',msg:'Se necesitan al menos 2 jugadores'});return;}
    start(r);return;
  }
  if(a==='state'){
    const r=rooms.get(c.room||'');if(!r)return;
    const p=r.players.get(c.playerId);if(!p)return;
    if(typeof m.lane==='number')p.lane=clamp(Math.round(m.lane),0,2);
    if(typeof m.jump==='boolean')p.jump=m.jump;
    broadcast(r,{type:'pstate',id:p.id,lane:p.lane,jump:p.jump});
    return;
  }
  if(a==='shoot'){
    const r=rooms.get(c.room||'');if(!r||!r.started)return;
    const sh=r.players.get(c.playerId);if(!sh||!sh.alive)return;
    let victim=null;
    for(const p of r.players.values()){
      if(p.id===sh.id||!p.alive)continue;
      if(p.lane===sh.lane){if(!victim||p.order<victim.order)victim=p;}
    }
    if(!victim){broadcast(r,{type:'shot',from:sh.id,lane:sh.lane,to:null});return;}
    if(victim.jump){broadcast(r,{type:'shot',from:sh.id,lane:sh.lane,to:victim.id,dodged:true});return;}
    let killed=false;
    if(victim.shield>0){victim.shield--;}
    else{victim.alive=false;killed=true;sh.kills++;broadcast(r,{type:'pstate',id:sh.id,kills:sh.kills});}
    broadcast(r,{type:'shot',from:sh.id,lane:sh.lane,to:victim.id,killed:killed});
    broadcast(r,{type:'pstate',id:victim.id,shield:victim.shield,alive:victim.alive});
    if(killed){broadcast(r,{type:'dead',id:victim.id});setTimeout(()=>{if(r.started&&aliveCount(r)<=1)endRound(r);},60);}
    return;
  }
  if(a==='dead'){
    const r=rooms.get(c.room||'');if(!r||!r.started)return;
    const p=r.players.get(c.playerId);if(!p||!p.alive)return;
    p.alive=false;
    broadcast(r,{type:'dead',id:p.id});
    setTimeout(()=>{if(r.started&&aliveCount(r)<=1)endRound(r);},60);
    return;
  }
  if(a==='revive'){
    const r=rooms.get(c.room||'');if(!r)return;
    const p=r.players.get(c.playerId);if(!p)return;
    if(!p.alive){
      p.alive=true;p.shield=1;p.lane=1;p.jump=false;
      broadcast(r,{type:'alive',id:p.id});
      broadcast(r,{type:'pstate',id:p.id,lane:p.lane,jump:p.jump,alive:true,shield:1});
    }
    return;
  }
  if(a==='leave'){
    const r=rooms.get(c.room||'');if(!r)return;
    const p=r.players.get(c.playerId);if(!p)return;
    leave(r,p);
    return;
  }
  if(a==='ping'){push(c,{type:'pong'});return;}
}

/* ---------- bucle de partida ---------- */
setInterval(()=>{
  const dt=TICK/1000;
  for(const r of rooms.values()){
    if(!r.started||r.over)continue;
    r.elapsed+=dt;
    r.distance+=r.speed*dt;
    r.speed=Math.min(MAX_SPEED,BASE_SPEED+r.distance*RAMP);
    broadcast(r,{type:'tick',distance:r.distance,speed:r.speed});
    if(r.elapsed>GRACE&&aliveCount(r)<=1)endRound(r);
  }
},TICK);

/* ---------- WebSocket ---------- */
function parseFrames(c){
  while(true){
    const b=c.buf;
    if(b.length<2)return;
    const b0=b[0],b1=b[1];
    const opcode=b0&0x0f;
    const masked=(b1&0x80)!==0;
    let len=b1&0x7f,off=2;
    if(len===126){if(b.length<4)return;len=b.readUInt16BE(2);off=4;}
    else if(len===127){if(b.length<10)return;len=Number(b.readBigUInt64BE(2));off=10;}
    let mask=null;
    if(masked){if(b.length<off+4)return;mask=b.slice(off,off+4);off+=4;}
    if(b.length<off+len)return;
    const payload=Buffer.from(b.slice(off,off+len));
    c.buf=b.slice(off+len);
    if(mask){for(let i=0;i<payload.length;i++)payload[i]^=mask[i&3];}
    if(opcode===0x8){try{c.ws.end();}catch(e){}onDisconnect(c);return;}
    else if(opcode===0x9){try{c.ws.write(encodeBuffer(payload,0xA));}catch(e){}}
    else if(opcode===0x1){handle(c,payload.toString('utf8'));}
  }
}
function onDisconnect(c){
  clients.delete(c.id);
  if(c.room){const r=rooms.get(c.room);if(r){const p=r.players.get(c.playerId);if(p)leave(r,p);}}
}

const server=http.createServer((req,res)=>{
  const u=new URL(req.url,'http://x');
  if(u.pathname==='/api'||u.pathname.startsWith('/api/')){handleApi(req,res,u);return;}
  serveStatic(req,res,u);
});
server.on('upgrade',(req,socket)=>{
  if(req.url!=='/ws'&&!req.url.startsWith('/ws')){socket.destroy();return;}
  const key=req.headers['sec-websocket-key'];
  if(!key){socket.destroy();return;}
  const accept=crypto.createHash('sha1').update(key+GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');
  const c={id:nid(),ws:socket,room:null,playerId:null,queue:[],waiter:null,buf:Buffer.alloc(0)};
  clients.set(c.id,c);
  socket.on('data',chunk=>{c.buf=Buffer.concat([c.buf,chunk]);parseFrames(c);});
  socket.on('close',()=>onDisconnect(c));
  socket.on('error',()=>{});
});

/* ---------- HTTP API (respaldo) ---------- */
function handleApi(req,res,u){
  if(req.method==='GET'&&u.pathname==='/api/poll'){
    const cid=u.searchParams.get('cid')||'';
    let c=clients.get(cid)||null;
    if(!c){
      c={id:nid(),ws:null,room:null,playerId:null,queue:[],waiter:null,buf:Buffer.alloc(0)};
      clients.set(c.id,c);
      res.setHeader('Content-Type','application/json');
      res.end(JSON.stringify([{type:'cid',cid:c.id}]));
      return;
    }
    const evs=drain(c);
    if(evs.length){res.setHeader('Content-Type','application/json');res.end(JSON.stringify(evs));return;}
    let done=false;
    const timer=setTimeout(()=>{if(!done){done=true;res.setHeader('Content-Type','application/json');res.end('[]');}},25000);
    c.waiter=()=>{if(!done){done=true;clearTimeout(timer);res.setHeader('Content-Type','application/json');res.end(JSON.stringify(drain(c)));}};
    return;
  }
  if(req.method==='POST'&&u.pathname==='/api'){
    let body='';
    req.on('data',d=>{body+=d;if(body.length>1e5)req.destroy();});
    req.on('end',()=>{
      let m;try{m=JSON.parse(body||'{}');}catch(e){m={};}
      let c=clients.get(m.cid||'')||null;
      if(!c){c={id:nid(),ws:null,room:null,playerId:null,queue:[],waiter:null,buf:Buffer.alloc(0)};clients.set(c.id,c);}
      handle(c,JSON.stringify(m));
      const evs=drain(c);
      res.setHeader('Content-Type','application/json');
      res.end(JSON.stringify({ok:true,cid:c.id,events:evs}));
    });
    return;
  }
  res.writeHead(404,{'Content-Type':'application/json'});
  res.end('{"ok":false}');
}
function serveStatic(req,res,u){
  let p;
  try{p=decodeURIComponent(u.pathname);}catch(e){res.writeHead(400);res.end();return;}
  if(p==='/')p='/index.html';
  const file=path.join(__dirname,p);
  if(!file.startsWith(__dirname)){res.writeHead(403);res.end();return;}
  fs.readFile(file,(err,data)=>{
    if(err){res.writeHead(404,{'Content-Type':'text/plain'});res.end('Not found');return;}
    res.setHeader('Content-Type',MIME[path.extname(file).toLowerCase()]||'application/octet-stream');
    res.end(data);
  });
}

server.listen(PORT,'0.0.0.0',()=>console.log('MrBoroo server en http://0.0.0.0:'+PORT+' (WS /ws + API /api)'));
