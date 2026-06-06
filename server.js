const express = require('express');
const path = require('path');
const fs = require('fs');
const cron = require('node-cron');
const initSqlJs = require('sql.js');

const app = express();
const PORT = process.env.PORT || 3000;
const DB_PATH = path.join(__dirname, 'data', 'qrcode.db');
const STATE_PATH = path.join(__dirname, 'data', 'sync-state.json');
const CACHE_PATH = path.join(__dirname, 'data', 'ae-cache.json');
const SCHEDULE_PATH = path.join(__dirname, 'data', 'schedule-config.json');

app.use(express.json({ limit: '8mb' }));
app.use(express.static(path.join(__dirname, 'public')));
app.get('/jsQR.js', (req, res) => res.sendFile(path.join(__dirname, 'node_modules', 'jsqr', 'dist', 'jsQR.js')));

const AUTH_USER = 'rasisnc';
const AUTH_PASS = 'Gianluca1';
function basicAuth(req, res, next){
  const header = req.headers.authorization || '';
  const [type, token] = header.split(' ');
  if(type === 'Basic' && token){
    const decoded = Buffer.from(token, 'base64').toString();
    const i = decoded.indexOf(':');
    if(i !== -1 && decoded.slice(0,i) === AUTH_USER && decoded.slice(i+1) === AUTH_PASS) return next();
  }
  res.setHeader('WWW-Authenticate', 'Basic realm="QR Manager"');
  res.status(401).send('Accesso negato');
}
app.use(basicAuth);

let SQL, db;
let cache = {};
const DEFAULT_SYNC_CONFIG = { batchSize: 25, delayMs: 1500, cacheHours: 12, maxRetries: 2 };
let syncState = { running:false, startedAt:null, finishedAt:null, total:0, checked:0, updated:0, skipped:0, cached:0, errors:0, currentIndex:0, currentMatricola:'', lastMessage:'Pronto.', lastSamples:[], stopRequested:false, config:DEFAULT_SYNC_CONFIG };

function readJson(file, fallback){ try { return fs.existsSync(file) ? JSON.parse(fs.readFileSync(file,'utf8')) : fallback; } catch { return fallback; } }
function writeJson(file, data){ fs.writeFileSync(file, JSON.stringify(data, null, 2)); }
function saveDb(){ fs.writeFileSync(DB_PATH, Buffer.from(db.export())); }
function loadCache(){ cache = readJson(CACHE_PATH, {}); }
function saveCache(){ writeJson(CACHE_PATH, cache); }
function loadState(){ const s = readJson(STATE_PATH, {}); syncState = { ...syncState, ...s, running:false, stopRequested:false }; }
function saveState(){ writeJson(STATE_PATH, syncState); }
function nowIso(){ return new Date().toISOString(); }
function nowIt(){ return new Date().toLocaleString('it-IT'); }
function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

function all(sql, params={}){ const st = db.prepare(sql); st.bind(params); const out=[]; while(st.step()) out.push(st.getAsObject()); st.free(); return out; }
function run(sql, params={}){ const st = db.prepare(sql); st.bind(params); st.step(); st.free(); }
function columns(table){ return db.exec(`PRAGMA table_info(${table})`)[0]?.values.map(r=>r[1]) || []; }
function ensureColumn(table,col,type){ if(!columns(table).includes(col)) db.run(`ALTER TABLE ${table} ADD COLUMN ${col} ${type}`); }
function migrate(){
  ensureColumn('results','cf_tecnico','TEXT');
  ensureColumn('results','denominazione','TEXT');
  ensureColumn('results','link_qr','TEXT');
  ensureColumn('results','last_sync','TEXT');
  ensureColumn('results','data_vp','TEXT');
  ensureColumn('results','sync_error','TEXT');
  ensureColumn('results','sync_attempts','INTEGER DEFAULT 0');
  db.run(`UPDATE results SET data_vp = ultima_vp WHERE (data_vp IS NULL OR data_vp='') AND ultima_vp IS NOT NULL`);
  db.run(`UPDATE results SET link_qr = (SELECT url FROM urls WHERE rowid=results.url_id) WHERE link_qr IS NULL OR link_qr=''`);
  saveDb();
}

function cleanText(html){ return String(html||'').replace(/<script[\s\S]*?<\/script>/gi,' ').replace(/<style[\s\S]*?<\/style>/gi,' ').replace(/<[^>]+>/g,' ').replace(/&nbsp;/g,' ').replace(/&#39;/g,"'").replace(/&quot;/g,'"').replace(/&amp;/g,'&').replace(/\s+/g,' ').trim(); }
function pick(text, regex){ const m = text.match(regex); return m ? String(m[1]||'').trim() : ''; }
function parseAePage(html){
  const text = cleanText(html);
  return {
    matricola: pick(text,/Matricola:\s*(.*?)\s+Stato:/i),
    stato: pick(text,/Stato:\s*(.*?)\s+Informazioni Dispositivo/i),
    data_vp: pick(text,/Ultima Verificazione Periodica\s+Data:\s*(\d{2}\/\d{2}\/\d{4})/i),
    risultato_vp: pick(text,/Ultima Verificazione Periodica\s+Data:\s*\d{2}\/\d{2}\/\d{4}\s+(.*?)\s+PIVA Laboratorio:/i),
    piva_laboratorio: pick(text,/PIVA Laboratorio:\s*([0-9]+)/i),
    cf_tecnico: pick(text,/CF Tecnico:\s*([A-Z0-9*]+)/i),
    ultima_trasmissione: pick(text,/Ultima Trasmissione da Dispositivo\s+Data:\s*(\d{2}\/\d{2}\/\d{4})/i),
    versione_software: pick(text,/Ultima versione software del dispositivo\s+Data Invio Manutenzione:\s*\d{2}\/\d{2}\/\d{4}\s+Versione:\s*([^\s]+)/i) || pick(text,/Ultima versione software del dispositivo.*?Versione:\s*([^\s]+)/i),
    partita_iva: pick(text,/Esercente\s+Partita IVA:\s*([0-9]+)/i),
    denominazione: pick(text,/Esercente\s+Partita IVA:\s*[0-9]+\s+Denominazione:\s*(.*?)\s+Elenco matricole/i),
    raw_preview: text.slice(0,1000)
  };
}
function normalizeUrlInput(url){ const v=String(url||'').trim(); if(!v) throw new Error('Link QR mancante'); if(!/^https?:\/\//i.test(v)) throw new Error('Il QR non contiene un link valido'); return v; }
function hasUsefulData(d){ return Boolean(d && (d.matricola || d.stato || d.ultima_trasmissione || d.denominazione)); }
function isCacheValid(e,h){ if(!e?.savedAt) return false; return Date.now() - new Date(e.savedAt).getTime() < Number(h)*3600000; }
async function fetchAndParse(url, opts={}){
  const safe = normalizeUrlInput(url); const cacheHours = Number(opts.cacheHours ?? DEFAULT_SYNC_CONFIG.cacheHours);
  if(cacheHours>0 && isCacheValid(cache[safe], cacheHours)) return { ...cache[safe].payload, fromCache:true };
  const res = await fetch(safe, { headers:{ 'User-Agent':'Mozilla/5.0 QR Manager' }});
  const body = await res.text(); const payload = { statusCode:res.status, data:parseAePage(body), rawPreview:cleanText(body).slice(0,800), fromCache:false };
  if(res.ok && hasUsefulData(payload.data)){ cache[safe] = { savedAt:nowIso(), payload }; saveCache(); }
  return payload;
}
function upsertImportedResult(url, data){
  const now = nowIso(); const matricola = String(data.matricola||'').trim(); if(!matricola) throw new Error('Matricola non trovata nella pagina AE');
  run(`INSERT OR IGNORE INTO urls (url,last_checked) VALUES ($url,$now)`, {$url:url,$now:now});
  run(`UPDATE urls SET last_checked=$now WHERE url=$url`, {$url:url,$now:now});
  const urlId = all(`SELECT rowid AS id FROM urls WHERE url=$url LIMIT 1`, {$url:url})[0]?.id || null;
  run(`INSERT INTO results (url_id,matricola,stato,ultima_vp,data_vp,risultato_vp,partita_iva_vp,cf_tecnico,ultima_trasmissione,versione_fw,partita_iva,denominazione,link_qr,last_sync,sync_error,sync_attempts)
    VALUES ($url_id,$matricola,$stato,$data_vp,$data_vp,$risultato_vp,$piva_laboratorio,$cf_tecnico,$ultima_trasmissione,$versione_software,$partita_iva,$denominazione,$url,$now,'',0)
    ON CONFLICT(matricola) DO UPDATE SET url_id=COALESCE($url_id,url_id), stato=COALESCE(NULLIF($stato,''),stato), ultima_vp=COALESCE(NULLIF($data_vp,''),ultima_vp), data_vp=COALESCE(NULLIF($data_vp,''),data_vp), risultato_vp=COALESCE(NULLIF($risultato_vp,''),risultato_vp), partita_iva_vp=COALESCE(NULLIF($piva_laboratorio,''),partita_iva_vp), cf_tecnico=COALESCE(NULLIF($cf_tecnico,''),cf_tecnico), ultima_trasmissione=COALESCE(NULLIF($ultima_trasmissione,''),ultima_trasmissione), versione_fw=COALESCE(NULLIF($versione_software,''),versione_fw), partita_iva=COALESCE(NULLIF($partita_iva,''),partita_iva), denominazione=COALESCE(NULLIF($denominazione,''),denominazione), link_qr=$url,last_sync=$now,sync_error='',sync_attempts=0`, {
      $url_id:urlId,$matricola:matricola,$stato:data.stato||'',$data_vp:data.data_vp||'',$risultato_vp:data.risultato_vp||'',$piva_laboratorio:data.piva_laboratorio||'',$cf_tecnico:data.cf_tecnico||'',$ultima_trasmissione:data.ultima_trasmissione||'',$versione_software:data.versione_software||'',$partita_iva:data.partita_iva||'',$denominazione:data.denominazione||'',$url:url,$now:now
  });
  saveDb(); return { matricola, url_id:urlId, last_sync:now };
}

function publicSyncState(){ const percent = syncState.total ? Math.round(syncState.checked/syncState.total*100) : 0; return { ...syncState, percent }; }
function addSample(x){ syncState.lastSamples.unshift(x); syncState.lastSamples = syncState.lastSamples.slice(0,10); }
function buildQueue(mode='new'){
  const rows = all(`SELECT r.matricola,r.ultima_trasmissione AS prima_ultima_trasmissione,r.url_id,COALESCE(r.link_qr,u.url) AS url,r.last_sync FROM results r LEFT JOIN urls u ON u.rowid=r.url_id WHERE COALESCE(r.link_qr,u.url) IS NOT NULL AND COALESCE(r.link_qr,u.url)<>'' ORDER BY CASE WHEN r.last_sync IS NULL OR r.last_sync='' THEN 0 ELSE 1 END, r.last_sync ASC, r.matricola ASC`);
  if(mode==='resume' && syncState.currentIndex>0 && syncState.currentIndex<rows.length) return rows.slice(syncState.currentIndex);
  syncState.currentIndex=0; return rows;
}
async function syncOne(row, config){
  syncState.currentMatricola=row.matricola; syncState.lastMessage=`Controllo ${syncState.checked+1}/${syncState.total}: ${row.matricola}`; saveState();
  let lastErr=null;
  for(let a=0; a<=Number(config.maxRetries||0); a++){
    try{
      const {statusCode,data,fromCache}=await fetchAndParse(row.url,{cacheHours:config.cacheHours}); if(fromCache) syncState.cached++;
      if(!hasUsefulData(data)){ syncState.skipped++; run(`UPDATE results SET last_sync=$now,sync_error=$e,sync_attempts=COALESCE(sync_attempts,0)+1 WHERE matricola=$m`,{$now:nowIso(),$e:`Dati non trovati HTTP ${statusCode}`,$m:row.matricola}); saveDb(); addSample({matricola:row.matricola,esito:'saltato',http:statusCode}); return; }
      const saved=upsertImportedResult(row.url,data); syncState.updated++; addSample({matricola:saved.matricola,esito:'aggiornato',ultima_trasmissione:data.ultima_trasmissione||'',cliente:data.denominazione||'',cache:fromCache}); return;
    }catch(e){ lastErr=e; if(a<Number(config.maxRetries||0)) await sleep(Math.max(1000, Number(config.delayMs||1000))); }
  }
  syncState.errors++; run(`UPDATE results SET last_sync=$now,sync_error=$e,sync_attempts=COALESCE(sync_attempts,0)+1 WHERE matricola=$m`,{$now:nowIso(),$e:lastErr?.message||'Errore',$m:row.matricola}); saveDb(); addSample({matricola:row.matricola,esito:'errore',errore:lastErr?.message||'Errore'});
}
async function processSyncQueue(mode='new', input={}){
  if(syncState.running) return syncState;
  const config={...DEFAULT_SYNC_CONFIG,...input,batchSize:Math.max(1,Number(input.batchSize||25)),delayMs:Math.max(300,Number(input.delayMs||1500)),cacheHours:Math.max(0,Number(input.cacheHours??12)),maxRetries:Math.max(0,Number(input.maxRetries??2))};
  const queue=buildQueue(mode); syncState={...syncState,running:true,stopRequested:false,startedAt:nowIt(),finishedAt:null,total: mode==='resume'?Math.max(syncState.total||queue.length,queue.length+syncState.currentIndex):queue.length,checked:mode==='resume'?(syncState.checked||0):0,updated:mode==='resume'?(syncState.updated||0):0,skipped:mode==='resume'?(syncState.skipped||0):0,cached:mode==='resume'?(syncState.cached||0):0,errors:mode==='resume'?(syncState.errors||0):0,currentMatricola:'',lastMessage:'Sync avviata',config,lastSamples:mode==='resume'?(syncState.lastSamples||[]):[]}; if(mode!=='resume') syncState.currentIndex=0; saveState();
  while(queue.length && !syncState.stopRequested){ const batch=queue.splice(0,config.batchSize); for(const row of batch){ if(syncState.stopRequested) break; await syncOne(row,config); syncState.checked++; syncState.currentIndex++; syncState.lastMessage=`Salvato progresso ${syncState.checked}/${syncState.total}`; saveState(); saveDb(); await sleep(config.delayMs); } saveDb(); saveState(); await sleep(Math.max(1000,config.delayMs)); }
  syncState.running=false; syncState.finishedAt=nowIt(); syncState.currentMatricola=''; syncState.lastMessage=syncState.stopRequested?'Sync interrotta. Puoi riprenderla.':'Sync completata.'; saveState(); saveDb(); return syncState;
}

function getScheduleConfig(){ return readJson(SCHEDULE_PATH,{enabled:false,frequency:'daily',time:'01:00',dayOfWeek:'1',dayOfMonth:'1',batchSize:25,delayMs:1800,cacheHours:12,maxRetries:2}); }
function saveScheduleConfig(c){ writeJson(SCHEDULE_PATH,c); }
function shouldRunSchedule(c,now){ if(!c.enabled) return false; const hh=String(now.getHours()).padStart(2,'0'), mm=String(now.getMinutes()).padStart(2,'0'); if(`${hh}:${mm}`!==c.time) return false; if(c.frequency==='daily') return true; if(c.frequency==='weekly') return String(now.getDay())===String(c.dayOfWeek); if(c.frequency==='monthly') return String(now.getDate())===String(c.dayOfMonth); return false; }

app.get('/api/results',(req,res)=>{
  const q=String(req.query.q||'').trim(); const vpMonthRaw=String(req.query.vpMonth||'').trim(); const vpMonth=vpMonthRaw?vpMonthRaw.padStart(2,'0'):''; const vpYear=String(req.query.vpYear||'').trim();
  const page=Math.max(1,Number(req.query.page||1)); const perPage=Math.min(Math.max(1,Number(req.query.perPage||100)),500); const offset=(page-1)*perPage; const like=`%${q}%`;
  const where=`WHERE (($q='' OR r.matricola LIKE $like OR r.stato LIKE $like OR r.partita_iva LIKE $like OR r.partita_iva_vp LIKE $like OR r.denominazione LIKE $like OR COALESCE(r.link_qr,u.url) LIKE $like)) AND ($vpMonth='' OR substr(COALESCE(r.data_vp,r.ultima_vp,''),4,2)=$vpMonth) AND ($vpYear='' OR substr(COALESCE(r.data_vp,r.ultima_vp,''),7,4)=$vpYear)`;
  const params={$q:q,$like:like,$vpMonth:vpMonth==='00'?'':vpMonth,$vpYear:vpYear}; const total=Number(all(`SELECT COUNT(*) AS total FROM results r LEFT JOIN urls u ON u.rowid=r.url_id ${where}`,params)[0]?.total||0); const totalPages=Math.max(1,Math.ceil(total/perPage));
  const rows=all(`SELECT r.matricola,r.stato,COALESCE(r.data_vp,r.ultima_vp) AS data_vp,r.risultato_vp,r.partita_iva_vp AS piva_laboratorio,r.cf_tecnico,r.ultima_trasmissione,r.versione_fw AS versione_software,r.partita_iva,r.denominazione,COALESCE(r.link_qr,u.url) AS link_qr,r.last_sync,r.sync_error FROM results r LEFT JOIN urls u ON u.rowid=r.url_id ${where} ORDER BY r.matricola LIMIT $limit OFFSET $offset`,{...params,$limit:perPage,$offset:offset});
  res.json({rows,page,perPage,total,totalPages});
});
app.post('/api/import-url',async(req,res)=>{ try{ const url=normalizeUrlInput(req.body?.url); const {statusCode,data}=await fetchAndParse(url,{cacheHours:0}); const saved=upsertImportedResult(url,data); res.json({ok:true,http:statusCode,saved,data,message:`QR importato e salvato: ${saved.matricola}`}); }catch(e){ res.status(400).json({ok:false,error:e.message}); }});
app.post('/api/sync/start',(req,res)=>{ if(syncState.running) return res.json({ok:true,message:'Sync già in corso',state:publicSyncState()}); const mode=req.body?.resume?'resume':'new'; processSyncQueue(mode,req.body||{}).catch(e=>{syncState.running=false;syncState.errors++;syncState.lastMessage='Errore sync: '+e.message;saveState();console.error(e);}); res.json({ok:true,message:mode==='resume'?'Ripresa sync avviata':'Sync intelligente avviata',state:publicSyncState()}); });
app.post('/api/sync/stop',(req,res)=>{ syncState.stopRequested=true; syncState.lastMessage='Richiesta interruzione ricevuta.'; saveState(); res.json({ok:true,state:publicSyncState()}); });
app.get('/api/sync/status',(req,res)=>res.json(publicSyncState()));
app.post('/api/cache/clear',(req,res)=>{ cache={}; saveCache(); res.json({ok:true,message:'Cache svuotata'}); });
app.get('/api/schedule',(req,res)=>res.json(getScheduleConfig()));
app.post('/api/schedule/save',(req,res)=>{ const b=req.body||{}; const c={enabled:!!b.enabled,frequency:['daily','weekly','monthly'].includes(b.frequency)?b.frequency:'daily',time:String(b.time||'01:00'),dayOfWeek:String(b.dayOfWeek||'1'),dayOfMonth:String(b.dayOfMonth||'1'),batchSize:Math.max(1,Number(b.batchSize||25)),delayMs:Math.max(300,Number(b.delayMs||1800)),cacheHours:Math.max(0,Number(b.cacheHours??12)),maxRetries:Math.max(0,Number(b.maxRetries??2))}; saveScheduleConfig(c); res.json({ok:true,config:c,message:'Programmazione salvata'}); });
app.get('/download-db',(req,res)=>{ saveDb(); res.download(DB_PATH,'qrcode.db'); });

let lastScheduleRunKey='';
cron.schedule('* * * * *', async()=>{ try{ const c=getScheduleConfig(); const n=new Date(); if(!shouldRunSchedule(c,n) || syncState.running) return; const key=`${n.toISOString().slice(0,10)}-${n.getHours()}-${n.getMinutes()}-${c.frequency}`; if(key===lastScheduleRunKey) return; lastScheduleRunKey=key; await processSyncQueue('new',c); }catch(e){ console.error('ERRORE SYNC PROGRAMMATA',e); } });

async function initDb(){ SQL=await initSqlJs({ locateFile:file=>path.join(__dirname,'node_modules','sql.js','dist',file) }); if(!fs.existsSync(DB_PATH)) throw new Error('Database non trovato'); db=new SQL.Database(fs.readFileSync(DB_PATH)); migrate(); loadCache(); loadState(); }
initDb().then(()=>app.listen(PORT,()=>console.log(`Server avviato: http://localhost:${PORT}`))).catch(e=>{console.error(e);process.exit(1);});
