const express=require("express");
const http=require("http");
const fs=require("fs");
const fetch=require("node-fetch");
const WS=require("ws");
const {exec}=require("child_process");

const app=express();
const server=http.createServer(app);

app.use(express.static("frontend"));

const LIVE_MAX=43200;

if(!fs.existsSync("data"))
fs.mkdirSync("data");

const pairs={};
const building={};

const sort=a=>a.sort((x,y)=>x.time-y.time);

function buildHistory(symbol){

return new Promise((resolve,reject)=>{

if(building[symbol]){
resolve();
return;
}

building[symbol]=true;

console.log("BUILD HISTORY:",symbol);

exec(
`node historyEngine.js ${symbol}`,
(err,stdout,stderr)=>{

building[symbol]=false;

if(stdout)console.log(stdout);

if(stderr)console.log(stderr);

if(err){

console.log(
"BUILD ERROR",
symbol,
err.message
);

reject(err);

return;

}

console.log(
"BUILD COMPLETE:",
symbol
);

resolve();

});

});

}

async function loadPair(symbol){

if(pairs[symbol])
return pairs[symbol];

let hp=`data/${symbol}-history.json`;

let lp=`data/${symbol}-live.json`;

if(!fs.existsSync(hp)){

console.log(
"NO HISTORY FILE:",
symbol
);

await buildHistory(symbol);

}

let history=[];
let live=[];

try{

if(fs.existsSync(hp))
history=JSON.parse(
fs.readFileSync(hp)
);

if(fs.existsSync(lp))
live=JSON.parse(
fs.readFileSync(lp)
);

}catch(e){

console.log(
"LOAD ERROR",
symbol,
e.message
);

}

pairs[symbol]={
symbol,
history,
live,
trades:[]
};

console.log(
"PAIR",
symbol,
"HISTORY",
history.length,
"LIVE",
live.length
);

startWS(symbol);

return pairs[symbol];

}

function save(symbol){

let p=pairs[symbol];

fs.writeFileSync(
`data/${symbol}-live.json`,
JSON.stringify(p.live)
);

}

function build(symbol){

let p=pairs[symbol];

let map={};

for(let old of p.live)
map[old.time]={...old};

for(let t of p.trades){

let k=
Math.floor(t.time/60000)
*60000;

if(!map[k]){

let prev=
p.live.length?
p.live[p.live.length-1].flow:
p.history.length?
p.history[p.history.length-1].flow:
0;

map[k]={
time:k,
open:t.price,
high:t.price,
low:t.price,
close:t.price,
buy:0,
sell:0,
flow:prev,
rfSell:0,
rfBuy:0
};

}

let c=map[k];

c.high=Math.max(
c.high,
t.price
);

c.low=Math.min(
c.low,
t.price
);

c.close=t.price;

if(t.side==="Buy")
c.buy+=t.size;
else
c.sell+=t.size;

}

let out=sort(
Object.values(map)
);

let flow=0;

let rfSell=0;

let rfBuy=0;

for(let i=0;i<out.length;i++){

if(out[i].buy>out[i].sell)
flow++;

else if(
out[i].sell>out[i].buy
)
flow--;

out[i].flow=flow;

out[i].makerSell=
out[i].buy;

out[i].makerBuy=
out[i].sell;

if(i){

if(
out[i].makerSell>
out[i-1].makerSell
)
rfSell++;

else if(
out[i].makerSell<
out[i-1].makerSell
)
rfSell--;

if(
out[i].makerBuy>
out[i-1].makerBuy
)
rfBuy++;

else if(
out[i].makerBuy<
out[i-1].makerBuy
)
rfBuy--;

}

out[i].rfSell=rfSell;

out[i].rfBuy=rfBuy;

}

return out;

}

function startWS(symbol){

const ws=new WS(
"wss://stream.bybit.com/v5/public/linear"
);

ws.on("open",()=>{

console.log(
"WS",
symbol,
"CONNECTED"
);

ws.send(JSON.stringify({
op:"subscribe",
args:[
`publicTrade.${symbol}`
]
}));

});

ws.on("message",m=>{

let j=JSON.parse(m);

if(!j.data)
return;

let p=pairs[symbol];

for(let t of j.data){

p.trades.push({
time:t.T,
price:+t.p,
size:+t.v,
side:t.S
});

}

if(p.trades.length>100){

let built=build(symbol);

let map={};

for(let c of built)
map[c.time]=c;

p.live=sort(
Object.values(map)
);

while(
p.live.length>LIVE_MAX
)
p.live.shift();

p.trades=[];

save(symbol);

console.log(
symbol,
"LIVE",
p.live.length,
"FLOW",
p.live.length?
p.live[
p.live.length-1
].flow:
0
);

}

});

ws.on("error",e=>{

console.log(
"WS ERROR",
symbol,
e.message
);

});

ws.on("close",()=>{

console.log(
"WS CLOSED",
symbol
);

});

}

app.get("/pairs",async(req,res)=>{

try{

let r=await fetch(
"https://api.binance.com/api/v3/exchangeInfo"
);

let j=await r.json();

res.json(
j.symbols
.filter(
x=>x.status==="TRADING"
)
.map(x=>x.symbol)
);

}catch(e){

res.json([]);

}

});

app.get("/data/:symbol",async(req,res)=>{

try{

let symbol=
req.params.symbol
.toUpperCase();

await loadPair(symbol);

let p=pairs[symbol];

res.json(sort(

[...p.history,...p.live]

.filter(
x=>x.time&&x.time>1e12
)

));

}catch(e){

res.json([]);

}

});

app.get("/",(req,res)=>{

res.sendFile(
__dirname+
"/frontend/home.html"
);

});

loadPair("BTCUSDT");

server.listen(3000,()=>{

console.log("SERVER 3000");

});