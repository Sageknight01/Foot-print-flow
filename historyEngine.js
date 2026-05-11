const fs=require("fs");
const fetch=require("node-fetch");

const SYMBOL=process.argv[2]||"BTCUSDT";

if(!fs.existsSync("data"))
fs.mkdirSync("data");

let history=[];

const sort=a=>a.sort((x,y)=>x.time-y.time);

function save(){

fs.writeFileSync(
`data/${SYMBOL}-history.json`,
JSON.stringify(history)
);

console.log(
"SAVED:",
SYMBOL,
history.length
);

}

async function loadKlines(){

console.log(
"LOADING KLINES:",
SYMBOL
);

let start=new Date("2026-04-01T00:00:00Z").getTime();

let end=Date.now();

let out=[];

while(start<end){

let r=await fetch(
`https://api.binance.com/api/v3/klines?symbol=${SYMBOL}&interval=1m&startTime=${start}&limit=1000`
);

let j=await r.json();

if(!Array.isArray(j)||!j.length)
break;

for(let k of j)
out.push({
time:+k[0],
open:+k[1],
high:+k[2],
low:+k[3],
close:+k[4],
buy:0,
sell:0,
flow:0
});

console.log(
"KLINES:",
out.length,
new Date(out[out.length-1].time).toISOString()
);

start=j[j.length-1][0]+60000;

await new Promise(x=>setTimeout(x,80));
}

history=sort(out.filter(x=>x.time));

console.log(
"KLINES READY:",
SYMBOL,
history.length
);

}

async function loadFlow(){

console.log(
"LOADING FLOW:",
SYMBOL
);

let start=history[0].time;

let end=Date.now();

let map={};

while(start<end){

let next=start+4*60*60*1000;

let r=await fetch(
`https://api.binance.com/api/v3/aggTrades?symbol=${SYMBOL}&startTime=${start}&endTime=${next}&limit=1000`
);

let j=await r.json();

console.log(
"BLOCK:",
SYMBOL,
new Date(start).toISOString(),
"TRADES:",
Array.isArray(j)?j.length:"BAD"
);

if(Array.isArray(j)){

for(let t of j){

let k=String(Math.floor(t.T/60000)*60000);

if(!map[k])
map[k]={buy:0,sell:0};

t.m
?map[k].sell+=+t.q
:map[k].buy+=+t.q;

}

}

start=next+1;

await new Promise(x=>setTimeout(x,80));

}

let flow=0;

history=history.map((c,i)=>{

let key=String(c.time);

let m=map[key]||{buy:0,sell:0};

if(m.buy>m.sell)flow++;
else if(m.sell>m.buy)flow--;

let out={
...c,
buy:m.buy,
sell:m.sell,
flow
};

if(i<10)
console.log(
"CANDLE:",
SYMBOL,
i,
new Date(c.time).toISOString(),
"BUY:",m.buy,
"SELL:",m.sell,
"FLOW:",flow
);

return out;

});

console.log(
"FLOW READY:",
SYMBOL,
history.length
);

console.log(
"FLOW SAMPLE:",
history.slice(0,10).map(x=>x.flow)
);

}

(async()=>{

await loadKlines();

await loadFlow();

save();

console.log(
"DONE:",
SYMBOL
);

})();