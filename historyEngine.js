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

let start=
Date.now()-
7*24*60*60*1000;

let end=Date.now();

let out=[];

while(start<end){

let r=await fetch(
`https://api.binance.com/api/v3/klines?symbol=${SYMBOL}&interval=1m&startTime=${start}&limit=1000`
);

let j=await r.json();

if(!Array.isArray(j)||!j.length)
break;

for(let k of j){

let volume=+k[5];

let takerBuy=+k[9];

let takerSell=
volume-takerBuy;

if(takerSell<0)
takerSell=0;

out.push({

time:+k[0],

open:+k[1],
high:+k[2],
low:+k[3],
close:+k[4],

volume,

buy:takerBuy,
sell:takerSell,

makerSell:takerBuy,
makerBuy:takerSell,

flow:0,

rfDelta:0

});

}

console.log(
"KLINES:",
out.length,
new Date(
out[out.length-1].time
).toISOString()
);

start=
j[j.length-1][0]+60000;

await new Promise(
x=>setTimeout(x,50)
);

}

history=sort(
out.filter(x=>x.time)
);

console.log(
"KLINES READY:",
SYMBOL,
history.length
);

}

function buildFlow(){

let flow=0;

let rf=0;

for(let i=0;i<history.length;i++){

let c=history[i];

if(c.buy>c.sell)
flow++;

else if(c.sell>c.buy)
flow--;

c.flow=flow;

if(i){

let prev=
history[i-1];

let prevDelta=
prev.makerSell-
prev.makerBuy;

let currentDelta=
c.makerSell-
c.makerBuy;

if(currentDelta>prevDelta)
rf++;

else if(currentDelta<prevDelta)
rf--;

}

c.rfDelta=rf;

}

console.log(
"FLOW READY:",
SYMBOL,
history.length
);

}

(async()=>{

await loadKlines();

buildFlow();

save();

console.log(
"DONE:",
SYMBOL
);

})();