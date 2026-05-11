function sort(a){return a.sort((x,y)=>x.time-y.time)}

function updateFlow(p){

let grouped={};

for(let t of p.trades){

let k=Math.floor(t.time/60000)*60000;

if(!grouped[k]){

grouped[k]={
time:k,
open:t.price,
high:t.price,
low:t.price,
close:t.price,
buy:0,
sell:0
};

}

let c=grouped[k];

c.high=Math.max(c.high,t.price);
c.low=Math.min(c.low,t.price);
c.close=t.price;

if(t.side==="Buy")
c.buy+=t.size;
else
c.sell+=t.size;

}

let out=sort(Object.values(grouped));

for(let i=0;i<out.length;i++){

let prev=
i?
out[i-1].flow:
p.live.length?
p.live[p.live.length-1].flow:
p.history.length?
p.history[p.history.length-1].flow:
0;

let dir=0;

if(out[i].buy>out[i].sell)
dir=1;
else if(out[i].sell>out[i].buy)
dir=-1;

out[i].flow=prev+dir;

}

return out;

}

module.exports={updateFlow};