const express = require('express');
const app = express();
const fs = require("fs");
const moment = require("moment");

const port_num = 32999;
const secretary = require("./secretary");

var lock = true;

fs.watch("new_dict.txt",(eventType,fileName)=>{
    if(lock){
        lock = false ;
        secretary.build_es_new_ik_words().then(function(result){
            lock = true;
            console.log(result);
        }).catch(function(err){
            lock = true;
            console.log(err);
        });
    }
})


app.use(express.static(__dirname , {
    etag : true,
    lastModified : true
}))

app.use("/update_words" , function(req,res,next){
    new Promise(function(resolve,reject){
        fs.readFile("gogo_list.txt","utf-8" , function(err,data){
            if(err){
                reject(err);
            } else {
                resolve(data);
            }
        })
    }).then(function(result){
        res.set("Last-Modified",moment(fs.statSync("gogo_list.txt").mtime).format("ddd MMM DD YYYY HH:mm:ss [GMT]ZZ"));
        res.send(result);
    }).catch(function(err){
        console.log(err);
        res.status(500).send("ErrorQQ");
    })
});

app.use("/",function(req,res,next){
    res.send("HiHi!!!");
})

app.listen(port_num,function(){
    console.log(`HiHi!!! Update ES-Word Server is running at ${port_num}.`);
});
