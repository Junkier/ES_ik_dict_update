const express = require('express');
const app = express();
const fs = require("fs");
const moment = require("moment");

const port_num = 32999;

// express.static("/" , {
//     "etag" : ,
//     "lastModified" : ,
//     "setHeaders" :
// });
app.use(express.static(__dirname , {
    etag : true,
    lastModified : true
}))


app.use("/update_words" , function(req,res,next){
    new Promise(function(resolve,reject){
        fs.readFile("good.txt","utf-8" , function(err,data){
            if(err){
                reject(err);
            } else {
                // console.log(fs.statSync("good.txt").mtime);
                // console.log(moment(fs.statSync("good.txt").mtime).format("ddd MMM DD YYYY HH:mm:ss [GMT]ZZ"));
                resolve(data);
            }
        })
    }).then(function(result){
        // res.set("lastModified",(new Date()).toString());
        res.set("Last-Modified",moment(fs.statSync("good.txt").mtime).format("ddd MMM DD YYYY HH:mm:ss [GMT]ZZ"));
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
