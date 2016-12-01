const elasticsearch = require("elasticsearch");
const moment = require("moment");
const fs = require("fs");
const readLine = require("lei-stream").readLine;
const exec = require("child_process").exec;

const client = new elasticsearch.Client({
    hosts: [
        '192.168.142.106:9201/',
    ],
    log: 'info',
    requestTimeout: Infinity
});

let SHARDING_NUM = 5
let new_ver_dict = "new_dict.txt";
let old_ver_dict = "old_dict.txt";
let gogo_list = "gogo_list.txt";

exports.build_es_new_ik_words = build_es_new_ik_words;

function build_es_new_ik_words(){
    return new Promise(function(resolve,reject){
        console.log("=".repeat(50));
        console.log("Start to build gogo_list.txt for es update.");
        // StepI :
        // Renew Index. (Delete and Rebuild index)
        renew_index().then(m_I_result=>{
        // Step II :
        // Indexing New version words.
            console.log(`[StepI] renew_index is ${m_I_result}`);
            return indexing_new_words();
        }).then(m_II_result=>{
        // Step III :
        // Use Old_words to search , differing new words set.
            console.log(`[StepII] indexing_new_words is ${m_II_result}`);
            return differ_new_words_set();
        }).then(m_III_result=>{
        // Step IV :
        // Write down to gogo_list.txt for updating ES dict.
            console.log(`[StepIII] differ_new_words_set is done!!!`);
            return write_down_to_gogo_list(m_III_result);
        }).then(m_IV_result=>{
        // Step V :
        // Transfer New_dict into Old_dict for next building.
            console.log(`[StepIV] write_down_to_gogo_list is ${m_IV_result}!!!`);
            return transfer_new_to_old_dict();
        }).then(m_V_result=>{
            console.log(`[StepV] transfer_new_to_old_dict is ${m_V_result}!!!`);
            console.log("OK,we're done.");
            console.log("=".repeat(50));
            resolve("secretary done.");
        }).catch(main_err=>{
            console.log(main_err);
            reject("get Error");
        })
    });
}

function renew_index(){
    return new Promise(function(resolve,reject){
        client.indices.exists({
            index: "es_ik_update_words"
        }).then((bool_result) => {
            if(bool_result){
                client.indices.delete({index: "es_ik_update_words"}).then(()=>{
                    create_index("es_ik_update_words").then((result)=>{
                        resolve("done!!!");
                    }).catch(err=>{
                        console.log(err);
                    })
                }).catch(err=>{
                    console.log(err);
                })
            }else{
                create_index("es_ik_update_words").then((result)=>{
                    resolve("done!!!");
                }).catch(err=>{
                    console.log(err);
                })
            }
        });
    });

    function create_index(index_name){
        return client.indices.create({
                index: index_name ,
                body : {
                    "settings":{
                        "max_result_window":1000000,
                        "refresh_interval" : "10s",
                        "number_of_shards" : SHARDING_NUM ,
                        "number_of_replicas" : 0 ,
                        "analysis":{
                            "analyzer":"ik_smart"
                        }
                    },
                    "mappings": {
                        "_default_" : {
                            "_all" : {"enabled" : false }
                        },
                        "old_words" : {
                            "Properties" : {
                                "word" : { "type"  : "string" , "index" : "not_analyzed" }
                            }
                        }
                    }
                }
            })
    }
}

// 塞新詞.
function indexing_new_words(){
    return new Promise(function(resolve,reject){

        var BATCH_SIZE = 10000 , update_time = 0 ,
            QUEUE_LIMIT_NUM = 20 , SLEEP_TIME = 5000;
        var es_helper = new ES_GoodMan() ,
            memory_checker = new memoryCheck();

        var memory_issue = setInterval(memory_checker.printMemoryUsage, 1000);

        var rs = readLine(fs.createReadStream(new_ver_dict),{
            newline : '\n' ,
            autoNext : false
        });

        rs.on("data" , function(data){
            es_helper.raw_words.push(data.replace(/\r/g,""));
            if(es_helper.raw_words.length % BATCH_SIZE ===0){
                memory_checker.printSpeedInfo(update_time*BATCH_SIZE);
                var indices_data = es_helper.transfer_to_es_word(update_time,BATCH_SIZE);
                update_time++;
                es_helper.push_to_es(indices_data).then(function(result){
                    console.log(`Update time : ${update_time}!!!!`);
                    if(result["errors"]){
                        console.log(result["items"][0]);
                        reject(new Error("indexing_new_words get Error!!!"));
                    }
                    indices_data = null;
                }).catch(function(err){
                    console.log("幹Error啦!!!!");
                    reject(new Error(err));
                });

                // 2. es queue 比較嚴重
                es_helper.catch_queue_number().then(function(result){
                    var es_queue_num = result[0]["bulk.queue"];
                    if(parseInt(es_queue_num) >QUEUE_LIMIT_NUM ){
                        console.log(`ES queue number : ${parseInt(es_queue_num)} , so wait!!!`);
                        rs.pause();
                        setTimeout(function(){
                            rs.resume();
                            rs.next();
                            // console.log(`GC : ${memory_checker._cleanLock}`);
                        },SLEEP_TIME);
                    } else{
                        rs.next();
                    }
                }).catch(function(err){
                    console.log(err);
                });
            } else {
                rs.next();
            };

            // 1. memory too high (目前用不到)
            // if(memory_checker.is_memory_too_high() ){
            //     rs.pause();
            //     if(memory_checker.is_memory_change_stable() && !memory_checker._cleanLock){
            //         memory_checker.trigger_gc_come();
            //     }
            //     setTimeout(function(){
            //         rs.resume();
            //         // rs.next();
            //         console.log(`GC : ${memory_checker._cleanLock}`);
            //     },5000);
            // } else {
            //     rs.next();
            //     if(memory_checker._cleanLock){
            //         memory_checker._cleanLock = false;
            //     }
            // }
        })

        rs.on("end" , function(){
            if(es_helper.raw_words.length >0){
                var indices_data = es_helper.transfer_to_es_word(update_time,BATCH_SIZE);
                update_time++;
                es_helper.push_to_es(indices_data).then(function(result){
                    console.log(`readFile 結束 , 共 ${update_time} 次.`);
                    if(result["errors"]){
                        console.log(result);
                        reject(new Error("indexing_new_words get Error , insert time out !!!"));
                    } else{
                        clearInterval(memory_issue);
                        console.log(`Use time : ${(Date.now() - memory_checker._startTime)/1000} sec.`);
                        setTimeout(function(){
                            resolve("done!!!");
                        },5000);
                        // resolve("done!!!");
                    }
                    indices_data = null;
                }).catch(function(err){
                    console.log("幹Error啦!!!!");
                    reject(new Error(err));
                });
            }
        });

        function memoryCheck(){
            var $self = this;
            this._startTime = Date.now();
            this._lastMemoryUsed = 0;
            this._cleanLock = false;
            this.msTos = function(v){
                return parseInt(v/1000,0);
            };
            this.getSpentTime = function(){
                return Date.now() - $self._startTime;
            };
            this.printSpeedInfo = function(counter){
                var t = $self.msTos($self.getSpentTime());
                var s = counter / t;
                if (!isFinite(s)) s = counter;
                console.log('read %s lines, speed: %sL/S', counter, s.toFixed(0));
            };
            this.printMemoryUsage = function(){
                var info = process.memoryUsage();
                function mb (v) {
                    return (v / 1024 / 1024).toFixed(2) + 'MB';
                }
                $self._lastMemoryUsed = info.heapTotal/1024/1024;
                console.log('rss=%s, heapTotal=%s, heapUsed=%s', mb(info.rss), mb(info.heapTotal), mb(info.heapUsed));
            };
            // this.is_memory_too_high = function(){
            //     var info = process.memoryUsage() ;
            //     return (info.heapTotal/1024/1024 >200 ) ? true : false;
            // };
            // this.is_memory_change_stable = function(){
            //     var p = (process.memoryUsage().heapTotal/1024/1024 - $self._lastMemoryUsed)/ $self._lastMemoryUsed;
            //     console.log(`Change percent : ${(p*100).toFixed(2)}%`);
            //     return p < 0.001;
            // };
            // this.trigger_gc_come = function(){
            //     console.log("GC GOGOGO!!!!");
            //     // 要搭配 node --expose-gc server.js 指令QQ
            //     $self._cleanLock = true;
            //     global.gc();
            // }
        }

        function ES_GoodMan(){
            var $self = this;
            this.raw_words = [];
            this.transfer_to_es_word = function(count,batch_size){
                var out_list = [] ;
                $self.raw_words.map((word,doc_number)=>{
                    out_list.push(
                        {index  : {_index : "es_ik_update_words" , _type: "new_words" , _id: doc_number+1+count*batch_size }},
                        {"word" : word }
                    )
                })
                $self.raw_words = [];
                return out_list;
            };
            this.push_to_es = function(i_data){
                return client.bulk({
                    refresh : true,
                    body : i_data,
                    timeout: "10s"
                })
            };
            this.catch_queue_number = function(){
                return client.cat.threadPool({
                    format : "json",
                    v : true
                });
            };
        }
    });
}

// 用舊詞 search , 比較和新詞的差異
function differ_new_words_set(){
    return new Promise(function(resolve,reject){

        // 切成多次搜尋QQ
        fs.readFile(old_ver_dict,"utf-8",function(err,data){
            if(err) {reject(err)}
            else{
                var search_data = data.split(/\r\n/g).filter(word=>word.length>0);
                client.search({
                    index: "es_ik_update_words",
                    type: "new_words",
                    body: {
                        "query": {
                           "bool": {
                              "filter": {
                                 "bool": {
                                    "must_not": [
                                       {
                                          "terms": {
                                             "word": search_data
                                          }
                                       }
                                    ]
                                 }
                              }
                           }
                       },
                       "size":1000000
                    }
                }).then(result=>{
                    if(result["hits"]){
                        resolve(result);
                    } else {
                        reject(new Error("differ_new_words_set get Error!!!"));
                    }
                })
            };
        })
    });
}

// 寫入到 gogo_list , 等著被 ES-cluster update.
function write_down_to_gogo_list(result){
    return new Promise(function(resolve,reject){
        if(result["hits"]["hits"].length === 0){
            resolve("0 update , done!!!");
        };
        var new_words_set = result["hits"]["hits"].filter(ele=>ele["_source"]["word"].length>0)
                                                  .map(ele=>ele["_source"]["word"] + "\n").reduce((a,b)=>a+=b);
        // 改成用 append!?
        fs.writeFile(gogo_list,new_words_set,function(err){
            if(err){reject(err)}
            else{ resolve("done!!!")};
        });
    })
}
// 將新 dict 轉成 old dict .
function transfer_new_to_old_dict(){
    return new Promise(function(resolve,reject){
        exec(`cp ${new_ver_dict} ${old_ver_dict}`, function (error, stdout, stderr) {
          if (error) { reject(error) }
          else{ resolve("done!!!"); }
        });
    })
}
