const elasticsearch = require("elasticsearch");
const moment = require("moment");
const fs = require("fs");
const exec = require("child_process").exec;

const client = new elasticsearch.Client({
    hosts: [
        '192.168.142.106:9201/',
    ],
    log: 'info',
    requestTimeout: Infinity
});

let SHARDING_NUM = 5

build_es_new_ik_words();

function build_es_new_ik_words(){
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
    }).catch(main_err=>{
        console.log(main_err);
    })
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
        fs.readFile("new_dict.txt","utf-8",function(err,data){
            if(err) {reject(err)}
            else{
                var indices_data = [];
                data.split(/\r\n/g).filter(word=>word.length>0)
                    .map((word,doc_number)=>{
                        indices_data.push(
                            {index  : {_index : "es_ik_update_words" , _type: "new_words" , _id: doc_number+1 }},
                            {"word" : word }
                        )
                    });
                client.bulk({
                    refresh : true,
                    body : indices_data
                }).then(result=>{
                    if(result["errors"]){
                        reject(new Error("indexing_new_words get Error!!!"));
                    } else {
                        resolve("done!!!");
                    }
                })
            };
        })
    });
}

// 用舊詞 search , 比較和新詞的差異
function differ_new_words_set(){
    return new Promise(function(resolve,reject){
        fs.readFile("old_dict.txt","utf-8",function(err,data){
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
                        }
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
        var new_words_set = result["hits"]["hits"].map(ele=>ele["_source"]["word"] + "\n").reduce((a,b)=>a+=b);
        fs.writeFile("gogo_list.txt",new_words_set,function(err){
            if(err){reject(err)}
            else{ resolve("done!!!")};
        });
    })
}
// 將新 dict 轉成 old dict .
function transfer_new_to_old_dict(){
    return new Promise(function(resolve,reject){
        exec("cp new_dict.txt old_dict.txt", function (error, stdout, stderr) {
          if (error) { reject(error) }
          else{ resolve("done!!!"); }
        });
    })
}
