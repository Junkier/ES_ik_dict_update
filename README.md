Elasticsearch --- ik 字典新舊詞分離 script.

功能:

server.js:
    webserver，做為 Elasticsearch-ik update 的 web service.
    update 的 base dictionary 為 gogo_list.txt
    (常駐型 web service.)

secretary.js:
    舊字典字數太多，可用此比對出 每日的新增詞
    製作成 gogo_list.txt ，再給 http server 取用
    做為 Elasticsearch-ik 熱詞更新的 base dictionary.
    (結合crontab 使用!!!)
