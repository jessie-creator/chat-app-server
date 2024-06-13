# Chat App Server

## 環境安裝
1. install kafka (需要在linux或macOS環境下運行)

    到[Apache Kafka官方網站](https://kafka.apache.org/downloads)下載，我們的系統使用的是kafka_2.13-2.7.0的版本

2. 解壓縮Kafka

3. 根據需求修改properity檔案。
    
    若要增加broker數，要在config路徑下新增server.properties的檔案，並定義broker.id、broker的Hostame和Port Number。

    `
        // server-1.properties
        broker.id=0
        listeners=PLAINTEXT://localhost:9092
        advertised.listeners=PLAINTEXT://localhost:9092
    `

4. 下載專案chat-app-server
    `git clone https://github.com/jessie-creator/chat-app-server.git`

5. 註冊Mongo DB帳號，並根據models中的schema去建資料庫

6. 取得Mongo DB URI，更新Mongo DB URI

7. 確認server的Hostname和Port Number

## 啟動server
1. 啟動zookeeper
    `bin/zookeeper-server-start.sh config/zookeeper.properties`

2. 啟動kafka server，將要運行的所有server皆啟動
    `bin/kafka-server-start.sh config/server.properties`

3. 啟動server
    `node server.js`
