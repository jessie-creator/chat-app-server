# Chat App Server

## 環境安裝
1. install kafka

到[Apache Kafka官方網站t](https://kafka.apache.org/downloads)下載，我們的系統使用的是kafka_2.13-2.7.0的版本

2. 解壓縮Kafka

3. 根據需求修改properity檔案。
    
若要增加broker數，要在config路徑下新增server.properties的檔案，並定義broker.id、broker的Hostame和Port Number。

`// server-1.properties
broker.id=0
listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092`

4. 下載專案chat-app-server
`git clone https://github.com/jessie-creator/chat-app-server.git`

5. 申請Mongo DB的帳號

6. 使用