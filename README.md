# Chat App Server

## 環境安裝
### 1. install kafka (需要在linux或macOS環境下運行)
到[Apache Kafka官方網站](https://kafka.apache.org/downloads)下載，我們的系統使用的是kafka_2.13-2.7.0的版本

### 2. 解壓縮Kafka

### 3. 根據需求修改properity檔案。
若要增加broker數，要在config路徑下新增server.properties的檔案，並定義broker.id、broker的Hostame和Port Number。

```
// server-1.properties
broker.id=0
listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
```

### 4. 下載專案chat-app-server
```
git clone https://github.com/jessie-creator/chat-app-server.git
```

### 5. 修改.env檔，並將其放置到您的專案根目錄下
```
MONGODB_URI=your_mongodb_uri
KAFKA_BROKER=host1:port1
KAFKA_BROKER1=host2:port2
KAFKA_BROKER2=host3:port3
PORT=your_server_port
``` 
- 請先註冊Mongo DB帳號，並根據models中的schema去建資料庫，接著複製您的uri到.env檔中。
- 須注意，連接Mongo DB前，須確保您的server IP有被加入Network Access中，若沒有，請先加入才能正常連線。


## 啟動server
### 1. 啟動zookeeper
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### 2. 啟動kafka server，將要運行的所有server皆啟動
```
bin/kafka-server-start.sh config/server.properties
```

### 3. 啟動server
```
node server.js
```
