const { Kafka } = require('kafkajs');
const bodyParser = require('body-parser');
const express = require("express");
const mongoose = require("mongoose");
const passport = require("passport");
const LocalStrategy = require("passport-local").Strategy;
const cors = require("cors");
const jwt = require("jsonwebtoken");
const multer = require("multer");
const EventEmitter = require('events');
const app = express();
const port = 8000;
const User = require("./models/user");
const Message = require("./models/message");
const MessageWithChatRoom = require("./models/MessageWithChatRoom");
const ChatRoom = require("./models/ChatRoom");

app.use(cors());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(passport.initialize());

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092', 'localhost:9093', 'localhost:9094']
});

const producer = kafka.producer();
let producerConnected = false;
const consumers = {};

mongoose
    .connect("mongodb+srv://jessiechou0307:5mG5q3XJyy8ZJtZd@cluster0.hm0xvfi.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0", {
        useNewUrlParser: true,
        useUnifiedTopology: true,
    })
    .then(() => {
        console.log("Connected to Mongo Db");
    })
    .catch((err) => {
        console.log("Error connecting to MongoDb", err);
    });
/*
app.listen(port, () => {
    console.log("Server running on port 8000");
});*/


async function createChatRoomsForFriends() {
    try {
        const users = await User.find().populate("friends");

        for (const user of users) {
            for (const friend of user.friends) {
                const sortedNames = [user.name, friend.name].sort();
                const chatRoomName = `${sortedNames[0]}-${sortedNames[1]}`;

                const existingChatRoom = await ChatRoom.findOne({
                    name: chatRoomName,
                    members: { $all: [user._id, friend._id] }
                });

                if (!existingChatRoom) {
                    const chatRoom = new ChatRoom({
                        name: chatRoomName,
                        image: "photo-stickers/1.png",
                        members: [user._id, friend._id],
                    });
                    await chatRoom.save();
                    console.log(`Chat room created: ${chatRoomName}`);
                }
            }
        }
    } catch (err) {
        console.error("Error in creating chat rooms:", err);
    }
}
/*
createChatRoomsForFriends().then(() => {
    console.log("Chat rooms created");
    //mongoose.disconnect();
}).catch(err => {
    console.error("Error creating chat rooms:", err);
    //mongoose.disconnect();
})
*/
const ensureProducerConnected = async () => {
    if (!producerConnected) {
        await producer.connect();
        producerConnected = true;
    }
};
/*
const ensureConsumerConnected = async () => {
    if (!consumer.isConnected()) {
        await consumer.connect();
        await consumer.subscribe({ topic: 'acceptedFriendsTopic', fromBeginning: true });
    }
};*/

const safeReplace = (str) => (str ? str.replace(/[^a-zA-Z0-9.-]/g, '_') : '');

const userConsumers = {};
const initConsumer = async (userId) => {
    if (userConsumers[userId]) {
        return;
    }

    const consumer = kafka.consumer({ groupId: `${userId}-group` });
    await consumer.connect();

    const chatRooms = await ChatRoom.find({ members: userId });
    for (const chatRoom of chatRooms) {
        await consumer.subscribe({ topic: chatRoom._id.toString(), fromBeginning: true });
    }

    //await ensureConsumerConnected();
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const { action, data } = JSON.parse(message.value.toString());
            switch (action) {
                /*
                case 'login':
                    await handleLogin(data);
                    break;
                case 'login_response':
                    await handleLoginResponse(data);
                    break;
                    */
                case 'getAcceptedFriends':
                    await handleGetAcceptedFriends(data, requestId);
                    break;
                case 'getAcceptedFriendsResponse':
                    await handleGetAcceptedFriendsResponse(data, requestId);
                    break;
                case 'sendMessage':
                    await handleSendMessage(data);
                    break;
                case 'addFriend':
                    await handleAddFriend(data);
                    break;
                case 'acceptFriendRequest':
                    await handleAcceptFriendRequest(data);
                    break;
                case 'getUserInfo':
                    await handleGetUserInfo(data);
                    break;
                case 'getAllUsers':
                    await handleGetAllUsers(data);
                    break;
                case 'allUsers':
                    await handleAllUsers(data);
                    break;
                case 'userInfo':
                    await handleUserInfo(data);
                    break;
                case 'getMessages':
                    await handleGetMessages(data);
                    break;
                case 'messagesResponse':
                    await handleMessagesResponse(data);
                    break;
                case 'deleteMessages':
                    await handleDeleteMessages(data);
                    break;
                case 'deleteMessagesResponse':
                    await handleDeleteMessagesResponse(data);
                    break;
                case 'getFriends':
                    await handleGetFriends(data);
                    break;
                case 'friendsResponse':
                    await handleFriendsResponse(data);
                    break;
                case 'getSentFriendRequests':
                    await handleGetSentFriendRequests(data);
                    break;
                case 'sentFriendRequestsResponse':
                    await handleSentFriendRequestsResponse(data);
                    break;
                default:
                    console.log(`Unknown action: ${action}`);
            }
        },
    });

    userConsumers[userId] = consumer;
    return consumer;
};

User.find({}).then(users => {
    users.forEach(user => initConsumer(safeReplace(user._id.toString())));
});
ChatRoom.find({}).then(chatrooms => {
    chatrooms.forEach(chatroom => initConsumer(safeReplace(chatroom._id.toString())));
});

/*
const handleLogin = async ({ email, password, userId }) => {
    try {
        console.log('Handling login for:', email);
        const user = await User.findOne({ email });

        if (!user || user.password !== password) {
            console.log('Invalid email or password');
            await producer.send({
                topic: safeReplace(userId.toString()),
                messages: [{ value: JSON.stringify({ action: 'login_response', success: false, data: { email, message: 'Invalid email or password' } }) }],
            });
            return;
        }

        const token = createToken(user._id);
        console.log('Login successful, generated token:', token);

        await producer.send({
            topic: safeReplace(userId.toString()),
            messages: [{ value: JSON.stringify({ action: 'login_response', success: true, data: { email, token } }) }],
        });
    } catch (err) {
        console.error('Error logging in user:', err);
        await producer.send({
            topic: safeReplace(userId.toString()),
            messages: [{ value: JSON.stringify({ action: 'login_response', success: false, data: { email, message: 'Error logging in user' } }) }],
        });
    }
};*/

//const loginEventEmitter = new EventEmitter();
/*
const handleLoginResponse = async (data) => {
    console.log('Received login response:', data);
    loginEventEmitter.emit(`login_response_${data.email}`, data);
};*/

const handleGetAcceptedFriends = async (data, requestId) => {
    const { userId } = data;

    try {
        const user = await User.findById(userId).populate(
            "friends",
            "name email image"
        );
        const acceptedFriends = user.friends;

        await producer.send({
            topic: requestId,
            messages: [{ value: JSON.stringify({ action: 'getAcceptedFriendsResponse', data: { acceptedFriends } }) }],
        });
        console.log('Accepted friends processed and sent to Kafka');
    } catch (error) {
        console.error('Error processing accepted friends:', error);
        await producer.send({
            topic: requestId,
            messages: [{ value: JSON.stringify({ error: 'Internal Server Error' }) }],
        });
    }
};

const responseEventEmitter = new EventEmitter();
const handleGetAcceptedFriendsResponse = async (data, requestId) => {
    console.log(`Received accepted friends response: ${JSON.stringify(data)}`);
    responseEventEmitter.emit(requestId, data);
};
/*
const handleSendMessage = async ({ senderId, recipientId, messageType, messageText, imageFile }) => {
    const newMessage = new Message({
        senderId,
        recipientId,
        messageType,
        message: messageText,
        timestamp: new Date(),
        imageUrl: messageType === "image" ? imageFile : null,
    });
    try {
        await newMessage.save();
        console.log('Message sent successfully');
    } catch (err) {
        console.error('Error sending message:', err);
    }
};*/

const handleAddFriend = async ({ currentUserId, selectedUserId }) => {
    try {
        //update the recepient's friendRequestsArray!
        await User.findByIdAndUpdate(selectedUserId, { $push: { friendRequests: currentUserId } });
        //update the sender's sentFriendRequests array
        await User.findByIdAndUpdate(currentUserId, { $push: { sentFriendRequests: selectedUserId } });
        console.log('Friend request sent successfully');
    } catch (err) {
        console.error('Error sending friend request:', err);
    }
};

const handleAcceptFriendRequest = async ({ senderId, recipientId }) => {
    try {
        //retrieve the documents of sender and the recipient
        const sender = await User.findById(senderId);
        const recipient = await User.findById(recipientId);
        sender.friends.push(recipientId);
        recipient.friends.push(senderId);
        recipient.friendRequests = recipient.friendRequests.filter(id => id.toString() !== senderId);
        sender.sentFriendRequests = sender.sentFriendRequests.filter(id => id.toString() !== recipientId);
        await sender.save();
        await recipient.save();
        console.log('Friend request accepted successfully');
    } catch (err) {
        console.error('Error accepting friend request:', err);
    }
};
/*
const handleGetUserInfo = async ({ userId, requesterId }) => {
    try {
        const user = await User.findById(userId);
        if (user) {
            await ensureProducerConnected();
            await producer.send({
                topic: requesterId,
                messages: [{ value: JSON.stringify({ action: 'userInfo', data: user }) }],
            });
        } else {
            console.error('User not found');
            await producer.send({
                topic: requesterId,
                messages: [{ value: JSON.stringify({ action: 'userInfo', data: { error: 'User not found' } }) }],
            });
        }
    } catch (err) {
        console.error('Error getting user info:', err);
        await producer.send({
            topic: requesterId,
            messages: [{ value: JSON.stringify({ action: 'userInfo', data: { error: 'Error getting user info' } }) }],
        });
    }
};
*/
const handleUserInfo = async (data) => {
    console.log('Received user info:', data);
};

const handleGetAllUsers = async ({ loggedInUserId, requesterId }) => {
    try {
        const users = await User.find({ _id: { $ne: loggedInUserId } });
        await ensureProducerConnected();
        await producer.send({
            topic: requesterId,
            messages: [{ value: JSON.stringify({ action: 'allUsers', data: users }) }],
        });
    } catch (err) {
        console.error('Error retrieving users:', err);
    }
};

const handleAllUsers = async (data) => {
    console.log('All users received:', data);
};

const handleGetMessages = async ({ senderId, recepientId }) => {
    try {
        const messages = await Message.find({
            $or: [
                { senderId: senderId, recepientId: recepientId },
                { senderId: recepientId, recepientId: senderId },
            ],
        }).populate("senderId", "_id name");

        await ensureProducerConnected();
        await producer.send({
            topic: `${senderId}-${recepientId}`,
            messages: [{ value: JSON.stringify({ action: 'messagesResponse', data: { senderId, recepientId, messages } }) }],
        });
    } catch (err) {
        console.error('Error fetching messages:', err);
        await producer.send({
            topic: `${senderId}-${recepientId}`,
            messages: [{ value: JSON.stringify({ action: 'messagesResponse', data: { error: 'Error fetching messages' } }) }],
        });
    }
};

const handleMessagesResponse = async (data) => {
    console.log('Received messages:', data);
};

const handleDeleteMessages = async ({ messages }) => {
    try {
        await Message.deleteMany({ _id: { $in: messages } });
        await ensureProducerConnected();
        await producer.send({
            topic: 'deleteMessagesTopic',
            messages: [{ value: JSON.stringify({ action: 'deleteMessagesResponse', success: true }) }],
        });
    } catch (err) {
        console.error('Error deleting messages:', err);
        await producer.send({
            topic: 'deleteMessagesTopic',
            messages: [{ value: JSON.stringify({ action: 'deleteMessagesResponse', success: false, message: 'Error deleting messages' }) }],
        });
    }
};

const handleDeleteMessagesResponse = async (data) => {
    console.log('Delete messages response received:', data);
};

const handleGetFriends = async ({ userId }) => {
    try {
        const user = await User.findById(userId).populate("friends").lean();
        if (!user) {
            await producer.send({
                topic: 'friendsTopic',
                messages: [{ value: JSON.stringify({ action: 'friendsResponse', success: false, data: { userId, message: 'User not found' } }) }],
            });
            return;
        }

        const friendIds = user.friends.map((friend) => friend._id);

        await ensureProducerConnected();
        await producer.send({
            topic: 'friendsTopic',
            messages: [{ value: JSON.stringify({ action: 'friendsResponse', success: true, data: { userId, friendIds } }) }],
        });
    } catch (err) {
        console.error('Error getting friends:', err);
        await producer.send({
            topic: 'friendsTopic',
            messages: [{ value: JSON.stringify({ action: 'friendsResponse', success: false, data: { userId, message: 'Error getting friends' } }) }],
        });
    }
};

const handleFriendsResponse = async (data) => {
    console.log('Received friends response:', data);
};

const handleGetSentFriendRequests = async ({ userId }) => {
    try {
        const user = await User.findById(userId).populate("sentFriendRequests", "name email image").lean();
        const sentFriendRequests = user.sentFriendRequests;

        await ensureProducerConnected();
        await producer.send({
            topic: 'friendRequestsTopic',
            messages: [{ value: JSON.stringify({ action: 'sentFriendRequestsResponse', success: true, data: { userId, sentFriendRequests } }) }],
        });
    } catch (err) {
        console.error('Error getting sent friend requests:', err);
        await producer.send({
            topic: 'friendRequestsTopic',
            messages: [{ value: JSON.stringify({ action: 'sentFriendRequestsResponse', success: false, message: 'Error getting sent friend requests' }) }],
        });
    }
};

const handleSentFriendRequestsResponse = async (data) => {
    console.log('Received sent friend requests response:', data);
};

app.post("/register", (req, res) => {
    const { name, email, password, image } = req.body;

    // create a new User object
    const newUser = new User({ name, email, password, image });

    // save the user to the database
    newUser
        .save()
        .then(() => {
            res.status(200).json({ message: "User registered successfully" });
        })
        .catch((err) => {
            console.log("Error registering user", err);
            res.status(500).json({ message: "Error registering the user!" });
        });
});

//function to create a token for the user
const createToken = (userId) => {
    // Set the token payload
    const payload = { userId };
    // Generate the token with a secret key and expiration time
    const token = jwt.sign(payload, "Q$r2K6W8n!jCW%Zk", { expiresIn: "1h" });
    return token;
};

//endpoint for logging in of that particular user
/*
app.post("/login", async (req, res) => {
    const { email, password } = req.body;

    if (!email || !password) {
        return res.status(400).json({ message: "Email and the password are required" });
    }

    try {
        const user = await User.findOne({ email });

        if (!user) {
            return res.status(404).json({ message: "User not found" });
        }

        const topic = safeReplace(user._id.toString());

        await ensureProducerConnected();
        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify({ action: 'login', data: { email, password, userId: user._id } }) }],
        });

        const loginResponsePromise = new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                loginEventEmitter.removeListener(`login_response_${email}`, onResponse);
                reject(new Error('No response from authentication service'));
            }, 20000);

            const onResponse = (payload) => {
                clearTimeout(timeout);
                resolve(payload);
            };

            loginEventEmitter.once(`login_response_${email}`, onResponse);
        });

        const payload = await loginResponsePromise;
        if (payload) {
            console.log("success");
            res.status(200).json({ token: payload.token });
        } else {
            console.log("failed");
            res.status(401).json({ message: payload.message });
        }
    } catch (err) {
        console.error('Error sending message:', err);
        res.status(500).json({ message: "Error sending login request" });
    }
});
*/
app.post("/login", (req, res) => {
    const { email, password } = req.body;
    //check if the email and password are provided
    if (!email || !password) {
        return res
            .status(404)
            .json({ message: "Email and the password are required" });
    }
    //check for that user in the database
    User.findOne({ email })
        .then((user) => {
            if (!user) {
                //user not found
                return res.status(404).json({ message: "User not found" });
            }
            //compare the provided passwords with the password in the database
            if (user.password !== password) {
                return res.status(404).json({ message: "Invalid Password!" });
            }
            const token = createToken(user._id);
            res.status(200).json({ token });
        })
        .catch((error) => {
            console.log("error in finding the user", error);
            res.status(500).json({ message: "Internal server Error!" });
        });
});

//endpoint to access all the users except the user who's is currently logged in!
app.get("/users/:userId", (req, res) => {
    const loggedInUserId = req.params.userId;

    User.find({ _id: { $ne: loggedInUserId } })
        .then((users) => {
            res.status(200).json(users);
        })
        .catch((err) => {
            console.log("Error retrieving users", err);
            res.status(500).json({ message: "Error retrieving users" });
        });
});

//endpoint to send a request to a user
app.post("/friend-request", async (req, res) => {
    const { currentUserId, selectedUserId } = req.body;
    const topic = safeReplace(selectedUserId);
    try {
        await ensureProducerConnected();
        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify({ action: 'addFriend', data: { currentUserId, selectedUserId } }) }],
        });
        res.status(200).json({ message: "Friend request sent" });
    } catch (err) {
        console.error('Error sending message:', err);
        res.status(500).json({ message: "Error sending friend request" });
    }
});

//endpoint to show all the friend-requests of a particular user
app.get("/friend-request/:userId", async (req, res) => {
    try {
        const { userId } = req.params;

        //fetch the user document based on the User id
        const user = await User.findById(userId)
            .populate("freindRequests", "name email image")
            .lean();

        const freindRequests = user.freindRequests;

        res.json(freindRequests);
    } catch (error) {
        console.log(error);
        res.status(500).json({ message: "Internal Server Error" });
    }
});

//endpoint to accept a friend-request of a particular person
app.post("/friend-request/accept", async (req, res) => {
    const { senderId, recipientId } = req.body;
    const topic = safeReplace(senderId);
    try {
        await ensureProducerConnected();
        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify({ action: 'acceptFriendRequest', data: { senderId, recipientId } }) }],
        });
        res.status(200).json({ message: "Friend request accepted" });
    } catch (err) {
        console.error('Error sending message:', err);
        res.status(500).json({ message: "Error accepting friend request" });
    }
});

//endpoint to access all the friends of the logged in user!
/*
app.get("/accepted-friends/:userId", async (req, res) => {
    const { userId } = req.params;
    const requestId = `acceptedFriends_${userId}_${Date.now()}`;
    const topic = 'acceptedFriendsTopic';

    console.log(`Requesting accepted friends for user: ${userId}`);

    try {
        await ensureProducerConnected();
        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify({ action: 'getAcceptedFriends', data: { userId }, requestId }) }],
        });

        const friendsResponsePromise = new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                res.status(500).json({ error: "No response from friends service" });
                reject(new Error('No response from friends service'));
            }, 10000); // 10秒超时

            const onResponse = (payload) => {
                clearTimeout(timeout);
                resolve(payload);
            };

            consumer.once('message', async (message) => {
                const { action, data, error } = JSON.parse(message.value.toString());
                if (message.topic === requestId) {
                    if (error) {
                        res.status(500).json({ error });
                    } else if (action === 'getAcceptedFriendsResponse') {
                        res.json(data.acceptedFriends);
                    }
                    onResponse();
                }
            });
        });

        await friendsResponsePromise;
    } catch (error) {
        console.error('Error requesting accepted friends:', error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});*/
app.get("/accepted-friends/:userId", async (req, res) => {
    try {
        const { userId } = req.params;
        const user = await User.findById(userId).populate(
            "friends",
            "name email image"
        );
        const acceptedFriends = user.friends;
        res.json(acceptedFriends);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

// endpoint to find all chatrooms a user is a member of
app.get("/chatrooms/:userId", async (req, res) => {
    try {
        const { userId } = req.params;
        const chatRooms = await ChatRoom.find({ members: userId }).populate("members", "name email image");
        res.json(chatRooms);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

// Configure multer for handling file uploads
const storage = multer.diskStorage({
    // Specify the desired destination folder
    destination: (req, file, cb) => cb(null, "files/"),
    filename: (req, file, cb) => {
        // Generate a unique filename for the uploaded file
        const uniqueSuffix = Date.now() + "-" + Math.round(Math.random() * 1e9);
        cb(null, uniqueSuffix + "-" + file.originalname);
    },
});

const upload = multer({ storage });
const handleSendMessage = async (data) => {
    const { senderId, chatRoomId, messageType, messageText, imageUrl } = data;
    //const imageUrl = messageType === "image" ? req.file.path : null;
    // Find the chat room
    const chatRoom = await ChatRoom.findById(chatRoomId).populate("members");

    // Create a new message
    const newMessage = new MessageWithChatRoom({
        senderId,
        chatRoomId,
        messageType,
        message: messageText,
        imageUrl,
        timestamp: new Date(),
    });
    await newMessage.save();

    // Notify all members in the chat room except the sender
    const recipientIds = chatRoom.members
        .filter(member => member._id.toString() !== senderId)
        .map(member => member._id);

    await notifyRecipients(recipientIds, newMessage);
};

// 处理获取用户信息
const handleGetUserInfo = async (data) => {
    const { userId, requesterId } = data;

    try {
        const user = await User.findById(userId);
        if (user) {
            await ensureProducerConnected();
            await producer.send({
                topic: requesterId,
                messages: [{ value: JSON.stringify({ action: 'userInfo', data: user }) }],
            });
        } else {
            console.error('User not found');
        }
    } catch (err) {
        console.error('Error getting user info:', err);
    }
};

// 发送消息到聊天室
/*
app.post("/messages", upload.single("imageFile"), async (req, res) => {
    try {
        const { senderId, chatRoomId, messageType, messageText } = req.body;
        const imageUrl = messageType === "image" ? req.file.path : null;

        // 确保生产者已连接
        await ensureProducerConnected();

        // 将消息发送到 Kafka 主题
        await producer.send({
            topic: chatRoomId,
            messages: [
                { value: JSON.stringify({ action: 'sendMessage', data: { senderId, chatRoomId, messageType, messageText, imageUrl } }) }
            ],
        });

        res.status(200).json({ message: "Message sent successfully" });
    } catch (error) {
        console.error('Error sending message:', error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});
*/
// 获取用户信息
/*
app.get("/user/:userId", async (req, res) => {
    const { userId } = req.params;
    const requesterId = req.query.requesterId;
    const topic = userId; // Assuming you use the userId as topic

    try {
        await ensureProducerConnected();
        const consumer = await initConsumer(requesterId);

        await consumer.subscribe({ topic: requesterId, fromBeginning: true });

        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify({ action: 'getUserInfo', data: { userId, requesterId } }) }],
        });

        // Listen for the response from Kafka
        const timeout = setTimeout(() => {
            res.status(500).json({ message: "No response from user info service" });
        }, 5000);

        const handleMessage = async ({ topic, partition, message }) => {
            const payload = JSON.parse(message.value.toString());
            if (payload.action === 'userInfo' && payload.data._id === userId) {
                clearTimeout(timeout);
                res.status(200).json(payload.data);
                await consumer.disconnect();
                delete userConsumers[requesterId]; // Clean up consumer after use
            }
        };

        await consumer.run({
            eachMessage: handleMessage
        });
    } catch (err) {
        console.error('Error sending message:', err);
        res.status(500).json({ message: "Error requesting user info" });
    }
});*/
/*
app.get("/user/:userId", async (req, res) => {
    const { userId } = req.params;
    const requesterId = req.query.requesterId;
    const topic = safeReplace(userId);

    try {
        await ensureProducerConnected();

        // 初始化用户消费者并订阅聊天室
        //await initConsumer(requesterId);
        const consumer = await initConsumer(requesterId);

        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify({ action: 'getUserInfo', data: { userId, requesterId } }) }],
        });

        // Listen for the response from Kafka
        const timeout = setTimeout(() => {
            res.status(500).json({ message: "No response from user info service" });
        }, 5000);

        const listener = async ({ topic, partition, message }) => {
            const payload = JSON.parse(message.value.toString());
            if (payload.action === 'userInfo' && payload.data._id === userId) {
                clearTimeout(timeout);
                res.status(200).json(payload.data);
                consumer.off('message', listener);
            }
        };

        consumer.on('message', listener);
    } catch (err) {
        console.error('Error sending message:', err);
        res.status(500).json({ message: "Error requesting user info" });
    }
});*/

//Endpoint to send a message to the chatroom
// Without Kafka
/*
app.post("/messages", upload.single("imageFile"), async (req, res) => {
    try {
        const { senderId, chatRoomId, messageType, messageText } = req.body;
        const imageUrl = messageType === "image" ? req.file.path : null;

        // Find the chat room
        const chatRoom = await ChatRoom.findById(chatRoomId).populate("members");

        if (!chatRoom) {
            return res.status(404).json({ error: "Chat room not found" });
        }

        // Create a new message
        const newMessage = new MessageWithChatRoom({
            senderId,
            chatRoomId,
            messageType,
            message: messageText,
            imageUrl,
            timestamp: new Date(),
        });

        await newMessage.save();

        // Notify all members in the chat room except the sender
        const recipientIds = chatRoom.members
            .filter(member => member._id.toString() !== senderId)
            .map(member => member._id);

        await notifyRecipients(recipientIds, newMessage);

        res.status(200).json({ message: "Message sent successfully" });
    } catch (error) {
        console.error('Error sending message:', error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});*/

app.post("/messages", upload.single("imageFile"), async (req, res) => {
    try {
        const { senderId, chatRoomId, messageType, messageText } = req.body;
        const imageUrl = messageType === "image" ? req.file.path : null;
        const topic = safeReplace(chatRoomId);
        await ensureProducerConnected();
        await producer.send({
            topic: topic,
            messages: [
                { value: JSON.stringify({ action: 'sendMessage', data: { senderId, chatRoomId, messageType, messageText, imageUrl } }) }
            ],
        });

        res.status(200).json({ message: "Message sent successfully" });
    } catch (error) {
        console.error('Error sending message:', error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

//function to notify recipients (you can implement your own notification logic here)
async function notifyRecipients(recipientIds, message) {
    for (const recipientId of recipientIds) {
        // Add your notification logic here, e.g., send a Kafka message, push notification, etc.
        console.log(`Notifying recipient: ${recipientId} about new message: ${message._id}`);
    }
}

//endpoint to get the userDetails to design the chat Room header
app.get("/user/:userId", async (req, res) => {
    const { userId } = req.params;
    const requesterId = req.query.requesterId;
    const topic = safeReplace(userId);

    try {
        await ensureProducerConnected();
        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify({ action: 'getUserInfo', data: { userId, requesterId } }) }],
        });

        // Listen for the response from Kafka
        const consumer = kafka.consumer({ groupId: `6664b2a0f2eafd2592792028-group` });
        await consumer.connect();
        await consumer.subscribe({ topic: '666c341fb9620b9c14720d31', fromBeginning: true });
        consumer.once('message', async (message) => {
            const payload = JSON.parse(message.value.toString());
            if (payload.action === 'userInfo' && payload.data._id === userId) {
                res.status(200).json(payload.data);
            }
        });

        // Set a timeout in case no response is received
        setTimeout(() => {
            res.status(500).json({ message: "No response from user info service" });
        }, 5000);

    } catch (err) {
        console.error('Error sending message:', err);
        res.status(500).json({ message: "Error requesting user info" });
    }
});

//endpoint to get all messages in the chatroom
/*
app.get("/messages/:chatRoomId", async (req, res) => {
    try {
        const { chatRoomId } = req.params;

        const messages = await MessageWithChatRoom.find({ chatRoomId }).populate("senderId", "_id name");

        res.json(messages);
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});*/
app.get("/messages/:userId/:chatRoomId", async (req, res) => {
    try {
        const { userId, chatRoomId } = req.params;
        // fetch the messages by kafka
        // suscribe the topic, and fetch the data
        /*
        const consumer = kafka.consumer({ groupId: `${userId}-group` });
        await consumer.connect();
        await consumer.subscribe({ topic: `${chatRoomId}`, fromBeginning: true });
        consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log({
                    partition,
                    offset: message.offset,
                    value: message.value.toString(),
                });
                messages.push(message.value.toString());
            },
        });
        res.send('Subscribed to topic');
        */
        const messages = await MessageWithChatRoom.find({ chatRoomId }).populate("senderId", "_id name");

        res.json(messages);
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

// Ensure consumer is connected and subscribed
const ensureConsumerSubscribed = async (userId, chatRoomId) => {
    if (consumers[chatRoomId]) {
        return consumers[chatRoomId];
    }

    const consumer = kafka.consumer({ groupId: `${userId}-group` });
    await consumer.connect();
    await consumer.subscribe({ topic: `${chatRoomId}`, fromBeginning: true });

    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                partition,
                offset: message.offset,
                value: message.value.toString(),
            });
            message.push(message.value.toString());
        },
    });

    consumers[chatRoomId] = consumer;
    return consumer;
};

// 订阅特定主题
app.get("/subscribe/:userId/:chatRoomId", async (req, res) => {
    const { userId, chatRoomId } = req.params;
    try {
        await ensureConsumerSubscribed(userId, chatRoomId);
        res.send('Subscribed to topic');
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

// 拉取消息的端点
/*
app.get("/messages/:chatRoomId", async (req, res) => {
    const { chatRoomId } = req.params;
    try {
        const messages = await MessageWithChatRoom.find({ chatRoomId }).populate("senderId", "_id name");
        res.json(messages);
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});*/

// 拉取Kafka消息的端点
let messages = [];
app.get("/fetch-messages/:chatRoomId", async (req, res) => {
    const { chatRoomId } = req.params;
    try {
        const chatMessages = messages.filter(msg => JSON.parse(msg).data.chatRoomId === chatRoomId);
        res.json(chatMessages);
        messages = messages.filter(msg => JSON.parse(msg).data.chatRoomId !== chatRoomId); // 清空已返回的消息，避免重复发送
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

app.post("/deleteMessages", async (req, res) => {
    const { messages } = req.body;
    const topic = 'deleteMessagesTopic';

    if (!Array.isArray(messages) || messages.length === 0) {
        return res.status(400).json({ message: "Invalid request body!" });
    }

    try {
        await ensureProducerConnected();
        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify({ action: 'deleteMessages', data: { messages } }) }],
        });

        // Listen for the response from Kafka
        consumer.once('message', async (message) => {
            const payload = JSON.parse(message.value.toString());
            if (payload.action === 'deleteMessagesResponse') {
                if (payload.success) {
                    res.status(200).json({ message: "Messages deleted successfully" });
                } else {
                    res.status(500).json({ message: payload.message });
                }
            }
        });

        // Set a timeout in case no response is received
        setTimeout(() => {
            res.status(500).json({ message: "No response from delete messages service" });
        }, 5000);

    } catch (err) {
        console.error('Error sending delete messages request:', err);
        res.status(500).json({ message: "Error requesting delete messages" });
    }
});

app.get("/friend-requests/sent/:userId", async (req, res) => {
    try {
        const { userId } = req.params;
        const user = await User.findById(userId).populate("sentFriendRequests", "name email image").lean();

        const sentFriendRequests = user.sentFriendRequests;

        res.json(sentFriendRequests);
    } catch (error) {
        console.log("error", error);
        res.status(500).json({ error: "Internal Server" });
    }
})

app.get("/friends/:userId", async (req, res) => {
    const { userId } = req.params;
    const topic = 'friendsTopic';

    try {
        await ensureProducerConnected();
        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify({ action: 'getFriends', data: { userId } }) }],
        });

        // Listen for the response from Kafka
        consumer.once('message', async (message) => {
            const payload = JSON.parse(message.value.toString());
            if (payload.action === 'friendsResponse' && payload.data.userId === userId) {
                if (payload.success) {
                    res.status(200).json(payload.data.friendIds);
                } else {
                    res.status(500).json({ message: payload.message });
                }
            }
        });

        // Set a timeout in case no response is received
        setTimeout(() => {
            res.status(500).json({ message: "No response from friends service" });
        }, 5000);

    } catch (err) {
        console.error('Error sending friends request:', err);
        res.status(500).json({ message: "Error requesting friends" });
    }
});

app.get("/friend-requests/sent/:userId", async (req, res) => {
    const { userId } = req.params;
    const topic = 'friendRequestsTopic';

    try {
        await ensureProducerConnected();
        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify({ action: 'getSentFriendRequests', data: { userId } }) }],
        });

        // Listen for the response from Kafka
        consumer.once('message', async (message) => {
            const payload = JSON.parse(message.value.toString());
            if (payload.action === 'sentFriendRequestsResponse' && payload.data.userId === userId) {
                if (payload.success) {
                    res.status(200).json(payload.data.sentFriendRequests);
                } else {
                    res.status(500).json({ message: payload.message });
                }
            }
        });

        // Set a timeout in case no response is received
        setTimeout(() => {
            res.status(500).json({ message: "No response from friend requests service" });
        }, 5000);

    } catch (err) {
        console.error('Error sending friend requests request:', err);
        res.status(500).json({ message: "Error requesting friend requests" });
    }
});

/*
app.get("/UserInfo/:userId", async (req, res) => {
    const { userId } = req.params;
    const topic = 'userInfoTopic';
    const requesterId = 'some_unique_id_for_request';

    try {
        await ensureProducerConnected();
        await producer.send({
            topic: topic,
            messages: [{ value: JSON.stringify({ action: 'getUserInfo', data: { userId, requesterId } }) }],
        });

        // Listen for the response from Kafka
        consumer.once('message', async (message) => {
            const payload = JSON.parse(message.value.toString());
            if (payload.action === 'userInfo' && payload.data.userId === userId && payload.data.requesterId === requesterId) {
                if (payload.success) {
                    res.status(200).json(payload.data.user);
                } else {
                    res.status(404).json({ message: payload.message });
                }
            }
        });

        // Set a timeout in case no response is received
        setTimeout(() => {
            res.status(500).json({ message: "No response from user info service" });
        }, 5000);

    } catch (err) {
        console.error('Error sending user info request:', err);
        res.status(500).json({ message: "Error requesting user info" });
    }
});*/
app.get("/UserInfo/:userId", async (req, res) => {
    try {
        const { userId } = req.params;
        const user = await User.findById(userId);

        if (!user) {
            return res.status(404).json({ message: 'User not found' });
        }

        res.status(200).json(user);
    } catch (error) {
        console.log("error", error);
        res.status(500).json({ message: "internal server error" })
    }
})

// 启动服务器时为每个聊天室运行消费者
/*
const initChatRoomConsumers = async () => {
    const chatRooms = await ChatRoom.find();
    for (const chatRoom of chatRooms) {
        runConsumer(chatRoom._id.toString());
    }
};*/

app.listen(port, async () => {
    console.log(`Server running on port ${port}`);
    //await initChatRoomConsumers();
});