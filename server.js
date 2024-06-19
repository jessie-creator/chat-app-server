require('dotenv').config();
const { Kafka } = require('kafkajs');
const express = require("express");
const bodyParser = require("body-parser");
const mongoose = require("mongoose");
const passport = require("passport");
const LocalStrategy = require("passport-local").Strategy;

const app = express();
const port = process.env.PORT;
const cors = require("cors");
app.use(cors());

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(passport.initialize());
const jwt = require("jsonwebtoken");

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: [process.env.KAFKA_BROKER, process.env.KAFKA_BROKER1, process.env.KAFKA_BROKER2]
});

const producer = kafka.producer();
let producerConnected = false;
const consumers = {};

const ensureProducerConnected = async () => {
    if (!producerConnected) {
        await producer.connect();
        producerConnected = true;
    }
};

mongoose
    .connect(process.env.MONGODB_URI, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
    })
    .then(() => {
        console.log("Connected to Mongo Db");
    })
    .catch((err) => {
        console.log("Error connecting to MongoDb", err);
    });

const User = require("./models/user");
const Message = require("./models/message");
const ChatRoom = require("./models/ChatRoom");
const multer = require("multer");

// Configure multer for handling file uploads
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, "files/"); // Specify the desired destination folder
  },
  filename: function (req, file, cb) {
    // Generate a unique filename for the uploaded file
    const uniqueSuffix = Date.now() + "-" + Math.round(Math.random() * 1e9);
    cb(null, uniqueSuffix + "-" + file.originalname);
  },
});

const upload = multer({ storage: storage });

//endpoint to register the user on db
app.post("/register", (req, res) => {
    const { uid, email, name, image } = req.body;

    User.findOne({ uid })
        .then((existingUser) => {
            if (existingUser) {
                res.status(200).json({ message: "User already exists" });
            } else {
                const newUser = new User({ uid, email, name, image });
                newUser
                    .save()
                    .then(() => {
                        res.status(200).json({ message: "User registered successfully" });
                    })
                    .catch((err) => {
                        console.log("Error registering user", err);
                        res.status(500).json({ message: "Error registering the user!" });
                    });
            }
        })
        .catch((err) => {
            console.log("Error checking user existence", err);
            res.status(500).json({ message: "Error checking user existence!" });
        });
});

//endpoint to access all the users except the user who's is currently logged in!
app.get("/users/:userId", (req, res) => {
    const loggedInUserId = req.params.userId;

    User.find({ uid: { $ne: loggedInUserId } })
        .then((users) => {
            res.status(200).json(users);
        })
        .catch((err) => {
            console.log("Error retrieving users", err);
            res.status(500).json({ message: "Error retrieving users" });
        });
});

app.get("/friends/:uid", (req, res) => {
    try {
        const { uid } = req.params;
        //const user = await User.findOne({ uid: uid })
        console.log(uid);
        User.findOne({ uid: uid }).populate("friends").then((user) => {
            if (!user) {
                return res.status(404).json({ message: "User not found" })
            }

            const friendIds = user.friends.map((friend) => friend.uid);

            res.status(200).json(friendIds);
        })
    } catch (error) {
        console.log("error", error);
        res.status(500).json({ message: "internal server error" })
    }
})

//endpoint to send a request to a user
app.post("/friend-request", async (req, res) => {
    const { currentUserId, selectedUserId } = req.body;

    try {
        console.log(`currentUserId: ${currentUserId}, selectedUserId: ${selectedUserId}`);

        const recipientUser = await User.findOne({ uid: selectedUserId });
        if (!recipientUser) {
            throw new Error(`Recipient user with uid ${selectedUserId} not found`);
        }
        const senderUser = await User.findOne({ uid: currentUserId });
        if (!senderUser) {
            throw new Error(`Sender user with uid ${currentUserId} not found`);
        }

        await User.findByIdAndUpdate(
            recipientUser._id,
            { $push: { friendRequests: senderUser._id } },
            { new: true }
        );
        await User.findByIdAndUpdate(
            senderUser._id,
            { $push: { sentFriendRequests: recipientUser._id } },
            { new: true }
        );

        res.sendStatus(200);
    } catch (error) {
        console.error("Error updating friend requests:", error);
        res.status(500).json({ message: error.message });
    }
});

//endpoint to show all the friend-requests of a particular user
app.get("/friend-request/:uid", async (req, res) => {
    try {
        const { uid } = req.params;
        const user = await User.findOne({ uid: uid })
            .populate("friendRequests", "uid name email image")
            .lean();

        if (!user) {
            return res.status(404).json({ message: "User not found" });
        }

        const friendRequests = user.friendRequests;

        res.json(friendRequests);
    } catch (error) {
        console.error("Error fetching friend requests:", error);
        res.status(500).json({ message: "Internal Server Error" });
    }
});

//endpoint to accept a friend-request of a particular person
app.post("/friend-request/accept", async (req, res) => {
    try {
        const { senderUid, recipientUid } = req.body;
        console.log(senderUid, recipientUid)
        const sender = await User.findOne({ uid: senderUid });
        const recipient = await User.findOne({ uid: recipientUid });

        if (!sender || !recipient) {
            return res.status(404).json({ message: "User not found" });
        }

        sender.friends.push(recipient._id);
        recipient.friends.push(sender._id);

        recipient.friendRequests = recipient.friendRequests.filter(
            (request) => request.toString() !== sender._id.toString()
        );

        sender.sentFriendRequests = sender.sentFriendRequests.filter(
            (request) => request.toString() !== recipient._id.toString()
        );

        await sender.save();
        await recipient.save();

        res.status(200).json({ message: "Friend Request accepted successfully" });
    } catch (error) {
        console.log(error);
        res.status(500).json({ message: "Internal Server Error" });
    }
});

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

app.get("/chatrooms/:userId", async (req, res) => {
    try {
        const { userId } = req.params;

        // 根据 uid 查找用户文档
        const user = await User.findOne({ uid: userId });

        if (!user) {
            return res.status(404).json({ message: "User not found" });
        }

        const chatRooms = await ChatRoom.find({ members: user._id }).populate("members", "uid name email image");
        res.json(chatRooms);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});
app.post("/messages", upload.single("imageFile"), async (req, res) => {
    try {
        const { senderUid, chatRoomId, messageType, messageText } = req.body;
        const user = await User.findOne({ uid: senderUid });
        if (!user) {
            return res.status(404).json({ message: "User not found" });
        }
        console.log("\nUser_Id = ", user._id);
        const senderId = user._id;
        const chatroom = await ChatRoom.findById(chatRoomId).populate("members", "_id");
        if (!chatroom) {
            return res.status(404).json({ message: "ChatRoom not found" });
        }

        const recepientIds = chatroom.members
            .map(member => member._id)
            .filter(id => !id.equals(senderId));
        console.log("recepientIds = ", recepientIds);
        const newMessage = new Message({
            chatRoomId,
            senderId,
            recepientIds,
            messageType,
            message: messageText,
            timestamp: new Date(),
            imageUrl: messageType === "image" ? req.file.path : null,
        });

        await newMessage.save();
        await ensureProducerConnected();
        await producer.send({
            topic: chatRoomId,
            messages: [
                { value: JSON.stringify({ action: 'sendMessage', data: { chatRoomId, senderId, recepientIds, messageType, messageText } }) }
            ],
        });
        res.status(200).json({ message: "Message sent Successfully" });
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

//endpoint to fetch the messages between sender and recepient in the chatRoom
app.get("/messages/:userUid/:chatRoomId", async (req, res) => {
    try {
        const { userUid, chatRoomId } = req.params;
        const user = await User.findOne({ uid: userUid });
        if (!user) {
            return res.status(404).json({ message: "User not found" });
        }
        console.log("\nUser_Id = ", user._id);
        const chatroom = await ChatRoom.findById(chatRoomId).populate("members", "_id");
        if (!chatroom) {
            return res.status(404).json({ message: "ChatRoom not found" });
        }

        const memberIds = chatroom.members.map(member => member._id);
        console.log("recepientIds = ", memberIds);

        const messages = await Message.find({
            $or: [
                { senderId: user._id, recepientIds: { $in: memberIds } },
                { senderId: { $in: memberIds }, recepientIds: user._id },
            ],
        }).populate([
            { path: 'senderId', select: '_id name' },
            { path: 'recepientIds', select: '_id name' }
        ]);
        console.log("response data = ", messages);
        res.json(messages);
    } catch (error) {
        console.log("Error fetching messages:", error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

///endpoint to get the userDetails to design the chat Room header
app.get("/user/:chatroomId", async (req, res) => {
    try {
        const { chatroomId } = req.params;
        console.log(chatroomId);
        //fetch the chatroom data from the chatroom ID
        const chatroom = await ChatRoom.findById(chatroomId).populate("members", "name image");
        console.log(chatroom);
        if (!chatroom) {
            return res.status(404).json({ message: "ChatRoom not found" });
        }

        const membersCount = chatroom.members.length;
        res.json({ chatroom, membersCount });
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

app.listen(port, async () => {
    console.log(`Server running on port ${port}`);
    //await initChatRoomConsumers();
});