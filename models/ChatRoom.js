const mongoose = require("mongoose");

const ChatRoomSchema = new mongoose.Schema({
    name: {
        type: String,
        required: true,
    },
    image: {
        type: String,
        required: true,
    },
    members: [
        {
            type: mongoose.Schema.Types.ObjectId,
            ref: "User",
        },
    ],
});

const ChatRoom = mongoose.model('ChatRoom', ChatRoomSchema);

module.exports = ChatRoom;