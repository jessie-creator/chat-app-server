const mongoose = require("mongoose");

const messageSchema = new mongoose.Schema({
  chatRoomId: { 
    type: mongoose.Schema.Types.ObjectId, 
    ref: 'ChatRoom', 
    required: true,
  },
  senderId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "User",
    required: true,
  },
  recepientIds: [
    {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      required: true,
    },
  ],
  messageType: {
    type: String,
    enum: ["text", "image"],
    required: true,
  },
  message: String,
  imageUrl: String,
  timeStamp: {
    type: Date,
    default: Date.now,
  },
});

const Message = mongoose.model('Message', messageSchema);

module.exports = Message;