package ekp

import (
	"bytes"
	"encoding/binary"
)

type MessageSet struct {
	messages map[int]*Message
	position int
	size int32
}
func NewMessageSet() (returnMessageSet *MessageSet) {
	returnMessageSet = &MessageSet{
		messages : make(map[int]*Message),
		position : 0,
		size : int32(0),
	}
	
	return
}
func (this *MessageSet) AddMessage(msg *Message) int{
	this.messages[this.position] = msg
	this.position++
	this.size += msg.getSize() + 8 + 4
	return this.position
}
//TODO optimize with copy function
func (this *MessageSet) getBytes() []byte {
	buffer := new(bytes.Buffer)
	
	for index := 0; index < this.position; index++ {
		//offset (default 0 since we're the producer and it doesn't matter)
		binary.Write(buffer, binary.BigEndian, int64(0))
		//messageSize
		binary.Write(buffer, binary.BigEndian, int32(this.messages[index].getSize()))
		//message
		binary.Write(buffer, binary.BigEndian, this.messages[index].getBytes())
	}
	
	return buffer.Bytes()
}
func (this *MessageSet) Reset() {
	this.position = 0
	this.size = 0
}
func (this *MessageSet) Position() int {
	return this.position
}
func (this *MessageSet) getSize() int32 {
	return this.size
}
