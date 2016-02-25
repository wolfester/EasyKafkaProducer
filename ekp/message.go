package ekp

import (
	"hash/crc32"
	"bytes"
	"encoding/binary"
)

type Message struct {
	magicByte int8
	attributes int8
	key []byte
	value [] byte
	
	bytes []byte
}
func NewMessage(key, value []byte) (returnMessage *Message) {
	returnMessage = &Message{
		magicByte : int8(0),
		attributes : int8(0),
		key : key,
		value : value,
	}
	
	returnMessage.init()
	
	return
}
func (this *Message) getBytes() []byte {
	return this.bytes
}
func (this *Message) getSize() int32 {
	return int32(len(this.bytes))
}
func (this *Message) init() {
	this.setBytes()
}
//TODO optimize with copy function
func (this *Message) setBytes() {
	buffer := new(bytes.Buffer)
	
	//crc placeholder
	binary.Write(buffer, binary.BigEndian, int32(0))
	//magicByte
	binary.Write(buffer, binary.BigEndian, this.magicByte)
	//attributes (0 means no compression)
	binary.Write(buffer, binary.BigEndian, this.attributes)
	//key length
	binary.Write(buffer, binary.BigEndian, int32(len(this.key)))
	//key
	binary.Write(buffer, binary.BigEndian, this.key)
	//value length
	binary.Write(buffer, binary.BigEndian, int32(len(this.value)))
	//value
	binary.Write(buffer, binary.BigEndian, this.value)
	
	this.bytes = buffer.Bytes()
	
	//get crc
	crc := crc32.ChecksumIEEE(this.bytes[4:])
	//inject crc
	binary.BigEndian.PutUint32(this.bytes[0:], crc)
}
