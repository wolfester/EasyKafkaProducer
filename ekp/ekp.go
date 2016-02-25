package ekp

import (
	"bytes"
	"encoding/binary"
	"net"
	"errors"
	"time"
	"fmt"
)

type EKP struct {
	hosts []string
	correlationId int32
	clientId string
	
	metadata *metadata
	
	staticBytes []byte
	correlationIdIndex int
	apiKeyIndex int
}

func NewEKP(hosts []string, clientId string) (returnSKP *SKP) {
	returnSKP =  &SKP{
		hosts :hosts,
		correlationId : 0,
		clientId : clientId,
	}
	
	returnSKP.init()
	
	return
}

//TODO optimize with copy function
func (this *EKP) ProduceMessageSet(topic string, messageSet *MessageSet) error {
	_, exists := this.metadata.topics[topic]
	if !exists {
		this.fetchMetadata()
	}
	_, exists = this.metadata.topics[topic]
	if !exists {
		return errors.New("Invalid topic.")
	}
	
	buffer := new(bytes.Buffer)
	
	//staticBytes
	binary.Write(buffer, binary.BigEndian, this.staticBytes)
	//requiredAcks
	binary.Write(buffer, binary.BigEndian, int16(1))
	//timeout
	binary.Write(buffer, binary.BigEndian, int32(3000))
	//number of topics we will write to (always 1)
	binary.Write(buffer, binary.BigEndian, int32(1))
	//topic name length
	binary.Write(buffer, binary.BigEndian, int16(len(topic)))
	//topic
	binary.Write(buffer, binary.BigEndian, []byte(topic))
	//number of partitions we will write to (always 1)
	binary.Write(buffer, binary.BigEndian, int32(1))
	
	//save partitionId index
	partitionIdIndex := buffer.Len()
	//partition we are writing to
	binary.Write(buffer, binary.BigEndian, int32(0))
	
	
	//messageSetSize
	binary.Write(buffer, binary.BigEndian, messageSet.getSize())
	//messageSet
	binary.Write(buffer, binary.BigEndian, messageSet.getBytes())
	
	requestBytes := buffer.Bytes()
	
	//injects
	//size
	binary.BigEndian.PutUint32(requestBytes[0:], uint32(buffer.Len() - 4))
	//apikey
	binary.BigEndian.PutUint16(requestBytes[this.apiKeyIndex:], uint16(0))
	//correlationId
	binary.BigEndian.PutUint32(requestBytes[this.correlationIdIndex:], uint32(this.correlationId))
	
	//try to send to round robin partition, fallback to others
	partitionId := this.metadata.topics[topic].partitionMap[int(this.correlationId) % len(this.metadata.topics[topic].partitionMap)]
	err := this.sendToPartition(requestBytes, topic, partitionId, partitionIdIndex)
	if err != nil {
		for _, partitionId = range this.metadata.topics[topic].partitionMap {
			err = this.sendToPartition(requestBytes, topic, partitionId, partitionIdIndex)
			if err != nil {
				continue
			}
			
			break
		}
	}
	
	this.correlationId++
	
	if err != nil {
		return errors.New("Unable to send data to kafka.")
	}
	
	return nil
}
func (this *EKP) sendToPartition(requestBytes []byte, topic string, partitionId int32, partitionIdIndex int) error {
	//inject partitionId
	binary.BigEndian.PutUint32(requestBytes[partitionIdIndex:], uint32(partitionId))
	
	//get leader to try first
	leaderId := this.metadata.topics[topic].partitions[partitionId].leader
	
	err := this.sendToKafka(requestBytes, leaderId)
	if err != nil {
		for _, id := range this.metadata.topics[topic].partitions[partitionId].replicas {
			//try replicas for the same partition
			if id == leaderId {
				continue
			}
			
			err := this.sendToKafka(requestBytes, id)
			if err != nil {
				continue
			}
			break
		}
	}
	
	if err != nil {
		return errors.New("Unable to send message to partition.")
	}
	
	return nil
}
func (this *EKP) sendToKafka(requestBytes []byte, brokerId int32) error {
	connectionString := this.metadata.brokers[brokerId].getConnectionString()
	
	addr, err := net.ResolveTCPAddr("tcp", connectionString)
	if err != nil {
		return err
	}
	
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Minute)
	
	_, err = conn.Write(requestBytes)
	if err != nil {
		return err
	}
	
	responseHeader := make([]byte, 4)
	_, err = conn.Read(responseHeader)
	if err != nil {
		return err
	}
	
	size := binary.BigEndian.Uint32(responseHeader[0:])
	
	responseBody := make([]byte, size)
	_, err = conn.Read(responseBody)
	if err != nil {
		return err
	}
	
	err = this.parseProduceResponse(responseBody)
	return err
}
func (this *EKP) parseProduceResponse(responseBody []byte) error {
	position := 0
	responseCorrelationId := int32(binary.BigEndian.Uint32(responseBody[position:]))
	position += 4
	if responseCorrelationId != this.correlationId {
		return errors.New("Error sending message to kafka.  CorrelationId mismatch.")
	}
	numTopics := int(binary.BigEndian.Uint32(responseBody[position:]))
	position += 4
	for x := 0; x < numTopics; x++ {
		topicNameLength := binary.BigEndian.Uint16(responseBody[position:])
		position += 2
		//topic := responseBody[position:position + int(topicNameLength)]
		position += int(topicNameLength)
		numPartitions := int(binary.BigEndian.Uint32(responseBody[position:]))
		position += 4
		for y := 0; y < numPartitions; y++ {
			//partition := binary.BigEndian.Uint32(responseBody[position:])
			position += 4
			errorCode := binary.BigEndian.Uint16(responseBody[position:])
			err := this.checkErrorCode(errorCode)
			if err != nil {
				return err
			}
			position += 2
			position += 8
		}
	}
	
	//return
	return nil
}

func (this *EKP) checkErrorCode(errorCode uint16) error {
	switch errorCode {
		case 6, 3:
			this.fetchMetadata()
			return nil
		case 0, 9:
			return nil
		default:
			return errors.New("Bad error code.")
	}
}

func (this *EKP) init() {
	this.setStaticBytes()	
	
	err := this.fetchMetadata()
	if err != nil {
		fmt.Println("Error fetching metadata from server.")
	}
}
func (this *EKP) setStaticBytes() {
	buffer := new(bytes.Buffer)
	
	//size placeholder
	binary.Write(buffer, binary.BigEndian, int32(0))
	//apikey index
	this.apiKeyIndex = buffer.Len()
	//apikey placeholder
	binary.Write(buffer, binary.BigEndian, int16(0))
	//apiversion
	binary.Write(buffer, binary.BigEndian, int16(0))
	//correlationId index
	this.correlationIdIndex = buffer.Len()
	//correlationId placeholder
	binary.Write(buffer, binary.BigEndian, int32(0))
	//clientId length
	binary.Write(buffer, binary.BigEndian, int16(len(this.clientId)))
	//clientId
	binary.Write(buffer, binary.BigEndian, []byte(this.clientId))
	
	this.staticBytes = buffer.Bytes()
}
func (this *EKP) fetchMetadata()  error {
	correlationId := this.correlationId
	
	buffer := new(bytes.Buffer)
	
	//staticbytes
	binary.Write(buffer, binary.BigEndian, this.staticBytes)
	//length of topic array (always 0)
	binary.Write(buffer, binary.BigEndian, int32(0))
	
	requestBytes := buffer.Bytes()
	
	//injects
	//size
	binary.BigEndian.PutUint32(requestBytes[0:], uint32(buffer.Len() - 4))
	//apikey
	binary.BigEndian.PutUint16(requestBytes[this.apiKeyIndex:], uint16(3))
	//correlationId
	binary.BigEndian.PutUint32(requestBytes[this.correlationIdIndex:], uint32(correlationId))
	
	for _, host := range this.hosts {
		addr, err := net.ResolveTCPAddr("tcp", host)
		if err != nil {
			continue
		}
		
		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			continue
		}
		
		conn.SetKeepAlive(true)
		conn.SetKeepAlivePeriod(time.Minute)
		
		_, err = conn.Write(requestBytes)
		if err != nil {
			continue
		}
		
		responseHeader := make([]byte, 4)
		_, err = conn.Read(responseHeader)
		if err != nil {
			continue
		}
		
		size := binary.BigEndian.Uint32(responseHeader[0:])
		
		responseBody := make([]byte, size)
		_, err = conn.Read(responseBody)
		if err != nil {
			continue
		}
		
		//compare correlationIds
		responseCorrelationId := int32(binary.BigEndian.Uint32(responseBody[0:]))
		if responseCorrelationId != correlationId {
			continue
		}
		
		this.correlationId++
		this.metadata = this.parseMetadataResponse(responseBody[4:])
		return nil
	}
	
	return errors.New("Unable to fetch metadata.")
}
func (this *EKP) parseMetadataResponse(res []byte) (returnMetadata *metadata) {
	returnMetadata = newMetadata()
	position := 0
	
	//brokers
	numBrokers := binary.BigEndian.Uint32(res[position:])
	position += 4
	
	for x := uint32(0); x < numBrokers; x++ {
		//nodeId
		nodeId := int32(binary.BigEndian.Uint32(res[position:]))
		position += 4
		
		//hostname
		hostLength := int(binary.BigEndian.Uint16(res[position:]))
		position += 2
		host := string(res[position:position+hostLength])
		position += hostLength
		
		//port
		port := int32(binary.BigEndian.Uint32(res[position:]))
		position += 4
		
		//push broker to returnMetadata
		returnMetadata.addBroker(newBroker(nodeId, host, port))
	}
	
	//topics
	numTopics := binary.BigEndian.Uint32(res[position:])
	position += 4
	
	for x := uint32(0); x < numTopics; x++ {
		//error code
		//TODO: actually handle error
		_ = binary.BigEndian.Uint16(res[position:])
		position += 2
		
		//name
		nameLength := int(binary.BigEndian.Uint16(res[position:]))
		position += 2
		name := string(res[position:position+nameLength])
		position += nameLength
		
		topic := newTopic(name)
		
		//partitions
		numPartitions := binary.BigEndian.Uint32(res[position:])
		position += 4
		
		for y := uint32(0); y < numPartitions; y++ {
			//error code
			//TODO: actually handle error
			_ = binary.BigEndian.Uint16(res[position:])
			position += 2
			
			//partitionId
			partitionId := int32(binary.BigEndian.Uint32(res[position:]))
			position += 4
			
			//leaderId
			leaderId := int32(binary.BigEndian.Uint32(res[position:]))
			position += 4
			
			//replicas
			numReplicas := binary.BigEndian.Uint32(res[position:])
			position += 4
			
			replicas := make([]int32, numReplicas)
			for z := uint32(0); z < numReplicas; z++ {
				//replicaId
				replicas[z] = int32(binary.BigEndian.Uint32(res[position:]))
				position += 4
			}
			
			//ISRs
			numIsr := binary.BigEndian.Uint32(res[position:])
			position += 4
			
			isr := make([]int32, numIsr)
			for z := uint32(0); z < numIsr; z++ {
				//isrId
				isr[z] = int32(binary.BigEndian.Uint32(res[position:]))
				position += 4
			}
			
			topic.addPartition(newPartition(partitionId, leaderId, replicas, isr))
		}
		
		//generate partitionMap
		partitionMap := make([]int32, len(topic.partitions))
		i := 0
		for id, _ := range topic.partitions {
			partitionMap[i] = id
			i++
		}
		
		topic.partitionMap = partitionMap
		
		returnMetadata.addTopic(topic)
	}
	
	return
}
