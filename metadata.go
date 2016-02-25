package ekp

import (
	"fmt"
)

type metadata struct {
	brokers map[int32]*broker
	topics map[string]*topic
}
func newMetadata() *metadata {
	return &metadata{
		brokers : make(map[int32]*broker),
		topics : make(map[string]*topic),
	}
}
func newMetadataWithBrokers(brokers map[int32]*broker) *metadata {
	return &metadata{
		brokers : brokers,
	}
}
func newMetadataWithBrokersAndTopics(brokers map[int32]*broker, topics map[string]*topic) *metadata {
	return &metadata{
		brokers : brokers,
		topics : topics,
	}
}
func (this *metadata) addTopic(topic *topic) {
	this.topics[topic.getName()] = topic
}
func (this *metadata) addBroker(broker *broker) {
	this.brokers[broker.getNodeId()] = broker
}

type broker struct {
	id int32
	host string
	port int32
}
func newBroker(id int32, host string, port int32) *broker {
	return &broker{
		id : id,
		host : host,
		port : port,
	}
}
func (this *broker) getConnectionString() string {
	return fmt.Sprintf("%s:%d", this.host, this.port)
}
func (this *broker) getNodeId() int32 {
	return this.id
}

type topic struct {
	name string
	partitions map[int32]*partition
	partitionMap []int32
}
func newTopic (name string) *topic {
	return &topic{
		name : name,
		partitions : make(map[int32]*partition),
	}
}
func newTopicWithPartitions(name string, partitions map[int32]*partition) *topic {
	return &topic{
		name : name,
		partitions : partitions,
	}
}
func (this *topic) addPartition(partition *partition) {
	this.partitions[partition.getPartitionId()] = partition
}
func (this *topic) getName() string {
	return this.name
}

type partition struct {
	id int32
	leader int32
	replicas []int32
	isr []int32
}
func newPartition(id, leader int32, replicas, isr []int32) *partition {
	return &partition{
		id : id,
		leader : leader,
		replicas : replicas,
		isr : isr,
	}
}
func (this *partition) getPartitionId() int32 {
	return this.id
}
func (this *partition) getLeaderId() int32 {
	return this.leader
}
