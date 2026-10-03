package broker

import "github.com/sathwikbairaboina2/kafka-go/internal/protocol"

func (b *Broker) topicMetadata(name string, allowAuto bool) protocol.MetadataTopic {
	t, code := b.ensureTopic(name, allowAuto)
	if code != protocol.ErrNone {
		return protocol.MetadataTopic{ErrorCode: code, Name: name}
	}
	parts := make([]protocol.MetadataPartition, t.Partitions)
	for i := range parts {
		parts[i] = protocol.MetadataPartition{
			Index:    int32(i),
			Leader:   b.cfg.NodeID,
			Replicas: []int32{b.cfg.NodeID},
			ISR:      []int32{b.cfg.NodeID},
		}
	}
	return protocol.MetadataTopic{Name: t.Name, Partitions: parts}
}

func (b *Broker) metadata(q *protocol.MetadataRequest) *protocol.MetadataResponse {
	cluster := b.cfg.ClusterID
	resp := &protocol.MetadataResponse{
		Brokers:      []protocol.MetadataBroker{{NodeID: b.cfg.NodeID, Host: b.cfg.Host, Port: b.cfg.Port}},
		ClusterID:    &cluster,
		ControllerID: b.cfg.NodeID,
	}
	allowAuto := q.AllowAutoCreate && b.cfg.AutoCreate
	if q.AllTopics {
		for _, t := range b.meta.List() {
			resp.Topics = append(resp.Topics, b.topicMetadata(t.Name, false))
		}
		return resp
	}
	for _, name := range q.Topics {
		resp.Topics = append(resp.Topics, b.topicMetadata(name, allowAuto))
	}
	return resp
}
