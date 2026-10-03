package broker

import (
	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
	"github.com/sathwikbairaboina2/kafka-go/internal/record"
)

// errInvalidRequiredAcks is Kafka's INVALID_REQUIRED_ACKS.
const errInvalidRequiredAcks int16 = 21

func (b *Broker) produce(q *protocol.ProduceRequest) *protocol.ProduceResponse {
	resp := &protocol.ProduceResponse{}
	for _, t := range q.Topics {
		tr := protocol.ProduceTopicResponse{Name: t.Name}
		_, topicCode := b.ensureTopic(t.Name, b.cfg.AutoCreate)
		for _, p := range t.Partitions {
			pr := protocol.ProducePartitionResponse{Index: p.Index, BaseOffset: -1, LogAppendTimeMs: -1, LogStartOffset: -1}
			switch {
			case q.Acks != -1 && q.Acks != 0 && q.Acks != 1:
				pr.ErrorCode = errInvalidRequiredAcks
			case topicCode != protocol.ErrNone:
				pr.ErrorCode = topicCode
			default:
				b.produceOne(t.Name, p, &pr)
			}
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp
}

// produceOne validates every batch before appending any, so a bad batch changes nothing.
func (b *Broker) produceOne(topic string, p protocol.ProducePartition, pr *protocol.ProducePartitionResponse) {
	part, ok := b.logs.Get(topic, p.Index)
	if !ok {
		pr.ErrorCode = protocol.ErrUnknownTopicOrPartition
		return
	}
	if len(p.Records) == 0 {
		pr.ErrorCode = protocol.ErrCorruptMessage
		return
	}
	batches, err := record.Split(p.Records)
	if err != nil {
		pr.ErrorCode = protocol.ErrCorruptMessage
		return
	}
	hs := make([]record.Header, len(batches))
	for i, batch := range batches {
		h, err := record.Parse(batch)
		if err != nil {
			pr.ErrorCode = protocol.ErrCorruptMessage
			return
		}
		if h.Transactional() || h.Control() {
			pr.ErrorCode = protocol.ErrInvalidRecord
			return
		}
		hs[i] = h
	}
	base, err := part.AppendMany(batches, hs)
	if err != nil {
		pr.ErrorCode = errUnknownServer
		return
	}
	pr.BaseOffset = base
	pr.LogStartOffset = part.LogStartOffset()
}
