package broker

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
	"github.com/sathwikbairaboina2/kafka-go/internal/storage"
)

const defaultFetchMaxBytes = 50 << 20

// fetchPass reads every requested partition once within the byte budget. It reports the bytes
// returned and whether any partition had an error (errors end the long poll early, like Kafka).
func (b *Broker) fetchPass(q *protocol.FetchRequest, maxBytes int) (*protocol.FetchResponse, int, bool) {
	resp := &protocol.FetchResponse{}
	total := 0
	hasErr := false
	for _, t := range q.Topics {
		tr := protocol.FetchTopicResponse{Name: t.Name}
		for _, fp := range t.Partitions {
			pr := protocol.FetchPartitionResponse{Index: fp.Index, HighWatermark: -1, LastStableOffset: -1, LogStart: -1}
			part, ok := b.logs.Get(t.Name, fp.Index)
			if !ok {
				pr.ErrorCode = protocol.ErrUnknownTopicOrPartition
				hasErr = true
				tr.Partitions = append(tr.Partitions, pr)
				continue
			}
			remaining := maxBytes - total
			if total == 0 || remaining > 0 {
				budget := int(fp.PartitionMaxBytes)
				if total > 0 && remaining < budget {
					budget = remaining
				}
				if total == 0 && budget < 0 {
					budget = 0
				}
				data, err := part.Read(fp.FetchOffset, budget)
				switch {
				case errors.Is(err, storage.ErrOffsetOutOfRange):
					pr.ErrorCode = protocol.ErrOffsetOutOfRange
					hasErr = true
				case err != nil:
					pr.ErrorCode = errUnknownServer
					hasErr = true
				default:
					if total > 0 && len(data) > remaining {
						data = nil // only the first partition with data may exceed the budget
					}
					pr.Records = data
					total += len(data)
				}
			}
			pr.HighWatermark = part.HighWatermark()
			pr.LastStableOffset = pr.HighWatermark
			pr.LogStart = part.LogStartOffset()
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp, total, hasErr
}

func (b *Broker) fetch(ctx context.Context, q *protocol.FetchRequest) *protocol.FetchResponse {
	maxBytes := int(q.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = defaultFetchMaxBytes
	}
	var timer *time.Timer
	var timeout <-chan time.Time
	if q.MaxWaitMs > 0 {
		timer = time.NewTimer(time.Duration(q.MaxWaitMs) * time.Millisecond)
		defer timer.Stop()
		timeout = timer.C
	}
	for {
		// Take the wait channels before reading so an append between the read and the wait is not lost.
		var cases []reflect.SelectCase
		for _, t := range q.Topics {
			for _, fp := range t.Partitions {
				if part, ok := b.logs.Get(t.Name, fp.Index); ok {
					cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(part.Wait())})
				}
			}
		}
		resp, total, hasErr := b.fetchPass(q, maxBytes)
		if total >= int(q.MinBytes) || hasErr || timeout == nil {
			return resp
		}
		cases = append(cases,
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timeout)},
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
		switch chosen, _, _ := reflect.Select(cases); chosen {
		case len(cases) - 2: // timed out: return whatever is there now
			final, _, _ := b.fetchPass(q, maxBytes)
			return final
		case len(cases) - 1: // connection or server is going away
			return resp
		}
	}
}

func (b *Broker) listOffsets(q *protocol.ListOffsetsRequest) *protocol.ListOffsetsResponse {
	resp := &protocol.ListOffsetsResponse{}
	for _, t := range q.Topics {
		tr := protocol.ListOffsetsTopicResponse{Name: t.Name}
		for _, lp := range t.Partitions {
			pr := protocol.ListOffsetsPartitionResponse{Index: lp.Index, Timestamp: -1, Offset: -1}
			part, ok := b.logs.Get(t.Name, lp.Index)
			if !ok {
				pr.ErrorCode = protocol.ErrUnknownTopicOrPartition
			} else if off, err := part.OffsetForTimestamp(lp.Timestamp); err != nil {
				pr.ErrorCode = errUnknownServer
			} else {
				pr.Offset = off
				if lp.Timestamp >= 0 {
					pr.Timestamp = lp.Timestamp
				}
			}
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp
}
