package protocol

// FetchPartition is one partition in a Fetch request.
type FetchPartition struct {
	Index             int32
	FetchOffset       int64
	PartitionMaxBytes int32
}

// FetchTopic is one topic in a Fetch request.
type FetchTopic struct {
	Name       string
	Partitions []FetchPartition
}

// FetchRequest is the Fetch v11 request. Forgotten topics and the rack id are read and discarded.
type FetchRequest struct {
	MaxWaitMs, MinBytes, MaxBytes int32
	IsolationLevel                int8
	SessionID, SessionEpoch       int32
	Topics                        []FetchTopic
}

// Decode reads a Fetch v11 request body.
func (q *FetchRequest) Decode(r *Reader, version int16) error {
	r.Int32() // replica_id
	q.MaxWaitMs = r.Int32()
	q.MinBytes = r.Int32()
	q.MaxBytes = r.Int32()
	q.IsolationLevel = r.Int8()
	q.SessionID = r.Int32()
	q.SessionEpoch = r.Int32()
	nt := r.ArrayLen()
	for i := 0; i < nt && r.Err() == nil; i++ {
		t := FetchTopic{Name: r.String()}
		np := r.ArrayLen()
		for j := 0; j < np && r.Err() == nil; j++ {
			var p FetchPartition
			p.Index = r.Int32()
			r.Int32() // current_leader_epoch
			p.FetchOffset = r.Int64()
			r.Int64() // log_start_offset
			p.PartitionMaxBytes = r.Int32()
			t.Partitions = append(t.Partitions, p)
		}
		q.Topics = append(q.Topics, t)
	}
	nf := r.ArrayLen() // forgotten_topics_data
	for i := 0; i < nf && r.Err() == nil; i++ {
		_ = r.String()
		np := r.ArrayLen()
		for j := 0; j < np && r.Err() == nil; j++ {
			r.Int32()
		}
	}
	_ = r.String() // rack_id
	return r.Err()
}

// FetchPartitionResponse is one partition in a Fetch response. Aborted transactions are written
// as null and the preferred read replica as -1.
type FetchPartitionResponse struct {
	Index                                     int32
	ErrorCode                                 int16
	HighWatermark, LastStableOffset, LogStart int64
	Records                                   []byte
}

// FetchTopicResponse is one topic in a Fetch response.
type FetchTopicResponse struct {
	Name       string
	Partitions []FetchPartitionResponse
}

// FetchResponse is the Fetch v11 response.
type FetchResponse struct {
	ErrorCode int16
	SessionID int32
	Topics    []FetchTopicResponse
}

// Encode writes a Fetch v11 response body.
func (p *FetchResponse) Encode(w *Writer, version int16) {
	w.Int32(0) // throttle_time_ms
	w.Int16(p.ErrorCode)
	w.Int32(p.SessionID)
	w.ArrayLen(len(p.Topics))
	for _, t := range p.Topics {
		w.String(t.Name)
		w.ArrayLen(len(t.Partitions))
		for _, pt := range t.Partitions {
			w.Int32(pt.Index)
			w.Int16(pt.ErrorCode)
			w.Int64(pt.HighWatermark)
			w.Int64(pt.LastStableOffset)
			w.Int64(pt.LogStart)
			w.ArrayLen(-1) // aborted_transactions
			w.Int32(-1)    // preferred_read_replica
			w.NullableBytes(pt.Records)
		}
	}
}
