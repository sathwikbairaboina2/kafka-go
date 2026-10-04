package protocol

// ProducePartition is one partition's records in a Produce request.
type ProducePartition struct {
	Index   int32
	Records []byte // sub-slice of the frame
}

// ProduceTopic is one topic in a Produce request.
type ProduceTopic struct {
	Name       string
	Partitions []ProducePartition
}

// ProduceRequest is the Produce v3-v7 request (the request layout is identical across those versions).
type ProduceRequest struct {
	TransactionalID *string
	Acks            int16
	TimeoutMs       int32
	Topics          []ProduceTopic
}

// Decode reads a Produce v3-v7 request body.
func (q *ProduceRequest) Decode(r *Reader, version int16) error {
	q.TransactionalID = r.NullableString()
	q.Acks = r.Int16()
	q.TimeoutMs = r.Int32()
	nt := r.ArrayLen()
	if nt > 0 {
		q.Topics = make([]ProduceTopic, 0, nt)
	}
	for i := 0; i < nt && r.Err() == nil; i++ {
		t := ProduceTopic{Name: r.String()}
		np := r.ArrayLen()
		if np > 0 {
			t.Partitions = make([]ProducePartition, 0, np)
		}
		for j := 0; j < np && r.Err() == nil; j++ {
			t.Partitions = append(t.Partitions, ProducePartition{Index: r.Int32(), Records: r.NullableBytes()})
		}
		q.Topics = append(q.Topics, t)
	}
	return r.Err()
}

// ProducePartitionResponse is one partition's result in a Produce response.
type ProducePartitionResponse struct {
	Index                                       int32
	ErrorCode                                   int16
	BaseOffset, LogAppendTimeMs, LogStartOffset int64
}

// ProduceTopicResponse is one topic in a Produce response.
type ProduceTopicResponse struct {
	Name       string
	Partitions []ProducePartitionResponse
}

// ProduceResponse is the Produce v3-v7 response.
type ProduceResponse struct{ Topics []ProduceTopicResponse }

// Encode writes a Produce response body; log_start_offset exists from v5.
func (p *ProduceResponse) Encode(w *Writer, version int16) {
	w.ArrayLen(len(p.Topics))
	for _, t := range p.Topics {
		w.String(t.Name)
		w.ArrayLen(len(t.Partitions))
		for _, pt := range t.Partitions {
			w.Int32(pt.Index)
			w.Int16(pt.ErrorCode)
			w.Int64(pt.BaseOffset)
			w.Int64(pt.LogAppendTimeMs)
			if version >= 5 {
				w.Int64(pt.LogStartOffset)
			}
		}
	}
	w.Int32(0) // throttle_time_ms
}
