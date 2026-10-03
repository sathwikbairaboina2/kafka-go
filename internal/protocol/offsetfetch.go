package protocol

// OffsetFetchTopic is one topic in an OffsetFetch request.
type OffsetFetchTopic struct {
	Name       string
	Partitions []int32
}

// OffsetFetchRequest is the OffsetFetch v7 request.
type OffsetFetchRequest struct {
	GroupID   string
	AllTopics bool // true when the topics array was null
	Topics    []OffsetFetchTopic
}

// Decode reads an OffsetFetch v7 request body.
func (q *OffsetFetchRequest) Decode(r *Reader, version int16) error {
	q.GroupID = r.String()
	nt := r.ArrayLen()
	if nt < 0 {
		q.AllTopics = true
	}
	for i := 0; i < nt && r.Err() == nil; i++ {
		t := OffsetFetchTopic{Name: r.String()}
		np := r.ArrayLen()
		for j := 0; j < np && r.Err() == nil; j++ {
			t.Partitions = append(t.Partitions, r.Int32())
		}
		q.Topics = append(q.Topics, t)
	}
	r.Bool() // require_stable
	return r.Err()
}

// OffsetFetchPartitionResponse is one partition's committed offset in an OffsetFetch response.
type OffsetFetchPartitionResponse struct {
	Index       int32
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
	ErrorCode   int16
}

// OffsetFetchTopicResponse is one topic in an OffsetFetch response.
type OffsetFetchTopicResponse struct {
	Name       string
	Partitions []OffsetFetchPartitionResponse
}

// OffsetFetchResponse is the OffsetFetch v7 response.
type OffsetFetchResponse struct {
	Topics    []OffsetFetchTopicResponse
	ErrorCode int16
}

// Encode writes an OffsetFetch v7 response body.
func (p *OffsetFetchResponse) Encode(w *Writer, version int16) {
	w.Int32(0) // throttle_time_ms
	w.ArrayLen(len(p.Topics))
	for _, t := range p.Topics {
		w.String(t.Name)
		w.ArrayLen(len(t.Partitions))
		for _, pt := range t.Partitions {
			w.Int32(pt.Index)
			w.Int64(pt.Offset)
			w.Int32(pt.LeaderEpoch)
			w.NullableString(pt.Metadata)
			w.Int16(pt.ErrorCode)
		}
	}
	w.Int16(p.ErrorCode)
}
