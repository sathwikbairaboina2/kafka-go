package protocol

// ListOffsetsPartition is one partition in a ListOffsets request.
type ListOffsetsPartition struct {
	Index     int32
	Timestamp int64
}

// ListOffsetsTopic is one topic in a ListOffsets request.
type ListOffsetsTopic struct {
	Name       string
	Partitions []ListOffsetsPartition
}

// ListOffsetsRequest is the ListOffsets v2 request.
type ListOffsetsRequest struct {
	ReplicaID      int32
	IsolationLevel int8
	Topics         []ListOffsetsTopic
}

// Decode reads a ListOffsets v2 request body.
func (q *ListOffsetsRequest) Decode(r *Reader, version int16) error {
	q.ReplicaID = r.Int32()
	q.IsolationLevel = r.Int8()
	nt := r.ArrayLen()
	for i := 0; i < nt && r.Err() == nil; i++ {
		t := ListOffsetsTopic{Name: r.String()}
		np := r.ArrayLen()
		for j := 0; j < np && r.Err() == nil; j++ {
			t.Partitions = append(t.Partitions, ListOffsetsPartition{Index: r.Int32(), Timestamp: r.Int64()})
		}
		q.Topics = append(q.Topics, t)
	}
	return r.Err()
}

// ListOffsetsPartitionResponse is one partition's result in a ListOffsets response.
type ListOffsetsPartitionResponse struct {
	Index     int32
	ErrorCode int16
	Timestamp int64
	Offset    int64
}

// ListOffsetsTopicResponse is one topic in a ListOffsets response.
type ListOffsetsTopicResponse struct {
	Name       string
	Partitions []ListOffsetsPartitionResponse
}

// ListOffsetsResponse is the ListOffsets v2 response.
type ListOffsetsResponse struct{ Topics []ListOffsetsTopicResponse }

// Encode writes a ListOffsets v2 response body.
func (p *ListOffsetsResponse) Encode(w *Writer, version int16) {
	w.Int32(0) // throttle_time_ms
	w.ArrayLen(len(p.Topics))
	for _, t := range p.Topics {
		w.String(t.Name)
		w.ArrayLen(len(t.Partitions))
		for _, pt := range t.Partitions {
			w.Int32(pt.Index)
			w.Int16(pt.ErrorCode)
			w.Int64(pt.Timestamp)
			w.Int64(pt.Offset)
		}
	}
}
