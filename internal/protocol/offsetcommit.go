package protocol

// CommitPartition is one partition's offset in an OffsetCommit request.
type CommitPartition struct {
	Index       int32
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
}

// CommitTopic is one topic in an OffsetCommit request.
type CommitTopic struct {
	Name       string
	Partitions []CommitPartition
}

// OffsetCommitRequest is the OffsetCommit v2-v7 request. v2-v4 carry a retention time (ignored), v6 adds
// the leader epoch (-1 before) and v7 the group instance id.
type OffsetCommitRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID *string
	Topics          []CommitTopic
}

// Decode reads an OffsetCommit request body.
func (q *OffsetCommitRequest) Decode(r *Reader, version int16) error {
	q.GroupID = r.String()
	q.GenerationID = r.Int32()
	q.MemberID = r.String()
	if version >= 7 {
		q.GroupInstanceID = r.NullableString()
	}
	if version <= 4 {
		r.Int64() // retention_time_ms
	}
	nt := r.ArrayLen()
	for i := 0; i < nt && r.Err() == nil; i++ {
		t := CommitTopic{Name: r.String()}
		np := r.ArrayLen()
		for j := 0; j < np && r.Err() == nil; j++ {
			p := CommitPartition{Index: r.Int32(), Offset: r.Int64(), LeaderEpoch: -1}
			if version >= 6 {
				p.LeaderEpoch = r.Int32()
			}
			p.Metadata = r.NullableString()
			t.Partitions = append(t.Partitions, p)
		}
		q.Topics = append(q.Topics, t)
	}
	return r.Err()
}

// CommitPartitionResponse is one partition's result in an OffsetCommit response.
type CommitPartitionResponse struct {
	Index     int32
	ErrorCode int16
}

// CommitTopicResponse is one topic in an OffsetCommit response.
type CommitTopicResponse struct {
	Name       string
	Partitions []CommitPartitionResponse
}

// OffsetCommitResponse is the OffsetCommit v2-v7 response.
type OffsetCommitResponse struct{ Topics []CommitTopicResponse }

// Encode writes an OffsetCommit response body; throttle exists from v3.
func (p *OffsetCommitResponse) Encode(w *Writer, version int16) {
	if version >= 3 {
		w.Int32(0) // throttle_time_ms
	}
	w.ArrayLen(len(p.Topics))
	for _, t := range p.Topics {
		w.String(t.Name)
		w.ArrayLen(len(t.Partitions))
		for _, pt := range t.Partitions {
			w.Int32(pt.Index)
			w.Int16(pt.ErrorCode)
		}
	}
}
