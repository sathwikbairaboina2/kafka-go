package protocol

// OffsetFetchTopic is one topic in an OffsetFetch request.
type OffsetFetchTopic struct {
	Name       string
	Partitions []int32
}

// OffsetFetchRequest is the OffsetFetch v1-v7 request. A null topics array (v2+) asks for every
// committed offset of the group. Versions 6 and 7 are flexible (KIP-482).
type OffsetFetchRequest struct {
	GroupID   string
	AllTopics bool // true when the topics array was null
	Topics    []OffsetFetchTopic
}

// Decode reads an OffsetFetch request body.
func (q *OffsetFetchRequest) Decode(r *Reader, version int16) error {
	flexible := version >= 6
	if flexible {
		q.GroupID = r.CompactString()
	} else {
		q.GroupID = r.String()
	}
	var nt int
	if flexible {
		nt = r.CompactArrayLen()
	} else {
		nt = r.ArrayLen()
	}
	if nt < 0 {
		q.AllTopics = true
	}
	for i := 0; i < nt && r.Err() == nil; i++ {
		var t OffsetFetchTopic
		var np int
		if flexible {
			t.Name = r.CompactString()
			np = r.CompactArrayLen()
		} else {
			t.Name = r.String()
			np = r.ArrayLen()
		}
		for j := 0; j < np && r.Err() == nil; j++ {
			t.Partitions = append(t.Partitions, r.Int32())
		}
		if flexible {
			r.SkipTaggedFields()
		}
		q.Topics = append(q.Topics, t)
	}
	if version >= 7 {
		r.Bool() // require_stable
	}
	if flexible {
		r.SkipTaggedFields()
	}
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

// OffsetFetchResponse is the OffsetFetch v1-v7 response.
type OffsetFetchResponse struct {
	Topics    []OffsetFetchTopicResponse
	ErrorCode int16
}

// Encode writes an OffsetFetch response body. Throttle exists from v3, the leader epoch from v5, the
// top-level error code from v2 and compact encodings from v6.
func (p *OffsetFetchResponse) Encode(w *Writer, version int16) {
	flexible := version >= 6
	if version >= 3 {
		w.Int32(0) // throttle_time_ms
	}
	if flexible {
		w.CompactArrayLen(len(p.Topics))
	} else {
		w.ArrayLen(len(p.Topics))
	}
	for _, t := range p.Topics {
		if flexible {
			w.CompactString(t.Name)
			w.CompactArrayLen(len(t.Partitions))
		} else {
			w.String(t.Name)
			w.ArrayLen(len(t.Partitions))
		}
		for _, pt := range t.Partitions {
			w.Int32(pt.Index)
			w.Int64(pt.Offset)
			if version >= 5 {
				w.Int32(pt.LeaderEpoch)
			}
			if flexible {
				w.CompactNullableString(pt.Metadata)
			} else {
				w.NullableString(pt.Metadata)
			}
			w.Int16(pt.ErrorCode)
			if flexible {
				w.EmptyTaggedFields()
			}
		}
		if flexible {
			w.EmptyTaggedFields()
		}
	}
	if version >= 2 {
		w.Int16(p.ErrorCode)
	}
	if flexible {
		w.EmptyTaggedFields()
	}
}
