package protocol

// MetadataRequest is the Metadata v4 request.
type MetadataRequest struct {
	Topics          []string
	AllTopics       bool // true when the topics array was null
	AllowAutoCreate bool
}

// Decode reads a Metadata v4 request body.
func (q *MetadataRequest) Decode(r *Reader, version int16) error {
	n := r.ArrayLen()
	if n < 0 {
		q.AllTopics = true
	} else {
		q.Topics = make([]string, 0, n)
		for i := 0; i < n && r.Err() == nil; i++ {
			q.Topics = append(q.Topics, r.String())
		}
	}
	q.AllowAutoCreate = r.Bool()
	return r.Err()
}

// MetadataBroker is one broker in a Metadata response.
type MetadataBroker struct {
	NodeID int32
	Host   string
	Port   int32
}

// MetadataPartition is one partition in a Metadata response.
type MetadataPartition struct {
	ErrorCode     int16
	Index, Leader int32
	Replicas, ISR []int32
}

// MetadataTopic is one topic in a Metadata response.
type MetadataTopic struct {
	ErrorCode  int16
	Name       string
	Partitions []MetadataPartition
}

// MetadataResponse is the Metadata v4 response.
type MetadataResponse struct {
	Brokers      []MetadataBroker
	ClusterID    *string
	ControllerID int32
	Topics       []MetadataTopic
}

// Encode writes a Metadata v4 response body.
func (p *MetadataResponse) Encode(w *Writer, version int16) {
	w.Int32(0) // throttle_time_ms
	w.ArrayLen(len(p.Brokers))
	for _, b := range p.Brokers {
		w.Int32(b.NodeID)
		w.String(b.Host)
		w.Int32(b.Port)
		w.NullableString(nil) // rack
	}
	w.NullableString(p.ClusterID)
	w.Int32(p.ControllerID)
	w.ArrayLen(len(p.Topics))
	for _, t := range p.Topics {
		w.Int16(t.ErrorCode)
		w.String(t.Name)
		w.Bool(false) // is_internal
		w.ArrayLen(len(t.Partitions))
		for _, pt := range t.Partitions {
			w.Int16(pt.ErrorCode)
			w.Int32(pt.Index)
			w.Int32(pt.Leader)
			w.ArrayLen(len(pt.Replicas))
			for _, id := range pt.Replicas {
				w.Int32(id)
			}
			w.ArrayLen(len(pt.ISR))
			for _, id := range pt.ISR {
				w.Int32(id)
			}
		}
	}
}
