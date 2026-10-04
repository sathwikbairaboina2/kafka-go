package protocol

// HeartbeatRequest is the Heartbeat v0-v3 request (group instance id from v3).
type HeartbeatRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID *string
}

// Decode reads a Heartbeat request body.
func (q *HeartbeatRequest) Decode(r *Reader, version int16) error {
	q.GroupID = r.String()
	q.GenerationID = r.Int32()
	q.MemberID = r.String()
	if version >= 3 {
		q.GroupInstanceID = r.NullableString()
	}
	return r.Err()
}

// HeartbeatResponse is the Heartbeat v0-v3 response.
type HeartbeatResponse struct{ ErrorCode int16 }

// Encode writes a Heartbeat response body; throttle exists from v1.
func (p *HeartbeatResponse) Encode(w *Writer, version int16) {
	if version >= 1 {
		w.Int32(0) // throttle_time_ms
	}
	w.Int16(p.ErrorCode)
}
