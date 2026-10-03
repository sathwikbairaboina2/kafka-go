package protocol

// HeartbeatRequest is the Heartbeat v3 request.
type HeartbeatRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID *string
}

// Decode reads a Heartbeat v3 request body.
func (q *HeartbeatRequest) Decode(r *Reader, version int16) error {
	q.GroupID = r.String()
	q.GenerationID = r.Int32()
	q.MemberID = r.String()
	q.GroupInstanceID = r.NullableString()
	return r.Err()
}

// HeartbeatResponse is the Heartbeat v3 response.
type HeartbeatResponse struct{ ErrorCode int16 }

// Encode writes a Heartbeat v3 response body.
func (p *HeartbeatResponse) Encode(w *Writer, version int16) {
	w.Int32(0) // throttle_time_ms
	w.Int16(p.ErrorCode)
}
