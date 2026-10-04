package protocol

// FindCoordinatorRequest is the FindCoordinator v0-v2 request (key_type exists from v1).
type FindCoordinatorRequest struct {
	Key     string
	KeyType int8
}

// Decode reads a FindCoordinator request body.
func (q *FindCoordinatorRequest) Decode(r *Reader, version int16) error {
	q.Key = r.String()
	if version >= 1 {
		q.KeyType = r.Int8()
	}
	return r.Err()
}

// FindCoordinatorResponse is the FindCoordinator v0-v2 response.
type FindCoordinatorResponse struct {
	ErrorCode    int16
	ErrorMessage *string
	NodeID       int32
	Host         string
	Port         int32
}

// Encode writes a FindCoordinator response body; throttle and error message exist from v1.
func (p *FindCoordinatorResponse) Encode(w *Writer, version int16) {
	if version >= 1 {
		w.Int32(0) // throttle_time_ms
	}
	w.Int16(p.ErrorCode)
	if version >= 1 {
		w.NullableString(p.ErrorMessage)
	}
	w.Int32(p.NodeID)
	w.String(p.Host)
	w.Int32(p.Port)
}
