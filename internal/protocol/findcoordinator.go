package protocol

// FindCoordinatorRequest is the FindCoordinator v2 request.
type FindCoordinatorRequest struct {
	Key     string
	KeyType int8
}

// Decode reads a FindCoordinator v2 request body.
func (q *FindCoordinatorRequest) Decode(r *Reader, version int16) error {
	q.Key = r.String()
	q.KeyType = r.Int8()
	return r.Err()
}

// FindCoordinatorResponse is the FindCoordinator v2 response.
type FindCoordinatorResponse struct {
	ErrorCode    int16
	ErrorMessage *string
	NodeID       int32
	Host         string
	Port         int32
}

// Encode writes a FindCoordinator v2 response body.
func (p *FindCoordinatorResponse) Encode(w *Writer, version int16) {
	w.Int32(0) // throttle_time_ms
	w.Int16(p.ErrorCode)
	w.NullableString(p.ErrorMessage)
	w.Int32(p.NodeID)
	w.String(p.Host)
	w.Int32(p.Port)
}
