package protocol

// LeaveGroupRequest is the LeaveGroup v0-v1 request.
type LeaveGroupRequest struct{ GroupID, MemberID string }

// Decode reads a LeaveGroup request body.
func (q *LeaveGroupRequest) Decode(r *Reader, version int16) error {
	q.GroupID = r.String()
	q.MemberID = r.String()
	return r.Err()
}

// LeaveGroupResponse is the LeaveGroup v0-v1 response.
type LeaveGroupResponse struct{ ErrorCode int16 }

// Encode writes a LeaveGroup response body; throttle exists from v1.
func (p *LeaveGroupResponse) Encode(w *Writer, version int16) {
	if version >= 1 {
		w.Int32(0) // throttle_time_ms
	}
	w.Int16(p.ErrorCode)
}
