package protocol

// RequestHeader is the parsed request header.
type RequestHeader struct {
	APIKey, APIVersion int16
	CorrelationID      int32
	ClientID           *string
}

// ParseRequestHeader reads header v1, or v2 (with a tag buffer) when the version is flexible.
func ParseRequestHeader(r *Reader) (RequestHeader, error) {
	var h RequestHeader
	h.APIKey = r.Int16()
	h.APIVersion = r.Int16()
	h.CorrelationID = r.Int32()
	h.ClientID = r.NullableString()
	if IsFlexible(h.APIKey, h.APIVersion) {
		r.SkipTaggedFields()
	}
	if err := r.Err(); err != nil {
		return RequestHeader{}, err
	}
	return h, nil
}

// AppendResponseHeader writes the correlation id, plus an empty tag buffer for response header v1.
func AppendResponseHeader(w *Writer, h RequestHeader) {
	w.Int32(h.CorrelationID)
	if ResponseHeaderVersion(h.APIKey, h.APIVersion) == 1 {
		w.EmptyTaggedFields()
	}
}
