package server

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/sathwikbairaboina2/kafka-go/internal/protocol"
)

// minFrame is the smallest legal frame: api key, version, correlation id and a null client id.
const minFrame = 10

func (s *Server) serveConn(c net.Conn) {
	defer c.Close()
	log := s.opts.Logger.With("remote", c.RemoteAddr().String())
	br := bufio.NewReaderSize(c, 64<<10)
	var sizeBuf [4]byte
	var frame []byte
	for {
		_ = c.SetReadDeadline(time.Now().Add(s.opts.IdleTimeout))
		if _, err := io.ReadFull(br, sizeBuf[:]); err != nil {
			return
		}
		size := int32(binary.BigEndian.Uint32(sizeBuf[:]))
		if size < minFrame || size > s.opts.MaxFrameBytes {
			log.Warn("rejecting frame size", "size", size, "max", s.opts.MaxFrameBytes)
			return
		}
		if cap(frame) < int(size) {
			frame = make([]byte, size)
		}
		frame = frame[:size]
		if _, err := io.ReadFull(br, frame); err != nil {
			return
		}
		r := protocol.NewReader(frame)
		h, err := protocol.ParseRequestHeader(r)
		if err != nil {
			log.Warn("bad request header", "err", err)
			return
		}
		var out []byte
		if !protocol.IsSupported(h.APIKey, h.APIVersion) {
			if h.APIKey != protocol.KeyApiVersions {
				log.Warn("unsupported api version, closing", "key", h.APIKey, "version", h.APIVersion)
				return
			}
			// An unsupported ApiVersions version is answered in v0 format so the client can downgrade.
			w := protocol.NewWriter(256)
			w.Int32(0) // size placeholder
			w.Int32(h.CorrelationID)
			(&protocol.ApiVersionsResponse{ErrorCode: protocol.ErrUnsupportedVersion, Keys: protocol.SupportedKeys()}).Encode(w, 0)
			out = w.Buf()
		} else {
			resp, respond, err := s.h.Handle(s.ctx, h, r)
			if err != nil {
				log.Warn("handler error, closing connection", "key", h.APIKey, "version", h.APIVersion, "err", err)
				return
			}
			if !respond {
				continue
			}
			w := protocol.NewWriter(4 + 5 + len(resp))
			w.Int32(0) // size placeholder
			protocol.AppendResponseHeader(w, h)
			w.Raw(resp)
			out = w.Buf()
		}
		binary.BigEndian.PutUint32(out, uint32(len(out)-4))
		_ = c.SetWriteDeadline(time.Now().Add(s.opts.IdleTimeout))
		if _, err := c.Write(out); err != nil {
			return
		}
	}
}
