package ezghflags

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// sseEvent is one Server-Sent Events frame. Fields are per frame: an id isn't carried over to
// later frames.
type sseEvent struct {
	event string // "message" when the frame names none
	id    string
	data  string // data lines joined with "\n"
}

// sseReader reads the event-stream format (the WHATWG HTML "Server-sent events" parsing rules)
// from an HTTP body: comments are skipped, lines end in LF or CRLF, and a frame with no data
// isn't dispatched.
type sseReader struct {
	br  *bufio.Reader
	max int // the largest frame accepted, in bytes
}

func newSSEReader(r io.Reader, max int) *sseReader {
	return &sseReader{br: bufio.NewReaderSize(r, 64<<10), max: max}
}

var errFrameTooLarge = errors.New("ezghflags: stream frame too large")

// next answers the next dispatched frame. io.EOF ends the stream; a final frame without its
// blank line is dropped.
func (s *sseReader) next() (sseEvent, error) {
	var (
		ev      sseEvent
		data    bytes.Buffer
		hasData bool
		size    int
	)
	for {
		line, err := s.readLine(s.max - size)
		if err != nil {
			return sseEvent{}, err
		}
		size += len(line)
		if len(line) == 0 {
			if hasData {
				if ev.event == "" {
					ev.event = "message"
				}
				ev.data = data.String()
				return ev, nil
			}
			ev, hasData, size = sseEvent{}, false, 0
			data.Reset()
			continue
		}
		if line[0] == ':' {
			continue
		}
		field, value := line, []byte(nil)
		if i := bytes.IndexByte(line, ':'); i >= 0 {
			field, value = line[:i], line[i+1:]
			value = bytes.TrimPrefix(value, []byte(" "))
		}
		switch string(field) {
		case "event":
			ev.event = string(value)
		case "id":
			ev.id = string(value)
		case "data":
			if hasData {
				data.WriteByte('\n')
			}
			data.Write(value)
			hasData = true
		}
	}
}

// readLine reads one line without its LF or CRLF, refusing lines longer than limit.
func (s *sseReader) readLine(limit int) ([]byte, error) {
	var line []byte
	for {
		chunk, err := s.br.ReadSlice('\n')
		if len(line)+len(chunk) > limit+2 {
			return nil, fmt.Errorf("%w (over %d bytes)", errFrameTooLarge, s.max)
		}
		line = append(line, chunk...)
		switch {
		case err == nil:
			line = bytes.TrimSuffix(line[:len(line)-1], []byte("\r"))
			return line, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			return nil, io.EOF
		default:
			return nil, err
		}
	}
}
