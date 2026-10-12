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
// from an HTTP body: a leading BOM is stripped, comments are skipped, lines end in LF, CRLF or a
// lone CR, and a frame with no data isn't dispatched.
type sseReader struct {
	br      *bufio.Reader
	max     int    // the largest frame accepted, in bytes
	onLine  func() // called after every complete line, comments included
	started bool   // the BOM check is done
	skipLF  bool   // the last line ended in CR, so a leading LF belongs to it
}

func newSSEReader(r io.Reader, max int) *sseReader {
	return &sseReader{br: bufio.NewReaderSize(r, 64<<10), max: max}
}

var errFrameTooLarge = errors.New("ezghflags: stream frame too large")

var bom = []byte("\xEF\xBB\xBF")

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
		if s.onLine != nil {
			s.onLine()
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

// readLine reads one line without its ending (LF, CRLF or CR), refusing lines longer than limit.
// A line still unterminated at the end of the stream is dropped (io.EOF).
func (s *sseReader) readLine(limit int) ([]byte, error) {
	if !s.started {
		s.started = true
		if b, _ := s.br.Peek(len(bom)); bytes.Equal(b, bom) {
			_, _ = s.br.Discard(len(bom))
		}
	}
	var line []byte
	for {
		if _, err := s.br.Peek(1); err != nil {
			return nil, err
		}
		chunk, _ := s.br.Peek(s.br.Buffered())
		if s.skipLF {
			s.skipLF = false
			if chunk[0] == '\n' {
				_, _ = s.br.Discard(1)
				continue
			}
		}
		i := bytes.IndexAny(chunk, "\r\n")
		n := i
		if i < 0 {
			n = len(chunk)
		}
		if len(line)+n > limit {
			return nil, fmt.Errorf("%w (over %d bytes)", errFrameTooLarge, s.max)
		}
		line = append(line, chunk[:n]...)
		if i < 0 {
			_, _ = s.br.Discard(n)
			continue
		}
		s.skipLF = chunk[i] == '\r'
		_, _ = s.br.Discard(i + 1)
		return line, nil
	}
}
