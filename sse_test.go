package ezghflags

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
)

// chunks is a reader that answers one chunk per Read, so frames arrive split across reads.
type chunks struct{ parts []string }

func (c *chunks) Read(b []byte) (int, error) {
	if len(c.parts) == 0 {
		return 0, io.EOF
	}
	n := copy(b, c.parts[0])
	c.parts[0] = c.parts[0][n:]
	if c.parts[0] == "" {
		c.parts = c.parts[1:]
	}
	return n, nil
}

func readAll(t *testing.T, r io.Reader) []sseEvent {
	t.Helper()
	sr := newSSEReader(r, 1<<20)
	var out []sseEvent
	for {
		ev, err := sr.next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		out = append(out, ev)
	}
}

func TestSSE(t *testing.T) {
	cases := []struct {
		name string
		in   io.Reader
		want []sseEvent
	}{
		{
			name: "frames split across reads",
			in:   &chunks{parts: []string{"ev", "ent: snap", "shot\nid: 1", "2\nda", "ta: {\"a\":", "1}\n", "\n", "event: end\ndata: {}\n\n"}},
			want: []sseEvent{{event: "snapshot", id: "12", data: `{"a":1}`}, {event: "end", data: "{}"}},
		},
		{
			name: "one byte at a time",
			in:   iotest.OneByteReader(strings.NewReader("event: snapshot\nid: 3\ndata: x\n\n")),
			want: []sseEvent{{event: "snapshot", id: "3", data: "x"}},
		},
		{
			name: "comments ignored",
			in:   strings.NewReader(": open\n\n: heartbeat\n\nevent: end\n: inside\ndata: {\"reason\":\"rotate\"}\n\n"),
			want: []sseEvent{{event: "end", data: `{"reason":"rotate"}`}},
		},
		{
			name: "id and event carried",
			in:   strings.NewReader("event: changed\nid: 7\ndata: {\"version\":7}\n\n"),
			want: []sseEvent{{event: "changed", id: "7", data: `{"version":7}`}},
		},
		{
			name: "multi-line data joined with newlines",
			in:   strings.NewReader("data: one\ndata: two\ndata:three\n\n"),
			want: []sseEvent{{event: "message", data: "one\ntwo\nthree"}},
		},
		{
			name: "CRLF line endings",
			in:   strings.NewReader("event: snapshot\r\nid: 4\r\ndata: a\r\ndata: b\r\n\r\n"),
			want: []sseEvent{{event: "snapshot", id: "4", data: "a\nb"}},
		},
		{
			name: "only one leading space is stripped; a field without a colon is empty",
			in:   strings.NewReader("data:  two spaces\ndata\n\n"),
			want: []sseEvent{{event: "message", data: " two spaces\n"}},
		},
		{
			name: "a frame without data isn't dispatched",
			in:   strings.NewReader("event: nothing\nid: 1\n\nevent: x\ndata: y\n\n"),
			want: []sseEvent{{event: "x", data: "y"}},
		},
		{
			name: "an unterminated last frame is dropped",
			in:   strings.NewReader("event: snapshot\ndata: partial"),
			want: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := readAll(t, c.in)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %#v\nwant %#v", got, c.want)
			}
		})
	}
}

func TestSSEEventTooLarge(t *testing.T) {
	sr := newSSEReader(strings.NewReader("data: "+strings.Repeat("x", 100)+"\n\n"), 64)
	if _, err := sr.next(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("want a size error, got %v", err)
	}
}
