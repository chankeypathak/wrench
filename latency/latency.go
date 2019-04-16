package latency

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/golang/snappy"
)

type DataType int

const (
	Tar DataType = iota
	TarGz
	Gzip
	Snappy
	Raw
	Unsupported
)

type LatencyReader interface {
	ConvertFileExtensionToDataType(filename string) DataType
	ReadByType(r io.Reader, dataType DataType) error
}

type LatencyWriter interface {
	Close() error
	Write(p []byte) (n int, err error)
	WriteSimple(offset, latency uint32) (n int, err error)
	WriteOffsetAndLatency(ol OffsetAndLatency) (n int, err error)
}

type latencyReader struct {
	processor func(ol *OffsetAndLatency) error
}

type latencyWriter struct {
	file       *os.File
	w          io.Writer
	compressed bool
	mu         sync.Mutex
}

type OffsetAndLatency struct {
	Offset  uint32
	Latency uint32
}

func getAsBytes(offset, latency uint32) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b, offset)
	binary.LittleEndian.PutUint32(b[4:], latency)
	return b
}

func (ol *OffsetAndLatency) getAsBytes() []byte {
	return getAsBytes(ol.Offset, ol.Latency)
}

func NewReader(processor func(ol *OffsetAndLatency) error) (LatencyReader, error) {
	if processor == nil {
		return nil, errors.New("processor func cannot be nil")
	}
	lr := &latencyReader{
		processor: processor,
	}
	return lr, nil
}

func (lr *latencyReader) readAndProcess(r io.Reader) error {

	b := make([]byte, 8)
	o := b[:4]
	l := b[4:]

	for {
		n, err := r.Read(b)
		if n > 0 {
			ol := &OffsetAndLatency{
				Offset:  binary.LittleEndian.Uint32(o),
				Latency: binary.LittleEndian.Uint32(l),
			}
			if err := lr.processor(ol); err != nil {
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (lr *latencyReader) readTarBall(r io.Reader) error {

	t := tar.NewReader(r)
	for {
		h, err := t.Next()
		if h != nil {
			if err := lr.ReadByType(t, lr.ConvertFileExtensionToDataType(h.Name)); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (lr *latencyReader) readSnappy(r io.Reader, downstream func(io.Reader) error) error {
	s := snappy.NewReader(r)
	return downstream(s)
}

func (lr *latencyReader) readGzip(r io.Reader, downstream func(io.Reader) error) error {
	g, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer g.Close()
	return downstream(g)
}

func (lr *latencyReader) ConvertFileExtensionToDataType(filename string) DataType {
	// Plain filehttps://golang.org/pkg/sync
	if strings.HasSuffix(filename, ".dat") {
		return Raw
		// snappy compressed
	} else if strings.HasSuffix(filename, ".snappy") {
		return Snappy
		// gzipped tarball
	} else if strings.HasSuffix(filename, ".tgz") || strings.HasSuffix(filename, ".tar.gz") {
		return TarGz
		// uncompressed tarball (may contain compressed files)
	} else if strings.HasSuffix(filename, ".tar") {
		return Tar
		// gzip file
	} else if strings.HasSuffix(filename, ".gz") {
		return Gzip
	}
	return Unsupported
}

func (lr *latencyReader) ReadByType(r io.Reader, dataType DataType) error {
	var err error

	switch dataType {
	case Raw:
		err = lr.readAndProcess(r)
	case Snappy:
		err = lr.readSnappy(r, lr.readAndProcess)
	case TarGz:
		err = lr.readGzip(r, lr.readTarBall)
	case Tar:
		err = lr.readTarBall(r)
	case Gzip:
		err = lr.readGzip(r, lr.readAndProcess)
	default:
		err = errors.New("Unsupported DataType")
	}
	return err
}

func NewFileWriter(filename string, compressed bool) (LatencyWriter, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	var downstream io.Writer = f

	// Compression will wrap its own buffer around downstream
	// so if we know we will not compress, we add a separate
	// buffer layer here
	if !compressed {
		downstream = bufio.NewWriter(downstream)
	}
	return newWriter(f, downstream, compressed)
}

func NewWriter(downstream io.Writer, compressed bool) (LatencyWriter, error) {
	return newWriter(nil, downstream, compressed)
}

func newWriter(f *os.File, downstream io.Writer, compressed bool) (LatencyWriter, error) {
	w := downstream
	if compressed {
		w = snappy.NewBufferedWriter(downstream)
	}
	lw := &latencyWriter{
		file:       f,
		w:          w,
		compressed: compressed,
	}
	return lw, nil
}

func (w *latencyWriter) Close() error {
	if w.compressed {
		s := w.w.(io.WriteCloser)
		if err := s.Close(); err != nil {
			return err
		}
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (w *latencyWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

func (w *latencyWriter) WriteOffsetAndLatency(ol OffsetAndLatency) (n int, err error) {
	return w.Write(ol.getAsBytes())
}

func (w *latencyWriter) WriteSimple(offset, latency uint32) (n int, err error) {
	return w.Write(getAsBytes(offset, latency))
}
