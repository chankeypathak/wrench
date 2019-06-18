package recordprovider

import (
	"encoding/binary"
	"math"
	"math/rand"
	"time"

	"github.com/vwdsrc/wrench/config"
)

var providers map[string]config.RecordProvider

func init() {
	providers = make(map[string]config.RecordProvider)

	Register("zero", &zeroProvider{})
	Register("random", &randomProvider{})
}

func Register(name string, r config.RecordProvider) {
	providers[name] = r
}

func GetProvider(name string) config.RecordProvider {
	return providers[name]
}

func DefaultGetTimestamp(msg []byte) int64 {
	return int64(binary.LittleEndian.Uint64(msg))
}

func DefaultInjectTimestamp(msg []byte, timestamp int64) []byte {
	binary.LittleEndian.PutUint64(msg, uint64(timestamp))
	return msg
}

type zeroProvider struct{}

func (zp *zeroProvider) GetRecords(o *config.Options) [][]byte {
	records := make([][]byte, 0)
	sliceSize := int(math.Max(float64(o.PayloadSize), 8))
	b := make([]byte, sliceSize)
	return append(records, b)
}

func (zp *zeroProvider) InjectTimestamp(msg []byte, timestamp int64) []byte {
	return DefaultInjectTimestamp(msg, timestamp)
}

func (zp *zeroProvider) GetTimestamp(msg []byte) int64 {
	return DefaultGetTimestamp(msg)
}

type randomProvider struct{}

func (rp *randomProvider) GetRecords(o *config.Options) [][]byte {
	records := make([][]byte, 0)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Generate packets to fill 100 MB of random data but at least one
	count := int(math.Max(float64((100*1024*1024)/o.PayloadSize), 1))
	sliceSize := int(math.Max(float64(o.PayloadSize), 8))
	for i := 0; i < count; i++ {
		b := make([]byte, sliceSize)
		r.Read(b[8:])
		records = append(records, b)
	}
	return records
}

func (rp *randomProvider) InjectTimestamp(msg []byte, timestamp int64) []byte {
	return DefaultInjectTimestamp(msg, timestamp)
}

func (rp *randomProvider) GetTimestamp(msg []byte) int64 {
	return DefaultGetTimestamp(msg)
}
