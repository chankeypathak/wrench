package recordprovider

import (
	"math"
	"math/rand"
	"time"

	"github.com/vwdsrc/wrench/config"
)

type RecordProvider interface {
	GetRecords(o *config.Options) [][]byte
}

var providers map[string]RecordProvider

func init() {
	providers = make(map[string]RecordProvider)

	Register("zero", &zeroProvider{})
	Register("random", &randomProvider{})
}

func Register(name string, r RecordProvider) {
	providers[name] = r
}

func GetProvider(name string) RecordProvider {
	return providers[name]
}

type zeroProvider struct{}

func (zp *zeroProvider) GetRecords(o *config.Options) [][]byte {
	return nil
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
