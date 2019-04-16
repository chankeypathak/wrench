package wrench

import (
	"encoding/binary"
	"log"
	"net"
	"time"

	"github.com/vwdsrc/wrench/config"
)

// DetermineClockSkew checks the options on whether this instance is
// a) a subscriber and then opens a listening port to receive the time from a publisher or
// b) a publisher and tries to connect to the subscriber to publish its time
func DetermineClockSkew(o *config.Options) error {

	var err error
	if o.NumSubs > 0 && o.NumPubs == 0 && o.SubPort != "" {
		o.ClockSkew, err = determineClockSkew(o.SubPort)
	} else if o.NumSubs == 0 && o.NumPubs > 0 && o.SubIP != "" && o.SubPort != "" {
		err = publishClock(o.SubIP, o.SubPort)
	}
	return err
}

func determineClockSkew(subPort string) (int64, error) {

	skew := int64(0)

	log.Printf("Waiting for publisher synchronization on port %s\n", subPort)

	service := ":" + subPort
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	if err != nil {
		return skew, err
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return skew, err
	}
	conn, err := listener.Accept()
	if err != nil {
		return skew, err
	}
	var buf [10]byte

	_, err = conn.Read(buf[0:])
	now := time.Now().UnixNano()
	if err != nil {
		return skew, err
	}
	conn.Close()
	listener.Close()

	sent := int64(binary.LittleEndian.Uint64(buf[0:]))

	skew = now - sent
	log.Printf("Clock skew is %d\n", skew)

	return skew, nil
}

func publishClock(subIP, subPort string) error {
	log.Printf("Starting publisher synchronization to %s:%s\n", subIP, subPort)
	service := subIP + ":" + subPort
	conn, err := net.Dial("tcp4", service)
	if err != nil {
		return err
	}
	tb := make([]byte, 10)
	now := time.Now().UnixNano()
	binary.LittleEndian.PutUint64(tb, uint64(now))
	_, err = conn.Write(tb)
	if err != nil {
		return err
	}
	conn.Close()
	time.Sleep(1 * time.Second)

	return nil
}
