package subscriber

import (
	"expvar"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/meta"
)

// Statistics for the CQ service.
const (
	statPointsWritten = "points_written"
	statWriteFailures = "write_failures"
)

type PointsWriter interface {
	WritePoints(p *cluster.WritePointsRequest) error
}

// unique set that identifies a given subscription
type subEntry struct {
	db   string
	rp   string
	name string
}

type Service struct {
	subs      map[subEntry]PointsWriter
	MetaStore interface {
		Databases() ([]meta.DatabaseInfo, error)
	}
	NewPointsWriter func(u url.URL) (PointsWriter, error)
	Logger          *log.Logger
	statMap         *expvar.Map
	ticker          *time.Ticker
}

func NewService() *Service {
	return &Service{
		subs:            make(map[subEntry]PointsWriter),
		NewPointsWriter: newPointsWriter,
		Logger:          log.New(os.Stderr, "[subscriber] ", log.LstdFlags),
		statMap:         influxdb.NewStatistics("subscriber", "subscriber", nil),
	}
}

func (s *Service) Open() error {
	if s.MetaStore == nil {
		panic("no meta store")
	}
	s.Update()
	s.ticker = time.NewTicker(time.Minute)
	go s.pollForMetaUpdates()

	return nil
}

func (s *Service) Close() error {
	if s.ticker != nil {
		s.ticker.Stop()
	}
	return nil
}

func (s *Service) pollForMetaUpdates() {
	for range s.ticker.C {
		s.Update()
	}
}

// start new and stop deleted subscriptions.
func (s *Service) Update() error {
	dbis, err := s.MetaStore.Databases()
	if err != nil {
		return err
	}
	allEntries := make(map[subEntry]bool, 0)
	// Add in new subscriptions
	for _, dbi := range dbis {
		for _, rpi := range dbi.RetentionPolicies {
			for _, si := range rpi.Subscriptions {
				se := subEntry{
					db:   dbi.Name,
					rp:   rpi.Name,
					name: si.Name,
				}
				allEntries[se] = true
				if _, ok := s.subs[se]; ok {
					continue
				}
				var bm BalanceMode
				switch si.Mode {
				case "ALL":
					bm = ALL
				case "ANY":
					bm = ANY
				default:
					return fmt.Errorf("unknown balance mode %q", si.Mode)
				}
				writers := make([]PointsWriter, len(si.Destinations))
				statMaps := make([]*expvar.Map, len(writers))
				for i, dest := range si.Destinations {
					u, err := url.Parse(dest)
					if err != nil {
						return err
					}
					w, err := s.NewPointsWriter(*u)
					if err != nil {
						return err
					}
					writers[i] = w
					tags := map[string]string{
						"database":         se.db,
						"retention_policy": se.rp,
						"name":             se.name,
						"mode":             si.Mode,
						"destination":      dest,
					}
					key := strings.Join([]string{"subscriber", se.db, se.rp, se.name, dest}, ":")
					statMaps[i] = influxdb.NewStatistics(key, "subscriber", tags)
				}
				s.subs[se] = &balancewriter{
					bm:       bm,
					writers:  writers,
					statMaps: statMaps,
				}
			}
		}
	}

	// Remove deleted subs
	for se := range s.subs {
		if !allEntries[se] {
			delete(s.subs, se)
		}
	}

	return nil
}

func (s *Service) WritePoints(p *cluster.WritePointsRequest) {
	for se, sub := range s.subs {
		if p.Database == se.db && p.RetentionPolicy == se.rp {
			err := sub.WritePoints(p)
			if err != nil {
				s.Logger.Println(err)
				s.statMap.Add(statWriteFailures, 1)
			}
		}
	}
	s.statMap.Add(statPointsWritten, int64(len(p.Points)))
}

type BalanceMode int

const (
	ALL BalanceMode = iota
	ANY
)

// balances writes across PointsWriters according to BalanceMode
type balancewriter struct {
	bm       BalanceMode
	writers  []PointsWriter
	statMaps []*expvar.Map
	i        int
}

func (b *balancewriter) WritePoints(p *cluster.WritePointsRequest) error {
	switch b.bm {
	case ALL:
		var lastErr error
		for i, w := range b.writers {
			err := w.WritePoints(p)
			if err != nil {
				lastErr = err
				b.statMaps[i].Add(statWriteFailures, 1)
			} else {
				b.statMaps[i].Add(statPointsWritten, int64(len(p.Points)))
			}
		}
		return lastErr
	case ANY:
		var err error
		// Try as many times as we have writers
		for range b.writers {
			i := b.i
			w := b.writers[i]
			b.i = (b.i + 1) % len(b.writers)
			if err = w.WritePoints(p); err == nil {
				b.statMaps[i].Add(statPointsWritten, int64(len(p.Points)))
				break
			}
			b.statMaps[i].Add(statWriteFailures, 1)
		}
		return err
	}
	return fmt.Errorf("unsupported balance mode %q", b.bm)
}

// Creates a PointsWriter from the given URL
func newPointsWriter(u url.URL) (PointsWriter, error) {
	switch u.Scheme {
	case "udp":
		return NewUDP(u.Host), nil
	default:
		return nil, fmt.Errorf("unknown destination scheme %s", u.Scheme)
	}
}
