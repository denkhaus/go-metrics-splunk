package splunk

import (
	"fmt"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
)

var (
	logger = logrus.New().WithField("pkg", "go-metrics-splunk")
)

type splunkForwarder struct {
	reg      metrics.Registry
	ch       chan SplunkMessage
	interval time.Duration
	client   *SplunkClient
	addr     string
	retry    int
}

// Splunk starts a Splunk metrics forwarder from the given registry at each d interval.
func Splunk(r metrics.Registry, d time.Duration, splunkAddr string, retry int) {
	rep := &splunkForwarder{
		ch:       make(chan SplunkMessage),
		addr:     splunkAddr,
		retry:    retry,
		reg:      r,
		interval: d,
	}

	if err := rep.createClient(); err != nil {
		logger.Errorf("unable to create client: %s", err)
		return
	}
	rep.run()
}

func (r *splunkForwarder) createClient() error {
	cli, err := retry(func() (interface{}, error) {
		return NewSplunkClient(r.addr)
	}, r.retry, "create client")
	if err != nil {
		return err
	}
	r.client = cli.(*SplunkClient)
	return nil
}

func (r *splunkForwarder) run() {
	sendTicker := time.Tick(r.interval)
	go r.client.Stream(r.ch)

	for {
		select {
		case <-sendTicker:
			r.send()
		}
	}
}

func (r *splunkForwarder) send() {
	r.reg.Each(func(name string, i interface{}) {
		now := time.Now()

		switch m := i.(type) {
		case metrics.Counter:
			r.ch <- SplunkMessage{
				Name: fmt.Sprintf("%s.count", name),
				Fields: map[string]interface{}{
					"value": m.Count(),
				},
				Time: now,
			}
		case metrics.Gauge:
			r.ch <- SplunkMessage{
				Name: fmt.Sprintf("%s.gauge", name),
				Fields: map[string]interface{}{
					"value": m.Value(),
				},
				Time: now,
			}
		case metrics.GaugeFloat64:
			r.ch <- SplunkMessage{
				Name: fmt.Sprintf("%s.gauge", name),
				Fields: map[string]interface{}{
					"value": m.Value(),
				},
				Time: now,
			}
		case metrics.Histogram:
			ps := m.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			r.ch <- SplunkMessage{
				Name: fmt.Sprintf("%s.histogram", name),
				Fields: map[string]interface{}{
					"count":    m.Count(),
					"max":      m.Max(),
					"mean":     m.Mean(),
					"min":      m.Min(),
					"stddev":   m.StdDev(),
					"variance": m.Variance(),
					"p50":      ps[0],
					"p75":      ps[1],
					"p95":      ps[2],
					"p99":      ps[3],
					"p999":     ps[4],
					"p9999":    ps[5],
				},
				Time: now,
			}
		case metrics.Meter:
			r.ch <- SplunkMessage{
				Name: fmt.Sprintf("%s.meter", name),
				Fields: map[string]interface{}{
					"count": m.Count(),
					"m1":    m.Rate1(),
					"m5":    m.Rate5(),
					"m15":   m.Rate15(),
					"mean":  m.RateMean(),
				},
				Time: now,
			}
		case metrics.Timer:
			ps := m.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			r.ch <- SplunkMessage{
				Name: fmt.Sprintf("%s.timer", name),
				Fields: map[string]interface{}{
					"count":    m.Count(),
					"max":      m.Max(),
					"mean":     m.Mean(),
					"min":      m.Min(),
					"stddev":   m.StdDev(),
					"variance": m.Variance(),
					"p50":      ps[0],
					"p75":      ps[1],
					"p95":      ps[2],
					"p99":      ps[3],
					"p999":     ps[4],
					"p9999":    ps[5],
					"m1":       m.Rate1(),
					"m5":       m.Rate5(),
					"m15":      m.Rate15(),
					"meanrate": m.RateMean(),
				},
				Time: now,
			}
		}
	})
}
