package subscriber_test

import (
	"net/url"
	"testing"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/services/subscriber"
)

type MetaStore struct {
	DatabasesFn func() ([]meta.DatabaseInfo, error)
}

func (m MetaStore) Databases() ([]meta.DatabaseInfo, error) {
	return m.DatabasesFn()
}

type Subscription struct {
	WritePointsFn func(*cluster.WritePointsRequest) error
}

func (s Subscription) WritePoints(p *cluster.WritePointsRequest) error {
	return s.WritePointsFn(p)
}

func TestServiceWritePoints_IgnoreNonMatch(t *testing.T) {
	ms := MetaStore{}
	ms.DatabasesFn = func() ([]meta.DatabaseInfo, error) {
		return []meta.DatabaseInfo{
			{
				Name: "db0",
				RetentionPolicies: []meta.RetentionPolicyInfo{
					{
						Name: "rp0",
						Subscriptions: []meta.SubscriptionInfo{
							{Name: "s0", Mode: "ANY", Destinations: []string{"udp://h0:9093", "udp://h1:9093"}},
						},
					},
				},
			},
		}, nil
	}

	prs := make(chan *cluster.WritePointsRequest, 2)
	urls := make(chan url.URL, 2)
	newPointsWriter := func(u url.URL) (subscriber.PointsWriter, error) {
		urls <- u
		sub := Subscription{}
		sub.WritePointsFn = func(p *cluster.WritePointsRequest) error {
			prs <- p
			return nil
		}
		return sub, nil
	}

	s := subscriber.NewService()
	s.MetaStore = ms
	s.NewPointsWriter = newPointsWriter

	err := s.Update()

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	for _, expURLStr := range []string{"udp://h0:9093", "udp://h1:9093"} {
		var u url.URL
		expURL, _ := url.Parse(expURLStr)
		select {
		case u = <-urls:
		default:
			t.Fatal("expected urls")
		}
		if expURL.String() != u.String() {
			t.Fatalf("unexpected url: got %s exp %s", u.String(), expURL.String())
		}
	}

	// Write points that don't match any subscription.
	s.WritePoints(&cluster.WritePointsRequest{
		Database:        "db1",
		RetentionPolicy: "rp0",
	})
	s.WritePoints(&cluster.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp2",
	})

	// Shouldn't get any prs back
	select {
	case pr := <-prs:
		t.Fatalf("unexpected points request %v", pr)
	default:
	}
}

func TestServiceWritePoints_ModeALL(t *testing.T) {
	ms := MetaStore{}
	ms.DatabasesFn = func() ([]meta.DatabaseInfo, error) {
		return []meta.DatabaseInfo{
			{
				Name: "db0",
				RetentionPolicies: []meta.RetentionPolicyInfo{
					{
						Name: "rp0",
						Subscriptions: []meta.SubscriptionInfo{
							{Name: "s0", Mode: "ALL", Destinations: []string{"udp://h0:9093", "udp://h1:9093"}},
						},
					},
				},
			},
		}, nil
	}

	prs := make(chan *cluster.WritePointsRequest, 2)
	urls := make(chan url.URL, 2)
	newPointsWriter := func(u url.URL) (subscriber.PointsWriter, error) {
		urls <- u
		sub := Subscription{}
		sub.WritePointsFn = func(p *cluster.WritePointsRequest) error {
			prs <- p
			return nil
		}
		return sub, nil
	}

	s := subscriber.NewService()
	s.MetaStore = ms
	s.NewPointsWriter = newPointsWriter

	err := s.Update()

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	for _, expURLStr := range []string{"udp://h0:9093", "udp://h1:9093"} {
		var u url.URL
		expURL, _ := url.Parse(expURLStr)
		select {
		case u = <-urls:
		default:
			t.Fatal("expected urls")
		}
		if expURL.String() != u.String() {
			t.Fatalf("unexpected url: got %s exp %s", u.String(), expURL.String())
		}
	}

	// Write points that match subscription with mode ALL
	expPR := &cluster.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp0",
	}
	s.WritePoints(expPR)

	// Should get pr back twice
	for i := 0; i < 2; i++ {
		var pr *cluster.WritePointsRequest
		select {
		case pr = <-prs:
		default:
			t.Fatal("expected points request")
		}
		if pr != expPR {
			t.Errorf("unexpected points request: got %v, exp %v", pr, expPR)
		}
	}
}

func TestServiceWritePoints_ModeANY(t *testing.T) {
	ms := MetaStore{}
	ms.DatabasesFn = func() ([]meta.DatabaseInfo, error) {
		return []meta.DatabaseInfo{
			{
				Name: "db0",
				RetentionPolicies: []meta.RetentionPolicyInfo{
					{
						Name: "rp0",
						Subscriptions: []meta.SubscriptionInfo{
							{Name: "s0", Mode: "ANY", Destinations: []string{"udp://h0:9093", "udp://h1:9093"}},
						},
					},
				},
			},
		}, nil
	}

	prs := make(chan *cluster.WritePointsRequest, 2)
	urls := make(chan url.URL, 2)
	newPointsWriter := func(u url.URL) (subscriber.PointsWriter, error) {
		urls <- u
		sub := Subscription{}
		sub.WritePointsFn = func(p *cluster.WritePointsRequest) error {
			prs <- p
			return nil
		}
		return sub, nil
	}

	s := subscriber.NewService()
	s.MetaStore = ms
	s.NewPointsWriter = newPointsWriter

	err := s.Update()

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	for _, expURLStr := range []string{"udp://h0:9093", "udp://h1:9093"} {
		var u url.URL
		expURL, _ := url.Parse(expURLStr)
		select {
		case u = <-urls:
		default:
			t.Fatal("expected urls")
		}
		if expURL.String() != u.String() {
			t.Fatalf("unexpected url: got %s exp %s", u.String(), expURL.String())
		}
	}
	// Write points that match subscription with mode ANY
	expPR := &cluster.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp0",
	}
	s.WritePoints(expPR)

	// Validate we get the pr back just once
	var pr *cluster.WritePointsRequest
	select {
	case pr = <-prs:
	default:
		t.Fatal("expected points request")
	}
	if pr != expPR {
		t.Errorf("unexpected points request: got %v, exp %v", pr, expPR)
	}

	// shouldn't get it a second time
	select {
	case pr = <-prs:
		t.Fatalf("unexpected points request %v", pr)
	default:
	}
}

func TestServiceWritePoints_Multiple(t *testing.T) {
	ms := MetaStore{}
	ms.DatabasesFn = func() ([]meta.DatabaseInfo, error) {
		return []meta.DatabaseInfo{
			{
				Name: "db0",
				RetentionPolicies: []meta.RetentionPolicyInfo{
					{
						Name: "rp0",
						Subscriptions: []meta.SubscriptionInfo{
							{Name: "s0", Mode: "ANY", Destinations: []string{"udp://h0:9093", "udp://h1:9093"}},
						},
					},
					{
						Name: "rp1",
						Subscriptions: []meta.SubscriptionInfo{
							{Name: "s1", Mode: "ALL", Destinations: []string{"udp://h2:9093", "udp://h3:9093"}},
						},
					},
				},
			},
		}, nil
	}

	prs := make(chan *cluster.WritePointsRequest, 4)
	urls := make(chan url.URL, 4)
	newPointsWriter := func(u url.URL) (subscriber.PointsWriter, error) {
		urls <- u
		sub := Subscription{}
		sub.WritePointsFn = func(p *cluster.WritePointsRequest) error {
			prs <- p
			return nil
		}
		return sub, nil
	}

	s := subscriber.NewService()
	s.MetaStore = ms
	s.NewPointsWriter = newPointsWriter

	err := s.Update()

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	for _, expURLStr := range []string{"udp://h0:9093", "udp://h1:9093", "udp://h2:9093", "udp://h3:9093"} {
		var u url.URL
		expURL, _ := url.Parse(expURLStr)
		select {
		case u = <-urls:
		default:
			t.Fatal("expected urls")
		}
		if expURL.String() != u.String() {
			t.Fatalf("unexpected url: got %s exp %s", u.String(), expURL.String())
		}
	}

	// Write points that don't match any subscription.
	s.WritePoints(&cluster.WritePointsRequest{
		Database:        "db1",
		RetentionPolicy: "rp0",
	})
	s.WritePoints(&cluster.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp2",
	})

	// Write points that match subscription with mode ANY
	expPR := &cluster.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp0",
	}
	s.WritePoints(expPR)

	// Validate we get the pr back just once
	var pr *cluster.WritePointsRequest
	select {
	case pr = <-prs:
	default:
		t.Fatal("expected points request")
	}
	if pr != expPR {
		t.Errorf("unexpected points request: got %v, exp %v", pr, expPR)
	}

	// shouldn't get it a second time
	select {
	case pr = <-prs:
		t.Fatalf("unexpected points request %v", pr)
	default:
	}

	// Write points that match subscription with mode ALL
	expPR = &cluster.WritePointsRequest{
		Database:        "db0",
		RetentionPolicy: "rp1",
	}
	s.WritePoints(expPR)

	// Should get pr back twice
	for i := 0; i < 2; i++ {
		select {
		case pr = <-prs:
		default:
			t.Fatal("expected points request")
		}
		if pr != expPR {
			t.Errorf("unexpected points request: got %v, exp %v", pr, expPR)
		}
	}
}
