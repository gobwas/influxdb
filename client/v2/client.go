package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/influxdb/influxdb/models"
)

type Config struct {
	URL       *url.URL
	Username  string
	Password  string
	UserAgent string
	Timeout   time.Duration
}

type BatchPointsConfig struct {
	Precision        string
	Database         string
	RetentionPolicy  string
	WriteConsistency string
}

type Client interface {
	Write(pb BatchPoints) error

	Query(q Query) (*Response, error)
}

func NewClient(conf Config) Client {
	if conf.UserAgent == "" {
		conf.UserAgent = "InfluxDBClient"
	}
	return &client{
		url:        conf.URL,
		username:   conf.Username,
		password:   conf.Password,
		useragent:  conf.UserAgent,
		httpClient: &http.Client{Timeout: conf.Timeout},
	}
}

type client struct {
	url        *url.URL
	username   string
	password   string
	useragent  string
	httpClient *http.Client
}

type BatchPoints interface {
	AddPoint(p *Point)
	Points() []*Point

	Precision() string
	SetPrecision(s string)

	Database() string
	SetDatabase(s string)

	WriteConsistency() string
	SetWriteConsistency(s string)

	RetentionPolicy() string
	SetRetentionPolicy(s string)
}

func NewBatchPoints(c BatchPointsConfig) BatchPoints {
	return &batchpoints{
		database:         c.Database,
		precision:        c.Precision,
		retentionPolicy:  c.RetentionPolicy,
		writeConsistency: c.WriteConsistency,
	}
}

type batchpoints struct {
	sync.Mutex

	points           []*Point
	database         string
	precision        string
	retentionPolicy  string
	writeConsistency string
}

func (pb *batchpoints) AddPoint(p *Point) {
	pb.Lock()
	defer pb.Unlock()
	pb.points = append(pb.points, p)
}

func (pb *batchpoints) Points() []*Point {
	pb.Lock()
	defer pb.Unlock()
	return pb.points
}

func (pb *batchpoints) Precision() string {
	return pb.precision
}

func (pb *batchpoints) Database() string {
	return pb.database
}

func (pb *batchpoints) WriteConsistency() string {
	return pb.writeConsistency
}

func (pb *batchpoints) RetentionPolicy() string {
	return pb.retentionPolicy
}

func (pb *batchpoints) SetPrecision(p string) {
	pb.precision = p
}

func (pb *batchpoints) SetDatabase(db string) {
	pb.database = db
}

func (pb *batchpoints) SetWriteConsistency(wc string) {
	pb.writeConsistency = wc
}

func (pb *batchpoints) SetRetentionPolicy(rp string) {
	pb.retentionPolicy = rp
}

type Point struct {
	pt models.Point
}

func NewPoint(
	name string,
	tags map[string]string,
	fields map[string]interface{},
	t time.Time,
) *Point {
	return &Point{
		pt: models.NewPoint(name, tags, fields, t),
	}
}

func NewPointWithoutTime(
	name string,
	tags map[string]string,
	fields map[string]interface{},
) *Point {
	t := time.Now()
	return &Point{
		pt: models.NewPoint(name, tags, fields, t),
	}
}

func (p *Point) String() string {
	return p.pt.String()
}

func (p *Point) PrecisionString(precison string) string {
	return p.pt.PrecisionString(precison)
}

func (c *client) Write(bp BatchPoints) error {
	u := c.url
	u.Path = "write"

	var b bytes.Buffer
	for _, p := range bp.Points() {
		if _, err := b.WriteString(p.pt.PrecisionString(bp.Precision())); err != nil {
			return err
		}

		if err := b.WriteByte('\n'); err != nil {
			return err
		}
	}

	req, err := http.NewRequest("POST", u.String(), &b)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", c.useragent)
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	params := req.URL.Query()
	params.Set("db", bp.Database())
	params.Set("rp", bp.RetentionPolicy())
	params.Set("precision", bp.Precision())
	params.Set("consistency", bp.WriteConsistency())
	req.URL.RawQuery = params.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		var err = fmt.Errorf(string(body))
		return err
	}

	return nil
}

// Query defines a query to send to the server
type Query struct {
	Command   string
	Database  string
	Precision string
}

// Response represents a list of statement results.
type Response struct {
	Results []Result
	Err     error
}

// Error returns the first error from any statement.
// Returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != nil {
		return r.Err
	}
	for _, result := range r.Results {
		if result.Err != nil {
			return result.Err
		}
	}
	return nil
}

// Result represents a resultset returned from a single statement.
type Result struct {
	Series []models.Row
	Err    error
}

// Query sends a command to the server and returns the Response
func (c *client) Query(q Query) (*Response, error) {
	u := c.url

	u.Path = "query"
	values := u.Query()
	values.Set("q", q.Command)
	values.Set("db", q.Database)
	if q.Precision != "" {
		values.Set("epoch", q.Precision)
	}
	u.RawQuery = values.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.useragent)
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var response Response
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	decErr := dec.Decode(&response)

	// ignore this error if we got an invalid status code
	if decErr != nil && decErr.Error() == "EOF" && resp.StatusCode != http.StatusOK {
		decErr = nil
	}
	// If we got a valid decode error, send that back
	if decErr != nil {
		return nil, decErr
	}
	// If we don't have an error in our json response, and didn't get  statusOK, then send back an error
	if resp.StatusCode != http.StatusOK && response.Error() == nil {
		return &response, fmt.Errorf("received status code %d from server", resp.StatusCode)
	}
	return &response, nil
}
