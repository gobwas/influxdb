package client_example

import (
	"log"
	"net/url"
	"os"
	"time"

	"github.com/influxdb/influxdb/client/v2"
)

func ExampleNewClient() client.Client {
	u, _ := url.Parse("http://localhost:8086")

	// NOTE: this assumes you've setup a user and have setup shell env variables,
	// namely INFLUX_USER/INFLUX_PWD. If not just ommit Username/Password below.
	client := client.NewClient(client.Config{
		URL:      u,
		Username: os.Getenv("INFLUX_USER"),
		Password: os.Getenv("INFLUX_PWD"),
	})
	return client
}

func ExampleWrite() {
	// Make client
	u, _ := url.Parse("http://localhost:8086")
	c := client.NewClient(client.Config{
		URL: u,
	})

	// Create a new point batch
	pb := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  "BumbleBeeTuna",
		Precision: "s",
	})

	// Create a point and add to batch
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{
		"idle":   10.1,
		"system": 53.3,
		"user":   46.6,
	}
	pt := client.NewPoint("cpu_usage", tags, fields, time.Now())
	pb.AddPoint(pt)

	// Write the batch
	c.Write(pb)
}

func ExampleQuery() {
	// Make client
	u, _ := url.Parse("http://localhost:8086")
	c := client.NewClient(client.Config{
		URL: u,
	})

	q := client.Query{
		Command:   "SELECT count(value) FROM shapes",
		Database:  "square_holes",
		Precision: "ns",
	}
	if response, err := c.Query(q); err == nil && response.Error() == nil {
		log.Println(response.Results)
	}
}

func ExampleCreateDatabase() {
	// Make client
	u, _ := url.Parse("http://localhost:8086")
	c := client.NewClient(client.Config{
		URL: u,
	})

	q := client.Query{
		Command: "CREATE DATABASE telegraf",
	}
	if response, err := c.Query(q); err == nil && response.Error() == nil {
		log.Println(response.Results)
	}
}
