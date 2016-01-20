package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"

	"github.com/gliderlabs/logspout/router"
    "github.com/mittanareddy/go-rancher-metadata/metadata"
)

const (
	metadataUrl = "http://rancher-metadata/2015-07-25"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn  net.Conn
	route *router.Route
}

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route: route,
		conn:  conn,
	}, nil
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {
	racherMetaData := metadata.NewClient(metadataUrl)
	stackname := ""
	for m := range logstream {
	    stackNameTmp, err := racherMetaData.GetStackNameByContainerName(m.Container.Name)
		if err != nil {
			log.Println("Error reading metadata version: ", err)
        }else {
			stackname = stackNameTmp
		}		
		msg := LogstashMessage{
			Message: m.Data,
			Docker: DockerInfo{
				Name:     m.Container.Name,
				ID:       m.Container.ID,
				Image:    m.Container.Config.Image,
				Hostname: m.Container.Config.Hostname,
			},
			Rancher: RancherInfo{
				StackName: stackname,
			},
		}
		js, err := json.Marshal(msg)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
		_, err = a.conn.Write(js)
		if err != nil {
			log.Println("logstash:", err)
			continue
		}
	}
}

type DockerInfo struct {
	Name     string `json:"name"`
	ID       string `json:"id"`
	Image    string `json:"image"`
	Hostname string `json:"hostname"`
}

type RancherInfo struct {
    StackName string `json:"stackname"`
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message string     `json:"message"`
	Docker  DockerInfo `json:"docker"`
	Rancher RancherInfo `json:"rancher"`
}
