package geoip

import (
	"fmt"
	"net"

	"github.com/IncSW/geoip2"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/processors"
)

const sampleConfig = `
  ## db_path is the location of the MaxMind GeoIP2 City database
  db_path = "/var/lib/GeoIP/GeoLite2-City.mmdb"

  [[processors.geoip.lookup]
	# get the ip from the field "source_ip" and put the lookup results in the respective destination fields (if specified)
	field = "source_ip"
	dest_country = "source_country"
	dest_city = "source_city"
	dest_lat = "source_lat"
	dest_lon = "source_lon"
  `

type lookupEntry struct {
	Field       string `toml:"field"`
	DestCountry string `toml:"dest_country"`
	DestCity    string `toml:"dest_city"`
	DestLat     string `toml:"dest_lat"`
	DestLon     string `toml:"dest_lon"`
}

type GeoIP struct {
	DBPath  string          `toml:"db_path"`
	Lookups []lookupEntry   `toml:"lookup"`
	Log     telegraf.Logger `toml:"-"`
}

var reader *geoip2.CityReader

func (g *GeoIP) SampleConfig() string {
	return sampleConfig
}

func (g *GeoIP) Description() string {
	return "GeoIP looks up the country code, city name and latitude/longitude for IP addresses in the MaxMind GeoIP database"
}

func (g *GeoIP) Apply(metrics ...telegraf.Metric) []telegraf.Metric {
    if len(g.Lookups) == 0 {
        return metrics
    }

    for _, point := range metrics {
        ipValue, ok := point.GetField(g.Lookups[0].Field)
        if !ok {
            continue
        }

        ipAddress := net.ParseIP(ipValue.(string))
        if ipAddress == nil {
            g.Log.Errorf("Invalid IP address: %v", ipValue)
            continue
        }

        for _, lookup := range g.Lookups {
            if lookup.Field == "" {
                continue
            }

            value, ok := point.GetField(lookup.Field)
            if !ok {
                continue
            }

            ipAddress := net.ParseIP(value.(string))
            if ipAddress == nil {
                g.Log.Errorf("Invalid IP address: %v", value)
                continue
            }

            record, err := reader.Lookup(ipAddress)
            if err != nil {
                //g.Log.Errorf("GeoIP lookup error: %v", err)
                continue
            }

            if lookup.DestCountry != "" {
                point.AddField(lookup.DestCountry, record.Country.ISOCode)
            }

            if lookup.DestCity != "" {
                cityName, exists := record.City.Names["en"]
                if exists {
                    point.AddField(lookup.DestCity, cityName)
                }
            }

            if lookup.DestLat != "" {
                point.AddField(lookup.DestLat, record.Location.Latitude)
            }

            if lookup.DestLon != "" {
                point.AddField(lookup.DestLon, record.Location.Longitude)
            }
        }
    }

    return metrics
}

func (g *GeoIP) Init() error {
	r, err := geoip2.NewCityReaderFromFile(g.DBPath)
	if err != nil {
		return fmt.Errorf("Error opening GeoIP database: %v", err)
	} else {
		reader = r
	}
	return nil
}

func init() {
	processors.Add("geoip", func() telegraf.Processor {
		return &GeoIP{
			DBPath: "/usr/local/share/GeoIP/GeoLite2-City.mmdb",
		}
	})
}
