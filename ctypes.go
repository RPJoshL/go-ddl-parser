package ddl

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
)

// Location is a database type that stores a geographic
// point on the earth with a longitude and latitude.
// It can currently only used for Mariadb!
type Location struct {
	Longitude float64
	Latitude  float64
}

// Scan handles the scanning of the custom location type
// for a MariaDb datbase
func (g *Location) Scan(src interface{}) error {
	switch src.(type) {
	case []byte:
		var b = src.([]byte)
		if len(b) != 25 {
			return fmt.Errorf("expected []bytes with length 25, got %d", len(b))
		}

		var longitude float64
		var latitude float64
		buf := bytes.NewReader(b[9:17])
		err := binary.Read(buf, binary.LittleEndian, &longitude)
		if err != nil {
			return err
		}
		buf = bytes.NewReader(b[17:25])
		err = binary.Read(buf, binary.LittleEndian, &latitude)
		if err != nil {
			return err
		}
		*g = Location{longitude, latitude}
	default:
		return fmt.Errorf("expected []byte for Location type, got  %T", src)
	}

	return nil
}

// Value transforms the latitude and longitude into the
// "WKB" format the database understands.
// See https://dev.mysql.com/doc/refman/8.0/en/gis-data-formats.html
func (g Location) Value() (driver.Value, error) {

	buf := new(bytes.Buffer)
	// Padding
	binary.Write(buf, binary.LittleEndian, []byte{0, 0, 0, 0})
	// Point
	binary.Write(buf, binary.LittleEndian, []byte{1, 1, 0, 0, 0})
	// Data
	binary.Write(buf, binary.LittleEndian, g.Longitude)
	binary.Write(buf, binary.LittleEndian, g.Latitude)

	return buf.Bytes(), nil
}
