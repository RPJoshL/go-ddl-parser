package ddl

import (
	"crypto/rand"
	"math/big"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func RequireEnvString(name string, t *testing.T) string {
	if strVal, isSet := os.LookupEnv(name); isSet {
		return strVal
	} else {
		t.Fatalf("Required environment variable %q not set", name)
		return ""
	}
}

func RequireEnvInt(name string, t *testing.T) int {
	if strVal, isSet := os.LookupEnv(name); isSet {
		if intVal, err := strconv.Atoi(strVal); err != nil {
			t.Fatalf("Invalid number value given for the environment variable %q: %s", name, strVal)
		} else if intVal < 1 {
			t.Fatalf("Environment variable %q has to be greater than 0", name)
		} else {
			return intVal
		}
	} else {
		t.Fatalf("Required environment variable %q not set", name)
	}

	return 0
}

func DumpStruct(a ...interface{}) string {
	dump := spew.Sdump(a...)

	if strings.HasSuffix(dump, "\n") && len(dump) > 2 {
		dump = dump[0 : len(dump)-len("\n")]
	}

	return dump
}

// GenerateRandomString returns a securely generated random string.
// It will return an error if the system's secure random
// number generator fails to function correctly, in which
// case the caller should not continue.
func GenerateRandomString(n int) (string, error) {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	ret := make([]byte, n)
	for i := 0; i < n; i++ {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return "", err
		}
		ret[i] = letters[num.Int64()]
	}

	return string(ret), nil
}
