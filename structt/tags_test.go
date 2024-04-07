package structt

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestColumnTagTransformNegative(t *testing.T) {
	// Base struct to test
	baseTag := &ColumnTag{
		Name: "Thats_my_Special_22tag",
	}

	// Transform to string
	strTag := baseTag.ToTag()

	// Transform from string
	receivedTag := FromColumnTag(strTag)

	// Compare structs
	if diff := cmp.Diff(baseTag, receivedTag); diff != "" {
		t.Errorf("TestColumnTagTransformNegative() mismatch (-want +got):\n%s", diff)
	}
}

func TestColumnTagTransformPositive(t *testing.T) {
	// Base struct to test
	baseTag := &ColumnTag{
		Name:                "Thats_my_Special_22tag",
		IsPrimaryKey:        true,
		ForeignKeyReference: "workout.users.id",
		PointedKeyReference: "hello",
		AutoIncrement:       true,
	}

	// Transform to string
	strTag := baseTag.ToTag()

	// Transform from string
	receivedTag := FromColumnTag(strTag)

	// Compare structs
	if diff := cmp.Diff(baseTag, receivedTag); diff != "" {
		t.Errorf("TestColumnTagTransformPositive() mismatch (-want +got):\n%s", diff)
	}
}

func TestMetadataTagTransform(t *testing.T) {
	// Base struct to test
	baseTag := &MetadataTag{
		Schema: "workout",
		Table:  "user",
	}

	// Transform to string
	strTag := baseTag.ToTag()

	// Transform from string
	receivedTag := FromMetadataTag(strTag)

	// Compare structs
	if diff := cmp.Diff(baseTag, receivedTag); diff != "" {
		t.Errorf("TestColumnTagTransformNegative() mismatch (-want +got):\n%s", diff)
	}
}
