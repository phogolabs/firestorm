package aspect

import "cloud.google.com/go/datastore"

// Entity represents the entity
type Entity interface {
	Key() *datastore.Key
	Namespace() string
	Kind() string
}

// Prepare entity for datastore operation
func Prepare(entity Entity) {
	key := entity.Key()

	if key != nil {
		key.Kind = entity.Kind()
	}

	for key != nil {
		key.Namespace = entity.Namespace()
		key = key.Parent
	}
}

// String creates a pointer to a string
func String(value string) *string {
	return &value
}

// StringValue returns the string's value from pointer
func StringValue(value *string) string {
	if value != nil {
		return *value
	}

	return ""
}
