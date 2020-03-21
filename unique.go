package aspect

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

// PrimaryKey is the primary key name
const PrimaryKey = "__key__"

// UniqueConstraint represents a unique constraint in datastore
type UniqueConstraint struct {
	Client *datastore.Client
}

// Check the unique contrants
func (u *UniqueConstraint) Check(tx *datastore.Transaction, input Entity) error {
	key := input.Key()

	if key == nil {
		return fmt.Errorf("entity of kind '%v' has key that cannot be nil", input.Kind())
	}

	root := datastore.
		NewQuery(input.Kind()).
		Namespace(input.Namespace()).
		Transaction(tx).
		KeysOnly()

	properties, err := u.properties(input)
	if err != nil {
		return err
	}

	if key.Parent != nil {
		root = root.Ancestor(key.Parent)
	}

	for _, prop := range properties {
		if prop.Name == PrimaryKey {
			continue
		}

		if !u.unique(root, key, prop) {
			return u.violate(prop.Name, input.Kind())
		}
	}

	return nil
}

func (u *UniqueConstraint) properties(input interface{}) ([]datastore.Property, error) {
	var (
		properties = []datastore.Property{}
		objType    = cache.TypeMap(reflect.TypeOf(input))
		objValue   = reflect.Indirect(reflect.ValueOf(input))
	)

	for _, field := range objType.Names {
		if _, ok := field.Options["unique"]; ok {
			prop := datastore.Property{
				Name:  field.Name,
				Value: objValue.FieldByIndex(field.Index).Interface(),
			}

			properties = append(properties, prop)
		}
	}

	return properties, nil
}

func (u *UniqueConstraint) unique(query *datastore.Query, key *datastore.Key, prop datastore.Property) bool {
	query = query.Filter(fmt.Sprintf("%v =", prop.Name), prop.Value)
	iter := u.Client.Run(context.TODO(), query)

	for {
		pk, err := iter.Next(nil)

		if err == iterator.Done {
			break
		}

		if key.Incomplete() {
			return false
		}

		if !key.Equal(pk) {
			return false
		}
	}

	return true
}

func (u *UniqueConstraint) violate(prop, kind string) error {
	return fmt.Errorf("violation of unique key constraint '%v'. cannot insert duplicate key in kind '%s'", prop, kind)
}
