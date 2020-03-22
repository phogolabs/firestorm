package firestorm

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/datastore"
	"github.com/jmoiron/sqlx/reflectx"
	"google.golang.org/api/iterator"
)

var cache = reflectx.NewMapper("firestorm")

// UniqueConstraint represents a unique constraint in datastore
type UniqueConstraint struct {
	Client *datastore.Client
}

// CanInsert checks the entity whether it can be inserted
func (u *UniqueConstraint) CanInsert(tx *datastore.Transaction, input Entity) error {
	key := input.Key()

	if key != nil && !key.Incomplete() {
		var (
			empty = &PropertyList{}
			err   = tx.Get(key, empty)
		)

		switch {
		case err == datastore.ErrNoSuchEntity:
		case err == nil:
			return ErrorViolateKey("id", input.Kind())
		default:
			return err
		}
	}

	return u.Check(tx, input)
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
		if prop.Name == "__key__" {
			continue
		}

		if !u.unique(root, key, prop) {
			return ErrorViolateUnique(prop.Name, input.Kind())
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
		if _, ok := field.Options["unique"]; !ok {
			continue
		}

		prop := datastore.Property{
			Name:  field.Name,
			Value: objValue.FieldByIndex(field.Index).Interface(),
		}

		properties = append(properties, prop)
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
