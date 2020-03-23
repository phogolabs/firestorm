package firestorm

import (
	"reflect"

	"cloud.google.com/go/datastore"
)

// Indexer represents an entity indexer
type Indexer interface {
	Index(tx *datastore.Transaction) error
}

// IndexerFunc represents a checker func
type IndexerFunc func(tx *datastore.Transaction) error

// Index indexes the record
func (fn IndexerFunc) Index(tx *datastore.Transaction) error {
	return fn(tx)
}

// NewInsertIndexer represents an insert indexer
func NewInsertIndexer(key *datastore.Key, input interface{}) Indexer {
	var (
		tree   = mapper.Tree(reflect.TypeOf(input))
		entity = reflect.ValueOf(input)
	)

	fn := func(tx *datastore.Transaction) error {
		treeNext, err := tree.Keys(key, entity)
		if err != nil || len(treeNext) == 0 {
			return err
		}

		ops := []*datastore.Mutation{}

		for _, next := range treeNext {
			ops = append(ops, datastore.NewInsert(next.Key, next))
		}

		_, err = tx.Mutate(ops...)
		return err
	}

	return IndexerFunc(fn)
}

// NewUpdateIndexer represents an upsert check
func NewUpdateIndexer(key *datastore.Key, input interface{}) Indexer {
	var (
		kind   = reflect.TypeOf(input)
		tree   = mapper.Tree(kind)
		empty  = reflect.New(kind.Elem())
		entity = reflect.ValueOf(input)
	)

	fn := func(tx *datastore.Transaction) error {
		treeNext, err := tree.Keys(key, entity)
		if err != nil || len(treeNext) == 0 {
			return err
		}

		err = tx.Get(key, empty.Interface())

		switch {
		case err == datastore.ErrNoSuchEntity:
			return nil
		case err != nil:
			return err
		}

		treePrev, err := tree.Keys(key, empty)
		if err != nil {
			return err
		}

		ops := []*datastore.Mutation{}

		for index, next := range treeNext {
			prev := treePrev[index]

			if prev.Hash == next.Hash {
				continue
			}

			ops = append(ops, datastore.NewDelete(prev.Key))
			ops = append(ops, datastore.NewInsert(next.Key, next))
		}

		_, err = tx.Mutate(ops...)
		return err
	}

	return IndexerFunc(fn)
}

// NewDeleteIndexer represents an upsert check
func NewDeleteIndexer(key *datastore.Key, input interface{}) Indexer {
	var (
		kind   = reflect.TypeOf(input)
		tree   = mapper.Tree(kind)
		entity = reflect.ValueOf(input)
	)

	fn := func(tx *datastore.Transaction) error {
		err := tx.Get(key, input)

		switch {
		case err == datastore.ErrNoSuchEntity:
			return nil
		case err != nil:
			return err
		}

		treePrev, err := tree.Keys(key, entity)
		if err != nil || len(treePrev) == 0 {
			return err
		}

		ops := []*datastore.Mutation{}

		for _, prev := range treePrev {
			ops = append(ops, datastore.NewDelete(prev.Key))
		}

		_, err = tx.Mutate(ops...)
		return err
	}

	return IndexerFunc(fn)
}
