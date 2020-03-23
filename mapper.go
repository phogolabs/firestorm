package firestorm

import (
	"fmt"
	"reflect"
	"sync"

	"cloud.google.com/go/datastore"
	"github.com/fatih/structtag"
	"github.com/mitchellh/hashstructure"
)

var mapper = &IndexMapper{
	mu:    &sync.Mutex{},
	cache: make(map[reflect.Type]*IndexTree),
}

// IndexMapper represents the indexer
type IndexMapper struct {
	mu    *sync.Mutex
	cache map[reflect.Type]*IndexTree
}

// Tree returns the index tree
func (m *IndexMapper) Tree(t reflect.Type) *IndexTree {
	t = t.Elem()
	tree, ok := m.cache[t]

	if !ok {
		tree = m.build(t)
		m.cache[t] = tree
	}

	return tree
}

func (m *IndexMapper) build(t reflect.Type) *IndexTree {
	kv := make(map[string]*Index)

	for i := 0; i < t.NumField(); i++ {
		tags, err := structtag.Parse(string(t.Field(i).Tag))
		if err != nil {
			continue
		}

		index, err := tags.Get("index")
		if err != nil {
			continue
		}

		metadata, ok := kv[index.Name]

		if !ok {
			metadata = &Index{
				Name:   index.Name,
				Unique: true,
			}
		}

		metadata.Properties = append(metadata.Properties, i)
		kv[index.Name] = metadata
	}

	tree := IndexTree{}

	for _, index := range kv {
		tree = append(tree, index)
	}

	return &tree
}

// IndexTree represents the index
type IndexTree []*Index

// Keys returns the keys
func (t *IndexTree) Keys(key *datastore.Key, input reflect.Value) ([]*IndexKey, error) {
	keys := []*IndexKey{}

	for _, index := range *t {
		hash, err := index.Hash(input)
		if err != nil {
			return nil, err
		}

		keys = append(keys, t.key(key, index.Name, hash))
	}

	return keys, nil
}

func (t *IndexTree) key(key *datastore.Key, name string, hash uint64) *IndexKey {
	return &IndexKey{
		Key: &datastore.Key{
			Name:      fmt.Sprintf("%v", hash),
			Kind:      "position",
			Namespace: key.Namespace,
			Parent: &datastore.Key{
				Name:      name,
				Kind:      "index",
				Namespace: key.Namespace,
				Parent: &datastore.Key{
					Name:      "constraint",
					Kind:      "metadata",
					Namespace: key.Namespace,
					Parent: &datastore.Key{
						Name:      key.Kind,
						Kind:      "kind",
						Namespace: key.Namespace,
					},
				},
			},
		},
		Hash: hash,
	}
}

// Index represents the index
type Index struct {
	Name string
	// should be string
	Properties []int
	// Unique indexes
	Unique bool
}

// Hash calculates the index value
func (index *Index) Hash(v reflect.Value) (uint64, error) {
	var fingerprint []interface{}

	v = reflect.Indirect(v)

	for _, position := range index.Properties {
		value := v.Field(position).Interface()
		fingerprint = append(fingerprint, value)
	}

	return hashstructure.Hash(fingerprint, nil)
}

// IndexKey represents an index key
type IndexKey struct {
	Key  *datastore.Key `datastore:"__key__"`
	Hash uint64         `datastore:"-"`
}
