package firestorm

import (
	"fmt"
	"reflect"
	"sync"

	"cloud.google.com/go/datastore"
	"github.com/fatih/structtag"
	"github.com/mitchellh/hashstructure"
)

// IndexMapper represents the indexer
type IndexMapper struct {
	Mutex *sync.Mutex
	Cache map[reflect.Type]*IndexTree
}

// Tree returns the index tree
func (m *IndexMapper) Tree(t reflect.Type) *IndexTree {
	m.Mutex.Lock()

	switch t.Kind() {
	case reflect.Ptr:
		t = t.Elem()
	case reflect.Struct:
	default:
		return nil
	}

	tree, ok := m.Cache[t]

	if !ok {
		tree = m.build(t)
		m.Cache[t] = tree
	}

	m.Mutex.Unlock()
	return tree
}

func (m *IndexMapper) build(t reflect.Type) *IndexTree {
	maptree := make(map[string]*Index)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		tags, err := structtag.Parse(string(field.Tag))
		if err != nil {
			continue
		}

		tag, err := tags.Get("index")
		if err != nil {
			continue
		}

		index, ok := maptree[tag.Name]

		if !ok {
			index = &Index{
				Name: tag.Name,
			}
		}

		index.Properties = append(index.Properties, field.Index)

		maptree[index.Name] = index
	}

	tree := IndexTree{}

	for _, index := range maptree {
		tree = append(tree, index)
	}

	return &tree
}

// IndexTree represents the index
type IndexTree []*Index

// Keys returns the keys
func (t *IndexTree) Keys(key *datastore.Key, input reflect.Value) ([]*IndexKey, error) {
	if key == nil {
		return nil, datastore.ErrInvalidKey
	}

	keys := []*IndexKey{}

	for _, index := range *t {
		hash, err := index.Hash(input)
		if err != nil {
			return nil, err
		}

		indexKey := &IndexKey{
			Key: &datastore.Key{
				Name:      fmt.Sprintf("%v", hash),
				Kind:      fmt.Sprintf("%s_%s_index", key.Kind, index.Name),
				Namespace: key.Namespace,
			},
			Hash: hash,
		}

		keys = append(keys, indexKey)
	}

	return keys, nil
}

// Index represents the index
type Index struct {
	Name string
	// should be string
	Properties [][]int
}

// Hash calculates the index value
func (index *Index) Hash(v reflect.Value) (uint64, error) {
	var fingerprint []interface{}

	v = reflect.Indirect(v)

	for _, position := range index.Properties {
		value := v.FieldByIndex(position).Interface()
		fingerprint = append(fingerprint, value)
	}

	return hashstructure.Hash(fingerprint, nil)
}

// IndexKey represents an index key
type IndexKey struct {
	Key  *datastore.Key `datastore:"__key__"`
	Hash uint64         `datastore:"-"`
}
