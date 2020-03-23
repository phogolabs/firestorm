package firestorm

import (
	"sort"

	"cloud.google.com/go/datastore"
)

// Partial represents a partial entity
type Partial struct {
	Properties []string
	Entity     datastore.KeyLoader
}

// LoadKey loads the key
func (p *Partial) LoadKey(key *datastore.Key) error {
	return p.Entity.LoadKey(key)
}

// Load loads all properties that are not matching the partial modified
func (p *Partial) Load(props []datastore.Property) error {
	sort.Strings(p.Properties)

	var (
		count      = len(p.Properties)
		properties = []datastore.Property{}
	)

	for _, property := range props {
		index := sort.Search(count, func(i int) bool {
			return p.Properties[i] == property.Name
		})

		if index < count {
			continue
		}

		properties = append(properties, property)
	}

	return p.Entity.Load(properties)
}

// Save saves the partial
func (p *Partial) Save() ([]datastore.Property, error) {
	return p.Entity.Save()
}
