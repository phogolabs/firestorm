package firestorm_test

import (
	"testing"

	"cloud.google.com/go/datastore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestFirestorm(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Firestorm Suite")
}

type Entity struct {
	ID        *datastore.Key `datastore:"__key__"`
	FirstName string         `datastore:"first_name"`
	LastName  string         `datastore:"last_name"`
	Email     string         `datastore:"email" index:"email,unique"`
}

func (p *Entity) LoadKey(key *datastore.Key) error {
	p.ID = key
	return nil
}

func (p *Entity) Load(props []datastore.Property) error {
	return datastore.LoadStruct(p, props)
}

func (p *Entity) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(p)
}
