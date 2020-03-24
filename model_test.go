package firestorm_test

import (
	"cloud.google.com/go/datastore"
	"github.com/phogolabs/firestorm"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Partial", func() {
	var (
		entity  *Entity
		partial *firestorm.Partial
	)

	BeforeEach(func() {
		entity = &Entity{
			FirstName: "John",
			LastName:  "Doe",
			Email:     "john@example.com",
		}

		partial = &firestorm.Partial{
			Entity: entity,
		}
	})

	Describe("LoadKey", func() {
		It("loads the entity key", func() {
			key := datastore.NameKey("entity", "007", nil)
			Expect(partial.LoadKey(key)).To(Succeed())
			Expect(entity.ID).To(Equal(key))
		})
	})

	Describe("Load", func() {
		It("loads the entity", func() {
			props := []datastore.Property{
				{Name: "first_name", Value: "Mike"},
				{Name: "last_name", Value: "Freeman"},
			}

			Expect(partial.Load(props)).To(Succeed())
			Expect(entity.FirstName).To(Equal("Mike"))
			Expect(entity.LastName).To(Equal("Freeman"))
		})

		Context("when the partial properties are set", func() {
			BeforeEach(func() {
				partial.Properties = []string{"first_name"}
			})

			It("does not load the property that are not mached", func() {
				props := []datastore.Property{
					{Name: "first_name", Value: "Mike"},
					{Name: "last_name", Value: "Freeman"},
				}

				Expect(partial.Load(props)).To(Succeed())
				Expect(entity.FirstName).To(Equal("John"))
				Expect(entity.LastName).To(Equal("Freeman"))
			})
		})
	})

	Describe("Save", func() {
		It("saves the entity", func() {
			entityProps, err := entity.Save()
			Expect(err).NotTo(HaveOccurred())

			partialProps, err := partial.Save()
			Expect(err).NotTo(HaveOccurred())

			Expect(partialProps).To(Equal(entityProps))
		})
	})
})
