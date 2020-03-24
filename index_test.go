package firestorm_test

import (
	"context"
	"fmt"

	"cloud.google.com/go/datastore"
	"github.com/phogolabs/firestorm"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("IndexFunc", func() {
	It("executes the function", func() {
		var (
			count = 0
			tx    = &datastore.Transaction{}
		)
		fn := func(current *datastore.Transaction) error {
			Expect(current).To(Equal(tx))
			count++
			return fmt.Errorf("oh no")
		}

		indexer := firestorm.IndexerFunc(fn)
		Expect(indexer.Index(tx)).To(MatchError("oh no"))
		Expect(count).To(Equal(1))
	})
})

var _ = Describe("NewInsertIndexer", func() {
	var (
		ctx    context.Context
		entity *Entity
		client *datastore.Client
	)

	BeforeEach(func() {
		ctx = context.TODO()

		entity = &Entity{
			ID:        datastore.NameKey("entity", "007", nil),
			FirstName: "John",
			LastName:  "Doe",
			Email:     "john@example.com",
		}

		var err error
		client, err = datastore.NewClient(ctx, "foo-bar")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Delete(ctx, &datastore.Key{
			Name: "14491862341308332741",
			Kind: "entity_email_index",
		})).To(Succeed())

		Expect(client.Close()).To(Succeed())
	})

	It("inserts the new index successfully", func() {
		_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			indexer := firestorm.NewInsertIndexer(entity.ID, entity)
			return indexer.Index(tx)
		})

		Expect(err).NotTo(HaveOccurred())
	})

	Context("when the index already exists", func() {
		BeforeEach(func() {
			_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
				indexer := firestorm.NewInsertIndexer(entity.ID, entity)
				return indexer.Index(tx)
			})

			Expect(err).NotTo(HaveOccurred())
		})

		It("returns an error", func() {
			entity.ID = datastore.NameKey("entity", "080", nil)
			entity.FirstName = "Mike"
			entity.LastName = "Oha"
			entity.Email = "john@example.com"

			_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
				indexer := firestorm.NewInsertIndexer(entity.ID, entity)
				return indexer.Index(tx)
			})

			Expect(err).To(MatchError("rpc error: code = AlreadyExists desc = entity already exists"))
		})
	})
})

var _ = Describe("NewUpdateIndexer", func() {
	var (
		ctx    context.Context
		entity *Entity
		client *datastore.Client
	)

	BeforeEach(func() {
		ctx = context.TODO()

		entity = &Entity{
			ID:        datastore.NameKey("entity", "007", nil),
			FirstName: "John",
			LastName:  "Doe",
			Email:     "john@example.com",
		}

		var err error

		client, err = datastore.NewClient(ctx, "foo-bar")
		Expect(err).NotTo(HaveOccurred())

		_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			if _, err := tx.Put(entity.ID, entity); err != nil {
				return err
			}

			indexer := firestorm.NewInsertIndexer(entity.ID, entity)
			return indexer.Index(tx)
		})

		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Delete(ctx, &datastore.Key{
			Name: "14491862341308332741",
			Kind: "entity_email_index",
		})).To(Succeed())

		Expect(client.Delete(ctx, &datastore.Key{
			Name: "3468090598242639543",
			Kind: "entity_email_index",
		})).To(Succeed())

		Expect(client.Delete(ctx, entity.ID)).To(Succeed())
		Expect(client.Close()).To(Succeed())
	})

	It("updates the index successfully", func() {
		entity.Email = "level@example.com"

		_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			indexer := firestorm.NewUpdateIndexer(entity.ID, entity)
			return indexer.Index(tx)
		})

		Expect(err).NotTo(HaveOccurred())
	})

	Context("when the indexed value is not changed", func() {
		It("updates the index successfully", func() {
			_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
				indexer := firestorm.NewUpdateIndexer(entity.ID, entity)
				return indexer.Index(tx)
			})

			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("when the entity is missing", func() {
		var next *Entity

		BeforeEach(func() {
			next = &Entity{
				ID:        datastore.NameKey("entity", "099", nil),
				FirstName: "Johnatan",
				LastName:  "Peteresen",
				Email:     "j.p@example.com",
			}
		})

		It("returns an error", func() {
			_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
				indexer := firestorm.NewUpdateIndexer(next.ID, next)
				return indexer.Index(tx)
			})

			Expect(err).To(MatchError("datastore: no such entity"))
		})
	})

	Context("when the index already exists", func() {
		var next *Entity

		BeforeEach(func() {
			next = &Entity{
				ID:        datastore.NameKey("entity", "089", nil),
				FirstName: "Dorian",
				LastName:  "Oha",
				Email:     "dorian@example.com",
			}

			_, err := client.Put(ctx, next.ID, next)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			Expect(client.Delete(ctx, next.ID)).To(Succeed())
		})

		It("returns an error", func() {
			next.Email = entity.Email

			_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
				indexer := firestorm.NewUpdateIndexer(next.ID, next)
				return indexer.Index(tx)
			})

			Expect(err).To(MatchError("rpc error: code = AlreadyExists desc = entity already exists"))
		})
	})
})

// var _ = Describe("NewUpsertndexer", func() {
// 	var (
// 		ctx    context.Context
// 		entity *Entity
// 		client *datastore.Client
// 	)

// 	BeforeEach(func() {
// 		ctx = context.TODO()

// 		entity = &Entity{
// 			ID:        datastore.NameKey("entity", "007", nil),
// 			FirstName: "John",
// 			LastName:  "Doe",
// 			Email:     "john@example.com",
// 		}

// 		var err error

// 		client, err = datastore.NewClient(ctx, "foo-bar")
// 		Expect(err).NotTo(HaveOccurred())
// 	})

// 	AfterEach(func() {
// 		Expect(client.Delete(ctx, &datastore.Key{
// 			Name: "14491862341308332741",
// 			Kind: "entity_email_index",
// 		})).To(Succeed())

// 		Expect(client.Delete(ctx, entity.ID)).To(Succeed())
// 		Expect(client.Close()).To(Succeed())
// 	})

// })

var _ = Describe("NewDeleteIndexer", func() {
	var (
		ctx    context.Context
		entity *Entity
		client *datastore.Client
	)

	BeforeEach(func() {
		ctx = context.TODO()

		entity = &Entity{
			ID:        datastore.NameKey("entity", "007", nil),
			FirstName: "John",
			LastName:  "Doe",
			Email:     "john@example.com",
		}

		var err error

		client, err = datastore.NewClient(ctx, "foo-bar")
		Expect(err).NotTo(HaveOccurred())

		_, err = client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			if _, err := tx.Put(entity.ID, entity); err != nil {
				return err
			}

			indexer := firestorm.NewInsertIndexer(entity.ID, entity)
			return indexer.Index(tx)
		})

		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Delete(ctx, &datastore.Key{
			Name: "14491862341308332741",
			Kind: "entity_email_index",
		})).To(Succeed())

		Expect(client.Delete(ctx, entity.ID)).To(Succeed())
		Expect(client.Close()).To(Succeed())
	})

	It("deletes the index successfully", func() {
		_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			indexer := firestorm.NewDeleteIndexer(entity.ID, entity)
			return indexer.Index(tx)
		})

		Expect(err).ToNot(HaveOccurred())

		key := &datastore.Key{
			Name: "14491862341308332741",
			Kind: "entity_email_index",
		}

		keyIndex := &firestorm.IndexKey{}
		err = client.Get(ctx, key, keyIndex)
		Expect(err).To(MatchError("datastore: no such entity"))
	})

	Context("when the entity does not exist", func() {
		It("deletes the index successfully", func() {
			entity.ID.Name = "qwerty"

			_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
				indexer := firestorm.NewDeleteIndexer(entity.ID, entity)
				return indexer.Index(tx)
			})

			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("when the key is not valid", func() {
		It("returns an error", func() {
			_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
				indexer := firestorm.NewDeleteIndexer(nil, entity)
				return indexer.Index(tx)
			})

			Expect(err).To(MatchError("datastore: invalid key"))
		})
	})
})
