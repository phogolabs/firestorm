package firestorm_test

import (
	"reflect"
	"sync"

	"cloud.google.com/go/datastore"
	"github.com/phogolabs/firestorm"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("IndexMapper", func() {
	var mapper *firestorm.IndexMapper

	BeforeEach(func() {
		mapper = &firestorm.IndexMapper{
			Mutex: &sync.Mutex{},
			Cache: make(map[reflect.Type]*firestorm.IndexTree),
		}
	})

	Describe("Tree", func() {
		It("returns the index tree for given type", func() {
			maptree := mapper.Tree(reflect.TypeOf(Entity{}))
			Expect(maptree).NotTo(BeNil())

			list := *maptree
			Expect(list).To(HaveLen(1))
			Expect(list[0].Name).To(Equal("email"))
			Expect(list[0].Properties).To(Equal([][]int{[]int{3}}))
		})

		Context("when the type is pointer to struct", func() {
			It("returns the index tree for given type", func() {
				maptree := mapper.Tree(reflect.TypeOf(&Entity{}))
				Expect(maptree).NotTo(BeNil())

				list := *maptree
				Expect(list).To(HaveLen(1))
				Expect(list[0].Name).To(Equal("email"))
				Expect(list[0].Properties).To(Equal([][]int{[]int{3}}))
			})
		})

		Context("when the tree is cached", func() {
			BeforeEach(func() {
				mapper.Cache[reflect.TypeOf(Entity{})] = &firestorm.IndexTree{
					{Name: "random"},
				}
			})

			It("returns the index tree for given type", func() {
				maptree := mapper.Tree(reflect.TypeOf(&Entity{}))
				Expect(maptree).NotTo(BeNil())

				list := *maptree
				Expect(list).To(HaveLen(1))
				Expect(list[0].Name).To(Equal("random"))
			})
		})

		Context("when the type is not struct", func() {
			It("returns an empty tree", func() {
				maptree := mapper.Tree(reflect.TypeOf(0))
				Expect(maptree).To(BeNil())
			})
		})
	})
})

var _ = Describe("IndexTree", func() {
	var maptree *firestorm.IndexTree

	BeforeEach(func() {
		mapper := &firestorm.IndexMapper{
			Mutex: &sync.Mutex{},
			Cache: make(map[reflect.Type]*firestorm.IndexTree),
		}

		maptree = mapper.Tree(reflect.TypeOf(&Entity{}))
	})

	Describe("Keys", func() {
		It("returns the keys", func() {
			entity := &Entity{
				ID:        datastore.NameKey("entity", "007", nil),
				FirstName: "John",
				LastName:  "Doe",
				Email:     "john@example.com",
			}

			keys, err := maptree.Keys(entity.ID, reflect.ValueOf(entity))
			Expect(err).To(BeNil())
			Expect(keys).To(HaveLen(1))
			Expect(keys[0].Key.Name).To(Equal("14491862341308332741"))
			Expect(keys[0].Key.Kind).To(Equal("entity_email_index"))
			Expect(keys[0].Hash).To(Equal(uint64(14491862341308332741)))
		})

		Context("when the key is nil", func() {
			It("returns an error", func() {
				entity := &Entity{
					FirstName: "John",
					LastName:  "Doe",
					Email:     "john@example.com",
				}

				keys, err := maptree.Keys(entity.ID, reflect.ValueOf(entity))
				Expect(err).To(MatchError("datastore: invalid key"))
				Expect(keys).To(BeNil())
			})
		})
	})
})
