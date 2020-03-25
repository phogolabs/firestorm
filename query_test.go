package firestorm_test

import (
	"cloud.google.com/go/datastore"
	"github.com/phogolabs/firestorm"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Query", func() {
	var where *firestorm.Query

	BeforeEach(func() {
		where = &firestorm.Query{
			Ancestor: datastore.NameKey("007", "agent", nil),
			Cursor:   "CikSI2oSY2xpY2hlLWRldmVsb3BtZW50cg0LEgdjb250YWN0GAoMGAAgAA",
			Offset:   10,
			Limit:    100,
		}
	})

	It("builds the query successfully", func() {
		query, err := where.Build(datastore.NewQuery("test"))
		Expect(query).NotTo(BeNil())
		Expect(err).To(BeNil())
	})

	Context("when the cursor is wrong format", func() {
		BeforeEach(func() {
			where.Cursor = "wrong!!!"
		})

		It("returns an error", func() {
			query, err := where.Build(datastore.NewQuery("test"))
			Expect(query).To(BeNil())
			Expect(err).To(MatchError("illegal base64 data at input byte 5"))
		})
	})
})
