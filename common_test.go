package firestorm_test

import (
	"github.com/phogolabs/firestorm"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("String", func() {
	It("returns the value as pointer", func() {
		name := "john"
		Expect(firestorm.String(name)).To(Equal(&name))
	})
})

var _ = Describe("StringValue", func() {
	It("returns the string value", func() {
		name := "john"
		Expect(firestorm.StringValue(&name)).To(Equal(name))
	})

	Context("when the value is nil", func() {
		It("returns the string value", func() {
			Expect(firestorm.StringValue(nil)).To(BeEmpty())
		})
	})
})
