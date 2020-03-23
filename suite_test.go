package firestorm_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestFirestorm(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Firestorm Suite")
}
