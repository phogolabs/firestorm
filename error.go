package firestorm

import "fmt"

// ErrorViolate returns an error that violate a constriact
func ErrorViolate(name, prop, kind string) error {
	return fmt.Errorf("violation of %s key constraint '%v'. cannot insert duplicate key in kind '%s'", name, prop, kind)
}
