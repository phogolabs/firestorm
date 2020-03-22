package firestorm

import "fmt"

// ErrorViolateKey returns an error the key violation occurs
func ErrorViolateKey(prop, kind string) error {
	return ErrorViolate("primary", prop, kind)
}

// ErrorViolateUnique returns an error the key violation occurs
func ErrorViolateUnique(prop, kind string) error {
	return ErrorViolate("unique", prop, kind)
}

// ErrorViolate returns an error that violate a constriact
func ErrorViolate(name, prop, kind string) error {
	return fmt.Errorf("violation of %s key constraint '%v'. cannot insert duplicate key in kind '%s'", name, prop, kind)
}
