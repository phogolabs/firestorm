package firestorm

// String returns a pointer to string v.
func String(v string) *string {
	return &v
}

// StringValue returns the string from a string pointer v.
func StringValue(v *string) string {
	if v == nil {
		return ""
	}

	return *v
}
