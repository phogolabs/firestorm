package aspect

import (
	"sort"

	"cloud.google.com/go/datastore"
)

// PropertyList converts a []Property to implement PropertyLoadSaver.
type PropertyList []datastore.Property

// Load loads all of the provided properties into list.
// It does not first reset *list to an empty slice.
func (list *PropertyList) Load(properties []datastore.Property) error {
	for _, prop := range properties {
		list.Append(prop)
	}

	return nil
}

// Save saves all of list's properties as a slice of Properties.
func (list *PropertyList) Save() ([]datastore.Property, error) {
	return *list, nil
}

// Append inserts a property to *list
func (list *PropertyList) Append(prop datastore.Property) {
	var (
		items  = *list
		count  = len(items)
		filter = func(i int) bool {
			return (*list)[i].Name > prop.Name
		}
	)

	index := sort.Search(count, filter)

	if index > 0 && count > 0 && items[index-1].Name == prop.Name {
		return
	}

	items = append(items, datastore.Property{})
	copy(items[index+1:], items[index:])
	items[index] = prop

	*list = items
}

// LoadFromStruct loads all of the struct's  properties into l.
// It does not first reset *l to an empty slice.
func (list *PropertyList) LoadFromStruct(input interface{}, names ...string) error {
	properties, err := datastore.SaveStruct(input)
	if err != nil {
		return err
	}

	var (
		count  = len(names)
		output = []datastore.Property{}
	)

	if count == 0 {
		return list.Load(properties)
	}

	sort.Strings(names)

	for _, property := range properties {
		if index := sort.SearchStrings(names, property.Name); index < count {
			output = append(output, property)
		}
	}

	return list.Load(output)
}

// LoadStruct loads the properties from *list to dst.
// dst must be a struct pointer.
//
// The values of dst's unmatched struct fields are not modified,
// and matching slice-typed fields are not reset before appending to
// them. In particular, it is recommended to pass a pointer to a zero
// valued struct on each LoadStruct call.
func (list *PropertyList) LoadStruct(input interface{}) error {
	return datastore.LoadStruct(input, *list)
}
