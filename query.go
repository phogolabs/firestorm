package firestorm

import "cloud.google.com/go/datastore"

// Query is the query condition
type Query struct {
	Ancestor *datastore.Key
	Cursor   string
	Offset   int
	Limit    int
}

// Build builds the query
func (w *Query) Build(query *datastore.Query) (*datastore.Query, error) {
	if position := w.Cursor; position != "" {
		cursor, err := datastore.DecodeCursor(position)

		if err != nil {
			return nil, err
		}

		query = query.Start(cursor)
	}

	if w.Ancestor != nil {
		query = query.Ancestor(w.Ancestor)
	}

	if offset := w.Offset; offset > 0 {
		query = query.Offset(offset)
	}

	if limit := w.Limit; limit > 0 {
		query = query.Limit(limit)
	}

	return query, nil
}
