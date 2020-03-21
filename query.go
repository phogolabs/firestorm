package firestorm

import "cloud.google.com/go/datastore"

// Query represents a database query.
type Query struct {
	Ancestor *datastore.Key
	Where    []*Filter
	OrderBy  []string
	Offset   int
	Limit    int
}

// Build builds the actual query
func (q *Query) Build(query *datastore.Query) *datastore.Query {
	if q.Ancestor != nil {
		query = query.Ancestor(q.Ancestor)
	}

	for _, filter := range q.Where {
		query = query.Filter(filter.Expr, filter.Value)
	}

	for _, order := range q.OrderBy {
		query = query.Order(order)
	}

	if q.Offset > 0 {
		query = query.Offset(q.Offset)
	}

	if q.Limit > 0 {
		query = query.Limit(q.Limit)
	}

	return query
}

// Filter represents a database where.
type Filter struct {
	Expr  string
	Value interface{}
}
