package firestorm

import (
	"context"

	"cloud.google.com/go/datastore"
)

// Completion completes the given key
type Completion struct {
	Client *datastore.Client
}

// CompletionOf returns the completion
func CompletionOf(client *datastore.Client) *Completion {
	return &Completion{
		Client: client,
	}
}

// Key completes a given key
func (k *Completion) Key(ctx context.Context, key *datastore.Key, entity datastore.KeyLoader) error {
	if key.Incomplete() {
		return nil
	}

	incomplete := []*datastore.Key{key}
	complete, err := k.Client.AllocateIDs(ctx, incomplete)

	if err != nil {
		return err
	}

	return entity.LoadKey(complete[0])
}
