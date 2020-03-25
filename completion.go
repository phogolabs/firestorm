package firestorm

import (
	"context"

	"cloud.google.com/go/datastore"
)

// Completion completes the given key
type Completion struct {
	Key    *datastore.Key
	Client *datastore.Client
}

// CompletionOf returns the completion
func CompletionOf(client *datastore.Client, key *datastore.Key) *Completion {
	return &Completion{
		Key:    key,
		Client: client,
	}
}

// LoadKey completes a given key
func (k *Completion) LoadKey(ctx context.Context, entity datastore.KeyLoader) error {
	if !k.Key.Incomplete() {
		return nil
	}

	incomplete := []*datastore.Key{k.Key}
	complete, err := k.Client.AllocateIDs(ctx, incomplete)

	if err != nil {
		return err
	}

	return entity.LoadKey(complete[0])
}
