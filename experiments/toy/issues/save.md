# Save and load a store

We want to keep a store across restarts. Add to package `kv`:

- `func (s *Store) Save(w io.Writer) error` writes the whole store as JSON.
- `func Load(r io.Reader) (*Store, error)` reads what `Save` wrote into a new store, and fails on anything else.

A loaded store must behave exactly like the store that was saved.
