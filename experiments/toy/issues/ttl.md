# Entries that expire

Some callers cache values that go stale. Add expiry to `kv.Store`:

- `func (s *Store) SetWithTTL(key, value string, ttl time.Duration)` stores a value that expires `ttl` after it is set.
- `Set` keeps storing values that never expire, also when it replaces one that would have.
- An expired entry behaves as if it had been deleted: `Get` does not return it, and `Len` and `Keys` do not count it.
