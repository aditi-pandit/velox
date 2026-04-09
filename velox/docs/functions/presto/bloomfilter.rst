========================
Bloom Filter Functions
========================

Velox implements Presto's bloom filter functions using a
`Split-Block Bloom Filter (SBBF) <https://parquet.apache.org/docs/file-format/bloomfilter/>`_,
a SIMD-optimized probabilistic data structure that efficiently tests set
membership with a configurable false-positive probability.

Data Structure
--------------

The underlying `SplitBlockBloomFilter` partitions its bit array into
fixed-size blocks, each the width of a SIMD register (128 or 256 bits
depending on hardware). Insertions and lookups operate entirely within a
single block, enabling vectorized execution. The filter offers one-sided
error guarantees: a ``false`` result means the element was definitely not
inserted; a ``true`` result means the element was *probably* inserted, with
the false-positive rate controlled by the filter size.

Hashing
-------

All types are hashed with XXHash64 (seed 0) before insertion or lookup:

* Numeric types (``boolean``, ``tinyint``, ``smallint``, ``integer``,
  ``bigint``, ``real``, ``double``) — the raw storage bytes of the value are
  hashed.
* String types (``varchar``, ``varbinary``) — the content bytes of the value
  are hashed.

Serialization
-------------

The bloom filter state is serialized as ``varbinary``, consisting of the raw
block array with no additional header. The number of blocks is derived from
the byte length of the serialized form. Serialized filters are compatible with
Presto's SBBF format.

Functions
---------

.. function:: bloom_filter_agg(x) -> varbinary

    Aggregate function. Builds a Bloom filter from all non-null values of
    ``x`` and returns it serialized as ``varbinary``.

    ``x`` may be of type ``boolean``, ``tinyint``, ``smallint``, ``integer``,
    ``bigint``, ``real``, ``double``, ``varchar``, or ``varbinary``.

    The filter is sized for 10,000 expected elements with a 1% false-positive
    probability. Null values are ignored. If all input values are null the
    result is null.

    This function is a *final-only* aggregate: it cannot be used as an
    intermediate (partial) aggregate and does not support merging partial
    states.

    Example — build a filter over an integer column::

        SELECT bloom_filter_agg(id) FROM orders;

.. function:: bloom_filter_might_contain(bloom_filter, x) -> boolean

    Scalar function. Tests whether ``x`` *might* be present in the Bloom
    filter serialized as ``bloom_filter``.

    Returns ``true`` if ``x`` is probably in the filter (subject to the
    false-positive rate of the filter), and ``false`` if ``x`` is definitely
    *not* in the filter.

    ``x`` may be of type ``boolean``, ``tinyint``, ``smallint``, ``integer``,
    ``bigint``, ``real``, ``double``, ``varchar``, or ``varbinary``.

    Returns ``false`` when ``bloom_filter`` is null. Returns null when ``x``
    is null.

    Example — filter a join using a pre-built Bloom filter::

        SELECT *
        FROM orders
        WHERE bloom_filter_might_contain(
                (SELECT bloom_filter_agg(id) FROM small_table),
                orders.id
              );
