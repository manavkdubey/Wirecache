# ADR-003: Bounded candidate buffering with private streaming fallback

Status: accepted.

Known oversized responses and bypass traffic stream with backpressure. Eligible unknown-length responses buffer up to the configured entry-body limit. Allocation growth is capped at that limit; final snapshots own compact immutable Bytes.

If a chunk crosses the limit, the original caller receives the buffered prefix, the crossing chunk, and the remaining stream in order. The body is not cached. Transport chunks can retain their underlying bounded read buffers. Buffer compaction can briefly require an extra copy.

Transfer error or timeout before headers are sent produces 502 or 504. After streaming starts, transfer failure terminates the body. No incomplete body is committed as a cache success. Announced or unannounced trailers prevent admission; trailers are not forwarded in P0.

The upstream permit remains owned through buffering or the streaming body lifetime. Single-flight is not implemented yet; its eventual followers must fetch privately under admission limits after an oversized or otherwise unshareable result.
