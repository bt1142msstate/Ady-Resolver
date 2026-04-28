# Data Sources

Ady Resolver is built around real address records, but no free public source
tested here proves that every current Mississippi address is present. Treat the
public feeds as strong baselines and use authoritative or licensed data when
you need true production coverage.

## Supported Source Types

- **MS811/MARIS county point-address ZIPs**: best configured route for
  authoritative Mississippi point-address coverage when obtained through the
  MARIS distribution process. Use `--real-address-format maris` and keep
  `--require-ms-county-coverage` enabled.
- **Public MARIS Point Addressing ZIPs**: easiest public point-address source.
  It is useful, but public availability is county-limited and should not be
  presented as exhaustive.
- **Public MARIS parcel service**: broad statewide public fallback using
  parcel situs fields. It covers all county names in the configured service,
  but parcel situs rows are not equivalent to authoritative point addresses.
- **OpenAddresses processed extracts**: useful supplemental and development
  source. Current Mississippi extracts are not exhaustive and many records lack
  strong locality fields.
- **OpenAddresses direct ESRI source catalog**: queries current configured
  source services and caches normalized CSVs. This is useful supplemental data
  when source conform files expose situs locality fields.
- **USDOT National Address Database**: parser support exists, but the tested
  Mississippi rows were not useful for this project’s current reference cache.
- **Manual verified supplement**: local CSV/XLSX additions for confirmed
  missing addresses. The app can import these rows and update the live resolver
  reference index.

## Coverage Expectations

- The generator samples from loaded real source records and fails when the real
  pool is too small instead of inventing replacement addresses.
- `--require-ms-county-coverage` checks for all 82 Mississippi county names in
  source file paths. That is a coverage guard, not proof that every address in
  each county is present.
- ZIP-to-city enrichment only adds conservative derived variants when a ZIP has
  at least 25 real records and one city has at least a 98% share.
- Manual verified additions should be used for known misses from public feeds.
  They are tracked separately in
  `datasets/source_cache/manual_verified_ms/verified_addresses.csv`.

## Recommended Strategies

- **Best public baseline**: merge MARIS parcels, public MARIS Point Addressing,
  OpenAddresses processed extracts, OpenAddresses direct ESRI CSVs, and the
  manual verified supplement.
- **Best Mississippi address-point route**: obtain the local/state NG9-1-1 or
  full MS811/MARIS county point-address distribution.
- **Best postal deliverability route**: use a licensed USPS/CASS/DPV-capable
  source or API. That is validation-grade for mail delivery, but it is not the
  same thing as a free downloadable address list.

Generated `datasets/` and `runs/` directories are ignored because public-source
caches and full reference builds can be several GB. Rebuild them with the
README commands instead of committing them.
