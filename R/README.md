# NVM-prijsindex-pipeline (R)

Schone, reproduceerbare keten van ruwe NVM/Brainbay-leveringen naar hedonische
prijsindices per woningtype (WP4). Vervangt de Stata-scripts in `../Stata/`
(blijven staan als referentie; Stata-licentie is verlopen).

## Stappen

| stap | script | doet | output |
|---|---|---|---|
| 1 | `01_merge_sources.R` | ruwe leveringen inlezen en harmoniseren: pre-2022 (oud NVM-format, 1985–2021) + post-2022 (Brainbay, 2000–2023) | `01_merged_<tag>.rds` (lokaal) |
| 2 | `02_clean.R` | filters, adres-opschoning, ontdubbeling (overlap 2000–2021!), huis-id's voor repeat sales | `02_cleaned_<tag>.rds` |
| 3 | `03_export_geocode.R` | slank adresbestand voor GeoDMS-geocodering | `NVM_adressen_<tag>.csv` in `%SourceDataDir%/BAG/xy_source` |
| — | **GeoDMS** | geocoderen: BAG-Tools `cfg/Geocode.dms` op de machine met `E:/SourceData` (X/Y + BAG-attributen) | `NVM_adressen_<tag>_Geocoded.csv` (Temp) |
| 4a | `04a_merge_geocode.R` | geocodeerresultaat terugkoppelen op `geocode_id`: integriteitscheck, filter op matchniveau (`cfg$geocode_max_niveau`, default t/m PC6) en x/y, BAG-sentinels → NA, bouwjaar-consolidatie (BAG > Brainbay > NVM-klasse), hoogte-dummies | `04a_geocoded_<tag>.rds` |
| — | **GeoDMS** | spatial vars (UAI, reistijden, groen) via `../main/` | spatial-CSV |
| 4 | `04_import_spatial.R` | spatial vars terugkoppelen, schonen, afgeleide variabelen | `04_analysis_<variant>_<tag>.rds` |
| 5 | `05_estimate.R` | OLS per WP4-type met robuuste SE (HC1), spec-gestuurd | `Output/Estimates_<tag>_<spec>_<type>.csv` |

Draaien: `Rscript R/run_all.R` (stap 1–3), na de GeoDMS-run `Rscript R/run_all.R 4 5`.
Alle paden/parameters staan in `00_config.R`; de versietag `cfg$tag` zit in alle
outputnamen, zodat oude bestanden (o.a. de door RS gebruikte
`Estimates_20251024_*.csv`) nooit worden overschreven.

## Specs (stap 5)

Een schattingsvariant is een spec (naam, invoerset, locatievariabelen,
sampleperiode, wel/niet `limit`). `rsval`/`rsval_limit` reproduceren de
Stata-run van 20251024 (validatie; zie `99_validate_vs_stata.R`; bevroren
ijkpunt). `redev`/`redev_limit` is **de** actieve modelset — voor het
densification-paper én de RuimteScanner (besluit 2026-07-13: geen aparte
rs-spec): Redevelopment#18 `lntt_ovknoop`, #19 `uai_2012` (netwerkafstand),
#20 zonder groen, plus `d_hoogte_onbekend`-dummy bij appartementen.

## Output-format estimates

`term;estimate;std_error;t_value;p_value;ci_low;ci_high` met expliciete
termnamen: `lnsize`, `lnlotsize`, `nrooms`, `d_maintgood`, `d_highrise`,
`bouwperiode_1926_1950` … (referentie `va2002`), `trans_year_2012` …
(referentie = eerste sample-jaar), `lntt_500k`, `lntt_station`, `uai`,
`d_groennabij`, `constant`. Geen Stata-parmest-namen (`1.construction_period`)
meer — de namen zijn direct geldige GeoDMS-itemnamen. Per run komt er ook een
`Estimates_<tag>_modelinfo.csv` bij (n, R², spec, datum).

## Bewuste afwijkingen van de oude Stata-flow

1. **Start bij de échte ruwe leveringen** (`Brondata/Archief/nvm19852021_raw.dta`
   en `Brondata/NVM_2000_2023_raw.dta`) in plaats van eerder-gegeocodeerde
   tussenbestanden. `nvm20082022_raw_20240416.dta` is een subset van de
   2000–2023-levering en wordt overgeslagen.
2. **x/y-filter verschoven naar stap 4**: coördinaten bestaan pas na geocodering.
   De Stata-flow gooide niet-geocodeerde records al in de merge-stap weg.
3. **Ontdubbeling** op (adres, type, datum, prijs) met de post-2022-levering als
   prioriteit, i.p.v. Stata's fuzzy `houseid1`+datum+prijs. Vindt ook duplicaten
   waarvan m2/bouwjaar tussen de leveringen verschillen en is deterministisch
   (Stata hield een willekeurige rij over). Ontbrekende velden op de bewaarde rij
   worden nog steeds uit de duplicaten aangevuld.
4. **Toevoeging-opschoning**: alleen voorloopnullen strippen (Stata verwijderde
   álle nullen, waardoor "10" → "1").
5. **Reproduceerbaar**: vaste seed (`cfg$seed`) voor de random opsplitsing van
   te vaak verhandelde huis-id's (Stata's `runiform()` was ongezaaid).
6. **Twee impliciete Stata-`missing`-artefacten zijn gefixt**: in Stata is
   `missing > x` waar, waardoor onbekend onderhoud als 'goed' telde en een
   onbekende pandhoogte als hoogbouw. Nu: `d_maintgood`/`d_highrise` zijn 0 bij
   onbekend, en `maint_score` (0–2, NA = onbekend), `d_maint_onbekend` en
   `d_hoogte_onbekend` staan in de data zodat specs de onbekend-categorie apart
   kunnen meenemen. Oude gedrag terug te halen met `cfg$maint_na_as_good`/
   `cfg$highrise_na_as_high = TRUE`; de legacy-variant van stap 04 pint
   Stata-compat vast zodat `99_validate_vs_stata.R` reproduceerbaar blijft.
   De onderhoudsdummies worden ná de ontdubbeling afgeleid, zodat een score die
   maar in één levering zit ook op de bewaarde rij terechtkomt.
7. Value labels van .dta-bronnen vervallen bij inlezen (codes blijven); de ruwe
   .dta's blijven de bron voor labelbetekenissen.
8. **Filter op geocodeer-matchkwaliteit** (nieuw t.o.v. de oude flow, die álle
   matches doorliet t/m woonplaats-centroïdes): default gaan alleen matches t/m
   PC6-niveau (`niveau_code <= 13`) mee; strenger kan per spec.
9. **HTML-entities** (`&apos;`, `&#X27;`) worden gedecodeerd en woonplaats wordt
   nu ook geschoond. Let op: elke wijziging in de schoningslogica verschuift
   `trans_id`'s ⇒ nieuwe `cfg$tag` zetten en opnieuw geocoderen; de
   integriteitscheck in 04a dwingt dit af.

## Validatie

- Stap 1–2: vergelijking met `EnhancedData/NVM_1985_2023_20250519_cleaned.dta`
  (aantallen per jaar/type; verschillen verklaard door afwijkingen 2–3 hierboven).
- Stap 5: `99_validate_vs_stata.R` — zelfde input (legacy spatial-CSV 20251024),
  zelfde spec ⇒ coëfficiënten moeten samenvallen met `Estimates_20251024_*.csv`.
