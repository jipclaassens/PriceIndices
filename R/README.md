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
`Estimates_20251024_*.csv`) nooit worden overschreven. Een nieuwe schatting op
dezelfde data krijgt een eigen schattingstag `cfg$est_tag` in de
Estimates-namen (stap 05): `Estimates_<est_tag>_<spec>_<type>.csv`. Ronde
20260711 is de eerste op deze data, ronde 20261011 telt perceel 0 als onbekend
(afwijking 10).

## Specs (stap 5)

Een schattingsvariant is een spec (naam, invoerset, locatievariabelen,
sampleperiode, wel/niet `limit`). `rsval`/`rsval_limit` reproduceren de
Stata-run van 20251024 (validatie; zie `99_validate_vs_stata.R`; bevroren
ijkpunt). `redev` is de modelset van het densification-paper:
Redevelopment#18 `lntt_ovknoop`, #19 `uai_2012` (netwerkafstand), #20 zonder
groen, `lntt_500k_2024` als bereikbaarheidsmaat, plus een
`d_hoogte_onbekend`-dummy bij appartementen.

`rs_groen` (2026-07-17, RSopen#566/#481) = `redev` + twee LGN2024-groenfracties:
`fr_natuur_tot2500m` (aandeel natuur in de schijf 0–2500m) en `fr_water_500m`,
berekend in `main/SourceData/LGN.dms` (bron: `LGN<jaar>_m25.tif` uit de
RSopen-data; categorie-indeling in `main/Classifications/LGNKlasse.dms`). Dit
nuanceert het besluit van 2026-07-13 ("geen aparte rs-spec"): de RuimteScanner
leest voortaan de `rs_groen`-estimates (waardering groen), het paper blijft op
de groenloze `redev`. De default-run draait beide.

Bewust beperkt tot deze twee termen (besluit 2026-07-17): de
500m-binnenring-fracties (stadsgroen, landbouw, overiggroen, natuur-500) kregen
negatieve/wisselende tekens bij stedelijke woningtypen — ze meten buurtopbouw
(ruime suburbane wijk vs. duur stenig centrum), geen groenwaardering, en horen
daarom niet in een waarderings-indicator. Sensitiviteiten: `rs_groen_donut`
(natuur als donut 500–2500m; 0–500m telt niet mee) en `rs_groen_vol` (de volle
zes-categorieën-set; hogere R² maar implausibele tekens).

Sensitiviteiten (niet in de default-run): `redev_tt2020` (oude 500m-reistijd
i.p.v. de 2024-versie), `redev_limit` (alleen `lnsize` als objectkenmerk) en
`rs_groen_spline` (2026-10-11): `rs_groen` met een lineaire spline in
ln(oppervlakte), knopen per woningtype in `cfg$spline_knopen_m2` (p10, p50 en
p90 van de oppervlakte). De elasticiteit van de oppervlakte stijgt met de grootte
bij appartementen en rijtjeswoningen en daalt bij vrijstaande woningen; een
kwadraat in ln past even goed, maar schiet in de staarten door, de spline loopt
buiten de buitenste knopen recht door.
Draaien met `specs_actief <- "redev_tt2020"` vóór `source("05_estimate.R")`.

**Keuze tt_500k (2026-07-14)**: de 2024-versie
(`TraveltimeTo500k_Population_2024_From_100m_Points.tif`) is native 100m en
vervangt het 500m-raster van 2020 (dat door GeoDMS naar 100m werd opgerekt).
Empirisch fit 2020 wél iets beter voor de stedelijke types (R² ~+0.01) en 2024
beter voor vrijstaand; de 2024-reistijden hebben een dikkere rechterstaart
(p95 64 vs 45 min, correlatie 0.92). Bewuste keuze voor de betere bron.

## Output-format estimates

`term;estimate;std_error;t_value;p_value;ci_low;ci_high` met expliciete
termnamen: `lnsize`, `lnsize_knik<k>` (alleen spline-specs: max(lnsize - ln(k), 0)
met k in m2), `lnlotsize`, `d_perceel_onbekend` (vanaf ronde 20261011;
bij voorspellen met een bekend perceel 0), `nrooms`, `d_maintgood`, `d_highrise`,
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
10. **Perceel 0 is onbekend, geen 1 m2** (2026-10-11). Stata zette perceel 0 op
   1 m2, zodat ln(perceel) = 0. Bij grondgebonden woningen is perceel 0 (of de
   plaatshouder 1 m2) een ontbrekende waarde: in de oude levering (pre2022) geldt
   het voor 15% van de vrijstaande, 35% van de 2^1kap- en 44% van de
   rijtjeswoningen, in Brainbay voor 0,2 tot 1,5%, en die woningen worden
   geprijsd alsof ze een gewoon perceel hebben. Als ln 0 drukten ze de helling:
   in spec redev was de perceelelasticiteit 0,021 / 0,029 / 0,093 (rijtjes /
   2^1kap / vrijstaand). Nu krijgen ze de indicator `d_perceel_onbekend` en
   ln(perceel) = 0 (grens `cfg$perceel_onbekend_max_m2`); specs met
   `onbekend_dummies` nemen de indicator op, net als `06_volatility.R` (die
   sindsdien ook onder `cfg$est_tag` schrijft). De legacy-variant blijft
   Stata-gelijk. De regionale en nationale gemiddelden van het perceel
   (`main/SourceData/NVM.dms`, `RegionalAverages`) telden perceel 0 mee en lagen
   daardoor landelijk 1 tot 4% te laag; vanaf vintage 20261011
   (`ModelParameters/kenmerken_filedate`) tellen onbekende percelen van
   grondgebonden woningen niet meer mee. Bovendien is hun perceelgemiddelde
   vanaf die vintage meetkundig (gemiddelde van ln perceel), omdat het model in
   ln(perceel) rekent: met het rekenkundige gemiddelde lag de voorspelde prijs
   met de nieuwe elasticiteiten landelijk 12,6% (vrijstaand), 3,3% (2^1kap) en
   2,2% (rijtjes) te hoog. Regionaal alleen waar het gewogen aantal verkopen
   minstens `ModelParameters/Perceel_min_verkopen` is (gewicht 1 in de cel,
   lineair naar 0 op 5 km); daaronder blijft de cel leeg en vallen RS en paper
   terug op het landelijke gemiddelde. Appartementen houden het rekenkundige
   gemiddelde (geen perceelterm). De andere 17 tifs van 20261011 (alle
   kenmerken behalve het perceel van de drie grondgebonden types) zijn bit voor
   bit gelijk aan die van 20260711.

## Validatie

- Stap 1–2: vergelijking met `EnhancedData/NVM_1985_2023_20250519_cleaned.dta`
  (aantallen per jaar/type; verschillen verklaard door afwijkingen 2–3 hierboven).
- Stap 5: `99_validate_vs_stata.R` — zelfde input (legacy spatial-CSV 20251024),
  zelfde spec ⇒ coëfficiënten moeten samenvallen met `Estimates_20251024_*.csv`.
