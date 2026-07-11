# PriceIndices — NVM hedonische prijsindices

Pipeline van ruwe NVM/Brainbay-transactiedata naar hedonische prijsindices per
woningtype (WP4), voor de RuimteScanner én het densification-paper
(repo `Redevelopment`, issues #18–#20 aldaar).

## Structuur

- `R/` — **de actieve pipeline** (R, sinds juli 2026): merge → clean → geocode-export
  → [GeoDMS] → import+schoning → spec-gestuurde schattingen. Zie `R/README.md`
  voor stappen, output-format en bewuste afwijkingen van de oude Stata-flow.
- `Stata/` — legacy do-files (referentie; Stata-licentie verlopen, niet meer draaien).
- `main/` + `main.dms` — GeoDMS-config voor het ruimtelijke deel: spatial vars
  (UAI, reistijden, groen-nabijheid) en regionale gemiddelde woningkenmerken.
  De geocodering zelf (adres → X/Y + BAG-attributen) draait in de BAG-Tools-config.

## Data

Data staat op OneDrive: `VU/Projects/NVM Prijsindex/` (Brondata / EnhancedData /
Output). Canonieke ruwe bronnen: `Brondata/Archief/nvm19852021_raw.dta` (oud
format, 1985–2021) en `Brondata/NVM_2000_2023_raw.dta` (Brainbay, 2000–2023).
Grote tussenbestanden gaan naar `%LocalDataDir%/PriceIndices` (niet gesynct).

**Let op:** `Output/Estimates_20251024_*.csv` (zonder `_limit`) is in gebruik bij
de RuimteScanner — nooit overschrijven; nieuwe runs krijgen een nieuwe versietag.

## Draaien

R 4.5.x: `"C:/Program Files/R/R-4.5.3/bin/Rscript.exe" R/run_all.R` (stap 1–3),
na de GeoDMS-run `... R/run_all.R 4 5`. Configuratie in `R/00_config.R`.
