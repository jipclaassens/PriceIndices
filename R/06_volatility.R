# 06_volatility.R — lokale prijsvolatiliteit uit de NVM-transacties (afnemer:
# Redevelopment-paper, stage-2-frictievariabele "price volatility", real options).
#
# Maat: kwaliteitsgecorrigeerde lokale log-prijsindex per regio x jaar, daarna
# volatiliteit = sd van de jaar-op-jaar-groei van die index over 2000-2023.
#   1. hedonisch model per woningtype ZONDER transactiejaar-dummies en ZONDER
#      locatietermen: de nationale prijstrend en het lokale prijsniveau horen juist
#      in het residu (de index); tijdvaste niveaus vallen bij de Delta weer weg.
#   2. index_rt = gemiddelde residu per regio x jaar (>= min_n transacties);
#      groei g_rt = index_rt - index_r,t-1 (alleen opeenvolgende jaren);
#      vol_r = sd(g_rt) met >= min_groeijaren observaties.
#   3. drie korrels: gemeente_code (CBS 2024, uit de geocoder), pc4 (uit postcode),
#      en grid5km (floor(x/5000) x floor(y/5000), RD) — die laatste is
#      vintage-vrij te koppelen aan elke afnemer met RD-coordinaten.
#
# Kanttekeningen (bewust v1): dunne regio-jaren zijn uitgesloten i.p.v. geshrunken
# (min_n), waardoor sampling error de vol licht kan opblazen in dunne gebieden —
# gemeente-korrel dient als robuustheidsanker; een repeat-sales-variant (houseid)
# kan later als sensitiviteit.
#
# Output: Output/Volatility_<tag>_{gemeente,pc4,grid5km}.csv (;-gescheiden) met
# regio;n_trans;n_jaarindex;n_groei;vol_dlnp;mean_dlnp

if (!exists(".ri_script_dir")) {
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()
}
source(file.path(.ri_script_dir, "00_config.R"))
suppressPackageStartupMessages(library(fixest))

vol_min_year       <- 2000L   # zelfde venster als de redev-spec
vol_min_n_regiojr  <- 25L     # minimum transacties per regio x jaar voor een indexpunt
vol_min_groeijaren <- 10L     # minimum aantal jaar-op-jaar-groeiobservaties voor een sd
vol_cel_m          <- 5000L   # celgrootte grid-korrel (m, RD)

ri_log("Laad analyseset: %s", cfg$file_04_analysis("pipeline"))
dt <- readRDS(cfg$file_04_analysis("pipeline"))
dt <- dt[trans_year >= vol_min_year & !is.na(lnprice) & !is.na(building_type)]
ri_log("Transacties %d-2023: %s", vol_min_year, format(nrow(dt), big.mark = ","))

## -- residuen per woningtype (zonder jaar- en locatietermen, zie header) --------
dt[, resid_ := NA_real_]
for (type in c("apartment", "terraced", "semidetached", "detached")) {
  rhs <- c("lnsize",
           if (type != "apartment") "lnlotsize",
           "nrooms", "d_maintgood", "bouwperiode",
           if (type == "apartment") c("d_highrise", "d_hoogte_onbekend"))
  idx <- dt[, which(building_type == type & complete.cases(.SD)), .SDcols = rhs]
  m <- feols(as.formula(paste("lnprice ~", paste(rhs, collapse = " + "))), data = dt[idx])
  dt[idx, resid_ := resid(m)]
  ri_log("  %-13s n = %s, R2 = %.3f", type, format(length(idx), big.mark = ","), r2(m, "r2"))
}
dt <- dt[!is.na(resid_)]

## -- regiokorrels ---------------------------------------------------------------
dt[, pc4     := substr(postcode, 1, 4)]
dt[, grid5km := paste0(x %/% vol_cel_m, "_", y %/% vol_cel_m)]

bereken_vol <- function(dt, korrel) {
  idxtab <- dt[!is.na(get(korrel)) & get(korrel) != "",
               .(index = mean(resid_), n = .N), by = .(regio = get(korrel), jaar = trans_year)]
  idxtab <- idxtab[n >= vol_min_n_regiojr]
  setorder(idxtab, regio, jaar)
  idxtab[, g := fifelse(jaar == shift(jaar) + 1L, index - shift(index), NA_real_), by = regio]
  vol <- idxtab[, .(n_trans = sum(n), n_jaarindex = .N, n_groei = sum(!is.na(g)),
                    vol_dlnp = sd(g, na.rm = TRUE), mean_dlnp = mean(g, na.rm = TRUE)), by = regio]
  vol <- vol[n_groei >= vol_min_groeijaren]
  bestand <- file.path(cfg$dir_output, sprintf("Volatility_%s_%s.csv", cfg$tag, korrel))
  fwrite(vol, bestand, sep = ";")
  ri_log("%-8s: %s regio's (mediaan vol %.4f, p10 %.4f, p90 %.4f) -> %s",
         korrel, format(nrow(vol), big.mark = ","), median(vol$vol_dlnp),
         quantile(vol$vol_dlnp, .1), quantile(vol$vol_dlnp, .9), basename(bestand))
  invisible(vol)
}

for (korrel in c("gemeente_code", "pc4", "grid5km")) bereken_vol(dt, korrel)

## -- rolling volatiliteit per regio x besluitjaar (discrete-time hazard, Redevelopment) --
# vol_roll5[regio, J] = sd van de indexgroei over de vijf groeijaren t/m J-1 (minimaal 4
# beschikbaar): de lokale prijsonzekerheid zoals een beslisser die aan het BEGIN van jaar J
# kent — gelagd by design, dus bruikbaar als tijdvariërende verklaring in een hazard-model.
bereken_vol_rolling <- function(dt, korrel) {
  idxtab <- dt[!is.na(get(korrel)) & get(korrel) != "",
               .(index = mean(resid_), n = .N), by = .(regio = get(korrel), jaar = trans_year)]
  idxtab <- idxtab[n >= vol_min_n_regiojr]
  grid <- CJ(regio = unique(idxtab$regio), jaar = seq(min(idxtab$jaar), max(idxtab$jaar)))
  grid[idxtab, on = .(regio, jaar), index := i.index]
  setorder(grid, regio, jaar)
  grid[, g := index - shift(index), by = regio]
  # naast de sd ook het rolling GEMIDDELDE van de groei: de groeiverwachting. Capozza & Li:
  # zowel onzekerheid als verwachte groei verhogen de optiewaarde van wachten — een
  # volatiliteitscoefficient zonder groei-control is dus potentieel vertekend.
  grid[, vol_roll5 := frollapply(g, 5, function(v) if (sum(!is.na(v)) >= 4) sd(v, na.rm = TRUE) else NA_real_), by = regio]
  grid[, g_roll5   := frollapply(g, 5, function(v) if (sum(!is.na(v)) >= 4) mean(v, na.rm = TRUE) else NA_real_), by = regio]
  uit <- grid[!is.na(vol_roll5), .(regio, besluitjaar = jaar + 1L, vol_roll5, g_roll5)]
  bestand <- file.path(cfg$dir_output, sprintf("Volatility_rolling_%s_%s.csv", cfg$tag, korrel))
  fwrite(uit, bestand, sep = ";")
  ri_log("rolling %-13s: %s regio-jaren (%s regio's), besluitjaren %d-%d -> %s",
         korrel, format(nrow(uit), big.mark = ","), format(uniqueN(uit$regio), big.mark = ","),
         min(uit$besluitjaar), max(uit$besluitjaar), basename(bestand))
}
dt[, nationaal := "NL"]   # nationale reeks (1 'regio'): tbv hazard-variant zonder jaar-FE
for (korrel in c("gemeente_code", "grid5km", "nationaal")) bereken_vol_rolling(dt, korrel)
ri_log("Klaar.")
