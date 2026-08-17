# 05_estimate.R — hedonische prijsindices per woningtype (WP4), spec-gestuurd
#
# Een 'spec' beschrijft één schattingsvariant (RS, redev-paper, ...) als data:
# welke 04-analyseset, welke locatievariabelen, sampleperiode en of het de
# beperkte variant is (alleen lnsize als objectkenmerk, voor gemodelleerde
# nieuwbouw). Nieuwe varianten = nieuwe spec, geen gekopieerd script.
#
# Model (vertaling van PrijsIndex_tbv_RS.do):
#   lnprice ~ lnsize [+ lnlotsize] [+ nrooms] [+ d_highrise (app)] + d_maintgood
#             + bouwperiode-dummies (ref: va2002) + transactiejaar-dummies
#             + locatievariabelen,     OLS met heteroskedastie-robuuste SE (HC1)
#
# Output per spec x type: Output/Estimates_<tag>_<spec>_<type>[_limit].csv met
# kolommen term;estimate;std_error;t_value;p_value;ci_low;ci_high en expliciete
# termnamen (bouwperiode_1926_1950, trans_year_2012, constant, ...) — direct
# bruikbaar als GeoDMS-itemnamen, geen Stata-parmest-vertaling meer nodig.

if (!exists(".ri_script_dir")) {
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()
}
source(file.path(.ri_script_dir, "00_config.R"))
suppressPackageStartupMessages(library(fixest))

## ---------------------------------------------------------------------------
## Specs
## ---------------------------------------------------------------------------
maak_spec <- function(name, input_variant, covars_loc,
                      types = c("apartment", "terraced", "semidetached", "detached"),
                      min_year = 2000, limit = FALSE, onbekend_dummies = FALSE) {
  list(name = name, input_variant = input_variant, covars_loc = covars_loc,
       types = types, min_year = min_year, limit = limit,
       onbekend_dummies = onbekend_dummies)
}

specs_alle <- list(
  # Validatie: zelfde variabelen en gedrag als de Stata-run van 20251024 (legacy-input)
  rsval       = maak_spec("rsval",       "legacy", c("lntt_500k", "lntt_station", "uai", "d_groennabij")),
  rsval_limit = maak_spec("rsval_limit", "legacy", c("lntt_500k", "lntt_station", "uai", "d_groennabij"),
                          limit = TRUE),
  # Eén modelset voor paper én RS (besluit 2026-07-13): #18 OV-knooppunt i.p.v.
  # station-2006, #19 UAI-2012-netwerk, #20 zonder groen. De RS gebruikt dus
  # dezelfde redev-bestanden; een aparte rs-spec bestaat niet meer.
  # Reistijd 500k inwoners: 2024-versie (native 100m-raster) i.p.v. de 2020-versie
  # (500m-raster, opgerekt naar 100m -> GeoDMS factor-warning).
  redev       = maak_spec("redev",       "pipeline", c("lntt_500k_2024", "lntt_ovknoop", "uai_2012"),
                          onbekend_dummies = TRUE),
  # sensitiviteit: identiek aan redev, maar met de oude 2020-reistijd
  redev_tt2020 = maak_spec("redev_tt2020", "pipeline", c("lntt_500k", "lntt_ovknoop", "uai_2012"),
                          onbekend_dummies = TRUE),
  # RS-spec met groen (RSopen#566/#481): redev + de twee geloofwaardige groenblauwtermen
  # (besluit 2026-07-17): natuur als schijf 0-2500m (LGN2024) en water 0-500m, fracties
  # 0-1 (beta = log-prijseffect van 0% -> 100%). Stadsgroen is drievoudig getest
  # (LGN-stadsgroen 500m; BGT-groen incl. erf~0,35 op 500m en 100m) en kreeg telkens
  # negatieve tekens bij stedelijke woningtypen: de term meet buurtopbouw (ruim opgezette
  # wijk vs duur stenig centrum), geen groenwaardering, ook met lnlotsize als
  # perceel-control. Zie de rs_groen_g500/g100/vol-sensitiviteiten.
  # Het redevelopment-paper blijft bewust op de groenloze redev-spec (#20 aldaar).
  rs_groen    = maak_spec("rs_groen",    "pipeline", c("lntt_500k_2024", "lntt_ovknoop", "uai_2012",
                                                       "fr_natuur_tot2500m", "fr_water_500m"),
                          onbekend_dummies = TRUE),
  # sensitiviteiten: + BGT-groen (excl. natuur/landbouwcellen, erf ~0,35) op 500m resp.
  # 100m ('zicht op groen', vgl. PBL-parkeffect <100m) — negatieve tekens, zie boven
  rs_groen_g500 = maak_spec("rs_groen_g500", "pipeline", c("lntt_500k_2024", "lntt_ovknoop", "uai_2012",
                                                       "fr_natuur_tot2500m", "fr_groen_500m",
                                                       "fr_water_500m"),
                          onbekend_dummies = TRUE),
  rs_groen_g100 = maak_spec("rs_groen_g100", "pipeline", c("lntt_500k_2024", "lntt_ovknoop", "uai_2012",
                                                       "fr_natuur_tot2500m", "fr_groen_100m",
                                                       "fr_water_500m"),
                          onbekend_dummies = TRUE),
  # sensitiviteit: natuur als donut 500-2500m i.p.v. schijf (0-500m telt dan niet mee)
  rs_groen_donut = maak_spec("rs_groen_donut", "pipeline", c("lntt_500k_2024", "lntt_ovknoop", "uai_2012",
                                                       "fr_natuur_2500m", "fr_water_500m"),
                          onbekend_dummies = TRUE),
  # sensitiviteit: de volle zes-categorieen-set (run 2026-07-17: R2 hoger maar
  # binnenring-tekens implausibel als waardering)
  rs_groen_vol = maak_spec("rs_groen_vol", "pipeline", c("lntt_500k_2024", "lntt_ovknoop", "uai_2012",
                                                       "fr_natuur_500m", "fr_natuur_2500m",
                                                       "fr_landbouw_500m", "fr_stadsgroen_500m",
                                                       "fr_overiggroen_500m", "fr_water_500m"),
                          onbekend_dummies = TRUE),
  # limit-variant (alleen lnsize als objectkenmerk) heeft geen actieve afnemer meer:
  # RS leest hem niet en Redevelopment gebruikt vol model + reg_*-regiogemiddelden.
  # Alleen draaien als sensitiviteit: specs_actief <- "redev_limit"
  redev_limit = maak_spec("redev_limit", "pipeline", c("lntt_500k_2024", "lntt_ovknoop", "uai_2012"),
                          limit = TRUE, onbekend_dummies = TRUE)
)
if (!exists("specs_actief")) specs_actief <- c("redev", "rs_groen")

## ---------------------------------------------------------------------------
## Schatting
## ---------------------------------------------------------------------------
rhs_voor <- function(spec, type) {
  c("lnsize",
    if (!spec$limit && type != "apartment") "lnlotsize",
    if (!spec$limit) "nrooms",
    if (type == "apartment") "d_highrise",
    # missing-indicator voor onbekende pandhoogte (zie README, afwijking 6)
    if (type == "apartment" && isTRUE(spec$onbekend_dummies)) "d_hoogte_onbekend",
    "d_maintgood", "bouwperiode", "trans_year_f",
    spec$covars_loc)
}

tidy_fixest <- function(m, spec) {
  ct <- as.data.table(summary(m)$coeftable, keep.rownames = "term")
  setnames(ct, c("term", "estimate", "std_error", "t_value", "p_value"))
  ci <- confint(m)
  ct[, ci_low  := ci[[1]]]
  ct[, ci_high := ci[[2]]]
  ct[, term := gsub("^\\(Intercept\\)$", "constant", term)]
  ct[, term := gsub("^bouwperiode", "bouwperiode_", term)]
  ct[, term := gsub("^trans_year_f", "trans_year_", term)]

  # Referentieniveaus expliciet als 0 wegschrijven. fixest laat ze weg, maar de
  # GeoDMS-afnemers (RS, Redevelopment) zoeken elk kenmerk op met rlookup: een
  # ontbrekende basiscategorie geeft daar een null-coefficient en daarmee een
  # null-prijs. Stata's parmest deed dit met 'allbaselevels'.
  ref <- data.table(
    term      = c("bouwperiode_va2002", paste0("trans_year_", spec$min_year)),
    estimate  = 0, std_error = 0, t_value = NA_real_, p_value = NA_real_,
    ci_low    = 0, ci_high = 0)
  rbind(ct, ref[!term %in% ct$term], use.names = TRUE)[]
}

modelinfo <- list()

for (sp_naam in specs_actief) {
  spec <- specs_alle[[sp_naam]]
  if (is.null(spec)) stop("Onbekende spec: ", sp_naam)

  invoer <- cfg$file_04_analysis(spec$input_variant)
  if (!file.exists(invoer)) stop("Analyseset ontbreekt: ", invoer,
                                 " — draai eerst stap 04 (variant_04 = '", spec$input_variant, "').")
  a <- readRDS(invoer)

  for (tp in spec$types) {
    dat <- a[building_type == tp & trans_year >= spec$min_year]
    # factor pas na de sample-selectie: referentie = laagste jaar in de sample
    dat[, trans_year_f := factor(trans_year)]
    fml <- as.formula(paste("lnprice ~", paste(rhs_voor(spec, tp), collapse = " + ")))
    m <- feols(fml, data = dat, vcov = "hetero")   # HC1, zoals Stata's ', r'

    uit <- tidy_fixest(m, spec)
    bestand <- file.path(cfg$dir_output,
                         sprintf("Estimates_%s_%s_%s.csv", cfg$tag, spec$name, tp))
    fwrite(uit, bestand, sep = ";")
    ri_log("%-12s %-13s n=%9s  R2=%.4f  -> %s", spec$name, tp,
           format(m$nobs, big.mark = ","), r2(m, "r2"), basename(bestand))

    modelinfo[[length(modelinfo) + 1L]] <- data.table(
      spec = spec$name, housing_type = tp, limit = spec$limit,
      input_variant = spec$input_variant, min_year = spec$min_year,
      n_obs = m$nobs, r2 = r2(m, "r2"), adj_r2 = r2(m, "ar2"),
      run_datum = as.character(Sys.Date()))
  }
  rm(a); invisible(gc(verbose = FALSE))
}

info <- rbindlist(modelinfo)
info_bestand <- file.path(cfg$dir_output, sprintf("Estimates_%s_modelinfo.csv", cfg$tag))
if (file.exists(info_bestand)) {
  # regels van eerder gedraaide specs behouden; nu-gedraaide overschrijven
  oud <- fread(info_bestand, sep = ";", colClasses = list(character = "run_datum"))
  info <- rbindlist(list(oud[!info, on = .(spec, housing_type)], info),
                    use.names = TRUE, fill = TRUE)
  setorder(info, spec, housing_type)
}
fwrite(info, info_bestand, sep = ";")
ri_log("Modelinfo -> %s", info_bestand)
