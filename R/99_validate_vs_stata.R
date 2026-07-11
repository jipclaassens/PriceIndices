# 99_validate_vs_stata.R — R-schattingen vergelijken met de laatste Stata-run
#
# Vergelijkt Estimates_<tag>_rsval[_limit]_<type>.csv (R, stap 05 op de legacy
# spatial-CSV) met Output/Estimates_20251024_<type>[_limit].csv (Stata/parmest).
# Zelfde input + zelfde modelspecificatie => coefficienten moeten (vrijwel) exact
# overeenkomen; dit bewijst dat de Stata->R-vertaling van stap 5 klopt.

if (!exists(".ri_script_dir")) {
  f <- sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE))
  .ri_script_dir <- if (length(f)) dirname(normalizePath(f[1])) else getwd()
}
source(file.path(.ri_script_dir, "00_config.R"))

stata_tag <- "20251024"
bp_labels <- c("tm1925", "1926_1950", "1951_1965", "1966_1973",
               "1974_1981", "1982_1991", "1992_2001", "va2002")

map_parm <- function(parm) {
  uit <- rep(NA_character_, length(parm))
  uit[parm == "_cons"]          <- "constant"
  uit[parm == "lnsize"]         <- "lnsize"
  uit[parm == "lnlotsize"]      <- "lnlotsize"
  uit[parm %in% c("nrooms", "nroom")]                  <- "nrooms"
  uit[parm == "1.d_maintgood"]                         <- "d_maintgood"
  uit[parm == "1.d_highrise"]                          <- "d_highrise"
  uit[parm == "1.d_groennabij"]                        <- "d_groennabij"
  uit[parm == "lntt_500k_inw"]                         <- "lntt_500k"
  uit[parm %in% c("lntt_stations", "lntt_stations_2006")] <- "lntt_station"
  uit[parm %in% c("uai", "uai_2021")]                  <- "uai"
  i <- grepl("^[0-9]+\\.construction_period$", parm)
  uit[i] <- paste0("bouwperiode_", bp_labels[as.integer(sub("\\..*", "", parm[i]))])
  i <- grepl("^[0-9]{4}\\.trans_year$", parm)
  uit[i] <- paste0("trans_year_", sub("\\..*", "", parm[i]))
  uit
}

vergelijk <- function(type, limit = FALSE) {
  f_stata <- file.path(cfg$dir_output, sprintf("Estimates_%s_%s%s.csv", stata_tag, type,
                                               fifelse(limit, "_limit", "")))
  f_r     <- file.path(cfg$dir_output, sprintf("Estimates_%s_%s_%s.csv", cfg$tag,
                                               fifelse(limit, "rsval_limit", "rsval"), type))
  if (!file.exists(f_stata) || !file.exists(f_r)) {
    ri_log("OVERGESLAGEN %s (limit=%s): bestand ontbreekt", type, limit); return(NULL)
  }
  st <- fread(f_stata, sep = ";")
  st <- st[!grepl("[0-9]b\\.", parm) & !grepl("o\\.", parm)]         # basis/omitted eruit
  st[, term := map_parm(parm)]
  rr <- fread(f_r, sep = ";")

  j <- merge(st[, .(term, est_stata = estimate, se_stata = stderr)],
             rr[, .(term, est_r = estimate, se_r = std_error)],
             by = "term", all = TRUE)
  j[, d_est := est_r - est_stata]
  j[, d_se  := se_r - se_stata]

  cat(sprintf("\n--- %s%s ---\n", type, fifelse(limit, " (limit)", "")))
  cat(sprintf("termen: stata %d | r %d | gematcht %d\n",
              nrow(st), nrow(rr), j[!is.na(d_est), .N]))
  cat(sprintf("max |d coef| = %.3g   max |d se| = %.3g\n",
              j[, max(abs(d_est), na.rm = TRUE)], j[, max(abs(d_se), na.rm = TRUE)]))
  afw <- j[is.na(d_est) | abs(d_est) > 1e-6]
  if (nrow(afw)) { cat("afwijkend/ongematcht:\n"); print(afw) } else cat("alle termen identiek (tol 1e-6)\n")
  invisible(j)
}

for (tp in c("apartment", "terraced", "semidetached", "detached")) {
  vergelijk(tp, limit = FALSE)
  vergelijk(tp, limit = TRUE)
}
