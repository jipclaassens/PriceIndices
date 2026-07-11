# utils.R — gedeelde hulpfuncties voor de NVM-prijsindex-pipeline
# Wordt door elk stap-script ge-source'd (via 00_config.R).

ri_log <- function(...) {
  cat(format(Sys.time(), "%H:%M:%S"), "|", sprintf(...), "\n")
  flush.console()
}

`%||%` <- function(a, b) if (is.null(a) || (length(a) == 1 && is.na(a))) b else a

# Stata-namen normaliseren: 'rename obj_* *; rename hid_* *; rename *, lower'
norm_names <- function(x) tolower(sub("^hid_", "", sub("^obj_", "", x)))

# .dta inlezen als data.table met genormaliseerde namen; value labels vervallen
# (codes blijven staan; de ruwe .dta blijft de bron voor label-betekenissen)
read_dta_dt <- function(path, ...) {
  d <- haven::read_dta(path, ...)
  d <- haven::zap_formats(haven::zap_labels(d))
  data.table::setDT(d)
  data.table::setnames(d, norm_names(names(d)))
  d
}

# GeoDMS-datadirs uit het register (HKCU\Software\ObjectVision\<machine>\GeoDMS)
get_geodms_dir <- function(key = "SourceDataDir") {
  tryCatch({
    ov <- utils::readRegistry("Software\\ObjectVision", hive = "HCU", maxdepth = 3)
    for (mach in names(ov)) {
      g <- ov[[mach]][["GeoDMS"]]
      if (is.list(g) && !is.null(g[[key]]) && nzchar(g[[key]])) return(g[[key]])
    }
    NULL
  }, error = function(e) NULL)
}

## ---------------------------------------------------------------------------
## Adres-opschoning (vertaling van NVM_merge_pre2022_metna2022.do:167-243)
## ---------------------------------------------------------------------------

# Ongeldige UTF-8-bytes verwijderen (oude .dta's bevatten afgekapte tekens,
# bv. "Belgi\xc3"); regex-functies weigeren zulke strings
sanitize_utf8 <- function(x) {
  bad <- !validUTF8(x)
  bad[is.na(bad)] <- FALSE
  if (any(bad)) x[bad] <- iconv(x[bad], "UTF-8", "UTF-8", sub = "")
  x
}

# Huisnummertoevoeging: hoofdletters, junktekens en woordjes eruit.
# Bewuste afwijking van Stata: Stata verwijderde ALLE nullen (waardoor "10" -> "1");
# hier strippen we alleen voorloopnullen ("01" -> "1", "10" blijft "10").
clean_toevoeging <- function(x) {
  x <- toupper(sanitize_utf8(x))
  x[x %in% c("ONG", "ONBEKEND")] <- ""
  x <- gsub("-", "", x, fixed = TRUE)
  x <- gsub("!", "", x, fixed = TRUE)
  x <- gsub("+", "", x, fixed = TRUE)
  x <- gsub("/", "-", x, fixed = TRUE)   # na de streepjes-verwijdering, zoals in Stata
  x <- gsub(".", "", x, fixed = TRUE)
  for (tok in c("NR", "PP", "HUIS", "BG", "FLAT", "FL", "ONG")) x <- gsub(tok, "", x, fixed = TRUE)
  # 'F' alleen verwijderen als de waarde niet precies "F" is
  niet_f <- !is.na(x) & x != "F"
  x[niet_f] <- gsub("F", "", x[niet_f], fixed = TRUE)
  for (ch in c(" ", "#", "&", ";", "'", "*", "(", ")", ",", "%",
               "\\", "]", "^", "_", "`", "{", "|", "ª", "¬", "²")) {
    x <- gsub(ch, "", x, fixed = TRUE)
  }
  x <- chartr("ÀÌÍÈŠ", "AIIES", x)  # À Ì Í È Š
  x <- sub("^0+", "", x)                               # voorloopnullen
  x[is.na(x)] <- ""
  x
}

clean_postcode <- function(x) {
  x <- sanitize_utf8(x)
  x <- gsub(" ", "", x, fixed = TRUE)
  x <- gsub("!", "", x, fixed = TRUE)
  x[x %in% c("0000XX", "1000", "0000", "0000AA", "0000AB")] <- ""
  x[is.na(x)] <- ""
  x
}

clean_straatnaam <- function(x) {
  x <- sanitize_utf8(x)
  x <- gsub("[;&#.,*|]", "", x)
  x[is.na(x)] <- ""
  x
}

## ---------------------------------------------------------------------------
## Duplicaat-afhandeling
## ---------------------------------------------------------------------------

# Binnen duplicaatgroepen (kolom grp) ontbrekende waarden op de BEWAARDE
# (= eerste) rij per groep vullen met de eerste niet-NA waarde uit die groep,
# in de huidige sorteervolgorde. Vereist: dt gesorteerd op (grp, prioriteit).
# Alleen de eerste rij per groep wordt gevuld; de rest verdwijnt bij de dedup,
# dus het eindresultaat is identiek aan vullen van alle rijen (maar ~40x sneller
# dan de eerdere join-per-kolom-aanpak).
fill_from_dups <- function(dt, vars, grp) {
  vars <- intersect(vars, names(dt))
  g <- dt[[grp]]
  first_i <- dt[, .I[1L], by = c(grp)]$V1
  for (v in vars) {
    x <- dt[[v]]
    need <- first_i[is.na(x[first_i])]        # bewaarde rijen met een gat
    if (!length(need)) next
    cand <- which(!is.na(x) & (g %in% g[need]))
    if (!length(cand)) next
    donor <- cand[!duplicated(g[cand])]       # eerste niet-NA per groep (prioriteitsvolgorde)
    val <- x[donor][match(g[need], g[donor])]
    vul <- !is.na(val)
    if (any(vul)) data.table::set(dt, i = need[vul], j = v, value = val[vul])
  }
  invisible(dt)
}

# Kolomtypes van gedeelde kolommen gelijktrekken vóór rbindlist
harmonize_types <- function(a, b) {
  for (v in intersect(names(a), names(b))) {
    ca <- class(a[[v]])[1L]; cb <- class(b[[v]])[1L]
    if (identical(ca, cb)) next
    if (all(c(ca, cb) %in% c("numeric", "integer", "logical"))) {
      a[, (v) := as.numeric(get(v))]; b[, (v) := as.numeric(get(v))]
    } else {
      a[, (v) := as.character(get(v))]; b[, (v) := as.character(get(v))]
    }
  }
  invisible(NULL)
}

# Stata's int((x - groepsgemiddelde - 1e-4)/breedte): tolerantie-bucket rond het
# groepsgemiddelde (0 = binnen halve bucketbreedte van het gemiddelde)
trunc_bucket <- function(x, group_mean, width) {
  as.integer(trunc((x - group_mean - 1e-4) / width))
}

# Compacte frequentietabel als data.table (voor logs)
freq_dt <- function(x, name = "waarde") {
  d <- data.table::as.data.table(table(x, useNA = "ifany"))
  data.table::setnames(d, c(name, "n"))
  d
}
