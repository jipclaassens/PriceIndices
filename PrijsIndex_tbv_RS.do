ssc install  parmest

clear
capture log close
// cd "D:\OneDrive\OneDrive - Objectvision\VU\Projects\NVM Prijsindex"
cd "C:\Users\Jip\OneDrive - Objectvision\VU\Projects\NVM Prijsindex"
log using temp/prijs_index_maa2025.txt, text replace

///////////////////////////////////////////////////////////////////////////
//////////////////////// 		Prepare data		///////////////////////
///////////////////////////////////////////////////////////////////////////

global filedate = 20240926

// use Brondata/nvm19852022_geocoded_.dta, clear
import delimited PrivData_VU_NVM_points_${filedate}.csv, clear
save Temp/NVM_cleaned_enhanced_${filedate}.dta, replace

use Temp/NVM_cleaned_enhanced_${filedate}.dta, clear

drop org_rel

local replaceNullList = "bouwjaar lotsize tt_stations_2006 nroom"
foreach x of local replaceNullList{
// 	replace `x' = substr(`x', 2, length(`x')-2) if substr(`x',1,1)=="'" & substr(`x',-1,1)=="'"
	replace `x' = "" if `x' == "null"
	destring `x', replace
}

g lntt_500k_inw = ln(tt_500k_inw)
replace tt_stations_2006 = 1 if tt_stations_2006 < 1
g lntt_stations_2006 = ln(tt_stations_2006)
g lnprice = ln(price)
g lnsize = ln(size)
replace lotsize = 1 if lotsize == 0
g lnlotsize = ln(lotsize)

g pricem2 = price / size

g d_construnknown = 0
replace d_construnknown = 1 if bouwjaar == .
g d_constrlt1920 = 0
replace d_constrlt1920 = 1 if bouwjaar <= 1919 & bouwjaar != .  
g d_constr19201944 = 0 
replace d_constr19201944 = 1 if bouwjaar >= 1920 & bouwjaar <= 1944
g d_constr19451959 = 0 
replace d_constr19451959 = 1 if bouwjaar >= 1945 & bouwjaar <= 1959
g d_constr19601973 = 0 
replace d_constr19601973 = 1 if bouwjaar >= 1960 & bouwjaar <= 1973
g d_constr19741990 = 0 
replace d_constr19741990 = 1 if bouwjaar >= 1974 & bouwjaar <= 1990
g d_constr19911997 = 0 
replace d_constr19911997 = 1 if bouwjaar >= 1991 & bouwjaar <= 1997
g d_constrgt1997 = 0 
replace d_constrgt1997 = 1 if bouwjaar >= 1998 & bouwjaar != .

g construction_period_label = ""
replace construction_period_label = "Unknown" if d_construnknown == 1
replace construction_period_label = "Before 1920" if d_constrlt1920 == 1
replace construction_period_label = "1920-1944" if d_constr19201944 == 1
replace construction_period_label = "1945-1959" if d_constr19451959 == 1
replace construction_period_label = "1960-1973" if d_constr19601973 == 1
replace construction_period_label = "1974-1990" if d_constr19741990 == 1
replace construction_period_label = "1991-1997" if d_constr19911997 == 1
replace construction_period_label = "After 1997" if d_constrgt1997 == 1
encode construction_period_label, generate(construction_period)

label variable price "Transaction price in euro"
label variable pricem2 "transaction price in euro per m2"
label variable size "size of property in m2"
label variable lotsize "size of parcel in m2"
label variable nroom "Number of rooms"
label variable d_apartment "Building type: apartment"
label variable d_terraced "Building type: terraced"
label variable d_semidetached "Building type: semi-detached"
label variable d_detached "Building type: detached"
label variable d_maintgood "Maintenance state is good"
label variable trans_year "transaction year"
label variable d_highrise "Building is high-rise"
label variable lnprice "log (transaction price)"
label variable lnsize "log (size)"
label variable lnlotsize "log (lotsize)"
label variable lntt_stations_2006 "log (traveltime to a trainstation)"
label variable lntt_500k_inw "log (traveltime to 500.000 inhabitants)"
label variable d_groennabij_2017 "Has green space nearby"
label variable uai_2021 "Urban attractivity index"

drop d_cons*

save Temp/NVM_cleaned_enhanced_${filedate}_prepared.dta, replace
use Temp/NVM_cleaned_enhanced_${filedate}_prepared.dta, clear

// reg lnprice lnsize lnlotsize nroom d_maintgood i.trans_year if d_apartment != 1 & trans_year >= 2000, r

*************SAVE COEFFICIENTS*****************

//gg
reg lnprice lnsize lnlotsize nroom i.d_maintgood b6.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_apartment != 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_eengezins) 
use Temp/priceindex_${filedate}_eengezins.dta, clear
export delimited using E:\SourceData\NVM\Estimates_${filedate}_eengezins.csv, delimiter(";") replace

//app
use Temp/NVM_cleaned_enhanced_${filedate}_prepared.dta, clear
reg lnprice lnsize nroom i.d_highrise i.d_maintgood b6.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_apartment == 1 & trans_year >= 2000,r
parmest, saving(Temp/priceindex_${filedate}_meergezins) 
use Temp/priceindex_${filedate}_meergezins.dta, clear
export delimited using E:\SourceData\NVM\Estimates_${filedate}_meergezins.csv, delimiter(";") replace



///// PER WP4

//Apartment
use Temp/NVM_cleaned_enhanced_${filedate}_prepared.dta, clear
reg lnprice lnsize nroom i.d_highrise i.d_maintgood b6.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_apartment == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_apartment) 
use Temp/priceindex_${filedate}_apartment.dta, clear
export delimited using E:\SourceData\NVM\Estimates_${filedate}_apartment.csv, delimiter(";") replace

//Terraced
use Temp/NVM_cleaned_enhanced_${filedate}_prepared.dta, clear
reg lnprice lnsize lnlotsize nroom i.d_maintgood b6.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_terraced == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_terraced) 
use Temp/priceindex_${filedate}_terraced.dta, clear
export delimited using E:\SourceData\NVM\Estimates_${filedate}_terraced.csv, delimiter(";") replace

//Semi-detached
use Temp/NVM_cleaned_enhanced_${filedate}_prepared.dta, clear
reg lnprice lnsize lnlotsize nroom i.d_maintgood b6.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_semidetached == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_semidetached) 
use Temp/priceindex_${filedate}_semidetached.dta, clear
export delimited using E:\SourceData\NVM\Estimates_${filedate}_semidetached.csv, delimiter(";") replace

//Detached
use Temp/NVM_cleaned_enhanced_${filedate}_prepared.dta, clear
reg lnprice lnsize lnlotsize nroom i.d_maintgood b6.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_detached == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_detached) 
use Temp/priceindex_${filedate}_detached.dta, clear
export delimited using E:\SourceData\NVM\Estimates_${filedate}_detached.csv, delimiter(";") replace



////LIMITED INFO, voor gemodelleerde realiseerde panden
///// PER WP4
use Temp/NVM_cleaned_enhanced_${filedate}_prepared.dta, clear
//Apartment
reg lnprice lnsize i.d_highrise i.d_maintgood b6.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_apartment == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_apartment_limit) 
use Temp/priceindex_${filedate}_apartment_limit.dta, clear
export delimited using E:\SourceData\NVM\Estimates_${filedate}_apartment_limit.csv, delimiter(";") replace

//Terraced
use Temp/NVM_cleaned_enhanced_${filedate}_prepared.dta, clear
reg lnprice lnsize i.d_maintgood b6.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_terraced == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_terraced_limit) 
use Temp/priceindex_${filedate}_terraced_limit.dta, clear
export delimited using E:\SourceData\NVM\Estimates_${filedate}_terraced_limit.csv, delimiter(";") replace

//Semi-detached
use Temp/NVM_cleaned_enhanced_${filedate}_prepared.dta, clear
reg lnprice lnsize i.d_maintgood b6.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_semidetached == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_semidetached_limit) 
use Temp/priceindex_${filedate}_semidetached_limit.dta, clear
export delimited using E:\SourceData\NVM\Estimates_${filedate}_semidetached_limit.csv, delimiter(";") replace

//Detached
use Temp/NVM_cleaned_enhanced_${filedate}_prepared.dta, clear
reg lnprice lnsize i.d_maintgood b6.construction_period i.trans_year lntt_500k_inw lntt_stations_2006 uai_2021 i.d_groennabij if d_detached == 1 & trans_year >= 2000, r
parmest, saving(Temp/priceindex_${filedate}_detached_limit) 
use Temp/priceindex_${filedate}_detached_limit.dta, clear
export delimited using E:\SourceData\NVM\Estimates_${filedate}_detached_limit.csv, delimiter(";") replace



